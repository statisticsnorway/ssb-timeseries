"""Provides a Hive-partitioned, file-based I/O handler for Parquet format.

This handler stores datasets in a wide format (series as columns) with embedded metadata,
using a defined Hive-partitioned directory structure. For example:

.. code-block::

    <repository_root>/
    ├── data_type=AS_OF_AT/
    │   └── dataset=my_versioned_dataset/
    │       ├── as_of=2023-01-01T120000+0000/
    │       │   └── part-0.parquet
    │       └── as_of=2023-01-02T120000+0000/
    │           └── part-0.parquet
    └── data_type=NONE_AT/
        └── dataset=my_dataset/
            └── as_of=__HIVE_DEFAULT_PARTITION__/
                └── part-0.parquet
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import cast
from urllib.parse import unquote

import narwhals as nw
import pyarrow as pa
from dateutil.parser import parse
from narwhals.typing import FrameT

from .. import types
from ..config import Config
from ..dataframes import empty_frame
from ..dataframes import is_empty
from ..dataframes import merge_data
from ..dates import prepend_as_of
from ..dates import standardize_dates
from . import fs

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped"

PA_TIMESTAMP_UNIT = "ns"
PA_TIMESTAMP_TZ = "UTC"
PA_FILE_FORMAT = "parquet"
PA_PARTITIONING_FLAVOR = "hive"
PA_BEHAVIOR = (
    "overwrite_or_ignore"  #  'delete_matching' did not behave as expected (?!?)
)

PA_NUMERIC = "float64"
_TIMESTAMP = pa.timestamp(unit=PA_TIMESTAMP_UNIT, tz=PA_TIMESTAMP_TZ)  # type: ignore[call-overload]


class HiveFileSystem:
    """A filesystem abstraction for reading and writing Hive-partitioned datasets."""

    def __init__(
        self,
        repository: Any,
        set_name: str,
        set_type: types.SeriesType,
        as_of_utc: datetime | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        """Initialize the filesystem handler for a given dataset."""
        if isinstance(repository, dict):
            self.repository = repository
        else:
            cfg = Config.active()
            self.repository = cfg.repositories.get(repository)

        self.set_name = set_name
        self.data_type = set_type

        if as_of_utc is None and set_type.versioning == types.Versioning.AS_OF:
            raise ValueError(
                "An 'as of' datetime must be specified when the type has versioning of type Versioning.AS_OF."
            )
        self.as_of_utc = as_of_utc

    @property
    def root(self) -> str:
        """Return the root path of the configured repository."""
        ts_root = self.repository["directory"]["options"]["path"]
        return str(ts_root)

    @property
    def directory(self) -> str:
        """Return the data directory for the dataset."""
        return str(
            Path(self.root)
            / f"data_type={self.data_type.versioning!s}_{self.data_type.temporality!s}"
            / f"dataset={self.set_name}"
        )

    def read(self, *args, **kwargs) -> FrameT:
        """Read a partitioned dataset from the filesystem."""
        if not self.exists:
            return empty_frame()

        # Define the full schema, including the partition key, to avoid type inference errors
        # when the partition only contains nulls (as is the case for Versioning.NONE).
        # partition_schema = pa.schema( [pa.field("as_of", pa.timestamp("ns", tz="UTC"), nullable=True)])

        (_file_schema, partitioning) = _parquet_schema(
            self.data_type,
            {
                "name": self.set_name,
                "versioning": self.data_type.versioning,
                "temporality": self.data_type.temporality,
            },  # define minimal tag dict, because it can not be empty (TODO: relax that?)
            partition_by=["as_of"],
        )

        dataset = pa.dataset.dataset(  # type: ignore[call-overload]
            self.directory,
            format=PA_FILE_FORMAT,
            partitioning=partitioning,
            partition_base_dir=self.directory,
        )
        table = dataset.to_table()

        # The 'as_of' column is a storage detail and should not be part of the logical dataset
        if (
            "as_of" in table.column_names
            and self.data_type.versioning == types.Versioning.NONE
        ):
            table = table.drop(["as_of"])

        return table

    def write(self, data: FrameT, tags: dict | None = None) -> None:
        """Write data to the filesystem, partitioned by versioning scheme."""
        df = prepend_as_of(data, self.as_of_utc)
        df = standardize_dates(df)
        (file_schema, partitioning) = _parquet_schema(
            self.data_type,
            tags,
            partition_by=["as_of"],
        )
        if self.data_type.versioning == types.Versioning.NONE:
            old_data = self.read()
            if not is_empty(old_data):
                old_data = prepend_as_of(old_data, None)
                df = merge_data(
                    old=old_data,
                    new=df,
                    date_cols=set(self.data_type.date_columns),
                    temporality=self.data_type.temporality,
                )

        pa_table = nw.from_native(df).to_arrow()
        pa_table = pa_table.select(file_schema.names).cast(file_schema)

        pa.dataset.write_dataset(
            pa_table,
            base_dir=self.directory,
            partitioning=partitioning,
            existing_data_behavior=PA_BEHAVIOR,
            format=PA_FILE_FORMAT,
            schema=file_schema,
        )

    @property
    def exists(self) -> bool:
        """Check if the dataset directory exists."""
        return fs.exists(self.directory)

    def versions(self) -> list[datetime | str]:
        """List available versions by inspecting subdirectories."""
        if not self.exists or self.data_type.versioning != types.Versioning.AS_OF:
            return []

        version_dirs = fs.ls(self.directory)
        versions = []
        for d in version_dirs:
            if "as_of=" in d:
                version_str = d.split("as_of=")[-1]
                versions.append(parse(unquote(version_str)))
        return sorted(versions)


def _partitioning_schema(
    file_schema: pa.Schema,
    partition_by: list[str],
    flavor: str = PA_PARTITIONING_FLAVOR,
):
    """Return a partitioning schema from pa.Table.schema."""
    part_fields = [field for field in file_schema if field.name in partition_by]
    return pa.dataset.partitioning(pa.schema(part_fields), flavor=cast(str, flavor))  # type: ignore[call-overload]


def _parquet_schema(
    data_type: types.SeriesType,
    meta: dict[str, Any],
    partition_by: list[str],
) -> tuple[pa.Schema, pa.dataset.Partitioning]:
    """Translate dataset tags into a PyArrow schema with embedded metadata."""
    from .json_helpers import tags_to_json

    if not meta:
        raise ValueError("Tags can not be empty.")

    dataset_meta = deepcopy(meta)
    series_meta = dataset_meta.pop("series", {})

    date_col_fields = [
        pa.field(
            d,
            _TIMESTAMP,  # type: ignore[call-overload]
            nullable=True,
        )
        for d in set(data_type.date_columns) | {"as_of"}
    ]
    date_col_fields.sort(key=lambda x: x.name)

    if not series_meta:
        num_col_fields = [
            pa.field(
                series_key,
                PA_NUMERIC,
                nullable=True,
                metadata=tags_to_json(series_tags),
            )
            for series_key, series_tags in series_meta.items()
        ]
        num_col_fields.sort(key=lambda x: x.name)
    else:
        num_col_fields = []

    schema = pa.schema(
        date_col_fields + num_col_fields,
        metadata=cast(dict[str, str], tags_to_json(dataset_meta)),
    )
    partition_by_fields = [f for f in schema if f.name in partition_by]
    partitioning = pa.dataset.partitioning(
        pa.schema(partition_by_fields),
        flavor=cast(str, PA_PARTITIONING_FLAVOR),  # type: ignore[call-overload]
    )
    return (schema, partitioning)
