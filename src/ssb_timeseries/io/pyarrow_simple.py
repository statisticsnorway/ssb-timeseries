"""Provides a PyArrow-based simple, file-based I/O handler for Parquet format.

This handler stores datasets in a wide format (series as columns) with embedded metadata,
using a defined directory structure. For example:

.. code-block::

    <repository_root>/
    ├── AS_OF_AT/
    │   └── my_versioned_dataset/
    │       ├── my_versioned_dataset-as_of_20230101T120000+0000-data.parquet
    │       └── my_versioned_dataset-as_of_20230102T120000+0000-data.parquet
    └── NONE_AT/
        └── my_dataset/
            └── my_dataset-latest-data.parquet

It uses PyArrow for eager reading and writing.
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any
from typing import cast

import narwhals as nw
import pyarrow
import pyarrow.compute
from narwhals.typing import FrameT

from .. import types
from ..config import Config
from ..dataframes import empty_frame
from ..dataframes import is_empty
from ..dataframes import merge_data
from ..dataframes.date_cols import prepend_as_of
from ..dataframes.dates import datelike_to_utc
from ..dates import date_utc
from ..dates import utc_iso_no_colon
from ..logging import logger
from . import fs
from .dataset_ref import DatasetRef
from .parquet_schema import parquet_schema

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "


active_config = Config.active

# TODO: get this from config / dataset metadata:
PA_TIMESTAMP_UNIT = "ns"
PA_TIMESTAMP_TZ = "UTC"
PA_NUMERIC = "float64"


def _version_from_file_name(
    file_name: str, pattern: str | types.Versioning, group: int = 2
) -> str:
    """Extract a version marker from a filename using known patterns.

    The `persisted` convention is not handled here.
    It belongs to the archiving formats, which are the only place this library
    writes `_v<N>.parquet` files, and which keep their own version patterns.
    """
    if isinstance(pattern, types.Versioning):
        pattern = str(pattern)

    match pattern.lower():
        case "as_of":
            date_part = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}[+-][0-9]{4}"
            regex = f"(as_of_)({date_part})(-data.parquet)"
        case "names":
            # type is not implemented
            regex = "(_v)(*)(-data.parquet)"
        case "none":
            regex = "(.*)(latest)(-data.parquet)"
        case _:
            regex = pattern

    found = re.search(regex, file_name)
    if found is None:
        raise ValueError(
            f"pattern '{pattern}' does not match the file name '{file_name}'."
        )
    vs = found.group(group)
    logger.debug(
        "file: %s pattern:%s, regex%s \n--> version: %s ",
        file_name,
        pattern,
        regex,
        vs,
    )
    return vs


class FileSystem:
    """A filesystem abstraction for reading and writing dataset data."""

    def __init__(
        self,
        repository: Any,  # dict[str,str] | FileBasedRepository,
        path: str = "",
        **options: Any,
    ) -> None:
        """Initialize the filesystem handler for a repository.

        Which dataset an operation concerns is passed to that operation as a
        `DatasetRef`, so a single handler can serve any number of datasets.

        Args:
            repository: The repository configuration dictionary, or the name of
                a repository in the active configuration.
            path: Root path of the repository, passed in from the repository
                binding's `options` by the dispatcher.
            **options: Handler specific options. Unknown keys are accepted and
                retained so that handlers can be extended without changing the
                dispatcher.
        """
        if isinstance(repository, dict):
            self.repository = repository
        else:
            cfg = Config.active()
            self.repository = cfg.repositories.get(repository)

        self.options = options
        self.path = str(path)

    @property
    def root(self) -> str:
        """Return the root path of the configured repository."""
        return self.path

    def _filename(self, dataset: DatasetRef) -> str:
        """Construct the standard filename for a dataset's data file."""
        match str(dataset.data_type.versioning):
            case "AS_OF":
                safe_timestamp = utc_iso_no_colon(dataset.as_of_utc)
                file_name = f"{dataset.name}-as_of_{safe_timestamp}-data.parquet"
            case "NONE":
                file_name = f"{dataset.name}-latest-data.parquet"
            case _:
                raise ValueError("Unhandled versioning.")

        logger.debug(file_name)
        return file_name

    def _directory(self, dataset: DatasetRef) -> str:
        """Return the data directory for a dataset."""
        return os.path.join(
            self.root,
            f"{dataset.data_type.versioning!s}_{dataset.data_type.temporality!s}",
            dataset.name,
        )

    def fullpath(self, dataset: DatasetRef) -> str:
        """Return the full path to a dataset's data file."""
        return os.path.join(self._directory(dataset), self._filename(dataset))

    def read(
        self,
        dataset: DatasetRef,
        interval: str = "",  # TODO: Implement use av interval = Interval.all,
    ) -> pyarrow.Table:
        """Read a dataset's data from the filesystem.

        Returns an empty dataframe if the file is not found.

        Args:
            dataset: The dataset to read.
            interval: The part of the dataset to read. Not yet implemented.
        """
        logger.debug(interval)
        if not dataset.is_identified:
            # No version chosen, so there is no particular file to read.
            logger.debug(
                "No 'as of' for %s - return empty frame instead.", dataset.name
            )
            return empty_frame()
        if fs.exists(self.fullpath(dataset)):
            logger.info(
                "DATASET.read.start %s: Reading data from file %s",
                dataset.name,
                self.fullpath(dataset),
            )
            try:
                df = fs.read_parquet(self.fullpath(dataset), implementation="pyarrow")
                logger.info("DATASET.read.success %s: Read data.", dataset.name)
            except FileNotFoundError:
                logger.exception(
                    "DATASET.read.error %s: Read data failed. File not found: %s",
                    dataset.name,
                    self.fullpath(dataset),
                )
                df = empty_frame()

        else:
            df = empty_frame()
            logger.debug(
                f"No file {self.fullpath(dataset)} - return empty frame instead."
            )
        pa_table = datelike_to_utc(df)

        # The 'as_of' column is a storage detail and should not be part of the logical dataset
        if "as_of" in pa_table.column_names:
            pa_table = pa_table.drop(["as_of"])

        return cast(pyarrow.Table, pa_table)

    def write(
        self,
        dataset: DatasetRef,
        data: FrameT,
        tags: dict | None = None,
    ) -> None:
        """Write a dataset's data to the filesystem.

        If versioning is AS_OF, a new file is always created.
        If versioning is NONE, new data is merged into the existing file.

        Args:
            dataset: The dataset to write.
            data: The data to write.
            tags: The dataset's tags, used to derive the storage schema.
        """
        dataset.require_identified()
        new = nw.from_native(data)
        if dataset.data_type.versioning == types.Versioning.AS_OF:
            # consider a merge option for versioned writing?
            df = prepend_as_of(new, dataset.as_of_utc)
        else:
            old = self.read(dataset)
            if is_empty(old):
                df = new
            else:
                logger.debug(
                    f"Merging data with temporality: {dataset.data_type.temporality}"
                )
                logger.debug(f"Old data length: {len(old)}")
                logger.debug(f"New data length: {len(new)}")
                df = merge_data(
                    new=new,
                    old=old,
                    date_cols=dataset.data_type.date_columns,
                    temporality=dataset.data_type.temporality,
                )
                logger.debug(f"Merged data length: {len(df)}")

        logger.info(
            "DATASET.write.start %s: writing data to file\n\t%s\nstarted.",
            dataset.name,
            self.fullpath(dataset),
        )
        try:
            fs.write_parquet(
                data=df,
                path=self.fullpath(dataset),
                schema=parquet_schema(dataset.data_type, tags),
            )
        except Exception as e:
            logger.exception(
                "DATASET.write.error %s: writing data to file\n\t%s\nreturned exception: %s.",
                dataset.name,
                self.fullpath(dataset),
                e,
            )
            raise
        logger.info(
            "DATASET.write.success %s: writing data to file\n\t%s\nended.",
            dataset.name,
            self.fullpath(dataset),
        )

    def exists(self, dataset: DatasetRef) -> bool:
        """Check if the data file for a dataset exists.

        Args:
            dataset: The dataset to check.
        """
        return fs.exists(self.fullpath(dataset))

    def versions(
        self,
        dataset: DatasetRef,
        file_pattern: str | None = None,
    ) -> list[datetime | str]:
        """List all available version markers from a dataset's data directory.

        The versioning comes from the dataset ref, so no pattern argument is
        needed to know how this library names its versioned files.

        Args:
            dataset: The dataset to list versions for.
            file_pattern: Glob selecting the files to inspect, which is a
                property of this handler's storage rather than of the dataset.
                The default, None, reads it from the repository `options` and
                falls back to "*.parquet", the only suffix this handler writes.
        """
        if file_pattern is None:
            file_pattern = self.options.get("file_pattern", "*.parquet")

        files = fs.ls(self._directory(dataset), pattern=file_pattern)
        versions: list[str | datetime] = []
        if files:
            vs_strings = [
                _version_from_file_name(str(fname), dataset.data_type.versioning)
                for fname in files
            ]
            match dataset.data_type.versioning:
                case types.Versioning.AS_OF:
                    versions = sorted([date_utc(as_of) for as_of in vs_strings])
                case types.Versioning.NAMES:
                    versions = sorted(vs_strings)
                case types.Versioning.NONE:
                    versions = vs_strings
                case _:
                    raise ValueError(
                        f"versioning '{dataset.data_type.versioning}' not recognized."
                    )
        return versions
