"""Provides a function for creating a PyArrow schema with embedded metadata."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from typing import cast

import pyarrow

from .. import types
from .json_helpers import tags_to_json

# mypy: disable-error-code="arg-type"

# TODO: get this from config / dataset metadata:
PA_TIMESTAMP_UNIT = "ns"
PA_TIMESTAMP_TZ = "UTC"
PA_NUMERIC = "float64"

_TIMESTAMP = pyarrow.timestamp(unit=PA_TIMESTAMP_UNIT, tz=PA_TIMESTAMP_TZ)  # type: ignore[call-overload]


def parquet_schema(
    data_type: types.SeriesType,
    meta: dict[str, Any],
) -> pyarrow.Schema | None:
    """Translate dataset tags into a PyArrow schema with embedded metadata."""
    if not meta:
        raise ValueError("Tags can not be empty.")

    dataset_meta = deepcopy(meta)
    series_meta = dataset_meta.pop("series")

    if not series_meta:
        return None

    date_col_fields = [
        pyarrow.field(
            d,
            _TIMESTAMP,  # type: ignore[call-overload]
            nullable=False,
        )
        for d in set(data_type.date_columns)
    ]

    num_col_fields = [
        pyarrow.field(
            series_key,
            PA_NUMERIC,
            nullable=True,
            metadata=tags_to_json(series_tags),
        )
        for series_key, series_tags in series_meta.items()
    ]
    date_col_fields.sort(key=lambda x: x.name)
    num_col_fields.sort(key=lambda x: x.name)

    schema = pyarrow.schema(
        date_col_fields + num_col_fields,
        metadata=cast(dict[str, str], tags_to_json(dataset_meta)),
    )
    return schema
