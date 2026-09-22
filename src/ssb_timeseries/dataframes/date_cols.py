"""Dataframe date and time operations."""

from datetime import date
from datetime import datetime
from typing import cast

import narwhals as nw
import narwhals.selectors as ncs
from narwhals.typing import IntoFrame
from narwhals.typing import IntoFrameT

from ..dates import PA_TIMESTAMP_TZ
from ..dates import PA_TIMESTAMP_UNIT
from ..dates import date_utc
from ..dates import ensure_datetime


def temporal_columns(df: IntoFrame) -> list[str]:
    """Return names of all temporal columns of a dataframe."""
    nw_frame = nw.from_native(df)
    schema = nw_frame.schema
    return [name for name, dtype in schema.items() if dtype.is_temporal()]


def temporal_column_schema(df: IntoFrameT) -> nw.Schema:
    """Ensure all datetime columns of a dataframe are timezone naive."""
    nw_df = cast(nw.DataFrame, nw.from_native(df))
    return nw_df.select(ncs.datetime()).schema


def date_range(df: IntoFrame) -> list[datetime] | list[date]:
    """Get the minimum and maximum dates from temporal columns."""
    columns = temporal_columns(df)
    if not columns:
        raise ValueError("Dataframe contains no temporal columns.")

    frame = nw.from_native(df)

    bounds = frame.select(
        *(nw.col(column).min().alias(f"{column}_min") for column in columns),
        *(nw.col(column).max().alias(f"{column}_max") for column in columns),
    )

    if hasattr(bounds, "collect"):
        row = bounds.collect().row(0)
    else:
        row = bounds.row(0)

    return [ensure_datetime(min(row)), ensure_datetime(max(row))]


def prepend_as_of(
    df: nw.typing.IntoFrameT, as_of: datetime | None
) -> nw.typing.IntoFrameT:
    """Prepend column 'as_of' to dataframe."""
    nw_df = cast(nw.DataFrame, nw.from_native(df))

    if as_of is not None:
        as_of_value = date_utc(as_of)
        return nw_df.with_columns(nw.lit(as_of_value).alias("as_of")).to_native()
    else:
        # Create a column of typed nulls to avoid type inference errors
        nw_df = cast(nw.DataFrame, nw.from_native(df))
        as_of_nulls_series = nw.lit(None).cast(
            nw.Datetime(time_unit=PA_TIMESTAMP_UNIT, time_zone=PA_TIMESTAMP_TZ)
        )
        return nw_df.with_columns(as_of=as_of_nulls_series).to_native()
