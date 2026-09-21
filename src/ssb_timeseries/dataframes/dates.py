# ruff: noqa   #NOSONAR
"""
Helper module for date and time utility functions.

Notable examples include converting between UTC and local time, standardised string formats for file names, and (planned for future use) intervals.
"""

from __future__ import annotations

from datetime import datetime, tzinfo
from typing import Any, Iterable
from typing import Literal
from typing import TypeAlias
from typing import cast
from zoneinfo import ZoneInfo
from multipledispatch import dispatch

from dateutil import parser
from narwhals.typing import IntoFrameT, FrameT, IntoSeriesT
import narwhals as nw
import narwhals.selectors as ncs
import pyarrow as pa
from pandas import PeriodIndex
from ..logging import logger


from ..dates import (
    MAX_TIME_PRECISION,
    DEFAULT_TIMESPEC,
    NW_DEFAULT_TIME_UNIT,
    PA_TIMESTAMP_UNIT,
    PA_TIMESTAMP_TZ,
    DEFAULT_TZ,
    CET,
    UTC,
    TimeZone,
    validate_timezone,
)
from .date_cols import temporal_column_schema


def _nw_expr_datelike_to_datetime() -> list[nw.Expr]:
    """Returns a Narwhals expression to transform all Date columns to Datetime."""
    expressions = [cast(nw.Expr, ncs.by_dtype(nw.Object, nw.Date).cast(nw.Datetime))]
    return expressions


def _nw_expr_datetime_time_unit(
    schema=nw.Schema, time_unit: Literal["ns", "us", "ms", "s"] = NW_DEFAULT_TIME_UNIT
) -> list[nw.Expr]:
    """Returns a Narwhals expression to transform all Date columns to Datetime."""
    expressions = []
    for col_name, dtype in schema.items():
        if dtype in (nw.Date, nw.Datetime):
            expressions.append(
                nw.col(col_name).cast(
                    nw.Datetime(time_unit=time_unit, time_zone=dtype.time_zone)
                )
            )
    return expressions


def _nw_expr_tz_localize(
    schema: nw.Schema, target_tz: TimeZone | None
) -> list[nw.Expr]:
    """Returns a list of expressions to localize all tz anive Datetime columns to target time zone."""
    expressions = _nw_expr_datelike_to_datetime()
    tz = validate_timezone(target_tz)
    for col_name, dtype in schema.items():
        if (
            dtype in (nw.Date, nw.Datetime)
            and getattr(dtype, "time_zone", None) is None
        ):
            expressions.append(nw.col(col_name).dt.replace_time_zone(tz))
    return expressions


def _nw_expr_tz_convert(schema: dict, target_tz: TimeZone) -> list[nw.Expr]:
    """Scans the schema and returns a list of expressions for Datetime columns
    that are in a different timezone than the target.
    """
    expressions = []
    tz = validate_timezone(target_tz)
    for col_name, dtype in schema.items():
        if (
            dtype in (nw.Date, nw.Datetime)
            and dtype.time_zone is not None
            and dtype.time_zone != tz
        ):
            expressions.append(nw.col(col_name).dt.convert_time_zone(tz))
    return expressions


def datetime_time_unit(
    df: IntoFrameT, time_unit: Literal["ns", "us", "ms", "s"] = NW_DEFAULT_TIME_UNIT
) -> IntoFrameT:
    """Ensure all datetime columns of a dataframe use the same time unit."""
    nw_df = cast(nw.DataFrame, nw.from_native(df))
    expression = _nw_expr_datetime_time_unit(nw_df.schema, time_unit=time_unit)
    return nw_df.with_columns(expression).to_native()


def datelike_to_datetime(
    df: IntoFrameT,
) -> IntoFrameT:
    """Convert all datelike columns of a dataframe to Narwhals Datetime.

    Any timezone information (or lack there of) is passed through without explicit transformation.
    Note that this does not guarantee time zone information is completely untouched.
    Implicit localizations (typically to UTC) may still be triggered by Narwhals type transitions for some backends.

    """
    nw_df = cast(nw.DataFrame, nw.from_native(df))
    expression = _nw_expr_datelike_to_datetime()
    return nw_df.with_columns(expression).to_native()


def datetime_localize(df: IntoFrameT, target_tz: TimeZone = DEFAULT_TZ) -> IntoFrameT:
    """Ensure all datetime columns of a dataframe are timezone aware.

    Columns without timezone information are localized using 'target_tz' parameter if provided, otherwise falling back to default.

    If the dataframe contains datelike columns of types other than Datetime, ie. Date, Object or string representations, see the twin function 'datelike_localize()'
        >>> # xdoctest: +SETUP
        >>> import pandas as pd
        >>> from ssb_timeseries.dates import datelike_to_datetime
        >>> # ------------------------------------------------------------

        >>> df = pd.DataFrame({'time': [
        ...     '2022-01-01 11:30',
        ...     '2022-01-01 12:00',
        ...     '2022-01-01 12:30',
        ...     '2022-01-01 13:00'
        ... ]})
        >>> datetime_localize(datelike_to_datetime(df))
    """
    nw_df = cast(nw.DataFrame, nw.from_native(df))
    expression = _nw_expr_tz_localize(
        nw_df.schema,
        target_tz,
    )
    return nw_df.with_columns(expression).to_native()


def datelike_localize(df: IntoFrameT, target_tz: TimeZone = "") -> IntoFrameT:
    """Convert all datelike columns of a dataframe to timezone aware Narwhals Datetime."""
    df_with_dt_cols = datelike_to_datetime(df)
    return datetime_localize(df_with_dt_cols, target_tz)


def datelike_unlocalize(df: IntoFrameT) -> IntoFrameT:
    """Convert all datelike columns of a dataframe to timezone naive Narwhals Datetime."""
    df_with_dt_cols = datelike_to_datetime(df)
    return datetime_localize(df_with_dt_cols, None)


def datetime_convert_naive(df: IntoFrameT) -> IntoFrameT:
    """Ensure all datetime columns of a dataframe are timezone naive."""
    nw_df = cast(nw.DataFrame, nw.from_native(df))
    expression = _nw_expr_tz_localize(
        nw_df.schema,
        None,
    )
    return nw_df.with_columns(expression).to_native()


def datetime_convert_timezone(
    df: IntoFrameT,
    target_tz: TimeZone = "",
    unlocalized_tz: TimeZone = "",
) -> IntoFrameT:
    """Convert all datetime columns of a dataframe to target timezone.

    Ensures all datetime columns of a dataframe are timezone aware in the manner of datetime_localize:
    Columns without timezone information are first localized using the 'unlocalized_tz' parameter if it is provided, otherwise the localization will fall back to default.
    """
    df_localized = datetime_localize(df, target_tz=unlocalized_tz)
    nw_df = cast(nw.DataFrame, nw.from_native(df_localized))
    expression = _nw_expr_tz_convert(
        nw_df.schema,
        target_tz,
    )
    return nw_df.with_columns(expression).to_native()


def datelike_convert_timezone(
    df: IntoFrameT,
    target_tz: TimeZone = "",
    unlocalized_tz: TimeZone = "",
) -> IntoFrameT:
    """Convert all datelike columns of a dataframe to target timezone.

    Ensures all datetime columns of a dataframe are timezone aware in the manner of datetime_localize:
    Columns without timezone information are first localized using the 'unlocalized_tz' parameter if it is provided, otherwise the localization will fall back to default.
    """
    df_with_dt_cols = datelike_to_datetime(df)
    df_localized = datetime_localize(df_with_dt_cols, unlocalized_tz)
    return datetime_convert_timezone(df_localized, target_tz)


def datelike_convert_naive(
    df: IntoFrameT,
    unlocalized_tz: TimeZone = "",
) -> IntoFrameT:
    """Convert all datelike columns of a dataframe to target timezone.

    Ensures all datetime columns of a dataframe are timezone aware in the manner of datetime_localize:
    Columns without timezone information are first localized using the 'unlocalized_tz' parameter if it is provided, otherwise the localization will fall back to default.
    """
    df_with_dt_cols = datelike_to_datetime(df)
    df_localized = datelike_unlocalize(df_with_dt_cols)
    return datetime_convert_naive(df_localized)


def datetime_to_utc(
    df: IntoFrameT,
    unlocalized_tz: TimeZone = "",
) -> IntoFrameT:
    """Convert datetime columns of a dataframe to UTC.

    If dates have no timezone information, the data is assumed to be in the default timezone (CET if not configured otherwise).
    """
    return datetime_convert_timezone(df, UTC, unlocalized_tz)


def datelike_to_default_tz(df: IntoFrameT, unlocalized_tz: TimeZone = "") -> IntoFrameT:
    """Convert all datelike columns of a dataframe to DEFAULT_TZ."""
    df_localized = datelike_localize(df, target_tz=unlocalized_tz)
    return datetime_convert_timezone(df_localized, target_tz=DEFAULT_TZ)


def datelike_to_utc(df: IntoFrameT, unlocalized_tz: TimeZone = "") -> IntoFrameT:
    """Convert all datelike columns of a dataframe to UTC."""
    df_localized = datelike_localize(df, target_tz=unlocalized_tz)
    return datetime_to_utc(df_localized)

    # chaining expresssions should have performance advantages
    # ... but does not work
    # all_expressions = [
    #     *_nw_expr_datelike_to_datetime(),
    #     *_nw_expr_tz_localize(nw_frame.schema, str(DEFAULT_TZ)),
    #     *_nw_expr_tz_convert(nw_frame.schema, 'UTC'),
    # ]
    # return nw_frame.with_columns(*all_expressions).to_native()


def validate_dates(
    df: IntoFrameT,
    date_columns: Iterable[str],
    throw_error: bool = False,
) -> bool:
    """Check that all expected date columns are defined, are time zone aware dates and in UTC."""
    nw_df = cast(nw.DataFrame, nw.from_native(df))
    if nw_df.is_empty():
        return True

    columns_exist = [d in nw_df.columns for d in date_columns]
    if all(columns_exist):
        ...
    elif throw_error:
        columns_not_found = set(date_columns) - set(nw_df.columns)
        raise ValueError(f"Expected date columns {columns_not_found} was not found.")
    else:
        return False

    dates_are_utc = [
        d in nw_df.select(ncs.datetime(time_zone="UTC")).columns for d in date_columns
    ]
    if all(dates_are_utc):
        ...
    elif throw_error:
        all_date_cols = nw_df.select(ncs.datetime()).schema
        utc_date_cols = nw_df.select(ncs.datetime(time_zone="UTC")).schema
        non_utc = set(all_date_cols.keys()) - set(utc_date_cols.keys())
        raise ValueError(
            f"Some provided date columns where not UTC: {non_utc}\n{all_date_cols}."
        )
    else:
        return False

    return all(columns_exist) and all(dates_are_utc)


def standardize_dates(
    df: nw.typing.IntoFrameT,
    time_unit: Literal["ns", "us", "ms"] = NW_DEFAULT_TIME_UNIT,
) -> nw.typing.IntoFrameT:
    """Ensure that all date columns conform to the same standards.

    * Same datatype --> nw.dt.Timestamp?
    * Time zone aware + UTC for storage.
    * Configurable max precision?

    Other questions/ideas include:
    * Pandas Period indexes are nice -> consider conversions?
    * Pendulum or other libraries?
    """
    as_utc = datelike_to_utc(df)
    return datetime_time_unit(as_utc, time_unit=time_unit)


def period_index(col: IntoSeriesT, freq: str) -> PeriodIndex:
    """Returns a period index for a date or datetime series."""
    dates = nw.from_native(col, series_only=True).to_pandas()
    return PeriodIndex(dates, freq=freq)
