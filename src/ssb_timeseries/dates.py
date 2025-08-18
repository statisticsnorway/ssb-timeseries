# ruff: noqa   #NOSONAR
# type: ignore #NOSONAR
"""
Helper module for date and time utility functions.

Notable examples include converting between UTC and local time, standardised string formats for file names, and (planned for future use) intervals.
"""

from datetime import datetime, tzinfo
from typing import Any, Iterable
from typing import TypeAlias
from zoneinfo import ZoneInfo
from multipledispatch import dispatch

from dateutil import parser
from narwhals.typing import IntoFrameT, FrameT, IntoSeriesT
import narwhals as nw
import narwhals.selectors as ncs
from pandas import PeriodIndex

import ssb_timeseries as ts


# TODO: align these / control by configuration
MAX_TIME_PRECISION: str = "second"
DEFAULT_TIMESPEC: str = "seconds"
NW_DEFAULT_TIME_UNIT: str = "ns"

DEFAULT_TZ = ZoneInfo("Europe/Oslo")  # Will shift between CET and CEST
CET = ZoneInfo("CET")
UTC = ZoneInfo("UTC")
TimeZone: TypeAlias = ZoneInfo | str | None


def date_utc(some_date: datetime | str | None, **kwargs) -> datetime:
    """Convert datetime or date string to UTC.

    If date has no timezone information, the data is assumed to be in default timezone (CET).

    The output will be rounded to the precision specified by kwarg 'rounding'.
    Max precision 'second' will be used if none is provided.
    """
    if some_date is None or some_date == "":
        return date_round(now_utc())

    else:
        dt_type = ensure_datetime(some_date)
        tz_aware = ensure_tz_aware(dt_type)
        correct_tz = tz_aware.astimezone(tz=UTC)
        return date_round(correct_tz, **kwargs)


def date_local(some_date: datetime | str, **kwargs) -> datetime:
    """Convert date to default timezone.

    If not configured otherwise the default is Europe/Oslo which provides automatic shifts between CET and CEST.
    The output can be rounded to the precision specified by kwarg 'rounding'.
    Default precision 'minute' will be used if none is provided.
    """
    dt_type = ensure_datetime(some_date, tz=DEFAULT_TZ)
    return date_round(dt_type, **kwargs)


def date_cet(some_date: datetime | str, **kwargs) -> datetime:
    """Convert date to time_zone Europe/Oslo which provides automatic shifts between CET and CEST.

    The output can be rounded to the precision specified by kwarg 'rounding'.
    Default precision 'minute' will be used if none is provided.
    """
    dt_type = ensure_datetime(some_date, tz=CET)
    return date_round(dt_type, **kwargs)


def date_round(d: datetime, **kwargs) -> datetime:
    """Round date to specified by kwarg 'rounding' or default precision MAX_TIME_PRECISION.

    Rounding can take the values 'none', 'day', 'd', 'hour', 'h', 'minute', 'min', 'm', 'second', 'sec', or 's'.

    Default precision 'minute' is used if none is provided.
    """

    if not d:
        return d

    rounding = kwargs.get("rounding", MAX_TIME_PRECISION)
    match rounding.lower():
        case "day" | "d":
            out = d.replace(hour=0, minute=0, second=0, microsecond=0)
        case "hour" | "h":
            out = d.replace(minute=0, second=0, microsecond=0)
        case "minute" | "min" | "m":
            out = d.replace(second=0, microsecond=0)
        case "second" | "sec" | "s":
            out = d.replace(microsecond=0)
        case "none":
            out = d
    return out


def ensure_datetime(some_date_representation: Any, **kwargs) -> datetime:
    """Make sure that we are dealing with a datetime object, convert if possible.

    If input is None or empty strings will be converted to now_utc().
    """
    if isinstance(some_date_representation, datetime):
        return some_date_representation
    elif some_date_representation is None or some_date_representation == "":
        date_as_dt = now_utc(**kwargs)
    else:
        try:
            date_as_dt = some_date_representation.to_datetime()
        except (ValueError, TypeError, AttributeError):
            date_as_dt = parser.parse(some_date_representation)
        return date_as_dt
    return date_as_dt


def ensure_tz_aware(some_date: datetime) -> datetime:
    """Make sure that our datetime object is timezone aware.

    Assume CET if timezone information is missing.
    """
    if is_tz_naive(some_date):
        ts.logger.debug(
            "DATE_UTC catched a date without timezone info. This will become an error later. Assuming CET."
        )
        try:
            tz_aware = some_date.astimezone(tz=DEFAULT_TZ)
        except (ValueError, TypeError, AttributeError):
            tz_aware = some_date.replace(tzinfo=DEFAULT_TZ)
        return tz_aware
    else:
        return some_date


def is_tz_aware(d: datetime) -> bool:
    return d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None


def is_tz_naive(d: datetime) -> bool:
    return d.tzinfo is None or d.tzinfo.utcoffset(d) is None


def now_utc(**kwargs) -> datetime:
    """Return now in UTC."""
    t = datetime.now(tz=UTC)
    return date_round(t, **kwargs)


def now_cet(**kwargs) -> datetime:
    """Return now in CET."""
    t = datetime.now(tz=UTC)
    return date_round(t, **kwargs)


def utc_iso(d: Any, timespec: str = DEFAULT_TIMESPEC) -> str:
    """Convert date to UTC and return as an ISO formatted string."""
    return date_utc(d).isoformat(timespec=timespec)


def utc_iso_no_colon(d: datetime, timespec: str = DEFAULT_TIMESPEC) -> str:
    """Convert date to UTC and return as an ISO formatted string without the colons."""
    return utc_iso(d, timespec=timespec).replace(":", "")


def prepend_as_of(df: nw.typing.IntoFrameT, as_of: datetime) -> nw.typing.IntoFrameT:
    """Prepend column 'as_of' to dataframe."""
    return (
        nw.from_native(df).with_columns(nw.lit(date_utc(as_of)).alias("as_of"))
    ).to_native()


def local_timezone() -> ZoneInfo:
    """Return the local timezone of the computer."""
    return datetime.now().astimezone().tzinfo


def validate_timezone(tz: TimeZone = "") -> str:
    """Return a valid time zone as string or 'DEFAULT_TZ' for the empty string or 'None'."""
    if not tz:
        tz = DEFAULT_TZ
    return str(tz)


def _nw_expr_datelike_to_datetime() -> list[nw.Expr]:
    """Returns a Narwhals expression to transform all Date columns to Datetime."""
    expressions = [ncs.by_dtype(nw.Object, nw.Date).cast(nw.Datetime)]
    return expressions


def _nw_expr_datetime_time_unit(
    schema=nw.Schema, time_unit: str = NW_DEFAULT_TIME_UNIT
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


def _nw_expr_tz_localize(schema: nw.Schema, target_tz: TimeZone) -> list[nw.Expr]:
    """Returns a list of expressions to localize all tz anive Datetime columns to target time zone."""
    expressions = _nw_expr_datelike_to_datetime()
    tz = validate_timezone(target_tz)
    for col_name, dtype in schema.items():
        if dtype in (nw.Date, nw.Datetime) and dtype.time_zone is None:
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
    df: IntoFrameT, time_unit: str = NW_DEFAULT_TIME_UNIT
) -> IntoFrameT:
    """Ensure all datetime columns of a dataframe use the same time unit."""
    nw_df = nw.from_native(df)
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
    nw_df = nw.from_native(df)
    expression = _nw_expr_datelike_to_datetime()
    return nw_df.with_columns(expression).to_native()


def datetime_localize(df: IntoFrameT, target_tz: TimeZone = "") -> IntoFrameT:
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
    nw_df = nw.from_native(df)
    expression = _nw_expr_tz_localize(
        nw_df.schema,
        target_tz,
    )
    return nw_df.with_columns(expression).to_native()


def datelike_localize(df: IntoFrameT, target_tz: TimeZone = "") -> IntoFrameT:
    """Convert all datelike columns of a dataframe to timezone aware Narwhals Datetime.

    Equivalent to
        >>> # xdoctest: +SETUP
        >>> import pandas as pd
        >>> from ssb_timeseries.dates import datelike_to_datetime
        >>> # ------------------------------------------------------------

        >>> df = pd.DataFrame({'time': ['2022-01-01','2022-01-01','2022-01-01','2022-01-01']})
        >>> datetime_localize(datelike_to_datetime(df))
    """
    df_with_dt_cols = datelike_to_datetime(df)
    return datetime_localize(df_with_dt_cols, target_tz)


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
    nw_df = nw.from_native(df_localized)
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


def datetime_to_utc(
    df: IntoFrameT,
    unlocalized_tz: TimeZone = "",
) -> IntoFrameT:
    """Convert datetime columns of a dataframe to UTC.

    If dates have no timezone information, the data is assumed to be in the default timezone (CET if not configured otherwise).
    """
    return datetime_convert_timezone(df, UTC, unlocalized_tz)


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
    nw_df = nw.from_native(df)
    if nw_df.is_empty():
        return True

    columns_exist = [d in nw_df.columns for d in date_columns]
    if all(columns_exist):
        ...
    elif throw_error:
        columns_not_found = set(date_columns) - set(columns_exist)
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
    as_of: datetime | None = None,
    time_unit: str = NW_DEFAULT_TIME_UNIT,
) -> nw.typing.IntoFrameT:
    """Ensure that all date columns conform to the same standards.

    * Same datatype --> nw.dt.Timestamp?
    * Time zone aware + UTC for storage.
    * Configurable max precision?

    Other questions/ideas include:
    * Pandas Period indexes are nice -> consider conversions?
    * Pendulum or other libraries?
    """
    if as_of:
        df = prepend_as_of(df, as_of)

    as_utc = datelike_to_utc(df)
    return datetime_time_unit(as_utc, time_unit)


def period_index(col: IntoSeriesT, freq: str) -> PeriodIndex:
    """Returns a period index for a date or datetime series."""
    dates = nw.from_native(col, series_only=True).to_pandas()
    return PeriodIndex(dates, freq=freq)
