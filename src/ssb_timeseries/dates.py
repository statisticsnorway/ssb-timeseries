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
from .logging import logger


# TODO: align these / control by configuration
MAX_TIME_PRECISION: str = "second"
DEFAULT_TIMESPEC: str = "seconds"
NW_DEFAULT_TIME_UNIT: Literal["ns", "us", "ms"] = "ns"

# PyArrow related constants to ensure consistent typing for timestamp columns
PA_TIMESTAMP_UNIT: Literal["ns", "us", "ms", "s"] = "ns"
PA_TIMESTAMP_TZ: str = "UTC"

UTC = ZoneInfo("UTC")
CET = ZoneInfo("CET")
EUROPE = ZoneInfo("Europe/Oslo")  # Will shift between CET and CEST
DEFAULT_TZ = EUROPE

TimeZone: TypeAlias = ZoneInfo | str | None


def date_tz(some_date: datetime | str, tz: TimeZone, **kwargs) -> datetime:
    """Convert datetime or date string to specified TZ.

    The output will be rounded to the precision specified by kwarg 'rounding'.
    """
    if not some_date:
        raise ValueError("Date can not be empty!")
    if not tz:
        raise ValueError("Timezone can not be empty!")

    if isinstance(tz, ZoneInfo):
        to_tz = tz
    else:
        to_tz = ZoneInfo(tz)

    dt_type = ensure_datetime(some_date)
    with_tz = ensure_tz_aware(dt_type)
    # dt = with_tz.astimezone(tz=tz)
    dt = with_tz.astimezone(tz=to_tz)
    return date_round(dt, **kwargs)


def date_utc(some_date: datetime | str | None, **kwargs) -> datetime:
    """Convert datetime or date string to UTC (preserve instant).

    If date has no timezone information, the data is assumed to be in default timezone.
    The output will be rounded to the precision specified by kwarg 'rounding'.
    """
    if some_date is None or some_date == "":
        return date_round(now_utc())
    else:
        return date_tz(some_date, UTC)


def date_local(some_date: datetime | str, **kwargs) -> datetime:
    """Convert date to default timezone (preserve instant).

    If not configured otherwise the default is Europe/Oslo which provides automatic shifts between CET and CEST.
    The output can be rounded to the precision specified by kwarg 'rounding'.
    """
    if some_date is None or some_date == "":
        some_date = now_utc()

    return date_tz(some_date, DEFAULT_TZ)


def date_cet(some_date: datetime | str, **kwargs) -> datetime:
    """Convert date to time_zone CET (preserve instant).

    Note that this is NOT the same as Europe/Oslo.
    Europe/Oslo shifts between CET and CEST.
    For most cases, one should use `date_eur_no` or `date_tz(..., <TZ>.

    The output can be rounded to the precision specified by kwarg 'rounding'.
    """
    if some_date is None or some_date == "":
        some_date = now_utc()

    return date_tz(some_date, CET)


def date_eur_no(some_date: datetime | str, **kwargs) -> datetime:
    """Convert date to time_zone Europe/Oslo /preserve instant).

    Note that Europe/Oslo provides automatic shifts between CET and CEST.
    The output can be rounded to the precision specified by kwarg 'rounding'.
    """
    if some_date is None or some_date == "":
        some_date = now_utc()

    return date_tz(some_date, EUROPE)


def date_round(d: datetime, **kwargs) -> datetime:
    """Round date to specified by kwarg 'rounding' or default precision MAX_TIME_PRECISION.

    Rounding can take the values 'none', 'day', 'd', 'hour', 'h', 'minute', 'min', 'm', 'second', 'sec', or 's'.

    Default precision 'seconds' is used if none is provided.
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
        case "microsecond" | "mic" | "u" | "none":
            out = d
    return out


def ensure_datetime(some_date_representation: Any, **kwargs) -> datetime:
    """Make sure that a date representation is a datetime object, convert if possible."""
    if isinstance(some_date_representation, datetime):
        return some_date_representation
    else:
        try:
            date_as_dt = some_date_representation.to_datetime()
        except (ValueError, TypeError, AttributeError):
            date_as_dt = parser.parse(some_date_representation)
    return date_as_dt


def ensure_tz_aware(some_date: datetime, tz: ZoneInfo | str = DEFAULT_TZ) -> datetime:
    """Make sure that a datetime object is timezone aware.

    Assume DEAFAULT_TZ if timezone information is missing.
    """
    if isinstance(tz, ZoneInfo):
        to_tz = tz
    else:
        to_tz = ZoneInfo(tz)

    if is_tz_naive(some_date):
        logger.debug(
            "ENSURE_TZ_AWARE catched a date without timezone info. This may become an error later. Assuming {DEFAULT_TZ}."
        )
        return some_date.replace(tzinfo=to_tz)
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
    t = datetime.now(tz=CET)
    return date_round(t, **kwargs)


def now_eur_no(**kwargs) -> datetime:
    """Return now in Europe/Oslo."""
    t = datetime.now(tz=EUROPE)
    return date_round(t, **kwargs)


def utc_iso(d: Any, timespec: str = DEFAULT_TIMESPEC) -> str:
    """Convert date to UTC and return as an ISO formatted string."""
    return date_utc(d).isoformat(timespec=timespec)


def utc_iso_no_colon(d: datetime, timespec: str = DEFAULT_TIMESPEC) -> str:
    """Convert date to UTC and return as an ISO formatted string without the colons."""
    return utc_iso(d, timespec=timespec).replace(":", "")


def local_timezone() -> ZoneInfo:
    """Return the local timezone of the computer."""
    return cast(ZoneInfo, datetime.now().astimezone().tzinfo)


def validate_timezone(tz: TimeZone = "") -> str:
    """Return a valid time zone as string or 'DEFAULT_TZ' for the empty string or 'None'."""
    if not tz:
        tz = DEFAULT_TZ
    return str(tz)
