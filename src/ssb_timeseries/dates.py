# ruff: noqa
# type: ignore #NOSONAR
"""
Helper module for date and time utility functions.

Notable examples include converting between UTC and local time, standardised string formats for file names, and (planned for future use) intervals.
"""

from datetime import datetime as dt, tzinfo
from typing import Any
from zoneinfo import ZoneInfo

from dateutil import parser

from ssb_timeseries.logging import ts_logger


MAX_TIME_PRECISION = "second"
DEFAULT_TIMESPEC = "seconds"
DEFAULT_TZ = ZoneInfo("Europe/Oslo")  # Will shift between CET and CEST
UTC = ZoneInfo("UTC")


def date_utc(some_date: dt | str | None, **kwargs) -> dt:
    """Convert date to UTC.

    If date has no timezone information, the data is assumed to be in CET.

    The output will be rounded to the precision specified by kwarg 'rounding'. Max precision 'second' will be used if none is provided.
    """
    if some_date is None or some_date == "":
        return date_round(now_utc())
    else:
        dt_type = ensure_datetime(some_date)
        assert isinstance(dt_type, dt)
        tz_aware = ensure_tz_aware(dt_type)
        correct_tz = tz_aware.astimezone(tz=UTC)
        return date_round(correct_tz, **kwargs)


def date_local(some_date: dt | str, **kwargs) -> dt:
    """Convert date to Europe/Oslo timezone; ie shifting between CET and CEST.

    If the date has no timezone information, the data is assumed to be in Oslo timezone.

    The output will be rounded to the precision specified by kwarg 'rounding'. Default precision 'minute' will be used if none is provided.
    """
    dt_type = ensure_datetime(some_date, tz=DEFAULT_TZ)
    return date_round(dt_type, **kwargs)


def date_round(d: dt, **kwargs) -> dt:
    """Round date to specified by kwarg 'rounding' or default precision.

    Rounding can take the values 'none', 'day', 'd', 'hour', 'h', 'minute', 'min', 'm', 'second', 'sec', or 's'.

    Default precision 'minute' is used if none is provided.
    """

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


def ensure_datetime(some_date_representation: Any, **kwargs) -> dt:
    """Make sure that we are dealing with a datetime object, convert if possible.

    If input is None or empty strings will be converted to now_utc().
    """
    if isinstance(some_date_representation, dt):
        return some_date_representation
    elif some_date_representation is None or some_date_representation == "":
        date_as_dt = now_utc(**kwargs)
    else:
        try:
            date_as_dt = some_date_representation.to_datetime()
        except (ValueError, TypeError, AttributeError):
            # except AttributeError:
            # d = dt.strptime(d, "%Y-%m-%d")
            date_as_dt = parser.parse(some_date_representation)
        return date_as_dt
    return date_as_dt


def ensure_tz_aware(some_date: dt) -> dt:
    """Make sure that our datetime object is timezone aware.

    Assume CET if timezone information is missing.
    """
    # if some_date.tzinfo is None or some_date.tzinfo.utcoffset(some_date) is None:
    if is_tz_naive(some_date):
        ts_logger.debug(
            "DATE_UTC catched a date without timezone info. This will become an error later. Assuming CET."
        )
        try:
            tz_aware = some_date.astimezone(tz=DEFAULT_TZ)
        except (ValueError, TypeError, AttributeError):
            tz_aware = some_date.replace(tzinfo=DEFAULT_TZ)
        return tz_aware
    else:
        return some_date


def is_tz_aware(d: dt) -> bool:
    return d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None


def is_tz_naive(d: dt) -> bool:
    return d.tzinfo is None or d.tzinfo.utcoffset(d) is None


def now_utc(**kwargs) -> dt:
    """Return now in UTC."""
    t = dt.now(tz=UTC)
    return date_round(t, **kwargs)


def now_cet(**kwargs) -> dt:
    """Return now in CET."""
    t = dt.now(tz=UTC)
    return date_round(t, **kwargs)


def utc_iso(d: Any, timespec: str = DEFAULT_TIMESPEC) -> str:
    """Convert date to UTC and return as an ISO formatted string."""
    return date_utc(d).isoformat(timespec=timespec)


def utc_iso_no_colon(d: dt, timespec: str = DEFAULT_TIMESPEC) -> str:
    """Convert date to UTC and return as an ISO formatted string without the colons."""
    return utc_iso(d, timespec=timespec).replace(":", "")


class Interval:
    def __init__(self, f=None, t=None, **kwargs) -> None:
        """Interval(date_from, date_to)
        or a number of variations of named parameters: start/stop, begin/end, as_of, as_of_from/as_of_to, valid_from, valid_to -
        If only "as_of" is provided, start/stop are both set to this date.
        Interval.start defaults to datetime.min
        Interval.stop defaults to datetime.max
        """
        as_of = kwargs.get("as_of")
        as_of_from = kwargs.get("as_of_from")
        as_of_to = kwargs.get("as_of_to")
        valid_from = kwargs.get("valid_from")
        valid_to = kwargs.get("valid_to")
        start = kwargs.get("start")
        stop = kwargs.get("stop")
        from_ = kwargs.get("from_date")
        to = kwargs.get("to_date")
        date_from = kwargs.get("date_from")
        date_to = kwargs.get("date_to")
        from_date = kwargs.get("from_date")
        to_date = kwargs.get("to_date")
        begin = kwargs.get("begin")
        end = kwargs.get("end")

        precision: str = kwargs.get("precision", MAX_TIME_PRECISION)

        ts_logger.debug(f"Interval.__init__ with kwargs:\n{kwargs}")

        self.start = (
            f
            or as_of
            or start
            or as_of_from
            or from_
            or date_from
            or from_date
            or valid_from
            or begin
            or dt.min
        )
        self.stop = (
            t
            or as_of
            or stop
            or to
            or as_of_to
            or date_to
            or to_date
            or valid_to
            or end
            or dt.max
        )

        if precision:
            # "round" to MAX_TIME_precision if provided
            ...

        ts_logger.debug(f"Interval.__init__ returns self:\n{self}")

    def includes(self, *args: dt | list[dt]):
        ts_logger.debug(f"Interval.include args: {args}")
        if len(args) == 1:
            ts_logger.debug("Interval.include - single input")
            out: bool = args[0] >= self.start and args[0] <= self.stop
        else:
            out = [x >= self.start and x <= self.stop for x in args]
        ts_logger.debug(f"Interval.include returns:\n{out}")
        return out

    def all(self):
        self.start = dt.min
        self.stop = dt.max

    def __eq__(self, other) -> str:
        return (self.start, self.stop) == (other.start, other.stop)

    def __lt__(self, other) -> str:
        ts_logger.debug(
            f"Interval.lt: \n\t{self.stop} < ({other.start} < {other.stop})"
        )
        return self.stop < other.start

    def __gt__(self, other) -> str:
        ts_logger.debug(f"Interval.gt: \n\t{self.start} > {other.stop}")
        return self.start > other.stop

    def __le__(self, other) -> str:
        ts_logger.debug(
            f"Interval.le: \n\t{self.start} - {self.stop}\n\t{other.start} - {other.stop}"
        )
        return (self.start, self.stop) <= (other.start, other.stop)

    def __ge__(self, other) -> str:
        ts_logger.debug(
            f"Interval.ge: \n\t{self.start} - {self.stop}\n\t{other.start} - {other.stop}"
        )
        return (self.start, self.stop) >= (other.start, other.stop)

    def __repr__(self) -> str:
        expr = f"Interval(from_date = {self.start}, to_date = {self.stop}) "
        return expr

    def __str__(self) -> str:
        expr = f"Interval from {self.start} to {self.stop}."
        return expr
