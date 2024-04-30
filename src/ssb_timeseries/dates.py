# ruff: noqa
# from datetime import datetime, timedelta
import datetime
import time
from functools import wraps

import pytz
from dateutil import parser

from ssb_timeseries.logging import ts_logger

MAX_TIME_PRECISION = "Min"

dt = datetime.datetime
ts = time.time()


def date_round(d: dt, **kwargs) -> dt:

    rounding = kwargs.get("rounding", MAX_TIME_PRECISION)
    match rounding.lower():
        case "day" | "d":
            out = d.replace(hour=0, minute=0, second=0, microsecond=0)
        case "hour" | "h":
            out = d.replace(minute=0, second=0, microsecond=0)
        case "minute" | "min" | "m":
            out = d.replace(second=0, microsecond=0)
        case "second" | "sec" | "s" | _:
            out = d.replace(microsecond=0)
    return out


def date_utc(d: dt | str, **kwargs) -> dt:
    if d is None:
        d = now_utc()

    # tz = kwargs.get("from_tz")

    if not isinstance(d, dt):
        try:
            d = d.to_datetime()
        # except (ValueError, TypeError, AttributeError):
        except AttributeError:
            # d = dt.strptime(d, "%Y-%m-%d")
            d = parser.parse(d)

    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        ts_logger.debug(
            "DATE_UTC catched a date without timezone info. This will become an error later."
        )
        try:
            d = d.tz_localize("CET")
        # except (ValueError, TypeError, AttributeError):
        except AttributeError:
            d = d.replace(tzinfo=pytz.timezone("Europe/Oslo"))

    return d.astimezone(tz=pytz.utc)


def utc_iso(d: dt, timespec: str = "minutes", **kwargs) -> str:
    return date_utc(d, **kwargs).isoformat(timespec=timespec)


def date_cet(d: dt, **kwargs) -> dt:
    return d.astimezone(tz=pytz.timezone("Europe/Oslo"))


def now_utc(**kwargs) -> dt:
    t = dt.now(tz=pytz.utc)
    return date_round(t, **kwargs)


def now_cet(**kwargs) -> dt:
    t = dt.now(tz=pytz.timezone("Europe/Oslo"))
    return date_round(t, **kwargs)


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

        MAX_TIME_precision: str = kwargs.get("MAX_TIME_precision")

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

        if MAX_TIME_precision:
            # "round" to MAX_TIME_precision if provided
            pass

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
