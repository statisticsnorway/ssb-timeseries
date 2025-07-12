"""Intervals are crucial to data retrieval.

The most used are likely to be the trivial 'all' and 'last',
but more sophisticated ranges may be specified using all date attributes for a given series type.
Advanced cases may even blur the line to date arithmetics.
An extended interval specification could specify filters, eg 'if day()==Monday',
or time steps or windows relative to another date atribute, eg 'valid_at == as_of + 2 months'.
"""

from datetime import datetime

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self  # noqa: UP035 #backport to 3.10

import ssb_timeseries as ts

# mypy: disable-error-code="no-untyped-def,return-value,no-any-return,attr-defined,operator"
# ruff: noqa: ANN003


class Interval:
    """Intervals specify date ranges by way of inclusive 'start' and exclusive 'stop' datetimes."""

    def __init__(
        self, f: datetime | None = None, t: datetime | None = None, **kwargs
    ) -> None:
        """Interval(date_from, date_to).

        A number of variations of named parameters: start/stop, begin/end, as_of, as_of_from/as_of_to, valid_from, valid_to -
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

        precision = kwargs.get("precision", ts.dates.MAX_TIME_PRECISION)

        ts.logger.debug(f"Interval.__init__ with kwargs:\n{kwargs}")

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
            or datetime.min
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
            or datetime.max
        )

        if precision:
            # "round" to MAX_TIME_precision if provided
            ...

    def includes(self, *args: datetime | list[datetime]) -> bool | list[bool]:
        """Check if one or more dates are included in interval."""
        ts.logger.debug(f"Interval.include args: {args}")
        if len(args) == 1:
            ts.logger.debug("Interval.include - single input")
            out = args[0] >= self.start and args[0] <= self.stop
        else:
            out = [x >= self.start and x <= self.stop for x in args]
        ts.logger.debug(f"Interval.include returns:\n{out}")
        return out

    def all(self) -> None:
        """Set start and stop to widest possible span."""
        self.start = datetime.min
        self.stop = datetime.max

    def __eq__(self, other: object) -> bool:
        """Check if two intervals are equal."""
        return (self.start == other.start) and (self.stop == other.stop)

    def __lt__(self, other: Self) -> bool:
        """Check if the entire left side interval is before and outside the right side interval."""
        ts.logger.debug(
            f"Interval.lt: \n\t{self.stop} < ({other.start} < {other.stop})"
        )
        return self.stop < other.start

    def __gt__(self, other: Self) -> bool:
        """Check if the entire left side interval is after and outside the right side interval."""
        ts.logger.debug(f"Interval.gt: \n\t{self.start} > {other.stop}")
        return self.start > other.stop

    def __le__(self, other: Self) -> bool:
        """Check if the left side interval begins before and ends before or inside the right side interval."""
        ts.logger.debug(
            f"Interval.le: \n\t{self.start} - {self.stop}\n\t{other.start} - {other.stop}"
        )
        return (self.start, self.stop) <= (other.start, other.stop)

    def __ge__(self, other: Self) -> bool:
        """Check if the left side interval begins after or inside and ends after the right side interval."""
        ts.logger.debug(
            f"Interval.ge: \n\t{self.start} - {self.stop}\n\t{other.start} - {other.stop}"
        )
        return (self.start, self.stop) >= (other.start, other.stop)

    def __repr__(self) -> str:
        """Return a string representation able to reinitialize the interval."""
        expr = f"Interval(from_date = {self.start}, to_date = {self.stop}) "
        return expr

    def __str__(self) -> str:
        """Return a string representation of the interval."""
        expr = f"Interval from {self.start} to {self.stop}."
        return expr
