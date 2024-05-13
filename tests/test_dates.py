import logging
from datetime import datetime
from datetime import timedelta
from zoneinfo import ZoneInfo

import pytest

from ssb_timeseries.dates import Interval
from ssb_timeseries.dates import date_local
from ssb_timeseries.dates import date_round
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import utc_iso
from ssb_timeseries.dates import utc_iso_no_colon
from ssb_timeseries.logging import ts_logger

# mypy: disable-error-code="no-untyped-def,attr-defined"


def test_dateround_minutes_removes_seconds_keeps_minutes() -> None:
    assert date_round(
        date_utc("2024-03-31 15:17:47+00:00"), rounding="minute"
    ) == datetime(2024, 3, 31, 15, 17, 0, tzinfo=ZoneInfo("UTC"))


def test_utc_equals_utc_time_right_before_beginning_of_daylight_saving() -> None:
    assert date_utc("2024-03-31 00:00:00+00:00") == datetime(
        2024, 3, 31, 0, 0, 0, tzinfo=ZoneInfo("UTC")
    )


@pytest.mark.skip(
    reason="TODO: Fix AssertionError: assert datetime.datetime(2024, 3, 31, 1, 0) == datetime.datetime(2024, 3, 31, 1, 0, tzinfo=tzoffset(None, 3600))."
)
def test_cet_is_default() -> None:
    # date_cet returns same answer regardless even if timezone is not provided
    assert date_local("2024-03-31 01:00:00") == date_local("2024-03-31 01:00:00+01:00")


def test_utc_iso_strings() -> None:
    tz_aware = datetime.fromisoformat("2024-03-31 00:00:00+00:00")
    tz_naive = datetime.fromisoformat("2024-03-31 00:00:00")
    assert utc_iso(tz_aware).replace(":", "") == utc_iso_no_colon(tz_aware)
    assert utc_iso(tz_naive).replace(":", "") == utc_iso_no_colon(tz_naive)


def test_conversions_right_before_beginning_of_daylight_saving(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    d_utc = date_utc(date_local("2024-03-31 01:00:00"))
    d_local = date_local("2024-03-31 01:00:00")
    ts_logger.debug(
        f"UTC: {d_utc} local: {d_local} date_utc(local):{date_utc(d_local)}"
    )
    # right before beginning of daylight saving, Europe/Oslo = CET = UTC + 1h
    # assert d_local == d_utc
    assert date_utc(d_local) == d_utc


def test_conversions_right_after_beginning_of_daylight_saving(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    d_utc = date_utc(date_local("2024-03-31 03:00:00"))
    d_local = date_local("2024-03-31 03:00:00")
    ts_logger.debug(
        f"UTC: {d_utc} local: {d_local} date_utc(local):{date_utc(d_local)}"
    )
    # right after beginning of daylight saving, Europe/Oslo = CEST = UTC + 2h
    # assert d_local == d_utc
    assert date_utc(d_local) == d_utc


def test_conversions_right_before_end_of_daylight_saving(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    d_utc = date_utc(date_local("2024-10-27 01:45:00"))
    d_local = date_local("2024-10-27 01:45:00")
    ts_logger.debug(
        f"UTC: {d_utc} local: {d_local} date_utc(local):{date_utc(d_local)}"
    )
    # right before end of daylight saving, Europe/Oslo = CEST = UTC + 2h
    # assert d_local == d_utc
    assert date_utc(d_local) == d_utc


def test_conversions_right_after_end_of_daylight_saving(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    d_utc = date_utc(date_local("2024-10-27 02:15:00"))
    d_local = date_local("2024-10-27 02:15:00")
    ts_logger.debug(
        f"UTC: {d_utc} local: {d_local} date_utc(local):{date_utc(d_local)}"
    )
    # right after end of daylight saving, Europe/Oslo = CET = UTC + 1h
    # assert d_local == d_utc
    assert date_utc(d_local) == d_utc


def test_define_without_params() -> None:
    x = Interval()
    assert x.start == datetime.min and x.stop == datetime.max


def test_define_with_as_of_returns_start_equals_stop_equals_as_of() -> None:
    some_date = datetime.now()
    x = Interval(as_of=some_date)
    ts_logger.debug(x)
    assert x.start == some_date and x.stop == some_date


def test_define_without_named_fromdate_and_todate_returns_correct_interval(
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    date_from = datetime.now() - timedelta(days=7)
    date_to = datetime.now()
    x = Interval(date_from, date_to)
    ts_logger.debug(x)
    assert x.start == date_from and x.stop == date_to


def test_define_with_fromdate_and_todate_returns_correct_interval() -> None:
    date_from = datetime.now() - timedelta(days=7)
    date_to = datetime.now()
    x = Interval(start=date_from, stop=date_to)
    ts_logger.debug(x)
    assert x.start == date_from and x.stop == date_to


def test_define_works_with_many_variations_of_parameter_names() -> None:
    date_from = datetime.now() - timedelta(days=7)
    date_to = datetime.now()
    p = Interval(start=date_from, stop=date_to)
    q = Interval(from_date=date_from, to_date=date_to)
    r = Interval(as_of_from=date_from, as_of_to=date_to)
    s = Interval(valid_from=date_from, valid_to=date_to)
    t = Interval(begin=date_from, end=date_to)
    u = Interval(f=date_from, t=date_to)
    assert p.start == date_from and p.stop == date_to
    assert q.start == date_from and q.stop == date_to
    assert r.start == date_from and r.stop == date_to
    assert s.start == date_from and s.stop == date_to
    assert t.start == date_from and t.stop == date_to
    assert u.start == date_from and u.stop == date_to


def test_define_with_just_fromdate_returns_interval_larger_than_fromdate() -> None:
    date_from = datetime.now() - timedelta(days=7)
    x = Interval(start=date_from)
    assert x.start == date_from and x.stop == datetime.max


def test_define_with_just_todate_returns_interval_less_than_todate(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    date_to = datetime.now() - timedelta(days=7)
    x = Interval(stop=date_to)
    ts_logger.debug(x)
    assert x.start == datetime.min and x.stop == date_to


def test_interval_include_returns_true_for_date_inside_interval(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    date_inside = datetime.now()
    date_from = date_inside - timedelta(days=1)
    date_to = date_inside + timedelta(days=1)
    x = Interval(start=date_from, end=date_to)
    assert x.includes(date_inside)


def test_interval_include_returns_false_for_date_outside_interval(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    date_inside = datetime.now()
    date_from = date_inside - timedelta(days=1)
    date_to = date_inside + timedelta(days=1)
    date_outside = date_to + timedelta(days=1)
    x = Interval(begin=date_from, end=date_to)
    assert not x.includes(date_outside)


def test_interval_include_returns_correct_values_for_list_of_dates(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    date_inside = datetime.now()
    date_from = date_inside - timedelta(days=1)
    date_to = date_inside + timedelta(days=1)
    date_outside = date_to + timedelta(days=1)
    x = Interval(begin=date_from, end=date_to)
    test = x.includes(date_outside, date_inside, date_outside)
    assert test == [False, True, False]


def test_interval_a_equals_interval_b_true(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    t0 = datetime.now()
    d = timedelta(days=1)
    a_start = t0 - d
    a_stop = t0 + d
    b_start = a_start
    b_stop = a_stop
    a = Interval(begin=a_start, end=a_stop)
    b = Interval(begin=b_start, end=b_stop)
    assert a == b


def test_interval_a_equals_interval_b_false(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    t0 = datetime.now()
    d = timedelta(days=1)
    a_start = t0 - d
    a_stop = t0 + d
    b_start = a_start - d
    b_stop = a_stop

    a = Interval(begin=a_start, end=a_stop)
    b = Interval(begin=b_start, end=b_stop)
    assert not a == b


def test_interval_a_greater_than_interval_b(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    t0 = datetime.now()
    d = timedelta(days=1)
    y = timedelta(days=365)
    a_start = t0 - d
    a_stop = t0 + d

    # b0 earlier than a, not overlapping
    b0_start = a_start - y
    b0_stop = a_stop - y

    # earlier, but overlapping
    b1_start = a_start - d
    b1_stop = a_stop - d

    a = Interval(begin=a_start, end=a_stop)
    b0 = Interval(begin=b0_start, end=b0_stop)
    b1 = Interval(begin=b1_start, end=b1_stop)

    # a > b only for non overlapping case
    # and the opposites are not true either
    assert a > b0
    assert not a > b1
    assert not b0 > a
    assert not b1 > a


def test_interval_a_less_than_interval_b(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    t0 = datetime.now()
    d = timedelta(days=1)
    y = timedelta(days=365)
    a_start = t0 - d
    a_stop = t0 + d

    # b0 later than a, not overlapping
    b0_start = a_start + y
    b0_stop = a_stop + y

    # b1 later than a, but overlaps
    b1_start = a_start + d
    b1_stop = a_stop + d

    a = Interval(begin=a_start, end=a_stop)
    b0 = Interval(begin=b0_start, end=b0_stop)
    b1 = Interval(begin=b1_start, end=b1_stop)

    # a < b only for non overlapping case
    # and the opposites are not true either
    assert a < b0
    assert not a < b1
    assert not b0 < a
    assert not b1 < a


def test_interval_a_greater_than_or_equal_to_interval_b(caplog) -> None:
    # caplog.set_level(logging.DEBUG)

    t0 = datetime.now()
    d = timedelta(days=1)
    y = timedelta(days=365)
    a_start = t0 - d
    a_stop = t0 + d

    # earlier, not overlapping
    b0_start = a_start - y
    b0_stop = a_stop - y

    # earlier, but overlapping
    b1_start = a_start - d
    b1_stop = a_stop - d

    a = Interval(begin=a_start, end=a_stop)
    b0 = Interval(begin=b0_start, end=b0_stop)
    b1 = Interval(begin=b1_start, end=b1_stop)

    # a > b both for overlapping and non overlapping case
    # and the opposites are not true
    assert a >= b0
    assert a >= b1
    assert not b0 >= a
    assert not b1 >= a


def test_interval_a_less_than_or_equal_to_interval_b(caplog) -> None:
    # caplog.set_level(logging.DEBUG)

    t0 = datetime.now()
    d = timedelta(days=1)
    y = timedelta(days=365)
    a_start = t0 - d
    a_stop = t0 + d

    # b0 later than a, not overlapping
    b0_start = a_start + y
    b0_stop = a_stop + y

    # b1 later, but overlapping
    b1_start = a_start + d
    b1_stop = a_stop + d

    a = Interval(begin=a_start, end=a_stop)
    b0 = Interval(begin=b0_start, end=b0_stop)
    b1 = Interval(begin=b1_start, end=b1_stop)

    # a < b both for overlapping and non overlapping case
    # and the opposites are not true
    assert a <= b0
    assert a <= b1
    assert not b0 <= a
    assert not b1 <= a
