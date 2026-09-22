import logging
from datetime import date
from datetime import datetime
from datetime import timedelta
from zoneinfo import ZoneInfo
from unittest.mock import patch

import narwhals as nw
import narwhals.selectors as ncs
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import ssb_timeseries as ts
from ssb_timeseries.dates import *
from ssb_timeseries.dataframes.dates import *
from ssb_timeseries.dataframes.date_cols import *

# mypy: disable-error-code="no-untyped-def,attr-defined,name-defined,arg-type"
# ruff: noqa


def test_dateround_minutes_removes_seconds_keeps_minutes() -> None:
    assert date_round(
        date_utc("2024-03-31 15:17:47+00:00"), rounding="minute"
    ) == datetime(2024, 3, 31, 15, 17, 0, tzinfo=ZoneInfo("UTC"))


def test_utc_equals_utc_time_right_before_beginning_of_daylight_saving() -> None:
    assert date_utc("2024-03-31 00:00:00+00:00") == datetime(
        2024, 3, 31, 0, 0, 0, tzinfo=ZoneInfo("UTC")
    )


def test_europe_oslo_is_default(monkeypatch) -> None:
    monkeypatch.setenv("TZ", "Europe/Oslo")
    # E       AssertionError: assert datetime.datetime(2024, 3, 31, 3, 0, tzinfo=zoneinfo.ZoneInfo(key='Europe/Oslo')) == datetime.datetime(2024, 3, 31, 1, 0, tzinfo=tzoffset(None, 3600))
    # convert to utc before comparing, in order to "normalize" interpreted timezone
    naive_to_default_tz = date_local("2024-03-31 01:00:00")  # .astimezone(DEFAULT_TZ)
    cet = date_local("2024-03-31 01:00:00+01:00")
    assert date_utc(naive_to_default_tz) == date_utc(cet)


def test_utc_iso_strings() -> None:
    tz_aware = datetime.fromisoformat("2024-03-31 00:00:00+00:00")
    tz_naive = datetime.fromisoformat("2024-03-31 00:00:00")
    assert utc_iso(tz_aware).replace(":", "") == utc_iso_no_colon(tz_aware)
    assert utc_iso(tz_naive).replace(":", "") == utc_iso_no_colon(tz_naive)


@pytest.mark.parametrize(
    "utc_datetime, eur_datetime, expected_offset",
    [
        # DST start: CET → CEST
        pytest.param(
            datetime(2024, 3, 31, 0, 45),
            datetime(2024, 3, 31, 1, 45),
            timedelta(hours=1),
            id="before_dst_start",
        ),
        pytest.param(
            datetime(2024, 3, 31, 1, 15),
            datetime(2024, 3, 31, 3, 15),
            timedelta(hours=2),
            id="after_dst_start",
        ),
        # DST end: CEST → CET
        pytest.param(
            datetime(2024, 10, 27, 0, 15),
            datetime(2024, 10, 27, 2, 15),  # ambiguous, defaults to fold=0
            timedelta(hours=2),
            id="before_dst_end",
        ),
        pytest.param(
            datetime(2024, 10, 27, 1, 15),
            datetime(2024, 10, 27, 2, 15, fold=1),  # ambiguos
            timedelta(hours=1),
            id="after_dst_end",
        ),
        pytest.param(
            datetime(2024, 10, 27, 2, 15),
            datetime(2024, 10, 27, 3, 15),
            timedelta(hours=1),
            id="after_dst_end",
        ),
    ],
)
def test_eur_utc_conversions(
    utc_datetime: datetime,
    eur_datetime: datetime,
    expected_offset: timedelta,
) -> None:
    """Test timezone conversions across DST transitions."""

    d_utc = ensure_tz_aware(utc_datetime, UTC)
    d_eur = ensure_tz_aware(eur_datetime, EUROPE)

    # Verify the expected offset.
    assert d_eur.utcoffset() == expected_offset

    # Verify the wall-clock difference.
    assert d_eur.replace(tzinfo=None) == (d_utc.replace(tzinfo=None) + expected_offset)

    # Conversion preserves the instant.
    assert date_eur_no(d_utc) == d_eur
    assert date_utc(d_eur) == d_utc

    # Generic conversion behaves identically.
    assert date_tz(d_eur, "UTC") == d_utc
    assert date_tz(d_eur, UTC) == d_utc
    assert date_tz(d_utc, "Europe/Oslo") == d_eur
    assert date_tz(d_utc, EUROPE) == d_eur


def test_now_cet_returns_cet_timezone():
    """Test that now_cet returns the current time in CET."""
    now = now_cet()
    assert now.tzinfo == CET
    # Check that it's close to the actual now, ignoring microseconds for robustness
    assert (datetime.now(tz=CET) - now).total_seconds() < 1


def test_local_timezone_with_monkeypatch(monkeypatch):
    """Test that local_timezone correctly reflects the system's timezone."""
    new_york_tz = ZoneInfo("America/New_York")

    with patch("ssb_timeseries.dates.datetime") as mock_datetime:
        mock_datetime.now.return_value.astimezone.return_value.tzinfo = new_york_tz

        tz = local_timezone()

        assert tz == new_york_tz
