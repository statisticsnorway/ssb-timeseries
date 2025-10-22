import logging
from datetime import date
from datetime import datetime
from zoneinfo import ZoneInfo

import narwhals as nw
import narwhals.selectors as ncs
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import ssb_timeseries as ts
from ssb_timeseries.dates import *

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


def test_cet_is_default(monkeypatch) -> None:
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


def test_conversions_right_before_beginning_of_daylight_saving(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    d_utc = date_utc(date_local("2024-03-31 01:00:00"))
    d_local = date_local("2024-03-31 01:00:00")
    ts.logger.debug(
        f"UTC: {d_utc} local: {d_local} date_utc(local):{date_utc(d_local)}"
    )
    # right before beginning of daylight saving, Europe/Oslo = CET = UTC + 1h
    # assert d_local == d_utc
    assert date_utc(d_local) == d_utc


def test_conversions_right_after_beginning_of_daylight_saving(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    d_utc = date_utc(date_local("2024-03-31 03:00:00"))
    d_local = date_local("2024-03-31 03:00:00")
    ts.logger.debug(
        f"UTC: {d_utc} local: {d_local} date_utc(local):{date_utc(d_local)}"
    )
    # right after beginning of daylight saving, Europe/Oslo = CEST = UTC + 2h
    # assert d_local == d_utc
    assert date_utc(d_local) == d_utc


def test_conversions_right_before_end_of_daylight_saving(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    d_utc = date_utc(date_local("2024-10-27 01:45:00"))
    d_local = date_local("2024-10-27 01:45:00")
    ts.logger.debug(
        f"UTC: {d_utc} local: {d_local} date_utc(local):{date_utc(d_local)}"
    )
    # right before end of daylight saving, Europe/Oslo = CEST = UTC + 2h
    # assert d_local == d_utc
    assert date_utc(d_local) == d_utc


def test_conversions_right_after_end_of_daylight_saving(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    d_utc = date_utc(date_local("2024-10-27 02:15:00"))
    d_local = date_local("2024-10-27 02:15:00")
    ts.logger.debug(
        f"UTC: {d_utc} local: {d_local} date_utc(local):{date_utc(d_local)}"
    )
    # right after end of daylight saving, Europe/Oslo = CET = UTC + 1h
    # assert d_local == d_utc
    assert date_utc(d_local) == d_utc


def test_prepend_as_of_adds_a_provided_utc_date_to_a_new_column(
    caplog,
    xyz_at,
):
    caplog.set_level(logging.DEBUG)
    df_before = nw.from_native(xyz_at)
    schema_before = df_before.schema
    assert "as_of" not in schema_before
    as_of_utc = now_utc()
    df_after = prepend_as_of(xyz_at, as_of_utc)
    schema_after = nw.from_native(df_after).schema
    assert "as_of" in schema_after
    assert schema_after["as_of"].time_zone == "UTC"
    assert all(df_after["as_of"] == as_of_utc)


def test_prepend_as_of_converts_provided_date_to_utc(
    caplog,
    xyz_at,
):
    caplog.set_level(logging.DEBUG)
    df_before = nw.from_native(xyz_at)
    schema_before = df_before.schema
    assert "as_of" not in schema_before

    as_of_local = date_local("2024-03-31 01:00:00")
    df_after = prepend_as_of(xyz_at, as_of_local)
    schema_after = nw.from_native(df_after).schema
    assert "as_of" in schema_after
    assert schema_after["as_of"].time_zone == "UTC"
    assert all(df_after["as_of"] == date_utc(as_of_local))


# Prepare test data
XYZ_DATA = {"x": [10.5, 12.1, 11.8], "y": [13.5, 15.4, 11], "z": [10.1, 15.1, 12.8]}


def naive_at_date():
    """Return valid_at as list of tz naive Dates."""
    dates = {"valid_at": [date(2024, 1, 1), date(2024, 6, 25), date(2025, 10, 3)]}
    dates.update(XYZ_DATA)
    # dates.update({**XYZ_DATA})
    return dates


def naive_from_to_date():
    """Return valid_from_to as list of tz naive Dates."""
    dates = {
        "valid_from": [date(2024, 1, 1), date(2024, 7, 1), date(2025, 1, 1)],
        "valid_to": [date(2024, 7, 1), date(2025, 1, 1), date(2025, 7, 1)],
    }
    dates.update(XYZ_DATA)
    return dates


def naive_at_datetime():
    """Return valid_at as list of tz naive Dates."""
    dates = {
        "valid_at": [
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 6, 25, 12, 0, 0),
            datetime(2025, 10, 3, 12, 0, 0),
        ]
    }
    dates.update(XYZ_DATA)
    return dates


def naive_from_to_datetime():
    """Return valid_from_to as list of tz naive Dates."""
    dates = {
        "valid_from": [
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 7, 1, 12, 0, 0),
            datetime(2025, 1, 1, 12, 0, 0),
        ],
        "valid_to": [
            datetime(2024, 7, 1, 12, 0, 0),
            datetime(2025, 1, 1, 12, 0, 0),
            datetime(2025, 7, 1, 12, 0, 0),
        ],
    }
    dates.update(XYZ_DATA)
    return dates


def aware_at_datetime():
    """Return valid_at as list of tz naive Dates."""
    dates = {
        "valid_at": [
            datetime.fromisoformat("2024-01-01 12:00:00+01:00"),
            datetime.fromisoformat("2024-06-25 12:00:00+01:00"),
            datetime.fromisoformat("2025-10-03 12:00:00+01:00"),
        ]
    }
    dates.update(XYZ_DATA)
    return dates


def aware_from_to_datetime():
    """Return valid_from_to as list of tz naive Dates."""
    dates = {
        "valid_from": [
            datetime.fromisoformat("2024-01-01 12:00:00+01:00"),
            datetime.fromisoformat("2024-06-25 12:00:00+01:00"),
            datetime.fromisoformat("2025-10-03 12:00:00+01:00"),
        ],
        "valid_to": [
            datetime.fromisoformat("2024-01-01 12:00:00+01:00"),
            datetime.fromisoformat("2024-06-25 12:00:00+01:00"),
            datetime.fromisoformat("2025-10-03 12:00:00+01:00"),
        ],
    }
    dates.update(XYZ_DATA)
    return dates


NATIVE_TYPES = {
    "pandas": pd.DataFrame,
    "polars": (pl.DataFrame, pl.LazyFrame),
    "pyarrow": pa.Table,
}


@pytest.mark.parametrize(
    "testcase",
    [
        "naive_at_date",
        "naive_from_to_date",
        "naive_at_datetime",
        "naive_from_to_datetime",
    ],
)
@pytest.mark.parametrize("implementation", ["pandas", "polars", "pyarrow"])
def test_datelike_to_datetime(
    caplog,
    testcase,
    implementation,
):
    # Prepare a NARWHALS dataframe, ASSOCIATED with the correct implementation:
    # native_type = NATIVE_TYPES[implementation]
    data = eval(f"{testcase}()")
    df_from_dict = nw.from_dict(data, backend=implementation)
    assert isinstance(df_from_dict, nw.DataFrame)
    assert str(df_from_dict.implementation) == implementation

    # Turn it into correct NATIVE OBJECT and verify the type:
    native_frame = df_from_dict.to_native()
    assert isinstance(native_frame, native_type := NATIVE_TYPES[implementation])

    standardized = datelike_to_datetime(native_frame)
    assert isinstance(standardized, native_type)  # expect same type back
    standardized_nw_df = nw.from_native(standardized)

    # verify key behaviour
    date_types = set(standardized_nw_df.select(~ncs.numeric()).schema.dtypes())
    for dtype in date_types:
        assert isinstance(dtype, nw.dtypes.Datetime)

    num_types = standardized_nw_df.select(ncs.numeric()).schema.dtypes()
    assert len(num_types) == 3
    for dtype in set(num_types):
        assert isinstance(dtype, (nw.Float64))


@pytest.mark.parametrize(
    "testcase",
    [
        "naive_at_date",
        "naive_from_to_date",
        "naive_at_datetime",
        "naive_from_to_datetime",
        "aware_at_datetime",
        "aware_from_to_datetime",
    ],
)
@pytest.mark.parametrize("implementation", ["pandas", "polars", "pyarrow"])
def test_datelike_localize(
    caplog,
    testcase,
    implementation,
):
    native_type = NATIVE_TYPES[implementation]
    data = eval(f"{testcase}()")
    df_from_dict = nw.from_dict(data, backend=implementation)
    # a NARWHALS dataframe, ASSOCIATED with the correct implementation:
    assert isinstance(df_from_dict, nw.DataFrame)
    assert str(df_from_dict.implementation) == implementation
    # ... turned into its NATIVE object by narwhals
    native_frame = df_from_dict.to_native()
    assert isinstance(native_frame, native_type)

    standardized = datelike_localize(native_frame)
    assert isinstance(standardized, native_type)
    standardized_nw_df = nw.from_native(standardized)

    date_types = set(standardized_nw_df.select(~ncs.numeric()).schema.dtypes())
    for dtype in date_types:
        assert isinstance(dtype, nw.dtypes.Datetime)
        assert dtype.time_zone

    # numeric columns should not be affected
    num_types = set(standardized_nw_df.select(ncs.numeric()).schema.dtypes())
    for dtype in num_types:
        assert isinstance(dtype, (nw.Float64))


@pytest.mark.parametrize(
    "testcase",
    [
        "naive_at_date",
        "naive_from_to_date",
        "naive_at_datetime",
        "naive_from_to_datetime",
        "aware_at_datetime",
        "aware_from_to_datetime",
    ],
)
@pytest.mark.parametrize("implementation", ["pandas", "polars", "pyarrow"])
def test_datelike_to_utc(
    caplog,
    testcase,
    implementation,
):
    native_type = NATIVE_TYPES[implementation]
    data = eval(f"{testcase}()")
    df_from_dict = nw.from_dict(data, backend=implementation)
    # a NARWHALS dataframe, ASSOCIATED with the correct implementation:
    assert isinstance(df_from_dict, nw.DataFrame)
    assert str(df_from_dict.implementation) == implementation
    # ... turned into its NATIVE object by narwhals
    native_frame = df_from_dict.to_native()
    assert isinstance(native_frame, native_type)

    standardized = datelike_to_utc(native_frame)
    standardized_twice = datelike_to_utc(standardized)

    # Important: check idempotence
    assert (
        nw.from_native(standardized_twice).to_arrow()
        == nw.from_native(standardized).to_arrow()
    )
    # WTF: no __eq__ for nw.frames!?!

    assert isinstance(standardized, native_type)
    standardized_nw_df = nw.from_native(standardized)

    date_types = set(standardized_nw_df.select(~ncs.numeric()).schema.dtypes())
    for dtype in date_types:
        assert isinstance(dtype, nw.dtypes.Datetime)
        assert dtype.time_zone == "UTC"

    # numeric columns should not be affected
    num_types = set(standardized_nw_df.select(ncs.numeric()).schema.dtypes())
    for dtype in num_types:
        assert isinstance(dtype, (nw.Float64))


# TODO: PARAMETRIZE for all combinations of versioning and temporality
def test_datelike_to_utc_yields_same_result_as_scalar_variant(
    caplog,
    xyz_at,
):
    df = datelike_to_utc(xyz_at)
    nw_df = nw.from_native(df)
    ts.logger.debug(f"xyz:\n{xyz_at}\nnw_df:\n{nw_df}")
    assert "valid_at" in nw_df.select(ncs.datetime(time_zone="UTC")).columns
    assert all(xyz_at.columns == nw_df.columns)

    expected = [date_utc(d) for d in xyz_at["valid_at"]]
    ts.logger.debug(f"xyz:\n{expected}\nnw_df:\n{nw_df}")
    assert all(expected == nw_df["valid_at"].to_native())


def test_validate_dates_succeeds_given_all_expected_columns_in_utc(
    caplog, xyz_at, xyz_from_to
):
    valid_xyz_at = datelike_to_utc(xyz_at)
    valid_xyz_from_to = datelike_to_utc(xyz_from_to)
    assert validate_dates(
        valid_xyz_at,
        ["valid_at"],
    )
    assert validate_dates(
        valid_xyz_from_to,
        ["valid_from", "valid_to"],
    )


# TODO: PARAMETRIZE for multiple cases
def test_validate_dates_fails_for_non_utc_dates(caplog, xyz_at, xyz_from_to):
    assert not validate_dates(
        xyz_at,
        ["valid_at"],
        throw_error=False,
    )
    assert not validate_dates(
        xyz_from_to,
        ["valid_from", "valid_to"],
        throw_error=False,
    )

    with pytest.raises(ValueError):
        _ = validate_dates(xyz_at, ["valid_at"], throw_error=True)

    with pytest.raises(ValueError):
        _ = validate_dates(
            xyz_from_to,
            ["valid_from", "valid_to"],
            throw_error=True,
        )


def test_validate_dates_fails_for_missing_date_columns(caplog, xyz_at, xyz_from_to):
    assert not validate_dates(
        xyz_at,
        ["as_of", "valid_at"],
        throw_error=False,
    )
    assert not validate_dates(
        xyz_from_to,
        ["as_of", "valid_from", "valid_to"],
        throw_error=False,
    )
    with pytest.raises(ValueError):
        _ = validate_dates(xyz_at, ["as_of", "valid_at"], throw_error=True)

    with pytest.raises(ValueError):
        _ = validate_dates(
            xyz_from_to,
            ["as_of", "valid_from", "valid_to"],
            throw_error=True,
        )
