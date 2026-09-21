"""Reproducing https://github.com/statisticsnorway/ssb-timeseries/issues/249."""

import logging
from datetime import date
from datetime import datetime

import pandas as pd
import pytest

from ssb_timeseries.dataset import Dataset

from ..fixtures.dataset_factories import function_name_hex

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment"

test_logger = logging.getLogger(__name__)


@pytest.fixture
def dates_are_datetimes() -> list:
    dates = [
        datetime(2026, 1, 1),
        datetime(2026, 1, 2),
        datetime(2026, 1, 3),
    ]  # <--- datestimes!
    yield dates


@pytest.fixture
def dates_are_dates() -> list:
    dates = [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]  # <--- dates!
    yield dates


@pytest.fixture
def dates_are_strings() -> list:
    dates = ["2026-01-01", "2026-01-02", "2026-01-03"]  # <--- strings!
    yield dates


# -------- multiple config scenarios for sharing --------------------


@pytest.fixture(
    params=[
        pytest.param("dates_are_datetimes"),
        pytest.param("dates_are_dates"),
        pytest.param(
            "dates_are_strings",
            marks=pytest.mark.xfail(reason="Dates as trings are not supported."),
        ),
    ],
)
def dates(
    request,
) -> tuple:
    """Combines parameter sets with datasets of all types to create complete test cases."""
    the_dates = request.getfixturevalue(request.param)
    yield the_dates


# -------- --------------------


def test_reproduce_bug_in_issue_249_without_save_before_snapshot(
    caplog,
    dates,
):
    """This is as reported.

    With no .save() before .snapshot(), the code reported would have failed in a later step.
    """
    caplog.set_level(logging.DEBUG)

    df_a = pd.DataFrame({"valid_at": dates, "a": [1.1, 1.3, 1.4]})
    df_b = pd.DataFrame({"valid_at": dates, "a": [2.5, 2.6, 2.7]})

    ds_a = Dataset(name="ds_a", data=df_a, data_type="simple")
    ds_b = Dataset(name="ds_b", data=df_b, data_type="simple")

    ds_c = ds_a + ds_b
    ds_c.rename(
        function_name_hex()  # to make sure we have a new dataset
    )
    assert isinstance(ds_c, Dataset)

    with pytest.raises(FileNotFoundError):
        # ds_c.save()  # <--- not there
        ds_c.snapshot()  # <-- so expect FileNotFoundError here
        # (instead, it failed earlier, seeing a Pyarrow table rather than a Pandas df)


def test_reproduce_bug_in_issue_249_with_save_before_snapshot(
    caplog,
    dates,
):
    """This is *almost* as reported.

    Adding .save() before .snapshot(), the code should work.
    (And after fixing daterange, it does.)
    """
    caplog.set_level(logging.DEBUG)

    df_a = pd.DataFrame({"valid_at": dates, "a": [1.1, 1.3, 1.4]})
    df_b = pd.DataFrame({"valid_at": dates, "a": [2.5, 2.6, 2.7]})

    ds_a = Dataset(name="ds_a", data=df_a, data_type="simple")
    ds_b = Dataset(name="ds_b", data=df_b, data_type="simple")

    ds_c = ds_a + ds_b
    ds_c.rename(
        function_name_hex()
    )  # some datacol types different category of error for existing sets
    assert isinstance(ds_c, Dataset)

    ds_c.save()  #  <--- this was not in reported issue, but important!
    ds_c.snapshot()  # <- with it, this works after the date_range fix


def test_reproduce_bug_in_issue_249_does_calculation_really_make_a_difference(
    caplog,
    dates,
):
    """This is *almost* as reported.

    Adding .save() before .snapshot(), the code should work.
    After fixing, it does.
    """
    caplog.set_level(logging.DEBUG)

    df_a = pd.DataFrame({"valid_at": dates, "a": [1.1, 1.3, 1.4]})
    ds_a = Dataset(name="ds_a", data=df_a, data_type="simple")

    ds_a.save()
    ds_a.snapshot()
