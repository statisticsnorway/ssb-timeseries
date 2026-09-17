"""Construct in-memory datasets and raw data."""

from __future__ import annotations

import inspect
import uuid

import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType


# ----- helpers for tag_values + generating dataframes with data -----
@pytest.fixture(scope="session")
def tag_values():
    """Define series names for which to generate test data."""
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    yield tag_values


@pytest.fixture(scope="session")
def abc_at(tag_values):
    df = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-12-03",
        freq="MS",
        temporality="AT",
    )
    yield df


@pytest.fixture
def abc_from_to(tag_values):
    df = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-12-03",
        freq="MS",
        temporality="FROM_TO",
    )
    yield df


@pytest.fixture(scope="session")
def xyz_at():
    df = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
        temporality="AT",
    )
    yield df


@pytest.fixture(scope="session")
def xyz_from_to():
    df = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
        temporality="FROM_TO",
    )
    yield df


# ----- other helper(s) ------------------------------


def function_name_hex(n: int = 8) -> str:
    """Return name of calling function + *n* random characters.

    The approach (taking the first n characters of of a uuid) is likely,
    but not *guaranteed* to be unique.
    Here we prefer shorter.
    """
    return f"{inspect.stack()[1][3]!s}_{uuid.uuid4().hex[:n]}"


# ---- The actual fixtures ---------------------------


@pytest.fixture
def new_dataset_none_at(abc_at, buildup_and_teardown):
    """A fixture to create a new simple (non-versioned point in time) dataset before running the test."""
    x = Dataset(
        name=function_name_hex(8),
        data_type=SeriesType.simple(),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    yield x


@pytest.fixture
def new_dataset_as_of_at(abc_at, buildup_and_teardown):
    """A fixture to create a new versioned point in time dataset before running the test."""
    x = Dataset(
        name=function_name_hex(8),
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    yield x


@pytest.fixture
def new_dataset_none_from_to(abc_from_to, buildup_and_teardown):
    """A fixture to create a new non-versioned period dataset before running the test."""
    x = Dataset(
        name=function_name_hex(8),
        data_type=SeriesType.from_to(),
        data=abc_from_to,
        attributes=["A", "B", "C"],
    )
    yield x


@pytest.fixture
def new_dataset_as_of_from_to(abc_from_to, buildup_and_teardown):
    """A fixture to create a new versioned period dataset before running the test."""
    x = Dataset(
        name=function_name_hex(8),
        data_type=SeriesType.as_of_from_to(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_from_to,
        attributes=["A", "B", "C"],
    )

    yield x
