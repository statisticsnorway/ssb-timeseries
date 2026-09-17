"""Construct and save datasets that should exist before running tests."""

from __future__ import annotations

import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType

from .dataset_factories import new_dataset_as_of_at  # noqa: F401
from .dataset_factories import new_dataset_as_of_from_to  # noqa: F401
from .dataset_factories import new_dataset_none_at  # noqa: F401
from .dataset_factories import new_dataset_none_from_to  # noqa: F401

# mypy: ignore-errors


@pytest.fixture
def existing_none_at_set(abc_at, buildup_and_teardown):
    """Create a simple dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    x = Dataset(
        name="test-existing-dataset-none-at",
        data_type=SeriesType.simple(),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    x.save()
    yield x


@pytest.fixture
def existing_none_from_to_set(abc_from_to, buildup_and_teardown):
    """Create a non-versioned from-to dataset and save it."""
    x = Dataset(
        name="test-existing-dataset-none-from-to",
        data_type=SeriesType.from_to(),
        data=abc_from_to,
        attributes=["A", "B", "C"],
    )
    x.save()
    yield x


@pytest.fixture
def existing_as_of_at_set(abc_at, buildup_and_teardown):
    """Create an estimate (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    x = Dataset(
        name="test-existing-dataset-as-of-at",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    x.save()
    yield x


@pytest.fixture
def existing_as_of_from_to_set(abc_from_to, buildup_and_teardown):
    """Create a versioned from-to dataset and save it."""
    x = Dataset(
        name="test-existing-dataset-as-of-from-to",
        data_type=SeriesType.as_of_from_to(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_from_to,
        attributes=["A", "B", "C"],
    )
    x.save()
    yield x


# ------ leftovers from the early days - consider replacing ------------


@pytest.fixture
def existing_simple_set(abc_at, buildup_and_teardown):
    """Create a simple dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    x = Dataset(
        name="test-existing-simple-dataset",
        data_type=SeriesType.simple(),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    x.save()
    yield x


@pytest.fixture
def existing_estimate_set(abc_at, buildup_and_teardown):
    """Create an estimate (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    x = Dataset(
        name="test-existing-estimate-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    x.save()
    yield x


@pytest.fixture
def existing_small_set(buildup_and_teardown):
    """Create an estimate (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    tags = {"A": ["a1", "a2", "a3"], "B": ["b"], "C": ["c"]}
    tag_values = [value for value in tags.values()]
    x = Dataset(
        name="test-existing-small-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2024-01-03",
            freq="YS",
        ),
        attributes=["A", "B", "C"],
        series_tags={"D": "d"},
        dataset_tags={"E": "e", "F": ["f1", "f2"]},
    )
    x.save()
    yield x
