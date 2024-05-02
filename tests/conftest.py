import os

# from typing import Generator
import pytest

# import logging
from ssb_timeseries import config
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# from ssb_timeseries.logging import ts_logger

# HOME = os.getenv("HOME")
# TIMESERIES_CONFIG = os.getenv("TIMESERIES_CONFIG")

# mypy: ignore-errors


@pytest.fixture(scope="function", autouse=False)
def remember_config():
    """A fixture to make sure that running tests do not change the configuration file."""
    config_file = os.getenv("TIMESERIES_CONFIG")
    if config_file:
        configuration = config.Config(configuration_file=config_file)
        print(
            f"Because TIMESERIES_CONFIG identifies a config file, before tests, read configuration: {configuration}"
        )

    # tests run here
    yield

    if config_file and os.path.isfile(config_file):
        print(
            f"To make sure the tests have not altered configurations:\n{config.Config()}"
        )
        print(f"revert to what we read above:\n{configuration}")
        configuration.save(config_file)


@pytest.fixture(scope="module", autouse=True)
def print_stuff():
    """Just testing pytest.fixtures."""
    print("Before test module")
    yield
    print("After test modules")


@pytest.fixture(scope="function", autouse=False)
def existing_simple_set():
    """A fixture to create simple dataset before running the test."""
    # create dataset and save
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    x = Dataset(
        name="test-existing-simple-dataset",
        data_type=SeriesType.simple(),
        series_tags=tags,
    )
    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    x.save()

    # tests run here
    yield x

    # TODO: delete file after test


@pytest.fixture(scope="function", autouse=False)
def existing_estimate_set():
    """A fixture to create simple dataset before running the test."""
    # create dataset and save
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    x = Dataset(
        name="test-existing-estimate-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags=tags,
    )
    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2023-01-03",
        freq="MS",
    )
    x.save()

    # tests run here
    yield x

    # TODO: delete file after test
