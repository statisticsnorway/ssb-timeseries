import inspect
import os

import pytest

from ssb_timeseries import config
from ssb_timeseries.config import CONFIG as ORIGINAL_CONFIG
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.fs import rmtree
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors

CONFIGURATION_FILE = ORIGINAL_CONFIG.configuration_file


class Helpers:
    @staticmethod
    def function_name() -> str:
        return str(inspect.stack()[1][3])


@pytest.fixture(scope="function", autouse=False)
def conftest() -> Helpers:
    h = Helpers()
    return h


@pytest.fixture(scope="function", autouse=False)
def remember_config():
    """A fixture to make sure that running tests do not change the configuration file."""
    # config_file = os.getenv("TIMESERIES_CONFIG")
    # if config_file:
    # configuration = config.Config(configuration_file=config_file)
    if CONFIGURATION_FILE:
        configuration = ORIGINAL_CONFIG
        print(
            f"Because TIMESERIES_CONFIG identifies a config file, before tests, read configuration: {configuration}"
        )

    # tests run here
    yield

    if CONFIGURATION_FILE and os.path.isfile(CONFIGURATION_FILE):
        print(
            f"To make sure the tests have not altered configurations:\n{config.Config()}"
        )
        print(f"revert to what we read above:\n{configuration}")
        configuration.save(CONFIGURATION_FILE)


@pytest.fixture(scope="module", autouse=True)
def print_stuff():
    """Just testing pytest.fixtures."""
    print("Before test module")
    yield
    print("After test modules")


@pytest.fixture(scope="function", autouse=False)
def existing_simple_set():
    """A fixture to create simple dataset before running the test."""
    # buildup: create dataset and save
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

    # teardown
    rmtree(x.io.data_dir)


@pytest.fixture(scope="function", autouse=False)
def existing_estimate_set():
    """A fixture to create simple dataset before running the test."""
    # buildup: create dataset and save
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

    # teardown
    rmtree(x.io.data_dir)


@pytest.fixture(scope="function", autouse=False)
def new_dataset_none_at():
    """A fixture to create simple dataset before running the test."""
    # buildup: create dataset and save
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    x = Dataset(
        name="test-existing-simple-dataset",
        data_type=SeriesType.simple(),
        series_tags={"D": "d"},
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2022-10-03",
            freq="MS",
        ),
        name_pattern=["A", "B", "C"],
    )

    # tests run here
    yield x
    # file was not saved, so no teardown is necessary


@pytest.fixture(scope="function", autouse=False)
def new_dataset_as_of_at():
    """A fixture to create simple dataset before running the test."""
    # create dataset and save
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    x = Dataset(
        name="test-existing-simple-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags={"D": "d"},
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2022-10-03",
            freq="MS",
        ),
        name_pattern=["A", "B", "C"],
    )
    # tests run here
    yield x
    # file was not saved, so no teardown is necessary


@pytest.fixture(scope="function", autouse=False)
def new_dataset_as_of_from_to():
    """A fixture to create simple dataset before running the test."""
    # create dataset and save
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    x = Dataset(
        name="test-existing-simple-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags={"D": "d"},
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2022-10-03",
            freq="MS",
        ),
        name_pattern=["A", "B", "C"],
    )

    # tests run here
    yield x
    # file was not saved, so no teardown is necessary


@pytest.fixture(scope="function", autouse=False)
def new_dataset_none_from_to():
    """A fixture to create simple dataset before running the test."""
    # create dataset and save
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    x = Dataset(
        name="test-existing-simple-dataset",
        data_type=SeriesType.estimate(),
        series_tags={"D": "d"},
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2022-10-03",
            freq="MS",
        ),
        name_pattern=["A", "B", "C"],
    )

    # tests run here
    yield x
    # file was not saved, so no teardown is necessary
