import inspect
import logging
from pathlib import Path

import pytest

from ssb_timeseries import config
from ssb_timeseries import fs
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors

TEST_DIR = ""


class Helpers:
    configuration: config.Config = config.CONFIG

    @staticmethod
    def test_dir() -> str:
        return TEST_DIR

    @staticmethod
    def function_name() -> str:
        return str(inspect.stack()[1][3])


@pytest.fixture(scope="function", autouse=False)
def conftest() -> Helpers:
    h = Helpers()
    return h


@pytest.fixture(scope="function", autouse=True)
def reset_config_after():
    cfg_file = config.CONFIGURATION_FILE
    remembered_config = config.Config(cfg_file)
    config.CONFIG = remembered_config
    yield config.CONFIG
    remembered_config.save(cfg_file)


@pytest.fixture(scope="session", autouse=False)
def buildup_and_teardown(tmp_path_factory, caplog):
    """To make sure that tests do not change the configuration file."""
    caplog.set_level(logging.DEBUG)
    before_tests = config.CONFIG

    if before_tests.configuration_file:
        print(
            f"Before running tests:\nTIMESERIES_CONFIG: {before_tests.configuration_file}:\n{before_tests.to_json()}"
        )
        cfg_file = Path(before_tests.configuration_file).name
        config_file_for_testing = tmp_path_factory.mktemp("config") / cfg_file
        config.CONFIG.configuration_file = config_file_for_testing
        config.CONFIG.timeseries_root = tmp_path_factory.mktemp("series_data")
        config.CONFIG.bucket = tmp_path_factory.mktemp("production-bucket")
        config.CONFIG.save(config_file_for_testing)
        Helpers.configuration = config.CONFIG

    else:
        print(
            f"No configuration file found before tests:\nTIMESERIES_CONFIG: {before_tests.configuration_file}\n..raise error?"
        )

    print(f"Current configurations:\n{config.CONFIG}")

    # tests run here
    yield config.CONFIG

    if config.CONFIG != before_tests:
        print(
            f"Configurations was changed by tests:\n{config.CONFIG}\nReverting to original:\n{before_tests}"
        )
        before_tests.save(before_tests.configuration_file)
    else:
        print(
            f"Final configurations after tests was identical to orginal:\n{config.CONFIG}\nReverting to original:\n{before_tests}"
        )


@pytest.fixture(scope="function", autouse=False)
def existing_simple_set():
    """Create a simple dataset (and save so that files are existing) before running the test. Delete files afterwards."""
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
    fs.rmtree(x.io.data_dir)


@pytest.fixture(scope="function", autouse=False)
def existing_estimate_set():
    """Create an estimeat (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
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
    fs.rmtree(x.io.data_dir)


@pytest.fixture(scope="function", autouse=False)
def new_dataset_none_at():
    """A fixture to create simple dataset before running the test."""
    # buildup: create dataset and save
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    x = Dataset(
        name="test-existing-dataset-none-at",
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
        name="test-existing-dataset-as-of-at",
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
        name="test-existing-as-of-from-to",
        data_type=SeriesType.as_of_from_to(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags={"D": "d"},
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2022-10-03",
            freq="MS",
            temporality="FROM_TO",
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
        name="test-existing-dataset-none-from-to",
        data_type=SeriesType.from_to(),
        series_tags={"D": "d"},
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2022-10-03",
            freq="MS",
            temporality="FROM_TO",
        ),
        name_pattern=["A", "B", "C"],
    )

    # tests run here
    yield x
    # file was not saved, so no teardown is necessary
