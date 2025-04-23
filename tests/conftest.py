import inspect
import logging
import warnings
from copy import deepcopy

# from pathlib import Path
import pytest

from ssb_timeseries import config
from ssb_timeseries import fs
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.logging import set_up_logging_according_to_config
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors

ENV_VAR_NAME = "TIMESERIES_CONFIG"
_ENV_VAR_VALUE_BEFORE_TESTS = config.active_file()

# TEST_LOGGER = "ssb_timeseries"  # should it be ts package logger?
TEST_LOGGER = "tests"  # ... no, 'tests' is necessary, BUT requires a entry in config:
TEST_LOG_CONFIG = deepcopy(config.LOGGING_PRESETS["console+file"])
TEST_LOG_CONFIG["loggers"][TEST_LOGGER] = TEST_LOG_CONFIG["loggers"].pop(
    config.PACKAGE_NAME
)


class LogWarning(UserWarning):
    pass


class LogWarningFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        """Raises a real alert for all log messages with level warning or higher."""
        if record.levelno == logging.WARNING:
            warnings.warn(
                LogWarning(record.getMessage()),
                stacklevel=2,
            )
        return True


def console_log_handler() -> None:
    console = logging.StreamHandler()
    string_format = logging.Formatter(
        "%(name)s | %(levelname)s | %(asctime)s | %(message)s \n"
    )
    console.setFormatter(string_format)
    console.setLevel("INFO")
    return console


class Helpers:
    configuration: config.Config

    def __init__(self, configuration: config.Config | None = None) -> None:
        """Helpers for tests."""
        if configuration:
            self.configuration = configuration

    @staticmethod
    def function_name() -> str:
        return str(inspect.stack()[1][3])


@pytest.fixture(scope="function", autouse=False)
def conftest(buildup_and_teardown) -> Helpers:
    h = Helpers(configuration=buildup_and_teardown)
    return h


@pytest.fixture(
    scope="module",
    autouse=True,
)
def buildup_and_teardown(
    tmp_path_factory,
):
    """Reset config and logging between modules."""
    before_tests = config.CONFIG
    config_file_for_testing = str(
        fs.touch(tmp_path_factory.mktemp("config") / "config_for_tests.json")
    )
    assert config_file_for_testing != ""

    log_file_for_testing = fs.touch(
        tmp_path_factory.mktemp("logs") / "log_for_tests.log"
    )
    log_config = TEST_LOG_CONFIG
    log_config["handlers"]["file"]["filename"] = str(log_file_for_testing)

    config.active_file(config_file_for_testing)
    temp_configuration = config.Config(
        configuration_file=str(config_file_for_testing),
        log_file=str(log_file_for_testing),
        timeseries_root=str(tmp_path_factory.mktemp("series")),
        catalog=str(tmp_path_factory.mktemp("metadata")),
        bucket=str(tmp_path_factory.mktemp("bucket")),
        logging=log_config,
        ignore_file=True,
    )
    temp_configuration.save()
    assert fs.exists(temp_configuration.configuration_file)

    logger = set_up_logging_according_to_config(TEST_LOGGER, temp_configuration.logging)
    # logger.addHandler(console_log_handler())
    logger.addFilter(LogWarningFilter())
    yield temp_configuration
    logging.getLogger(TEST_LOGGER).removeFilter(LogWarningFilter())

    if before_tests.configuration_file:  # and isinstance(before_tests, config.Config):
        before_tests.save()
    else:
        config.unset_env_var()

    set_up_logging_according_to_config(config.PACKAGE_NAME, before_tests.logging)
    active_config_after = config.active_file()
    assert active_config_after == _ENV_VAR_VALUE_BEFORE_TESTS


@pytest.fixture(scope="session", autouse=False)
def tag_values():
    """Define series names for which to generate test data."""
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    yield tag_values


@pytest.fixture(scope="session", autouse=False)
def abc_at(tag_values):
    df = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-12-03",
        freq="MS",
        temporality="AT",
    )
    yield df


@pytest.fixture(scope="session", autouse=False)
def abc_from_to(tag_values):
    df = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-12-03",
        freq="MS",
        temporality="FROM_TO",
    )
    yield df


@pytest.fixture(scope="function", autouse=False)
def xyz_at():
    df = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
        temporality="AT",
    )
    yield df


@pytest.fixture(scope="function", autouse=False)
def xyz_from_to():
    df = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
        temporality="FROM_TO",
    )
    yield df


@pytest.fixture(scope="function", autouse=False)
def new_dataset_none_at(abc_at):
    """A fixture to create simple dataset before running the test."""
    x = Dataset(
        name="test-new-dataset-none-at",
        data_type=SeriesType.simple(),
        series_tags={"D": "d"},
        data=abc_at,
        attributes=["A", "B", "C"],
        dataset_tags={"E": "Eee"},
    )
    yield x


@pytest.fixture(scope="function", autouse=False)
def new_dataset_as_of_at(abc_at):
    """A fixture to create simple dataset before running the test."""
    x = Dataset(
        name="test-new-dataset-as-of-at",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags={"D": "d"},
        data=abc_at,
        attributes=["A", "B", "C"],
        dataset_tags={"E": "Eee"},
    )
    yield x


@pytest.fixture(scope="function", autouse=False)
def new_dataset_none_from_to(abc_from_to):
    """A fixture to create simple dataset before running the test."""
    x = Dataset(
        name="test-new-dataset-none-from-to",
        data_type=SeriesType.from_to(),
        series_tags={"D": "d"},
        data=abc_from_to,
        attributes=["A", "B", "C"],
        dataset_tags={"E": "Eee"},
    )
    yield x


@pytest.fixture(scope="function", autouse=False)
def new_dataset_as_of_from_to(abc_from_to):
    """A fixture to create simple dataset before running the test."""
    x = Dataset(
        name="test-new-as-of-from-to",
        data_type=SeriesType.as_of_from_to(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags={"D": "d"},
        data=abc_from_to,
        attributes=["A", "B", "C"],
        dataset_tags={"E": "Eee"},
    )

    yield x


@pytest.fixture(scope="function", autouse=False)
def one_new_set_for_each_data_type(
    new_dataset_none_at,
    new_dataset_none_from_to,
    new_dataset_as_of_at,
    new_dataset_as_of_from_to,
):
    """A fixture returning one example dataset for each data type in a list."""
    yield [
        new_dataset_none_at,
        new_dataset_none_from_to,
        new_dataset_as_of_at,
        new_dataset_as_of_from_to,
    ]


@pytest.fixture(scope="module", autouse=False)
def existing_simple_set(abc_at):
    """Create a simple dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    x = Dataset(
        name="test-existing-simple-dataset",
        data_type=SeriesType.simple(),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    x.save()
    yield x


@pytest.fixture(scope="function", autouse=False)
def existing_estimate_set(abc_at):
    """Create an estimeat (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    x = Dataset(
        name="test-existing-estimate-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    x.save()
    yield x


@pytest.fixture(scope="function", autouse=False)
def existing_from_to_set(abc_from_to):
    """Create an estimeat (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    x = Dataset(
        name="test-existing-small-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_from_to,
        attributes=["A", "B", "C"],
        series_tags={"D": "d"},
        dataset_tags={"E": "e", "F": ["f1", "f2"]},
    )
    x.save()
    yield


@pytest.fixture(scope="function", autouse=False)
def existing_small_set():
    """Create an estimeat (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
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


@pytest.fixture(scope="function", autouse=False)
def existing_sets(existing_estimate_set, existing_simple_set, existing_small_set):
    """A fixture returning one existing (previously saved) example dataset for each data type."""
    yield [
        existing_estimate_set,
        existing_simple_set,
        existing_small_set,
    ]
