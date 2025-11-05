from __future__ import annotations

import inspect
import logging
import uuid
import warnings
from copy import deepcopy
from pathlib import Path

import pytest

from ssb_timeseries import config
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.io import fs
from ssb_timeseries.logging import set_up_logging_according_to_config
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType

# mypy: ignore-errors

_ENV_VAR_VALUE_BEFORE_TESTS = config.active_file()

# TEST_LOGGER = "ssb_timeseries"  # should it be ts package logger?
TEST_LOGGER = "tests"  # ... no, 'tests' is necessary, BUT requires a entry in config:
TEST_LOG_CONFIG = deepcopy(config.LOGGING_PRESETS["console+file"])
TEST_LOG_CONFIG["loggers"][TEST_LOGGER] = TEST_LOG_CONFIG["loggers"].pop(
    config.PACKAGE_NAME
)


def pytest_configure(config):
    """Pytest hook to configure plugins."""
    try:
        from typeguard import config as typeguard_config

        # Policy can be 'warn' (default), 'error', or 'ignore'
        # 'ignore' will suppress the warning and let the tests pass.
        # fix typeguard.TypeHintWarning: Cannot resolve forward reference 'DataFrame[Any]'
        typeguard_config.forward_ref_policy = "ignore"
    except ImportError:
        pass  # typeguard is not installed


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
    console.setLevel("DEBUG")
    return console


class Helpers:
    configuration: config.Config
    repo: dict | None = None
    logger: str = TEST_LOGGER

    def __init__(self, configuration: config.Config | None = None) -> None:
        """Helpers for tests."""
        if configuration:
            self.configuration = configuration
            self.repo = self.configuration.repositories["test_1"]

    @staticmethod
    def function_name() -> str:
        """Return name of calling function."""
        return str(inspect.stack()[1][3])

    @staticmethod
    def function_name_hex(n: int = 8) -> str:
        """Return name of calling function + *n* random characters.

        The approach (taking the first n characters of of a uuid) is likely,
        but not *guaranteed* to be unique.
        Here we prefer shorter.
        """
        return f"{inspect.stack()[1][3]!s}_{uuid.uuid4().hex[:n]}"


@pytest.fixture(scope="function")
def conftest(buildup_and_teardown) -> Helpers:
    h = Helpers(configuration=buildup_and_teardown)
    return h


@pytest.fixture(scope="session")
def root_dir(tmp_path_factory):
    root = tmp_path_factory.mktemp("tests")
    yield root


def _repository_test_config(path: Path) -> dict[str, str]:
    """Configure repositories based on temp dir root path."""
    return {
        "test_1": {
            "name": "test_1",
            "directory": {
                "options": {
                    "path": str(path / "series_test_1"),
                },
                "handler": "simple-parquet",
            },
            "catalog": {
                "handler": "json",
                "options": {
                    "path": str(path / "metadata_test_1"),
                    # "hello": "world",
                    # superfluous options are ignored by the code, but will raise a typeguard error
                },
                # "hallo": "verden",
                # unexpected attributes are ignored by the code, but will raise a typeguard error
            },
            "default": True,
        },
        "test_2": {
            "name": "test_2",
            "directory": {
                "handler": "simple-parquet",
                "options": {
                    "path": str(path / "series_test_2"),
                },
            },
            "catalog": {
                "handler": "json",
                "options": {"path": str(path / "metadata_test_2")},
            },
        },
    }


def _snapshot_test_config(path: Path) -> dict[str, str]:
    """Configure snapshots based on temp dir root path."""
    return {
        "default": {
            "name": "snapshot-archive",
            "directory": {
                "handler": "snapshots",
                "options": {"path": str(path / "snapshots")},
            },
        },
    }


def _sharing_test_config(path: Path) -> dict[str, str]:
    """Return a sharing test configuration based on temp dir root path."""
    return {
        "default": {
            "directory": {
                "handler": "snapshots",
                "options": {"path": str(path / "shared" / "default")},
            }
        },
        "s123": {
            "directory": {
                "handler": "snapshots",
                "options": {"path": str(path / "shared" / "s123")},
            }
        },
        "s234": {
            "directory": {
                "handler": "snapshots",
                "options": {"path": str(path / "shared" / "s234")},
            }
        },
    }


@pytest.fixture(scope="module", autouse=True)
def buildup_and_teardown(
    root_dir,
):
    """Reset config and logging between modules."""
    before_tests = config.CONFIG
    config_file_for_testing = str(
        fs.touch(root_dir / "config" / "config_for_tests.json")
    )
    assert config_file_for_testing != ""

    log_file_for_testing = fs.touch(root_dir / "logs" / "log_for_tests.log")
    log_config = TEST_LOG_CONFIG
    log_config["handlers"]["file"]["filename"] = str(log_file_for_testing)

    config.active_file(config_file_for_testing)
    temp_configuration = config.Config(
        configuration_file=str(config_file_for_testing),
        log_file=str(log_file_for_testing),
        io_handlers=config.BUILTIN_IO_HANDLERS,
        repositories=_repository_test_config(root_dir),
        snapshots=_snapshot_test_config(root_dir),
        sharing=_sharing_test_config(root_dir),
        bucket=str(root_dir / "bucket"),
        logging=log_config,
        ignore_file=True,
    )
    temp_configuration.save()
    assert fs.exists(temp_configuration.configuration_file)

    logger = set_up_logging_according_to_config(TEST_LOGGER, temp_configuration.logging)
    logger.addHandler(console_log_handler())
    logger.addFilter(LogWarningFilter())
    yield temp_configuration
    logging.getLogger(TEST_LOGGER).removeFilter(LogWarningFilter())

    if before_tests.configuration_file:
        before_tests.save()
    else:
        config.unset_env_var()

    set_up_logging_according_to_config(config.PACKAGE_NAME, before_tests.logging)
    active_config_after = config.active_file()
    assert active_config_after == _ENV_VAR_VALUE_BEFORE_TESTS


# -----------------------------------------------------------------------------


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


@pytest.fixture(scope="function")
def abc_from_to(tag_values):
    df = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-12-03",
        freq="MS",
        temporality="FROM_TO",
    )
    yield df


# -----------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------


@pytest.fixture(scope="function")
def new_dataset_none_at(abc_at, buildup_and_teardown):
    """A fixture to create a new simple (non-versioned point in time) dataset before running the test."""
    x = Dataset(
        name=Helpers.function_name_hex(8),
        data_type=SeriesType.simple(),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    yield x


@pytest.fixture(scope="function")
def new_dataset_as_of_at(abc_at, buildup_and_teardown):
    """A fixture to create a new versioned point in time dataset before running the test."""
    x = Dataset(
        name=Helpers.function_name_hex(8),
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_at,
        attributes=["A", "B", "C"],
    )
    yield x


@pytest.fixture(scope="function")
def new_dataset_none_from_to(abc_from_to, buildup_and_teardown):
    """A fixture to create a new non-versioned period dataset before running the test."""
    x = Dataset(
        name=Helpers.function_name_hex(8),
        data_type=SeriesType.from_to(),
        data=abc_from_to,
        attributes=["A", "B", "C"],
    )
    yield x


@pytest.fixture(scope="function")
def new_dataset_as_of_from_to(abc_from_to, buildup_and_teardown):
    """A fixture to create a new versioned period dataset before running the test."""
    x = Dataset(
        name=Helpers.function_name_hex(8),
        data_type=SeriesType.as_of_from_to(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_from_to,
        attributes=["A", "B", "C"],
    )

    yield x


# -----------------------------------------------------------------------------


@pytest.fixture(
    params=[
        "new_dataset_none_at",
        "new_dataset_none_from_to",
    ],
    scope="function",
)
def one_new_set_for_each_unversioned_type(request):
    """A fixture returning one example dataset for each *unversioned* data type in a list."""
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    params=[
        "new_dataset_as_of_at",
        "new_dataset_as_of_from_to",
    ],
    scope="function",
)
def one_new_set_for_each_versioned_type(request):
    """A fixture returning one example dataset for each *versioned* data type in a list."""
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    params=[
        "new_dataset_none_at",
        "new_dataset_none_from_to",
        "new_dataset_as_of_at",
        "new_dataset_as_of_from_to",
    ],
    scope="function",
)
def one_new_set_for_each_data_type(request):
    """A fixture returning one example dataset for each data type in a list."""
    yield request.getfixturevalue(request.param)


# -----------------------------------------------------------------------------


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
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


# -----------------------------------------------------------------------------


@pytest.fixture(
    params=["existing_none_at_set", "existing_none_from_to_set"],
    scope="function",
)
def one_existing_set_for_each_unversioned_type(request):
    """A fixture returning one example dataset for each *unversioned* data type in a list."""
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    params=["existing_dataset_as_of_at", "existing_dataset_as_of_from_to"],
    scope="function",
)
def one_existing_set_for_each_versioned_type(request):
    """A fixture returning one example dataset for each *versioned* data type in a list."""
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    params=[
        "existing_none_at_set",
        "existing_none_from_to_set",
        "existing_estimate_set",
        "existing_as_of_from_to_set",
    ],
    scope="function",
)
def one_existing_set_for_each_data_type(request):
    """A parameterized fixture returning one saved dataset for each data type."""
    yield request.getfixturevalue(request.param)


# -----------------------------------------------------------------------------


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
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
