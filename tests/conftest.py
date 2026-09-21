from __future__ import annotations

import inspect
import logging
import uuid
import warnings
from copy import deepcopy
from pathlib import Path

import pytest

from ssb_timeseries import config
from ssb_timeseries.io import fs
from ssb_timeseries.logging import set_up_logging_according_to_config
from ssb_timeseries.types import SeriesType

from .fixtures.dataset_factories import abc_at  # noqa: F401
from .fixtures.dataset_factories import abc_from_to  # noqa: F401
from .fixtures.dataset_factories import new_dataset_as_of_at  # noqa: F401
from .fixtures.dataset_factories import new_dataset_as_of_from_to  # noqa: F401
from .fixtures.dataset_factories import new_dataset_none_at  # noqa: F401
from .fixtures.dataset_factories import new_dataset_none_from_to  # noqa: F401
from .fixtures.dataset_factories import tag_values  # noqa: F401
from .fixtures.dataset_factories import xyz_at  # noqa: F401
from .fixtures.dataset_factories import xyz_from_to  # noqa: F401
from .fixtures.persisted_datasets import existing_as_of_at_set  # noqa: F401
from .fixtures.persisted_datasets import existing_as_of_from_to_set  # noqa: F401
from .fixtures.persisted_datasets import existing_estimate_set  # noqa: F401
from .fixtures.persisted_datasets import existing_none_at_set  # noqa: F401
from .fixtures.persisted_datasets import existing_none_from_to_set  # noqa: F401
from .fixtures.persisted_datasets import existing_simple_set  # noqa: F401
from .fixtures.persisted_datasets import existing_small_set  # noqa: F401

# mypy: ignore-errors

_ENV_VAR_VALUE_BEFORE_TESTS = config.active_file()

ORIGINAL_LOGGER = logging.getLogger(config.PACKAGE_NAME)
# TEST_LOGGER = "ssb_timeseries"  # should it be ts package logger?
TEST_LOGGER = "tests"  # ... no, 'tests' is better,
# ... BUT requires an entry in config:
TEST_LOG_CONFIG = deepcopy(config.LOGGING_PRESETS["console+file"])
# However:
# TEST_LOG_CONFIG["loggers"][TEST_LOGGER] = TEST_LOG_CONFIG["loggers"].pop(
#     config.PACKAGE_NAME
# )
# ... fails, because tests running marimo via subprocess gets 'ssb_timeseries' rather than 'tests' (even if copying env!)
# --> add 'tests', rather than replace 'ssb_timeseries'
TEST_LOG_CONFIG["loggers"][TEST_LOGGER] = deepcopy(
    TEST_LOG_CONFIG["loggers"][config.PACKAGE_NAME]
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
    before_tests = config.Config.active()
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
        # log_file=str(log_file_for_testing),
        io_handlers=config.BUILTIN_IO_HANDLERS,
        repositories=_repository_test_config(root_dir),
        snapshots=_snapshot_test_config(root_dir),
        sharing=_sharing_test_config(root_dir),
        bucket=str(root_dir / "bucket"),
        logging=log_config,
        ignore_file=True,
    )
    temp_configuration.save(config_file_for_testing)
    temp_configuration.activate()
    assert fs.exists(temp_configuration.configuration_file)

    logger = set_up_logging_according_to_config(TEST_LOGGER, temp_configuration.logging)
    logger.addHandler(console_log_handler())
    logger.addFilter(LogWarningFilter())
    yield temp_configuration
    logging.getLogger(TEST_LOGGER).removeFilter(LogWarningFilter())

    if _ENV_VAR_VALUE_BEFORE_TESTS:
        before_tests.save()
        before_tests.activate()
        set_up_logging_according_to_config(config.PACKAGE_NAME, before_tests.logging)
    else:
        config.unset_env_var()
        config.Config().refresh()
        logging.getLogger(config.PACKAGE_NAME)

    assert config.active_file() == _ENV_VAR_VALUE_BEFORE_TESTS


# ---- get NEW sets per series (group of) series type ----------------


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
)
def one_new_set_for_each_data_type(request):
    """A fixture returning one example dataset for each data type in a list."""
    yield request.getfixturevalue(request.param)


# ---- get EXISTING sets per series (group of) series type ---------


@pytest.fixture(params=["NONE", "AS_OF"])
def versioning(request):
    yield request.param


@pytest.fixture(params=["AT", "FROM_TO"])
def temporality(request):
    yield request.param


@pytest.fixture()
def series_types(versioning, temporality):
    """SeriesType for every combination of versioning and temporality."""
    yield SeriesType(versioning, temporality)


@pytest.fixture(
    params=["existing_none_at_set", "existing_none_from_to_set"],
)
def one_existing_set_for_each_unversioned_type(request):
    """A fixture returning one example dataset for each *unversioned* data type in a list."""
    yield request.getfixturevalue(request.param)


@pytest.fixture(
    params=["existing_dataset_as_of_at", "existing_dataset_as_of_from_to"],
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
)
def one_existing_set_for_each_data_type(request):
    """A parameterized fixture returning one saved dataset for each data type."""
    yield request.getfixturevalue(request.param)
