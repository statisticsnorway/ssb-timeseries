"""Tests that logging (or not) is consistent with configurations.

Also, in tests_with_logging.logger.warning() should create proper warnings, ie warnings.warn(), unless explicitly ignored.
The latter guards against releasing with (log) warnings intended for debugging.
(While relatively harmless when occurring elsewhere,
such messages can create lots of visual noise in a Jupyter environment.)
"""

# import importlib
import logging
import warnings
from collections.abc import Generator
from copy import deepcopy
from typing import Any

import pytest

from ssb_timeseries.logging import set_up_logging_according_to_config

from .conftest import TEST_LOGGER
from .conftest import LogWarning
from .conftest import LogWarningFilter

# mypy: ignore-errors


####################### FIXTURES TO ENSURE RIGHT CONFIG ########################
class TestSetup:
    """Package logger and configuration for each test case."""

    __test__: bool = False
    name: str = TEST_LOGGER
    logger: logging.Logger | None = None
    configuration: dict[str, Any] | None = None

    def __init__(
        self, name: str, logger: logging.Logger, config: dict[str, Any]
    ) -> None:
        """Init."""
        self.name = name
        self.logger = logger
        self.configuration = config


@pytest.fixture(scope="function", autouse=True)
def ts_with_logging(buildup_and_teardown) -> Generator:
    """For test cases WITH logging, we can use the default test configurations as is."""
    log_config_before = buildup_and_teardown.logging
    cfg = deepcopy(log_config_before)
    logger_name = f"{TEST_LOGGER}.with_loggging"
    # logger_name = TEST_LOGGER
    assert cfg != {}
    cfg["loggers"][logger_name] = cfg["loggers"].pop(TEST_LOGGER)
    logger = set_up_logging_according_to_config(logger_name, cfg)
    logger.addFilter(LogWarningFilter())
    setup = TestSetup(logger_name, logger, cfg)
    yield setup
    logger = set_up_logging_according_to_config(TEST_LOGGER, log_config_before)
    buildup_and_teardown.save()


@pytest.fixture(scope="function", autouse=True)
def ts_without_logging(buildup_and_teardown) -> Generator:
    """For test cases WITHOUT logging, we ned to empty logging configurations and remove any previously added loggers."""
    log_config_before = deepcopy(buildup_and_teardown.logging)
    logger_name = f"{TEST_LOGGER}.without_loggging"
    # logger_name = TEST_LOGGER
    logger = set_up_logging_according_to_config(logger_name, {})
    setup = TestSetup(logger_name, logger, {})
    yield setup
    logger = set_up_logging_according_to_config(TEST_LOGGER, log_config_before)
    buildup_and_teardown.save()


#################################### TESTS #####################################


def test_logging_configuration_default_log_level_is_info(
    caplog: pytest.LogCaptureFixture,
    ts_with_logging: TestSetup,
) -> None:
    ts_with_logging.logger.info(ts_with_logging.name)
    assert ts_with_logging.logger.level == 20


def test_logging_configuration_is_specified_by_dict_config(
    caplog: pytest.LogCaptureFixture,
    ts_with_logging: TestSetup,
) -> None:
    dict_cfg = ts_with_logging.configuration
    is_valid_dict_config = isinstance(dict_cfg, dict) and dict_cfg.get("version", None)
    assert is_valid_dict_config
    # how to check? ... the above is necessary, but not sufficient for a valid dictConfig


def test_logging_configuration_log_handlers_are_defined(
    caplog: pytest.LogCaptureFixture,
    ts_with_logging: TestSetup,
) -> None:
    assert len(ts_with_logging.logger.handlers) > 1
    assert ts_with_logging.logger.hasHandlers()


def test_no_logging_configuration_is_empty_and_logger_has_nullhandler_only(
    caplog: pytest.LogCaptureFixture,
    ts_without_logging: TestSetup,
) -> None:
    assert ts_without_logging.configuration == {}
    assert len(ts_without_logging.logger.handlers) == 1
    assert isinstance(ts_without_logging.logger.handlers[0], logging.NullHandler)


@pytest.mark.filterwarnings(
    "ignore"
)  # .warning here should not generate warning or error
def test_no_configured_logging_does_not_log(
    caplog: pytest.LogCaptureFixture,
    ts_without_logging: TestSetup,
) -> None:
    message = "This message SHOULD NOT be found in captured logs."
    ts_without_logging.logger.warning(message)
    assert message not in caplog.text


@pytest.mark.xfail  #'test is broken, but std out looks right!?!' (ok for logger = ssb_timeseries)
@pytest.mark.filterwarnings("ignore")
@pytest.mark.parametrize(
    "level,message,expected",
    [
        (10, "DEBUG messages ARE NOT logged per defaults", False),
        (20, "INFO messages ARE logged per (library) defaults", True),
        (30, "WARNING messages ARE logged by Python defaults", True),
    ],
)
def test_configured_logging_logs_at_level_info_and_above(
    caplog: pytest.LogCaptureFixture,
    capsys,
    ts_with_logging: TestSetup,
    level,
    message,
    expected,
) -> None:
    caplog.clear()
    ts_with_logging.logger.log(level, message)
    stdout = "\n".join(capsys.readouterr().out) + "\n".join(capsys.readouterr().err)
    captured = (
        caplog.text
    ) + stdout  # caplog.text SHOULD suffice, but something goes wrong. Workaround with this?
    print(ts_with_logging.name)
    print(ts_with_logging.configuration)
    print(ts_with_logging.logger.__dict__)
    print(stdout)
    assert (message in captured) == expected


@pytest.mark.filterwarnings(
    "error"
)  # superfluous after logger.warning(...) --> warnings.warn(...)
def test_capture_warnings(
    ts_with_logging,
) -> None:
    with pytest.raises(UserWarning):
        ts_with_logging.logger.warning(
            "this warning message should be turned into a warning",
        )
        warnings.warn(
            UserWarning("if the above does not create a warning, this will"),
            stacklevel=2,
        )


def test_logged_warning_should_generate_real_warning(
    caplog: pytest.LogCaptureFixture,
    ts_with_logging,
) -> None:
    """Conftest buildup_and_teardown adds a log handler that create real warnings from logged warnings, in order to capture them with `pytest -W error`.

    This test aims to verify that this mechanism works.
    """
    with pytest.warns(LogWarning):
        ts_with_logging.logger.warning(
            "this log message should be turned into a warning"
        )
        assert "so we should never get here" == "so we should never get here"
