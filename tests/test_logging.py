"""Tests that logging (or not) is consistent with configurations.

Also, in tests logger.warning() should create proper warnings, ie warnings.warn(), unless explicitly ignored.
The latter guards against releasing with (log) warnings intended for debugging.
(While relatively harmless when occurring elsewhere,
such messages can create lots of visual noise in a Jupyter environment.)
"""

import warnings
from typing import TypeAlias

import pytest

import ssb_timeseries as ts_logging

from .conftest import LogWarning

Cfg: TypeAlias = type(ts_logging.configuration)


@pytest.fixture(scope="function", autouse=True)
def no_logging(buildup_and_teardown: Cfg) -> None:
    import ssb_timeseries as ts_no_logging

    cfg = buildup_and_teardown
    log_file = getattr(ts_logging.configuration, "log_file", "")
    dict_cfg = getattr(ts_logging.configuration, "logging", {})

    if log_file:
        cfg.log_file = ""

    if dict_cfg:
        cfg.logging = {}

    cfg.save()
    yield ts_no_logging


@pytest.mark.filterwarnings(
    "ignore"
)  # superfluous before logger.warning(...) --> warnings.warn(...)
def test_log_level_behaviour(caplog: pytest.LogCaptureFixture) -> None:
    log_file = getattr(ts_logging.configuration, "log_file", "")
    dict_cfg = getattr(ts_logging.configuration, "logging", {})

    ts_logging.logger.debug(
        "configuration.log_file: %s and .logging: %s",
        log_file,
        dict_cfg,
    )
    ts_logging.logger.debug("debug messages SHOULD NOT be visible by default")
    ts_logging.logger.info("info messages SHOULD be visible per library defaults")
    ts_logging.logger.warning("warnings SHOULD be visible too")

    assert log_file or dict_cfg  # otherwise we test the wrong thing
    assert "DEBUG" not in caplog.text
    # assert "INFO" in caplog.text
    assert "WARNING" in caplog.text


@pytest.mark.filterwarnings(
    "ignore"
)  # superfluous before logger.warning(...) --> warnings.warn(...)
def test_configured_logging_logs(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    ts_logging.logger.warning("#&#%¤#!!")
    assert "#&#%¤#!!" in caplog.text


@pytest.mark.filterwarnings(
    "ignore"
)  # superfluous before logger.warning(...) --> warnings.warn(...), meaningful after.
def test_no_configured_logging_does_not_log(
    caplog: pytest.LogCaptureFixture,
    no_logging: Cfg,
) -> None:
    caplog.set_level("DEBUG")
    ts_logging.logger.warning("warning message should be supressed ", stacklevel=2)
    assert "WARNING" in caplog.text


@pytest.mark.filterwarnings(
    "error"
)  # superfluous after logger.warning(...) --> warnings.warn(...)
def test_capture_warnings() -> None:
    with pytest.raises(UserWarning):
        ts_logging.logger.warning(
            "warning message should (eventually) be turned into a warning"
        )
        warnings.warn(
            UserWarning("if the above does not creaate a warning, this will"),
            stacklevel=2,
        )


@pytest.mark.xfail
def test_logged_warning_should_generate_real_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Not quite there yet.

    Idea: (in conftest module or session fixture?) create real warnings from log messages with log level warning,
    across ALL tests,
    so that they are captured with `pytest -W error`
    This test attempts to verify that the above mechanism works.

    DOES NOT WORK (YET?) ... why not?
    """
    with pytest.warns(LogWarning):
        ts_logging.logger.warning(
            "this log message should be turned into a proper warning"
        )
        assert "WARNING" in caplog.text
