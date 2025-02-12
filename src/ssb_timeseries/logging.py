"""Logging for time series library.

The library is created to be used in a context where every dataset read or write operation should be not only logged, but also generate events to be picked up by workflow control.

The logging module seeks to balance that with the general recommendation that code libraries leave it up to the applications using them to make decisions on logging; https://docs.python.org/3.10/howto/logging.html#library-config.
Since the library requires proper configuration anyway, the natural solution is to let the configuration file control the logging behaviour as well.

If both `logging` and `log_file` configuration fields are empty, logging should be disabled.

TO DO: Implement logging.dictConfig use of field `logging` from the timeseries configuration. If `logging` configurations does not identify loggers with active log handlers, logging should be disabled.

In the early PoC implementation the libary relies on a top level `log_file` field in the configuration and sets up a FileHandler and StreamHandler for logging to console. This behaviour is kept untill the dictConfig implementation is ready.

TO DO: Support easy set up of a 'workflow' log handler for log level INFO, so that proper configuration is all it takes to put these log entries onto a queue. (A good default configuration may be all it takes, assuming and existing queue?)

TODO: The log messages should include process identifiers that live at higher levels (in the scope of the calling code).
The time series library may be able to get this information by stack inspection. to enrich the log messages with process identifiers.
In that way, logs would provide lineage at the dataset level.

This module serves as a specification of required features by separates out code that could go into such a library.

"""

# Logging could/should(?) have its own Dapla library.
# consider if "everything" could/should be done in ssb_timeseries/__init__.py?

import functools
import logging
from datetime import datetime

from typing_extensions import Self

# from ssb_timeseries.config import Config
import ssb_timeseries as ts
from ssb_timeseries.types import F

# nosonar: disable comment
# import uuid

# mypy: disable-error-code="operator, no-untyped-def, return-value, import-untyped, arg-type"
# ruff: noqa: ANN002, ANN003

# ts.configuration:Config = Config.active()
# """On initial import, load configuration."""


_logger = logging.getLogger(__name__)
LOG_FILE = str(ts.configuration.log_file)
LOGGER_NAME = getattr(ts.configuration, "logger", "TIMESERIES")
if LOGGER_NAME:
    LOGGER = logging.getLogger(LOGGER_NAME)
else:
    LOGGER = logging.getLogger(__name__)

# use dictConfig instead? ==========================================
if LOG_FILE:
    # consider if this can be omitted?
    file_handler = logging.FileHandler(LOG_FILE)
    # file_handler.setFormatter(log_json)
    file_handler.setLevel(logging.INFO)
    _logger.addHandler(file_handler)

# unless truthy config evaluates to false ', log to console
if getattr(ts.configuration, "log_to_console", True):
    console = logging.StreamHandler()
    # BUG: format does not take effect in console? or just pytest behaviour that it did not?
    # console.setFormatter( log_string)
    console.setLevel(logging.INFO)
    _logger.addHandler(console)


# suggested convenience functionality intended for processes of the statistics production
# ... should be moved out of timeseries library
class EnterExitLog:
    """Class supporting decorator to log on enter and exit."""

    def __init__(self, name: str) -> None:
        """Enter/exit template for workflow process."""
        self.name = name

    def __enter__(self) -> Self:
        """Before each workflow process step, do this."""
        self.init_time = datetime.now()
        _logger.info(f"START: {self.name}.")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:  # noqa ANN001
        """After each workflow process step, do this."""
        self.end_time = datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        _logger.info(f"FINISH: {self.name}. Completed in: {self.elapsed_time} seconds.")


def log_start_stop(func: F) -> F:
    """Log start and stop of decorated function."""
    # nosonar  TODO: generalise: pass in functions to enter/exit?

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with EnterExitLog(func.__name__):
            out = func(*args, **kwargs)

        return out

    return wrapper


# belongs in config or outside timeseries library. ===============================
# ... turn into reasonable defaults?

# log_string = logging.Formatter(
#     "%(name)s | %(levelname)s | %(asctime)s | %(message)s \n"
# )
# log_json = logging.Formatter(
#     '{"name": "%(name)s"; "level": %(levelname)s; "timestamp": %(asctime)s; "message": "%(message)s" }'
# )

# automatic cloud logging config
# import google.cloud.logging
# client = google.cloud.logging.Client()
# client.setup_logging()
# ts_logger = LOGGER
