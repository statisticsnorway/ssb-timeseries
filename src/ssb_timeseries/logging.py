"""Logging for time series library.

The library is created to be used in a context where every dataset read or write operation should not only be logged, but also generate events to be picked up by centralised workflow control.
In some system contexts this is handled by a backend database or API.
However, the timeseries libary is designed to be possible to use with only a file system or object storage as backend.
Since in that case the library is closer to the reads and writes than the application(s) using it, and hence may be better positioned to take the responsibility for consistent logging.
(There is less opportunity for somethuing else to fail before logging takes place.)

This concern should be balanced against the general recommendation that code libraries leave it up to the applications using them to make decisions on logging; https://docs.python.org/3.10/howto/logging.html#library-config.
Since the library requires proper configuration anyway, the natural solution is to let the configuration file control the logging behaviour as well.

If the configuration contains a non empty `log_file` field, logs are written there and to console.
If instead a field `logging`is specified, it takes presedence, and is used with `logging.config.dictConfig`.
If neither is specified, or dictConfig does not specify any log handlers, logging should be disabled.

TO DO: Add support for a 'workflow' log handler that puts all entries with log level INFO onto a queue. (A default logging configuration may be sufficient?)

TODO: Logs should provide lineage at the dataset level, identifying which named processes reads and writes data.
That requires log messages to include process names or identifiers that are determined at higher levels.
The time series library may be able to get this information (in the scope of the calling code) by stack inspection. Otherwise, it must be passed in as parameters to read/write functions.
"""

# Consider whether:
#  - logging could/should(?) have its own Dapla library.
#  - "everything" could/should be done in ssb_timeseries/__init__.py?

import functools
import logging
from collections.abc import Callable
from datetime import datetime

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self  # noqa: UP035 #backport to 3.10

import ssb_timeseries as ts

# replaced:
# from ssb_timeseries.config import Config
# LOG_CONFIG = Config.active()
# """On initial import, load configuration."""

# nosonar: disable comment
# import uuid

# mypy: disable-error-code="operator, no-untyped-def, return-value, import-untyped, arg-type"
# ruff: noqa: ANN002, ANN003


LOG_CONFIG = getattr(ts.configuration, "logging", {})
LOG_FILE = str(getattr(ts.configuration, "log_file", ""))

LOGGER = logging.getLogger(getattr(LOG_CONFIG, "logger", __name__))
"""The logger name can be configured `logging.logger = '<name>'`"""

if LOG_CONFIG:
    # use dictConfig instead
    logging.config.dictConfig(LOG_CONFIG)
elif LOG_FILE:
    # consider if this can be omitted?
    file_handler = logging.FileHandler(LOG_FILE)
    log_json = logging.Formatter(
        '{"name": "%(name)s"; "level": %(levelname)s; "timestamp": %(asctime)s; "message": "%(message)s" }'
    )
    file_handler.setFormatter(log_json)
    file_handler.setLevel(logging.INFO)
    LOGGER.addHandler(file_handler)

    # unless truthy config evaluates to false ', log to console
    # if getattr(LOG_CONFIG, "log_to_console", True):
    console = logging.StreamHandler()
    log_string = logging.Formatter(
        "%(name)s | %(levelname)s | %(asctime)s | %(message)s \n"
    )
    # BUG: format does not take effect in console? or just pytest behaviour that it did not?
    console.setFormatter(log_string)
    console.setLevel(logging.INFO)
    LOGGER.addHandler(console)
else:
    LOGGER.addHandler(logging.NullHandler)


# suggested convenience functionality intended for processes of the statistics production
# ... should be moved out of timeseries library?
class EnterExitLog:
    """Class supporting decorator to log on enter and exit."""

    def __init__(self, name: str) -> None:
        """Enter/exit template for workflow process."""
        self.name = name

    def __enter__(self) -> Self:
        """Before each workflow process step, do this."""
        self.init_time = datetime.now()
        LOGGER.info(f"START: {self.name}.")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:  # noqa ANN001
        """After each workflow process step, do this."""
        self.end_time = datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        LOGGER.info(f"FINISH: {self.name}. Completed in: {self.elapsed_time} seconds.")


def log_start_stop(func: Callable) -> Callable:
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
