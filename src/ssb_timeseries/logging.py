"""Logging for time series library.

Logging every dataset read and write operation is useful for process monitoring and provides set level data lineage.
Writes are natural drivers for an automated event based workflow.

It depends on the system context which role the time series library should play.
Workflow control and monitoring should generally be managed by other components,
but the time series system should play nicely with whatever these might be.
In practical terms, that boils down to structured logging.
That in turn allow generating events from the log information.
In some system setups a backend database or an API could take care of logging and event generation.
However, the timeseries library could also be used with only a file system or object storage as backend.
In that case, the library is possibly the most central point close to the reads and writes.

Since the library requires proper configuration anyway, the natural solution is to let the configuration control the logging behaviour as well.
This also leave it up to the application to make decisions on logging;
in line with the general recommendation for code libraries, https://docs.python.org/3.10/howto/logging.html#library-config.

Legacy: If the configuration contains a non empty `log_file` field, logs are written there (and to console).

If `logging` field is specified, it takes presedence, and is used with `logging.config.dictConfig`.
If both are empty, or dictConfig does not specify any log handlers, logging should be disabled.

TO DO: Add support for a 'workflow' log handler that puts all entries with log level INFO onto a queue.
(A default/option for logging configuration may be sufficient?)

TODO: Logs should provide lineage at the dataset level, identifying which named processes reads and writes data.
That requires log messages to include process names, UUIDs or other identifiers.
The time series library may be able to get this information (from the scope of the calling code) by stack inspection.
Otherwise, it must be passed in as parameters to read/write functions.
"""

# Consider whether:
#  - logging could/should(?) have its own Dapla library.
#  - "everything" could/should be done in ssb_timeseries/__init__.py?

import functools
import logging
from collections.abc import Callable
from datetime import datetime
from logging.config import dictConfig
from typing import Any

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self  # noqa: UP035 #backport to 3.10

# import ssb_timeseries as ts

# mypy: disable-error-code="operator, no-untyped-def, return-value, import-untyped, arg-type"
# ruff: noqa: ANN002, ANN003

_STRING_FORMAT: str = "%(name)s | %(levelname)s | %(asctime)s | %(message)s \n"
_JSON_FORMAT: str = '{"name": "%(name)s"; "level": %(levelname)s; "timestamp": %(asctime)s; "message": "%(message)s" }'


def console_handler(
    format_string: str = _STRING_FORMAT,
) -> logging.StreamHandler:
    """Create handler for logging to console."""
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter(format_string))
    log_handler.setLevel("INFO")
    return log_handler


def file_handler(file: str, format_string: str = _JSON_FORMAT) -> logging.FileHandler:
    """Create handler for logging to file."""
    log_handler = logging.FileHandler(file)
    log_handler.setFormatter(logging.Formatter(format_string))
    log_handler.setLevel("INFO")
    return log_handler


# def coogle_cloud_log_handler():
# automatic cloud logging config
# import google.cloud.logging
# client = google.cloud.logging.Client()
# client.setup_logging()

# https://docs.python.org/3/howto/logging-cookbook.html#filters-contextual
# Consider custom adapter for adding context?


# suggested convenience functionality intended for processes of the statistics production
# ... should be moved out of timeseries library?
class EnterExitLog:
    """Class supporting decorator to log on enter and exit."""

    def __init__(self, name: str, logger: logging.Logger) -> None:
        """Enter/exit template for workflow process."""
        self.name = name

    def __enter__(self) -> Self:
        """Before each workflow process step, do this."""
        self.init_time = datetime.now()
        logger.info(f"START: {self.name}.")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:  # noqa ANN001
        """After each workflow process step, do this."""
        self.end_time = datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        if not exc_type:
            logger.info(
                "FINISH: %s completed successfully. Execution time: %s seconds.",
                self.name,
                self.elapsed_time,
            )
        else:
            logger.error(
                "FAILED: %s failed. Execution time: %s seconds.",
                self.name,
                self.elapsed_time,
            )


def log_start_stop(func: Callable) -> Callable:
    """Log start and stop of decorated function."""
    # nosonar  TODO: generalise: pass in functions to enter/exit?

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        name = kwargs.pop("logger", __package__)
        logger = logging.getLogger(name)
        with EnterExitLog(name=func.__name__, logger=logger):
            out = func(*args, **kwargs)

        return out

    return wrapper


class LoggerNameNotDefined(Exception):
    """The provided logger name is not defined in the accompanying configuration."""

    ...


def set_up_logging_according_to_config(
    name: str,
    log_config: dict[str, Any],
) -> logging.Logger:
    """Set up logging according to configuration.

    Normally, this happens only in :py:mod:`ssb_timeseries.__init__.py`, which is run when the package is first imported.
    (For later imports, a cached instance will be retrieved.)
    """
    logger = logging.getLogger(name)
    # loop instead of  logger.handlers.clear() is more robust?
    # (suggested as fix for ResourceWarnings)
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    logger.propagate = False
    logger.setLevel("INFO")

    if isinstance(log_config, dict) and "version" in log_config.keys():
        if name not in log_config["loggers"]:
            raise LoggerNameNotDefined(f"Logger '{name}' not defined in {log_config}")

        dictConfig(log_config)
        logger = logging.getLogger(name)
    else:
        # if log_config == {}: #TODO: check what happens for valid dictConfig if all handlers are disabled
        logger.addHandler(logging.NullHandler())

        # dictConfig(log_config)
        # legacy set up
        # logger.addHandler(console_handler())
        # logger.addHandler(file_handler(log_file))

    return logging.getLogger(name)


if __name__ == "__main__":
    ...
else:
    logger = logging.getLogger(__package__)
    # from ssb_timeseries.config import CONFIG
    # logger = set_up_logging_according_to_config(__package__,CONFIG)
