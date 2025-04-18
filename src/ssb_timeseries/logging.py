"""Time series should make use of standardised messaging and logging.

A key principle is that every dataset read or write operation should be logged with log level INFO. The plan is to take this one step further and add stack inspection to enrich the log messages with process identifiers. In that way, logs would provide lineage at the dataset level. Also, adding a log sink to put the (dataset write) messages on a message queue would allow event based workflow definitions.

These are features that may warrant their own shared Dapla libraries. This module serves as a specification of required features byd separates out code that could go into such a library.
"""

import functools
import logging
from datetime import datetime

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self  # noqa: UP035 #backport to 3.10

from ssb_timeseries.config import Config
from ssb_timeseries.types import F

# nosonar: disable comment
# import uuid
# automatic cloud logging config
# import google.cloud.logging
# client = google.cloud.logging.Client()
# client.setup_logging()

# mypy: disable-error-code="operator, no-untyped-def, return-value, import-untyped, arg-type"
# ruff: noqa: ANN002, ANN003

ts_logger = logging.getLogger("TIMESERIES")
log_string = logging.Formatter(
    "%(name)s | %(levelname)s | %(asctime)s | %(message)s \n"
)
log_json = logging.Formatter(
    '{"name": "%(name)s"; "level": %(levelname)s; "timestamp": %(asctime)s; "message": "%(message)s" }'
)

# nosonar: disable comment
# CONFIGURATION_FILE = str(os.environ.get("TIMESERIES_CONFIG"))
# CONFIG = Config(configuration_file=CONFIGURATION_FILE)
LOG_FILE = str(Config.active().log_file)


file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(log_json)
file_handler.setLevel(logging.INFO)
ts_logger.addHandler(file_handler)

# Also log to console.
console = logging.StreamHandler()
console.setFormatter(log_string)  # BUG: format does not take effect in console?
console.setLevel(logging.WARNING)
ts_logger.addHandler(console)

# nosonar: disable comment
# Google Cloud logging:
# from google.cloud.logging.handlers import CloudLoggingHandler
# cloud_handler = CloudLoggingHandler(client)
# ts_logger.addHandler(cloud_handler)


class EnterExitLog:
    """Class supporting decorator to log on enter and exit."""

    def __init__(self, name: str) -> None:
        """Enter/exit template for workflow process."""
        self.name = name

    def __enter__(self) -> Self:
        """Before each workflow process step, do this."""
        self.init_time = datetime.now()
        ts_logger.info(f"START: {self.name}.")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:  # noqa ANN001
        """After each workflow process step, do this."""
        self.end_time = datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        ts_logger.info(
            f"FINISH: {self.name}. Completed in: {self.elapsed_time} seconds."
        )


def log_start_stop(func: F) -> F:
    """Log start and stop of decorated function."""
    # nosonar  TODO: generalise: pass in functions to enter/exit?

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with EnterExitLog(func.__name__):
            out = func(*args, **kwargs)

        return out

    return wrapper


# nosonar disable comment
# def debug(func):  # ANN001, ANN201
#     """Print the function signature and return value."""

#     @functools.wraps(func)
#     def wrapper_debug(*args, **kwargs):
#         args_repr = [repr(a) for a in args]  # 1
#         kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]  # 2
#         signature = ", ".join(args_repr + kwargs_repr)  # 3
#         print(f"Calling {func.__name__}({signature})")
#         value = func(*args, **kwargs)
#         print(f"{func.__name__!r} returned {value!r}")  # 4
#         return value
#     return wrapper_debug
# """
#
# # @wraps??
# class Timer:
#     def __init__(self, name: str):
#         self.name: str = name
#
#     def __enter__(self):
#         self.init_time = datetime.now()
#         ts_logger.info("Started: {self.name}.")
#         return self
#
#     def __exit__(self, type, value, tb):
#         self.end_time = datetime.now()
#         self.elapsed_time = self.end_time - self.init_time
#         ts_logger.info("Finished: {self.name} in: {self.elapsed_time} seconds.")
#
# def funcion_timer(func) -> Callable[..., Function]:
#     @functools.wraps(func)
#     def wrapper(*args, **kwargs) -> Function:
#         with Timer(func.__name__):
#             return func(*args, **kwargs)
#
#     return wrapper
# """
