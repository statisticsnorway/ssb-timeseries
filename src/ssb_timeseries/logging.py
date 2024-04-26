# import uuid
# automatic cloud logging config
# import google.cloud.logging
# client = google.cloud.logging.Client()
# client.setup_logging()

import functools
import logging

import os

from datetime import datetime
from timeseries import config

ts_logger = logging.getLogger("TIMESERIES")
log_string = logging.Formatter("%(name)s | %(levelname)s | %(asctime)s | %(message)s")
log_json = logging.Formatter(
    '{"name": "%(name)s"; "level": %(levelname)s; "timestamp": %(asctime)s; "message": "%(message)s" }'
)

TIMESERIES_CONFIG: str = os.environ.get("TIMESERIES_CONFIG")
CONFIG = config.Config(configuration_file=TIMESERIES_CONFIG)

file_handler = logging.FileHandler(CONFIG.log_file)
file_handler.setFormatter(log_string)
file_handler.setLevel(logging.INFO)
ts_logger.addHandler(file_handler)

# Also log to console.
console = logging.StreamHandler()
console.setFormatter(log_string)
console.setLevel(logging.WARNING)
ts_logger.addHandler(console)

# Google Cloud logging:
# from google.cloud.logging.handlers import CloudLoggingHandler
# cloud_handler = CloudLoggingHandler(client)
# ts_logger.addHandler(cloud_handler)


# def warn(message: str) -> None:
#     ts_logger.warning(message)
#     # print(message)


# def info(message: str) -> None:
#     ts_logger.info(message)
#     # print(message)


# def debug(message: str) -> None:
#     ts_logger.debug(message)
#     # print(message)


class EnterExitLog:
    def __init__(self, name: str):
        self.name = name

    def __enter__(self):
        self.init_time = datetime.now()
        ts_logger.info(f"START: {self.name}.")
        return self

    def __exit__(self, type, value, tb):
        self.end_time = datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        ts_logger.info(
            f"FINISH: {self.name}. Completed in: {self.elapsed_time} seconds."
        )


def log_start_stop(func):
    """log start and stop of decorated function"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with EnterExitLog(func.__name__):
            out = func(*args, **kwargs)

        return out

    return wrapper


def debug(func):
    """Print the function signature and return value"""

    @functools.wraps(func)
    def wrapper_debug(*args, **kwargs):
        args_repr = [repr(a) for a in args]  # 1
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]  # 2
        signature = ", ".join(args_repr + kwargs_repr)  # 3
        print(f"Calling {func.__name__}({signature})")
        value = func(*args, **kwargs)
        print(f"{func.__name__!r} returned {value!r}")  # 4
        return value

    return wrapper_debug


"""

# @wraps??
class Timer:
    def __init__(self, name: str):
        self.name: str = name

    def __enter__(self):
        self.init_time = datetime.now()
        ts_logger.info("Started: {self.name}.")
        return self

    def __exit__(self, type, value, tb):
        self.end_time = datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        ts_logger.info("Finished: {self.name} in: {self.elapsed_time} seconds.")


def funcion_timer(func) -> Callable[..., Function]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Function:
        with Timer(func.__name__):
            return func(*args, **kwargs)

    return wrapper


"""
