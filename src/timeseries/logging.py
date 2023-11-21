# import uuid

# automatic cloud logging config
# import google.cloud.logging
# client = google.cloud.logging.Client()
# client.setup_logging()

from functools import wraps
import datetime
from pyclbr import Function
from typing import Callable

import logging
import os

ts_logger = logging.getLogger("TIMESERIES")
log_string = logging.Formatter("%(name)s | %(levelname)s | %(asctime)s | %(message)s")
log_json = logging.Formatter(
    '{"level": %(levelname)s; "timestamp": %(asctime)s; "message": "%(message)s" "name": "%(name)s" }'
)

# log to file
LOG_LOCATION: str = os.environ.get("TIMESERIES_ROOT", "/home/jovyan/sample-data")
file_handler = logging.FileHandler(f"{LOG_LOCATION}/timeseries.log")
file_handler.setFormatter(log_string)
file_handler.setLevel(logging.DEBUG)
ts_logger.addHandler(file_handler)

# Also log to console.
console = logging.StreamHandler()
console.setFormatter(log_string)
# console.setLevel(logging.INFO)
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
        self.name: str = name

    def __enter__(self):
        self.init_time = datetime.datetime.now()
        ts_logger.info("Finished: {self.name}.")
        return self

    def __exit__(self, type, value, tb):
        self.end_time = datetime.datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        ts_logger.info("Finished: {self.name} in: {self.elapsed_time} seconds.")


@wraps
def log_start_stop(func) -> Callable[..., Function]:
    def func_wrapper(*args, **kwargs) -> Function:
        with EnterExitLog(func.__name__):
            return func(*args, **kwargs)

    return func_wrapper


# @wraps??
class Timer:
    def __init__(self, name: str):
        self.name: str = name

    def __enter__(self):
        self.init_time = datetime.datetime.now()
        ts_logger.info("Started: {self.name}.")
        return self

    def __exit__(self, type, value, tb):
        self.end_time = datetime.datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        ts_logger.info("Finished: {self.name} in: {self.elapsed_time} seconds.")


@wraps
def funcion_timer(func) -> Callable[..., Function]:
    def func_wrapper(*args, **kwargs) -> Function:
        with EnterExitLog(func.__name__):
            return func(*args, **kwargs)

    return func_wrapper
