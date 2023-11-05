# import uuid
import datetime
from pyclbr import Function
from typing import Callable
from logging import Logger as log


def warn(message: str) -> None:
    # log.warn(message)
    print(message)


def info(message: str) -> None:
    # log.info(message)
    print(message)


def debug(message: str) -> None:
    # log.debug(message)
    print(message)


class EnterExitLog:
    def __init__(self, name: str):
        self.name: str = name

    def __enter__(self):
        self.init_time = datetime.datetime.now()
        log.info("Finished: {self.name}.")
        return self

    def __exit__(self, type, value, tb):
        self.end_time = datetime.datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        log.info("Finished: {self.name} in: {self.elapsed_time} seconds.")


def log_start_stop(func) -> Callable[..., Function]:
    def func_wrapper(*args, **kwargs) -> Function:
        with EnterExitLog(func.__name__):
            return func(*args, **kwargs)

    return func_wrapper


class Timer:
    def __init__(self, name: str):
        self.name: str = name

    def __enter__(self):
        self.init_time = datetime.datetime.now()
        print("Finished: {self.name}.")
        return self

    def __exit__(self, type, value, tb):
        self.end_time = datetime.datetime.now()
        self.elapsed_time = self.end_time - self.init_time
        print("Finished: {self.name} in: {self.elapsed_time} seconds.")


def funcion_timer(func) -> Callable[..., Function]:
    def func_wrapper(*args, **kwargs) -> Function:
        with EnterExitLog(func.__name__):
            return func(*args, **kwargs)

    return func_wrapper
