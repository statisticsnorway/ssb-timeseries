import os

# from typing import Generator
import pytest

# import logging

from timeseries import config

# from timeseries import fs
# from timeseries.logging import ts_logger

# HOME = os.getenv("HOME")
# TIMESERIES_CONFIG = os.getenv("TIMESERIES_CONFIG")


@pytest.fixture(scope="function", autouse=False)
def remember_config():
    """
    A fixture to make sure that running tests do not change the configuration file.
    """
    config_file = os.getenv("TIMESERIES_CONFIG")
    if config_file:
        configuration = config.Config(configuration_file=config_file)
        print(f"Before tests, reading configuration: {configuration}")

        # tests run here
        yield

        print(f"Config after tests:\n{config.Config()}")
        print(f"After tests, reset config to:\n{configuration}")
        configuration.save(config_file)


@pytest.fixture(scope="module", autouse=True)
def print_stuff():
    """
    Just testing pytest.fixtures.
    """
    print("Before test module")
    yield
    print("After test modules")
