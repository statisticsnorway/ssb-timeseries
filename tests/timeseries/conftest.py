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
        print(
            f"Because TIMESERIES_CONFIG identifies a config file, before tests, read configuration: {configuration}"
        )

    # tests run here
    yield

    if config_file and os.path.isfile(config_file):
        print(
            f"To make sure the tests have not altered configurations:\n{config.Config()}"
        )
        print(f"revert to what we read above:\n{configuration}")
        configuration.save(config_file)


@pytest.fixture(scope="module", autouse=True)
def print_stuff():
    """
    Just testing pytest.fixtures.
    """
    print("Before test module")
    yield
    print("After test modules")
