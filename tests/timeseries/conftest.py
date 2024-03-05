import os

# from typing import Generator
import pytest

# import logging

from timeseries import config

# from timeseries import fs
# from timeseries.logging import ts_logger

# HOME = os.getenv("HOME")
# TIMESERIES_CONFIG = os.getenv("TIMESERIES_CONFIG")


@pytest.fixture(scope="function", autouse=True)
def remember_config():
    """
    A fixture to make sure that running tests do not change the configuration file.
    """
    print("before tests")
    config_file = os.getenv("TIMESERIES_CONFIG")
    configuration = config.Config(configuration_file=config_file)
    print(configuration)

    # tests run here
    yield

    print("after tests")
    configuration.save(config_file)


# @pytest.fixture(scope="module", autouse=True)
# def print_stuff():
#     """
#     Just testing.
#     """
#     print("before test module")
#     yield
#     print("after test modules")
