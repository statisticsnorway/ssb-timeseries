import os
import logging
import pytest

from timeseries import config
from timeseries import fs
from timeseries.logging import ts_logger

# BUCKET = config.GCS
# JOVYAN = config.JOVYAN

HOME = os.getenv("HOME")
TIMESERIES_CONFIG = os.getenv("TIMESERIES_CONFIG")

ts_logger.warning("Visible despite pytest captureing logs?!")


def test_init_config_without_params() -> None:
    new_config = config.Config()
    ts_logger.debug(f"Created configuration: {new_config}")
    assert isinstance(new_config, config.Config)
    assert new_config.timeseries_root == config.DEFAULT_TIMESERIES_LOCATION
    assert new_config.log_file == config.DEFAULT_LOG_FILE_LOCATION


def test_init_config_timeseries_in_shared_bucket_logs_in_jovyan_home() -> None:
    new_config = config.Config(
        timeseries_root=config.GCS, log_file=config.DEFAULT_LOG_FILE_LOCATION
    )
    ts_logger.debug(
        f"Created configuration: {new_config} with root {new_config.timeseries_root}"
    )
    assert isinstance(new_config, config.Config)
    assert new_config.timeseries_root == config.GCS
    assert new_config.log_file == config.DEFAULT_LOG_FILE_LOCATION


def test_config_change() -> None:

    cfg_0 = config.Config()
    assert isinstance(cfg_0, config.Config)
    assert cfg_0.timeseries_root == config.DEFAULT_TIMESERIES_LOCATION

    home_series = os.path.join(HOME, "series_data")
    cfg_1 = config.Config(timeseries_root=home_series)
    cfg_1.save()

    cfg_2 = config.Config()
    assert isinstance(cfg_2, config.Config)
    assert cfg_2.timeseries_root == home_series

    # reset to original config
    cfg_0.save()


# @pytest.mark.skipif(
#     not TIMESERIES_CONFIG, reason="No environment variable pointing to configurations."
# )
def test_read_config_from_file() -> None:

    if fs.exists(TIMESERIES_CONFIG):
        ts_logger.debug(
            f"Environment variable TIMESERIES_CONFIG was found: {TIMESERIES_CONFIG}"
        )
        configuration = config.Config(configuration_file=TIMESERIES_CONFIG)
        assert isinstance(configuration, config.Config)
    else:
        new_config = config.Config(timeseries_root=config.GCS)
        ts_logger.debug(
            f"Env variable pointed to non-existing configuration file: {TIMESERIES_CONFIG}. Using {new_config}."
        )
        if isinstance(new_config, config.Config):
            new_config.save(
                timeseries_root=TIMESERIES_CONFIG,
                configuration_file=os.path.join(
                    HOME, ".config", "timeseries", "config.json"
                ),
            )
            # os.environ["HOME"] = TIMESERIES_CONFIG
            ts_logger.info(f"Configuration file: {TIMESERIES_CONFIG} created.")
        else:
            assert False

        config_from_file = TIMESERIES_CONFIG
        assert isinstance(config_from_file, config.Config)


@pytest.mark.skipif(HOME != "/home/bernhard", reason="None of your business.")
def test_fail(caplog):
    caplog.set_level(logging.DEBUG)
    print("print to std out")
    ts_logger.warning("ts_logger.warning: std out")
    assert True


# @pytest.mark.skipif(True, reason="...")
# def test_view_config_() -> None:
#     ts_logger.debug(f"TIMESERIES_CONFIG is defined {TIMESERIES_CONFIG}")

#     configuration = config.Config(configuration_file=TIMESERIES_CONFIG)
#     ts_logger.warning(f"Condfiguration:\n\t{str(configuration)}")
#     assert False


# @pytest.mark.skipif(not GCS_VISIBLE, reason="Can not see GCS.")
# def test_mkdir_dapla() -> None:

#     ts_logger.warning(f"Can see{config.GCS}")
#     a = f"temp-dir-while-running-tests-{uuid.uuid4()}"
#     fs.mkdir(os.path.join(config.GCS, "tests", a, "b", "c"))
#     assert fs.exists(os.path.join(config.GCS, "tests", "a", "b", "c"))


# # @pytest.mark.skipif(IS_DAPLA, reason="... now we are in Kansas!")
# def test_mkdir_local() -> None:
#     a = f"temp-dir-while-running-tests-{uuid.uuid4()}"
#     short_path = os.path.join(HOME, a)
#     if fs.exists(short_path):
#         ts_logger.warning(f"The directory {short_path} already existed!")
#         assert False
#     else:
#         long_path = os.path.join(HOME, a, "b", "c", "d")
#         ts_logger.warning(f"Root: {CONFIG.bucket}")
#         ts_logger.warning(f"Attempting to create local fs directory: {long_path}")
#         fs.mkdir(long_path)

#         assert fs.exists(long_path)
