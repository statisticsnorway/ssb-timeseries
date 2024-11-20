import logging
import uuid

import pytest

from ssb_timeseries import config
from ssb_timeseries import fs
from ssb_timeseries.logging import ts_logger
from tests.conftest import Helpers

# NOSONAR
# mypy: disable-error-code="no-untyped-def"

CONFIGURATION_FILE = config.CONFIGURATION_FILE
DEFAULT_TS_ROOT = config.DEFAULTS["timeseries_root"]


@pytest.fixture(scope="function", autouse=True)
def reset_config_after():
    cfg_file = config.CONFIGURATION_FILE
    remembered_config = config.Config(configuration_file=cfg_file)
    config.CONFIG = remembered_config
    yield config.CONFIG
    remembered_config.save(cfg_file)


def test_env_var_specifying_config_file_path(reset_config_after: config.Config) -> None:
    # neither approach works during tests?
    # env_attempt_1 = os.getenv("TIMESERIES_CONFIG", "")
    # if env_attempt_1:
    #     cfg_file = env_attempt_1
    # env_attempt_2 = os.environ["TIMESERIES_CONFIG"]
    # elif env_attempt_2:
    #     cfg_file = env_attempt_2
    # else:
    #
    cfg_file = CONFIGURATION_FILE

    assert cfg_file != ""
    assert cfg_file == CONFIGURATION_FILE
    # ... it seems that config.CONFIGURATION_FILE is more reliable than setting of env variables?


def test_config_file_exists() -> None:
    config_file = config.Config().configuration_file
    assert fs.exists(config_file)


def test_init_config_without_params(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    new_config = config.Config(preset="default")
    ts_logger.debug(f"Created configuration: {new_config}")
    assert isinstance(new_config, config.Config)
    for key in config.DEFAULTS.keys():
        assert new_config[key] == config.DEFAULTS[key]


def test_init_config_timeseries_in_shared_bucket_logs_in_jovyan_home(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    new_config = config.Config(
        timeseries_root=config.GCS, log_file=config.DEFAULTS["log_file"]
    )
    ts_logger.debug(
        f"Created configuration: {new_config} with root {new_config.timeseries_root}"
    )
    assert isinstance(new_config, config.Config)
    assert new_config.timeseries_root == config.GCS
    assert new_config.log_file == config.DEFAULTS["log_file"]


def test_config_defaults(reset_config_after: config.Config) -> None:
    # all of these should result in the same information, but in different objects
    cfg_0 = config.Config(preset="default")
    cfg_0.save()  # for the rare occasions when the default location changes
    cfg_1 = config.Config(configuration_file=cfg_0["configuration_file"])
    cfg_2 = config.Config(
        configuration_file=cfg_0["configuration_file"],
        preset="defaults",
    )

    assert id(cfg_0) != id(cfg_1) != id(cfg_2)
    assert cfg_0 == cfg_1 == cfg_2
    assert cfg_0.timeseries_root == cfg_1.timeseries_root == cfg_2.timeseries_root


def test_config_change(reset_config_after: config.Config) -> None:
    old = config.CONFIG
    if old.timeseries_root == config.JOVYAN:
        old.timeseries_root = config.GCS
    else:
        old.timeseries_root = config.JOVYAN
    old.save()
    new = config.Config(configuration_file=old.configuration_file)
    # we should have both a new object (a new id) and a new path for timeseries_root
    assert id(new) != id(old)
    assert new.timeseries_root != old.timeseries_root


def test_read_config_from_file() -> None:
    if fs.exists(CONFIGURATION_FILE):
        ts_logger.debug(
            f"Environment variable TIMESERIES_CONFIG was found: {CONFIGURATION_FILE}"
        )
        configuration = config.Config(configuration_file=CONFIGURATION_FILE)
        assert isinstance(configuration, config.Config)
    else:
        new_config = config.Config(timeseries_root=config.GCS)
        ts_logger.debug(
            f"Env variable pointed to non-existing configuration file: {CONFIGURATION_FILE}. Using {new_config}."
        )
        if isinstance(new_config, config.Config):
            new_config.save(path=CONFIGURATION_FILE)
            ts_logger.warning(
                f"Configuration file did not exist: {CONFIGURATION_FILE}. Created."
            )

        try_again = config.Config(configuration_file=CONFIGURATION_FILE)
        assert isinstance(try_again, config.Config)


def test_read_config_from_missing_json_file(
    caplog: pytest.LogCaptureFixture,
    reset_config_after: config.Config,
    conftest: Helpers,
) -> None:
    caplog.set_level(
        logging.DEBUG
    )  # setup: point to a config that does not exist (this should create the .json file):
    test_dir = config.CONFIG.bucket
    tmp_config = fs.path(test_dir, f"timeseries_temp_config{uuid.uuid4()}.json")
    configuration = config.Config(
        configuration_file=tmp_config,
        bucket=test_dir,
        timeseries_root=test_dir,
    )

    ts_logger.debug(
        f"Using testdir: {test_dir}. Created configuration: {tmp_config}\n{configuration}"
    )
    assert isinstance(configuration, config.Config)
    assert configuration.bucket == test_dir
    assert configuration.timeseries_root == test_dir
    assert configuration.log_file == config.DEFAULTS["log_file"]
