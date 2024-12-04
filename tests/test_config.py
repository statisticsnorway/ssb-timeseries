"""Tests for the ssb_timeseries config module.

We cover he following runtime scenarios:

- initial set up without a previously defined configuration:
    - applicable only to dev environments/tools with a human in front of the screen
- discovering and running with a predefined configuration
  - on a local computer (ie with full control of the environment)
  - in the dev tools in Statistcis Norways cloud environment
  - in automated production services in Statistcis Norways cloud environment
- editing and saving an existing configuration
- switching between preset configurations provided by the config module
- switching between existing configuration files

Running with a predefined configuration file depnds on:
- a) knowing which configuration file to use
    - the environment variable TIMESERIES_CONFIG
    - fall back to conventions / default locations if the env var is not found
- b) the configuration file being available

Fixtures
"""

import logging
import os
import uuid

import pytest

from ssb_timeseries import config
from ssb_timeseries import fs
from tests.conftest import Helpers

# NOSONAR
# mypy: disable-error-code="no-untyped-def"

# FIXTURES to handle the runtime scenarios:
# - make sure we use test configurations defined in conftest.py
#   (designed to make sure we do not pollute the user environment with test data)
# - control whether ENV VAR and CONF FILE exists or not before running tests
# - reset the state ENV VAR and CONF FILE after each test
#


@pytest.fixture(scope="function", autouse=True)
def reset_config_after():
    cfg_file = Helpers.configuration.configuration_file
    # TODO: make sure this also removes config if none existed before?
    yield Helpers.configuration
    Helpers.configuration.save()
    assert fs.exists(cfg_file)
    assert os.environ["TIMESERIES_CONFIG"] == cfg_file


@pytest.fixture(scope="function", autouse=True)
def ensure_config_file_exists_and_env_var_is_set_before_running():
    test_config = Helpers.configuration
    test_config_file = test_config.configuration_file
    logging.debug(
        f"Test running with configuration file: {test_config.configuration_file}"
    )
    assert fs.exists(test_config.configuration_file)
    assert os.environ["TIMESERIES_CONFIG"] == test_config_file

    yield test_config

    test_config.save(test_config_file)
    assert fs.exists(test_config_file)
    assert os.environ["TIMESERIES_CONFIG"] == test_config_file


@pytest.fixture(scope="function", autouse=True)
def reset_env_var_after():
    env_var = os.environ.get("TIMESERIES_CONFIG", "")
    assert env_var
    yield env_var
    if env_var:
        os.environ["TIMESERIES_CONFIG"] = env_var
        assert os.environ.get("TIMESERIES_CONFIG") == env_var


@pytest.fixture(scope="function", autouse=True)
def unset_env_var_before_running(reset_env_var_after):
    env_var = os.environ.pop("TIMESERIES_CONFIG", "")
    assert not os.environ.get("TIMESERIES_CONFIG")
    yield env_var


@pytest.fixture(scope="function", autouse=True)
def hide_file_before_running(reset_env_var_after):
    env_var = reset_env_var_after
    if env_var and fs.exists(env_var):
        temp_file_name = env_var + "_temp_backup_while_testing"
        fs.mv(env_var, temp_file_name)
    yield env_var
    if env_var:
        if fs.exists(temp_file_name):
            fs.mv(temp_file_name, env_var)
            assert fs.exists(env_var)


@pytest.fixture(scope="function", autouse=True)
def unset_env_var_and_hide_file_before_running(
    unset_env_var_before_running,
    hide_file_before_running,
):
    # assertions here test the fixtures ;)
    assert unset_env_var_before_running == hide_file_before_running
    assert not os.environ.get("TIMESERIES_CONFIG")
    assert not fs.exists(hide_file_before_running)
    yield


@pytest.mark.usefixtures("ensure_config_file_exists_and_env_var_is_set_before_running")
@pytest.mark.parametrize(
    "preset_name,attr,value",
    [
        ("defaults", "", ""),
        ("home", "", ""),
        ("gcs", "", ""),
        ("shared-test", "", ""),
        ("shared-prod", "", ""),
        ("jovyan", "", ""),
        ("dapla", "", ""),
    ],
)
def test_config_presets(
    preset_name,
    attr,
    value,
) -> None:
    # all of these should result in the same information, but in different objects

    cfg = config.Config(preset=preset_name)

    assert isinstance(cfg, config.Config)
    assert cfg.is_valid
    if attr:
        assert cfg.__getattribute__(attr) == value


def test_init_config_without_params(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    new_config = config.Config(preset="default")
    logging.debug(f"Created configuration: {new_config}")
    assert isinstance(new_config, config.Config)
    for key in config.DEFAULTS.keys():
        assert new_config[key] == config.DEFAULTS[key]


def test_init_with_incomplete_params_uses_defaults_for_missing(
    caplog: pytest.LogCaptureFixture,
    unset_env_var_and_hide_file_before_running: None,
) -> None:
    caplog.set_level(logging.DEBUG)
    assert os.environ.get("TIMESERIES_CONFIG") is None
    default_config = config.Config(preset="defaults")
    new_config = config.Config(
        timeseries_root=config.GCS,
        log_file=default_config["log_file"],
    )
    logging.debug(
        f"Created configuration: {new_config} with root {new_config.timeseries_root}"
    )
    assert isinstance(new_config, config.Config)
    assert new_config.is_valid
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
    cfg = config.CONFIG
    old_value = cfg.timeseries_root
    config_file = cfg.configuration_file
    if old_value == config.JOVYAN:
        new_value = config.GCS
    else:
        new_value = config.JOVYAN
    cfg.timeseries_root = new_value
    cfg.save()

    new = config.Config(configuration_file=config_file)
    # we should have both a new object (a new id) and a new path for timeseries_root
    assert id(new) != id(cfg)
    assert new.timeseries_root == new_value
    assert new.timeseries_root != old_value


def test_read_config_from_file(reset_config_after: config.Config) -> None:
    configuration = config.Config(configuration_file=config.CONFIGURATION_FILE)

    assert isinstance(configuration, config.Config)
    assert configuration.is_valid


def test_init_with_kwargs_and_specified_nonexisting_config_file_creates_new_file(
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

    logging.debug(
        f"Using testdir: {test_dir}. Created configuration: {tmp_config}\n{configuration}"
    )
    assert isinstance(configuration, config.Config)
    assert configuration.bucket == test_dir
    assert configuration.timeseries_root == test_dir
    assert configuration.log_file == config.DEFAULTS["log_file"]


def test_init_w_only_config_file_param_pointing_to_file_not_exists_raises_error(
    unset_env_var_and_hide_file_before_running,
) -> None:
    non_existing_file = os.path.join(os.getcwd(), "does_not_exist.json")
    os.environ["TIMESERIES_CONFIG"] = non_existing_file

    assert os.environ["TIMESERIES_CONFIG"] == non_existing_file
    assert not os.path.exists(non_existing_file)
    with pytest.raises(FileNotFoundError):
        cfg = config.Config(configuration_file=non_existing_file)
        assert cfg.is_valid


def test_init_w_no_params_and_env_var_pointing_to_non_existing_file_raises_error(
    caplog: pytest.LogCaptureFixture,
    unset_env_var_and_hide_file_before_running: None,
) -> None:
    non_existing_file = os.path.join(os.getcwd(), "does_not_exist.json")
    os.environ["TIMESERIES_CONFIG"] = non_existing_file

    assert os.environ["TIMESERIES_CONFIG"] == non_existing_file
    assert not os.path.exists(non_existing_file)
    with pytest.raises(config.EnvVarNotDefinedeError):
        cfg = config.Config()
        logging.debug(f"Created configuration: {cfg}")
