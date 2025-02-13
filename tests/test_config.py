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

# ================ fixtures: ===================

# - make sure we use test configurations defined in conftest.py
#   (designed to make sure we do not pollute the user environment with test data)
# - control whether ENV VAR and CONF FILE exists or not before running tests
# - reset the state ENV VAR and CONF FILE after each test


@pytest.fixture(scope="function", autouse=True)
def reset_config_after(buildup_and_teardown: config.Config):
    config_before_test = buildup_and_teardown
    cfg_file = config_before_test.configuration_file
    logging.warning(f"Before tests: {cfg_file}; exists: {fs.exists(cfg_file)}")
    # TODO: make sure this also removes config if none existed before?
    yield buildup_and_teardown
    config_before_test.save()
    assert fs.exists(cfg_file)
    assert config.active_file() == cfg_file


@pytest.fixture(scope="function", autouse=True)
def reset_env_var_after():
    env_var = config.active_file()
    assert env_var
    yield env_var
    if env_var:
        assert config.active_file(env_var) == env_var


@pytest.fixture(scope="function", autouse=True)
def ensure_config_file_exists_and_env_var_is_set_before_running(buildup_and_teardown):
    test_config = buildup_and_teardown
    test_config.save()
    test_config_file = test_config.configuration_file
    logging.debug(
        f"Test running with env var set to {config.active_file()}\nand configuration file: {test_config.configuration_file}"
    )
    assert fs.exists(test_config.configuration_file)
    assert config.active_file() == test_config.configuration_file

    yield test_config

    test_config.save(path=test_config_file)
    assert fs.exists(config.active_file(test_config.configuration_file))
    # assert fs.exists(test_config_file)
    # assert config.active_file()


@pytest.fixture(scope="function", autouse=True)
def unset_env_var_before_running(reset_env_var_after):
    env_var = os.environ.pop("TIMESERIES_CONFIG", "")
    assert not os.environ.get("TIMESERIES_CONFIG")
    yield {
        "unset_env_var": env_var,
    }
    # taken vare of by resetn_env_var_after: config.active_file(env_var)


@pytest.fixture(scope="function", autouse=True)
def hide_file_before_running(reset_env_var_after):
    env_var = reset_env_var_after
    if env_var and fs.exists(env_var):
        temp_file_name = env_var.replace(".json", "_temp_backup_while_testing.json")
        fs.mv(env_var, temp_file_name)
        d = {"configured": env_var, "hidden": temp_file_name}
    else:
        d = {"configured": "", "hidden": ""}
    yield d
    if d["hidden"]:
        if fs.exists(temp_file_name):
            fs.mv(temp_file_name, env_var)
            assert fs.exists(env_var)


@pytest.fixture(scope="function", autouse=True)
def unset_env_var_and_hide_file_before_running(
    unset_env_var_before_running,
    hide_file_before_running,
):
    # assertions here test the fixtures ;)
    assert (
        unset_env_var_before_running["unset_env_var"]
        == hide_file_before_running["configured"]
    )
    assert not os.environ.get("TIMESERIES_CONFIG")
    assert not fs.exists(hide_file_before_running["configured"])
    hide_file_before_running.update(unset_env_var_before_running)
    yield hide_file_before_running


# ======================= tests ==========================


def test_config_validation(
    caplog: pytest.LogCaptureFixture,
    buildup_and_teardown: config.Config,
) -> None:
    caplog.set_level(logging.DEBUG)
    configuration = buildup_and_teardown
    assert configuration.is_valid


# @pytest.mark.usefixtures(
#     "ensure_config_file_exists_and_env_var_is_set_before_running",
#     "reset_config_after",
# )
@pytest.mark.parametrize(
    "preset_name,attr,value",
    [
        ("defaults", "", ""),
        ("home", "", ""),
        ("shared-test", "", ""),
        ("shared-prod", "", ""),
        ("daplalab", "", ""),
    ],
)
def test_config_presets(
    reset_config_after,
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


def test_config_defaults(
    caplog: pytest.LogCaptureFixture,
    reset_config_after: config.Config,
) -> None:
    caplog.set_level(logging.DEBUG)
    # all of these should result in the same information, but in different objects
    cfg_0 = config.Config(preset="default")
    # cfg_0.save()  # for the rare occasions when the default location changes
    cfg_1 = config.Config(preset="defaults")
    cfg_2 = config.Config()
    logging.debug(f"compare:\n{cfg_0=}\n{cfg_1=}\n{cfg_2=}")
    assert id(cfg_0) != id(cfg_1) != id(cfg_2)
    assert cfg_0.__dict__() == cfg_1.__dict__() == cfg_2
    assert cfg_0.timeseries_root == cfg_1.timeseries_root == cfg_2.timeseries_root


def test_config_change(reset_config_after: config.Config) -> None:
    cfg = reset_config_after
    old_value = cfg.timeseries_root
    config_file = cfg.configuration_file
    if old_value == config.DAPLALAB_FUSE:
        new_value = config.SHARED_TEST
    else:
        new_value = config.DAPLALAB_FUSE
    cfg.timeseries_root = new_value
    cfg.save()

    new = config.Config(configuration_file=config_file)
    # we should have both a new object (a new id) and a new path for timeseries_root
    assert id(new) != id(cfg)
    assert new.timeseries_root == new_value
    assert new.timeseries_root != old_value


def test_read_config_from_file(
    unset_env_var_and_hide_file_before_running: config.Config,
) -> None:
    find_hidden_config_file = unset_env_var_and_hide_file_before_running["hidden"]
    logging.debug(f"Using config file: {find_hidden_config_file}")
    configuration = config.Config(configuration_file=find_hidden_config_file)

    assert isinstance(configuration, config.Config)
    assert configuration.is_valid


def test_init_of_not_already_existing_config_file_and_incomplete_config_params_raises_error(
    caplog: pytest.LogCaptureFixture,
    reset_config_after: config.Config,
    conftest: Helpers,
) -> None:
    caplog.set_level(logging.DEBUG)
    # setup: point to a config that does not exist (this should create the .json file):
    test_dir = config.CONFIG.bucket
    tmp_config = fs.path(test_dir, f"timeseries_temp_config_{uuid.uuid4()}.json")
    with pytest.raises(FileNotFoundError):
        configuration = config.Config(
            configuration_file=tmp_config,
            bucket=test_dir,
            timeseries_root=test_dir,
        )
        logging.debug(f"Created configuration: {configuration}")


def test_init_of_not_already_existing_config_file_with_complete_params_creates_new_file(
    caplog: pytest.LogCaptureFixture,
    reset_config_after: config.Config,
    conftest: Helpers,
) -> None:
    caplog.set_level(logging.DEBUG)
    # setup: point to a config that does not exist (this should create the .json file):
    test_dir = config.CONFIG.bucket
    tmp_config = fs.path(test_dir, f"timeseries_temp_config_{uuid.uuid4()}.json")

    configuration = config.Config(
        configuration_file=tmp_config,
        log_file=test_dir,
        bucket=test_dir,
        timeseries_root=test_dir,
        catalog=test_dir,
    )

    logging.debug(
        f"Using testdir: {test_dir}. Created configuration: {tmp_config}\n{configuration}"
    )
    assert isinstance(configuration, config.Config)
    assert configuration.bucket == test_dir
    assert configuration.timeseries_root == test_dir


def test_init_w_only_config_file_param_pointing_to_file_not_exists_raises_error(
    caplog: pytest.LogCaptureFixture,
    unset_env_var_and_hide_file_before_running,
) -> None:
    caplog.set_level(logging.DEBUG)

    non_existing_file = os.path.join(os.getcwd(), "does_not_exist.json")
    assert config.active_file(non_existing_file) == non_existing_file
    assert not fs.exists(non_existing_file)

    # to force reloading
    from ssb_timeseries import config as cfg

    with pytest.raises(FileNotFoundError):
        configuration = cfg.Config(configuration_file=non_existing_file)
        assert configuration.is_valid
        logging.debug(f"Created configuration: {configuration}")


def test_init_w_no_params_and_env_var_pointing_to_non_existing_file_raises_error(
    caplog: pytest.LogCaptureFixture,
    hide_file_before_running,
) -> None:
    caplog.set_level(logging.DEBUG)
    non_existing_file = hide_file_before_running["configured"]
    logging.debug(
        f"Using config file: {non_existing_file}\nand env var: {config.active_file()}"
    )

    # to force reloading
    from ssb_timeseries import config as cfg

    assert config.active_file(non_existing_file) == non_existing_file
    assert not fs.exists(non_existing_file)

    with pytest.raises(FileNotFoundError):
        configuration = cfg.Config()
        logging.debug(f"Created configuration: {configuration}")
