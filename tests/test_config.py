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

import json
import logging
import os
import uuid
from pathlib import Path

import pytest

# import ssb_timeseries as ts
import ssb_timeseries.io.archiving
from ssb_timeseries import config
from ssb_timeseries.io import fs
from tests.conftest import Helpers

# NOSONAR
# mypy: disable-error-code="no-untyped-def,call-overload,attr-defined,literal-required"

# ================================ FIXTURES: ===================================
# - make sure we use test configurations defined in conftest.py
#   (designed to make sure we do not pollute the user environment with test data)
# - control whether ENV VAR and CONF FILE exists or not before running tests
# - reset the state ENV VAR and CONF FILE after each test
test_logger = logging.getLogger(__name__)
REPO = "test_1"

IO_HANDLERS = {
    "parquet": {
        "handler": "ssb_timeseries.io.pyarrow_simple.FileSystem",
        "options": {"compression": "snappy"},
    },
    "json": {
        "handler": "ssb_timeseries.io.json_metadata.JsonMetaIO",
        "options": {},
    },
}


@pytest.fixture(scope="function", autouse=True)
def reset_config_after(buildup_and_teardown: config.Config):
    config_before_test = buildup_and_teardown
    cfg_file = config_before_test.configuration_file
    test_logger.debug(f"Before tests: {cfg_file}; exists: {fs.exists(cfg_file)}")
    # TODO: make sure this also removes config if none existed before?
    yield buildup_and_teardown
    config_before_test.save()
    config_before_test.activate()
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
    test_logger.debug(
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


# =================================== TESTS ===================================


def test_config_validation(
    caplog: pytest.LogCaptureFixture,
    buildup_and_teardown,
) -> None:
    caplog.set_level("DEBUG")
    configuration = buildup_and_teardown
    test_logger.warning(f"Created configuration: {configuration}")
    assert isinstance(configuration, config.Config)
    assert configuration.is_valid


MINIMAL_VALID_CONFIG = {
    "configuration_file": "config.json",
    "io_handlers": {"parquet": {"handler": "parquet", "options": {}}},
    "repositories": {
        "test_repo": {
            "name": "test-repo",
            "directory": {"handler": "parquet", "options": {}},
        }
    },
    "logging": {},
}


@pytest.mark.parametrize(
    "overrides,expected_valid",
    [
        pytest.param({}, True, id="minimal valid config"),
        pytest.param({"log_file": "log.log"}, True, id="optional field present"),
        pytest.param(
            {"archives": {"default": {"directory": {"handler": "s", "options": {}}}}},
            True,
            id="optional archives present",
        ),
        pytest.param({"unknown_future_field": 42}, True, id="undeclared extra field"),
        pytest.param({"configuration_file": None}, False, id="None instead of str"),
        pytest.param({"configuration_file": 42}, False, id="int instead of str"),
        pytest.param({"io_handlers": "parquet"}, False, id="str instead of dict"),
        pytest.param({"repositories": []}, False, id="list instead of dict"),
        pytest.param({"logging": None}, False, id="None instead of dict"),
        pytest.param({"log_file": {"path": "x"}}, False, id="dict instead of str"),
    ],
)
def test_is_valid_config_checks_declared_top_level_types(
    overrides, expected_valid: bool
) -> None:
    """Wrong top level types must be detected, and missing optional fields must not."""
    configuration = {**MINIMAL_VALID_CONFIG, **overrides}

    is_valid, reason = config.is_valid_config(configuration)

    assert is_valid is expected_valid, reason


@pytest.mark.parametrize(
    "missing_key",
    ["configuration_file", "io_handlers", "repositories", "logging"],
)
def test_is_valid_config_requires_the_mandatory_fields(missing_key: str) -> None:
    """Every mandatory field must be reported when absent."""
    configuration = {k: v for k, v in MINIMAL_VALID_CONFIG.items() if k != missing_key}

    is_valid, reason = config.is_valid_config(configuration)

    assert is_valid is False
    assert missing_key in str(reason)


def _handlers_config(*handlers: tuple[str, object]) -> dict:
    """A structurally valid configuration declaring only the given handlers."""
    return {
        **MINIMAL_VALID_CONFIG,
        "io_handlers": {
            name: {"handler": path, "options": {}} for name, path in handlers
        },
    }


def test_a_builtin_handler_that_no_longer_resolves_is_reported() -> None:
    """A deleted module must fail validation, not the first read that needs it.

    `is_valid_config` cannot catch this, because a handler is named by a string
    and a string is well formed whether or not anything answers to it.
    """
    configuration = _handlers_config(
        ("archive", "ssb_timeseries.io.snapshot.FileSystem")
    )

    is_valid, reason = config.validate_handlers(configuration)

    assert is_valid is False
    assert "archive" in str(reason)
    assert "ssb_timeseries.io.snapshot.FileSystem" in str(reason)


def test_a_builtin_handler_renamed_within_its_module_is_reported() -> None:
    """A renamed class leaves the module importable, so the module alone is not enough."""
    configuration = _handlers_config(
        ("json", "ssb_timeseries.io.json_metadata.NoSuchMetaIO")
    )

    is_valid, reason = config.validate_handlers(configuration)

    assert is_valid is False
    assert "NoSuchMetaIO" in str(reason)


@pytest.mark.parametrize("handler_path", [42, None, "json_metadata"])
def test_a_handler_that_is_not_a_module_class_path_is_reported(
    handler_path: object,
) -> None:
    """A value with nothing to resolve cannot be resolved by anyone."""
    configuration = _handlers_config(("json", handler_path))

    is_valid, reason = config.validate_handlers(configuration)

    assert is_valid is False
    assert "module.Class" in str(reason)


def test_a_handler_that_resolves_to_something_other_than_a_class_is_reported() -> None:
    """A module satisfies `getattr`, and cannot be instantiated into a handler.

    `ssb_timeseries.io` exposes its submodules, so the path is well formed and the
    module exists, yet nothing about it can serve as a handler.
    """
    configuration = _handlers_config(("json", "ssb_timeseries.io.json_metadata"))

    is_valid, reason = config.validate_handlers(configuration)

    assert is_valid is False
    assert "not to a class" in str(reason)


def test_a_handler_that_resolves_to_an_alias_under_another_name_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolution is not enough if the class is not the one the path names.

    A registry or configuration entry naming a class where it happens to be
    importable from resolves, and then reports a path that is not where the class
    is defined, which is the same defect as a typo in the class name.
    """
    monkeypatch.setattr(
        ssb_timeseries.io.archiving,
        "Archived",
        ssb_timeseries.io.archiving.Archive,
        raising=False,
    )
    configuration = _handlers_config(
        ("archive", "ssb_timeseries.io.archiving.Archived")
    )

    is_valid, reason = config.validate_handlers(configuration)

    assert is_valid is False
    assert "ssb_timeseries.io.archiving.Archive" in str(reason)


def test_a_handler_from_another_package_is_not_required_to_resolve() -> None:
    """A plugin absent from this environment is not a defect in the configuration.

    Handlers written outside the library are a supported feature, so a
    configuration naming one is valid even where that one is not installed.
    Requiring it to resolve would report a plugin author's own configuration as
    broken, in the one function meant to help diagnose configuration.
    """
    configuration = _handlers_config(("acme", "acme.io.parquet.AcmeFileSystem"))

    is_valid, reason = config.validate_handlers(configuration)

    assert is_valid is True, reason
    assert reason is None


def test_requiring_every_handler_surfaces_a_plugin_that_is_absent() -> None:
    """The strict policy exists for environments known to be complete."""
    configuration = _handlers_config(("acme", "acme.io.parquet.AcmeFileSystem"))

    is_valid, reason = config.validate_handlers(configuration, required="all")

    assert is_valid is False
    assert "acme" in str(reason)


def test_every_builtin_handler_named_in_the_registry_resolves() -> None:
    """The registry and the modules must agree, which is the contract the tests above protect.

    A wrong class name in `BUILTIN_IO_HANDLERS` went unnoticed for as long as the
    registry and the modules happened to agree, so the registry is checked through
    the same function that checks a configuration file.
    """
    is_valid, reason = config.validate_handlers(
        {
            **MINIMAL_VALID_CONFIG,
            "io_handlers": config.BUILTIN_IO_HANDLERS,
        }
    )

    assert is_valid is True, reason


def test_a_configuration_still_named_snapshots_warns_that_it_is_ignored() -> None:
    """A renamed section must say so, not quietly stop archiving.

    An old configuration keeps whatever else it declares, and an unknown key is
    accepted, so a `snapshots` section is simply set as an attribute nothing
    reads. Without a warning, `archive()` returns without writing anything and
    the only symptom is a missing archive.
    """
    with pytest.warns(DeprecationWarning, match="'archives'"):
        configuration = config.Config(
            **MINIMAL_VALID_CONFIG,
            snapshots={"default": {"directory": {"handler": "s", "options": {}}}},
        )

    # The old section is kept, so a configuration round trip does not lose it,
    # but nothing reads it, and the new one is absent rather than empty.
    assert configuration["snapshots"]
    assert configuration["archives"] is None


def test_a_section_the_configuration_does_not_declare_reads_as_none() -> None:
    """An optional section must be absent, not an attribute error.

    `archives` and `sharing` are declared as annotations, so they are not
    attributes until a configuration sets them, and no preset sets either.
    Reading them directly raised `AttributeError`, which turned "archiving is
    not configured" into a crash. `__getitem__` is the safe accessor.
    """
    configuration = config.Config(preset="daplalab")

    assert configuration["archives"] is None
    assert configuration["sharing"] is None
    assert configuration["repositories"]


def test_presets_are_valid_configurations() -> None:
    """All shipped presets must pass the validation they are tested against."""
    for preset_name, preset in config.PRESETS.items():
        is_valid, reason = config.is_valid_config(preset)

        assert is_valid, f"preset {preset_name} is invalid: {reason}"


@pytest.mark.parametrize(
    "preset_name,attr,value",
    [
        ("defaults", "", ""),
        ("home", "", ""),
        ("daplalab", "", ""),
        ("DEFAULTS", "", ""),
        ("HOME", "", ""),
        ("DAPLALAB", "", ""),
    ],
)
def test_config_presets_returns_valid_config_with_expected_value(
    caplog: pytest.LogCaptureFixture,
    reset_config_after,
    preset_name,
    attr,
    value,
) -> None:
    caplog.set_level("DEBUG")
    cfg = config.Config(preset=preset_name)

    test_logger.debug(f"Created configuration: {cfg}")
    assert isinstance(cfg, config.Config)
    assert cfg.is_valid
    if attr:
        assert cfg.__getattribute__(attr) == value
    assert cfg == config.PRESETS[preset_name.lower()]  # tests implementation :(


def test_init_config_without_params_returns_existing_config_specified_by_env_var(
    caplog: pytest.LogCaptureFixture,
    ensure_config_file_exists_and_env_var_is_set_before_running,
) -> None:
    caplog.set_level("DEBUG")
    new_config = config.Config()
    test_logger.debug(f"Created configuration: {new_config}")
    assert isinstance(new_config, config.Config)
    expected = config.DEFAULTS
    # expected.pop("logging")
    for key in [k for k in expected.keys() if k not in ["logging", "log_file"]]:
        assert new_config[key] == config.DEFAULTS[key]


def test_config_init_for_no_params_returns_same_as_preset_defaults(
    caplog: pytest.LogCaptureFixture,
    reset_config_after: config.Config,
) -> None:
    caplog.set_level("DEBUG")
    # all of these should result in the same information, but in different objects
    cfg_0 = config.Config(preset="default")
    cfg_1 = config.Config(preset="defaults")
    cfg_2 = config.Config()
    test_logger.debug(f"compare:\n{cfg_0=}\n{cfg_1=}\n{cfg_2=}")
    assert id(cfg_0) != id(cfg_1) != id(cfg_2)
    assert cfg_0.__dict__ == cfg_1.__dict__ == cfg_2

    r = config.constants.DAPLA_TEAM
    assert (
        cfg_0.repositories[r]["directory"]["options"]["path"]
        == cfg_1.repositories[r]["directory"]["options"]["path"]
        == cfg_2.repositories[r]["directory"]["options"]["path"]
    )


def test_config_change_persists_after_save(
    caplog: pytest.LogCaptureFixture,
    reset_config_after: config.Config,
) -> None:
    caplog.set_level("DEBUG")
    cfg = reset_config_after
    old_value = cfg.repositories[REPO]["directory"]
    config_file = cfg.configuration_file
    if old_value == config.constants.DAPLALAB_FUSE:
        new_value = config.constants.SHARED_TEST
    else:
        new_value = config.constants.DAPLALAB_FUSE
    cfg.repositories[REPO]["directory"] = new_value
    cfg.save()

    new = config.Config(configuration_file=config_file)
    test_logger.debug(f"Created configuration: {new}")

    assert id(new) != id(cfg)
    assert new.repositories[REPO]["directory"] == new_value
    assert new.repositories[REPO]["directory"] != old_value


def test_read_config_from_file(
    unset_env_var_and_hide_file_before_running: config.Config,
) -> None:
    find_hidden_config_file = unset_env_var_and_hide_file_before_running["hidden"]
    test_logger.debug(f"Using config file: {find_hidden_config_file}")
    configuration = config.Config(configuration_file=find_hidden_config_file)

    assert isinstance(configuration, config.Config)
    assert configuration.is_valid


def test_init_of_not_already_existing_config_file_and_incomplete_config_params_raises_error(
    caplog: pytest.LogCaptureFixture,
    reset_config_after: config.Config,
    conftest: Helpers,
) -> None:
    caplog.set_level("DEBUG")
    test_dir = reset_config_after.bucket
    tmp_config = fs.path(test_dir, f"timeseries_temp_config_{uuid.uuid4()}.json")
    with pytest.raises(FileNotFoundError):
        configuration = config.Config(
            configuration_file=tmp_config,
            repositories={REPO: {"name": REPO, "directory": test_dir}},
        )
        test_logger.debug(f"Created configuration: {configuration}")


def test_init_of_not_already_existing_config_file_with_complete_params_creates_new_file(
    caplog: pytest.LogCaptureFixture,
    reset_config_after: config.Config,
    conftest: Helpers,
) -> None:
    caplog.set_level("DEBUG")
    test_dir = reset_config_after.bucket
    tmp_config = fs.path(test_dir, f"timeseries_temp_config_{uuid.uuid4()}.json")

    configuration = config.Config(
        configuration_file=tmp_config,
        log_file=test_dir,
        io_handlers=IO_HANDLERS,
        repositories={
            "test_repo": {
                "name": "test-repo",
                "directory": {
                    "handler": "parquet",
                    "options": {"path": str(test_dir)},
                },
                "catalog": {
                    "handler": "json",
                    "options": {"path": str(test_dir)},
                },
            }
        },
        logging={},
    )

    test_logger.debug(
        f"Using testdir: {test_dir}. Created configuration: {tmp_config}\n{configuration}"
    )
    assert isinstance(configuration, config.Config)
    assert configuration.repositories["test_repo"]["directory"]["handler"] == "parquet"


def test_init_w_only_config_file_param_pointing_to_file_not_exists_raises_error(
    caplog: pytest.LogCaptureFixture,
    unset_env_var_and_hide_file_before_running,
) -> None:
    caplog.set_level("DEBUG")

    non_existing_file = os.path.join(os.getcwd(), "does_not_exist.json")
    assert config.active_file(non_existing_file) == non_existing_file
    assert not fs.exists(non_existing_file)

    # to force reloading
    from ssb_timeseries import config as cfg

    with pytest.raises(FileNotFoundError):
        configuration = cfg.Config(configuration_file=non_existing_file)
        assert configuration.is_valid
        test_logger.debug(f"Created configuration: {configuration}")


def test_loading_deprecated_handler_path_issues_warning(
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    reset_config_after,
) -> None:
    """The 'ssb_timeseries.io' module 'simple' has been renamed to 'pyarrow_simple'.

    Loading a config with the old handler path should issue a DeprecationWarning.
    """
    caplog.set_level("DEBUG")
    cfg = reset_config_after
    cfg.io_handlers = {
        "parquet": {
            "handler": "ssb_timeseries.io.simple.FileSystem",  # DEPRECATED
            "options": {},
        }
    }
    cfg.repositories = {
        "test_repo": {
            "directory": {"path": str(tmp_path), "handler": "parquet"},
            "catalog": {"path": str(tmp_path), "handler": "parquet"},
        }
    }
    cfg.activate()

    with pytest.warns(
        DeprecationWarning,
        match="The I/O handler 'ssb_timeseries.io.simple.FileSystem' is deprecated",
    ):
        from ssb_timeseries.io import _io_handler
        from ssb_timeseries.types import SeriesType

        _io_handler(
            handler_type="data",
            repository="test_repo",
            set_name="test",
            set_type=SeriesType.simple(),
        )


def test_init_w_no_params_and_env_var_pointing_to_non_existing_file_raises_error(
    caplog: pytest.LogCaptureFixture,
    hide_file_before_running,
) -> None:
    caplog.set_level("DEBUG")
    non_existing_file = hide_file_before_running["configured"]
    test_logger.debug(
        f"Using config file: {non_existing_file}\nand env var: {config.active_file()}"
    )

    # to force reloading
    from ssb_timeseries import config as cfg

    assert config.active_file(non_existing_file) == non_existing_file
    assert not fs.exists(non_existing_file)

    with pytest.raises(FileNotFoundError):
        configuration = cfg.Config()
        test_logger.debug(f"Created configuration: {configuration}")


# ============================ SAVE(): WHERE IT WRITES ============================


def complete_config(configuration_file: Path, **overrides: object) -> dict:
    """Build a complete valid configuration pointing at `configuration_file`.

    The result can be passed to `Config()` as keyword arguments,
    which is what keeps a configuration file out of the lookup.
    """
    payload = config.presets("default")
    payload["configuration_file"] = str(configuration_file)
    payload.update(overrides)
    return payload


def test_save_without_path_writes_to_the_configuration_file(
    tmp_path: Path,
) -> None:
    """A configuration without an explicit path saves to its own configuration_file."""
    cfg_file = tmp_path / "own.json"
    cfg = config.Config(**complete_config(cfg_file))

    cfg.save()

    assert json.loads(cfg_file.read_text())["configuration_file"] == str(cfg_file)


def test_save_with_path_writes_there_and_adopts_that_path(
    tmp_path: Path,
) -> None:
    """An explicit path wins over the current configuration_file, and replaces it."""
    cfg = config.Config(**complete_config(tmp_path / "own.json"))
    elsewhere = tmp_path / "elsewhere.json"

    cfg.save(path=str(elsewhere))

    assert cfg.configuration_file == str(elsewhere)
    assert json.loads(elsewhere.read_text())["configuration_file"] == str(elsewhere)
    assert not (tmp_path / "own.json").exists()


def test_save_without_a_path_and_without_a_configuration_file_raises() -> None:
    """Saving has nowhere to write when neither argument nor configuration_file says so."""
    cfg = config.Config(configuration_file="")

    with pytest.raises(ValueError, match="must have a value"):
        cfg.save()


def test_a_configuration_file_contributes_its_own_configuration_file_value(
    tmp_path: Path,
) -> None:
    """The file found through the environment variable decides where a save goes."""
    read_from = tmp_path / "a.json"
    write_to = tmp_path / "b.json"
    read_from.write_text(json.dumps(complete_config(write_to)))

    config.active_file(str(read_from))
    cfg = config.Config()
    read_from_before = read_from.read_text()

    assert cfg.configuration_file == str(write_to)
    cfg.save()
    assert read_from.read_text() == read_from_before
    assert json.loads(write_to.read_text())["configuration_file"] == str(write_to)


def test_a_preset_configuration_ignores_the_environment_variable(
    tmp_path: Path,
) -> None:
    """A preset carries its own configuration_file, whatever the environment variable says."""
    named_by_env_var = tmp_path / "named_by_env_var.json"
    named_by_env_var.write_text(json.dumps(complete_config(named_by_env_var)))
    config.active_file(str(named_by_env_var))

    cfg = config.Config(preset="defaults")

    assert cfg.configuration_file == str(
        config.PRESETS["defaults"]["configuration_file"]
    )
    assert cfg.configuration_file != str(named_by_env_var)
