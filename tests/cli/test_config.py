"""Test cases for the __main__ module."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from ssb_timeseries import config
from ssb_timeseries.__main__ import main


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


def test_config_with_no_args_is_help(
    runner: CliRunner,  # monkeypatch: pytest.MonkeyPatch, buildup_and_teardown
) -> None:
    """It exits with a status code of zero."""
    # monkeypatch.setenv("TIMESERIES_CONFIG", buildup_and_teardown.configuration_file)

    result_no_arg = runner.invoke(main, ["config"])
    result_help = runner.invoke(main, ["config", "--help"])

    assert result_no_arg.exit_code == 2
    assert result_help.exit_code == 0
    assert result_no_arg.output == result_help.output


def test_config_show(runner):
    result = runner.invoke(main, ["config", "show"])

    assert result.exit_code == 0
    assert json.loads(result.output) == config.Config.active().__dict__


def test_config_show_preset(runner):
    result_daplalab = runner.invoke(main, ["config", "show", "daplalab"])
    result_defaults = runner.invoke(main, ["config", "show", "defaults"])

    assert result_daplalab.exit_code == result_defaults.exit_code == 0
    assert json.loads(result_daplalab.output) == config.PRESETS["daplalab"]
    assert json.loads(result_defaults.output) == config.PRESETS["defaults"]
    assert result_defaults.output != result_daplalab.output


def test_config_show_unknown_preset(runner):
    result = runner.invoke(main, ["config", "show", "does-not-exist"])

    assert result.exit_code != 0
    assert "Unknown configuration preset" in result.output
    assert "does-not-exist" in result.output


def test_config_path(runner):
    result = runner.invoke(main, ["config", "path"])

    assert result.exit_code == 0
    assert result.output.strip() == str(config.Config.active().configuration_file)


def test_config_env_var(runner):
    result = runner.invoke(main, ["config", "env-var"])

    assert result.exit_code == 0
    assert result.output.strip() == config.ENV_VAR_NAME


def test_config_list(runner):
    result = runner.invoke(main, ["config", "list"])

    assert result.exit_code == 0
    assert result.output.splitlines() == list(config.PRESETS)
