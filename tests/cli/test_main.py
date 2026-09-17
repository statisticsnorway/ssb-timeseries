"""Test cases for the __main__ module."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from ssb_timeseries import __main__


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


def test_main_with_no_args_is_help(
    runner: CliRunner,  # monkeypatch: pytest.MonkeyPatch, buildup_and_teardown
) -> None:
    """It exits with a status code of zero."""
    # monkeypatch.setenv("TIMESERIES_CONFIG", buildup_and_teardown.configuration_file)
    result_no_arg = runner.invoke(__main__.main)
    result_help = runner.invoke(__main__.main, ["--help"])
    assert result_no_arg.exit_code == 2
    assert result_help.exit_code == 0
    assert result_no_arg.output == result_help.output
