"""Test cases for the __main__ module."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from ssb_timeseries import __main__

# mypy: ignore-errors


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


def test_main_succeeds(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, buildup_and_teardown
) -> None:
    """It exits with a status code of zero."""
    monkeypatch.setenv("TIMESERIES_CONFIG", buildup_and_teardown.configuration_file)
    result = runner.invoke(__main__.main)
    assert '"repositories"' in result.output
    assert result.exit_code == 0
