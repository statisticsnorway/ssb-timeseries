"""Test cases for the __main__ module."""

import pytest
from click.testing import CliRunner

from ssb_timeseries import __main__

# mypy: ignore-errors


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


@pytest.mark.skip(reason="TODO: Check that this will not mess up configurations.")
def test_main_succeeds(runner: CliRunner) -> None:
    """It exits with a status code of zero."""
    result = runner.invoke(__main__.main)
    assert result.exit_code == 0
