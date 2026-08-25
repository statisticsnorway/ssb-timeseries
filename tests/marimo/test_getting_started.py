"""Test Marimo notebooks that reside in the project documentation.

This setup allows this module to choose which notebooks to test, but also depends on conventions:
For this to work, asserts must reside in cells named 'test_...'.
"""

import subprocess
import sys

import pytest


@pytest.mark.xfail(reason="Notebook does not automatically load configuration.")
def test_marimo_notebooks_in_project_docs():
    # Run the script using the current Python interpreter
    result = subprocess.run(
        [sys.executable, "../../docs/marimo/getting_started.py"],
        capture_output=True,
        text=True,
    )

    # Assertions on script exit code and output
    assert result.returncode == 0
    assert "Expected Output" in result.stdout
