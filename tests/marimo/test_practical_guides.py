"""Test Marimo notebooks that reside in the project documentation.

This setup allows this module to choose which notebooks to test, but also depends on conventions:
For this to work, asserts must reside in cells named 'test_...'.
"""

import os
import subprocess
import sys

import pytest

from ssb_timeseries.config import ENV_VAR_NAME

NOTEBOOK_DIR = "marimo"


def subprocess_run_marimo_notebook(notebook_name, config):
    """Helper to run notebook with config as other tests.

    Running as script via subprocess.
    """
    environment = os.environ.copy()
    environment[ENV_VAR_NAME] = str(config.configuration_file)
    return subprocess.run(
        [
            sys.executable,  # use same Python environment and virtual environment
            f"{NOTEBOOK_DIR}/{notebook_name}",
        ],
        env=environment,
        capture_output=True,
        text=True,
    )


def import_and_run_marimo_app(notebook, config):
    """Helper to run notebook with config as other tests.

    Import and run app directly.
    """
    # from ..marimo import getting_started as notebook

    outputs, definitions = notebook.app.run()
    print(outputs)
    print(definitions)
    return outputs


def test_marimo_tutorial_quickstart(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "quickstart.py",
        buildup_and_teardown,
    )

    # print("Return Code:", result.returncode)
    # print("Standard Output:", result.stdout)
    # print("Standard Error:", result.stderr)
    assert result.returncode == 0


def test_marimo_tutorial_getting_started(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "getting_started.py",
        buildup_and_teardown,
    )

    assert result.returncode == 0


@pytest.mark.xfail(reason="Relative import from ../marimo fails.")
def test_marimo_tutorial_getting_started_experimental(buildup_and_teardown):
    # Run the script using the current Python interpreter
    result = import_and_run_marimo_app(
        "getting_started.py",
        buildup_and_teardown,
    )
    print(result)
    assert True
