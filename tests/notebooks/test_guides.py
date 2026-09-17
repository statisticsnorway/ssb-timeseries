"""Test Marimo notebooks that reside in the project documentation.

This setup allows this module to choose which notebooks to test, but also depends on conventions:
For this to work, asserts must reside in cells named 'test_...'.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from ssb_timeseries.config import ENV_VAR_NAME

if TYPE_CHECKING:
    from ssb_timeseries.config import Config

NOTEBOOK_DIR = "notebooks"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def subprocess_run_marimo_notebook(notebook_name: str, config: Config):
    """Helper to run notebook with config as other tests.

    Running as script via subprocess.
    """
    environment = os.environ.copy()
    environment[ENV_VAR_NAME] = str(config.configuration_file)
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(PROJECT_ROOT / "tools"), environment.get("PYTHONPATH", "")]
    )
    return subprocess.run(
        [
            sys.executable,  # use same Python environment and virtual environment
            f"{NOTEBOOK_DIR}/{notebook_name}",
        ],
        encoding="utf-8",
        env=environment,
        capture_output=True,
        text=True,
    )


def import_and_run_marimo_app(notebook_name: str, config: Config):
    """Helper to run notebook with config as other tests.

    Import and run app directly.
    """
    from importlib import import_module

    notebook = import_module(f"notebooks.{notebook_name}")

    outputs, definitions = notebook.app.run()
    print(outputs)
    print(definitions)
    return outputs


# ------------------------------------


@pytest.mark.xfail(reason="Relative import from ../marimo fails.")
def test_marimo_tutorial_getting_started_experimental(buildup_and_teardown):
    result = import_and_run_marimo_app(
        "getting_started.py",
        buildup_and_teardown,
    )
    assert result


# ------------------------------------


def test_marimo_quickstart(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "quickstart.py",
        buildup_and_teardown,
    )

    assert result.returncode == 0


def test_marimo_basic_usage(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "basic-usage.py",
        buildup_and_teardown,
    )

    assert result.returncode == 0


def test_marimo_calc_basic_arithmetic(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "calc-basic-arithmetic.py",
        buildup_and_teardown,
    )

    assert result.returncode == 0


def test_marimo_calc_with_time(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "calc-with-time.py",
        buildup_and_teardown,
    )

    assert result.returncode == 0


def test_marimo_calc_with_metadata(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "calc-with-metadata.py",
        buildup_and_teardown,
    )

    assert result.returncode == 0


def test_marimo_meta_search_and_filtering(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "meta-search-and-filtering.py",
        buildup_and_teardown,
    )

    assert result.returncode == 0


def test_marimo_meta_tag_maintenance(buildup_and_teardown):
    result = subprocess_run_marimo_notebook(
        "meta-tag-maintenance.py",
        buildup_and_teardown,
    )

    assert result.returncode == 0
