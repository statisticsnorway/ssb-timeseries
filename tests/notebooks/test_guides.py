"""Test Marimo notebooks that reside in the project documentation.

This setup allows this module to choose which notebooks to test, but also depends on conventions:
For this to work, asserts must reside in cells named 'test_...'.
"""

from __future__ import annotations

import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from tests.conftest import TEST_LOG_CONFIG
from tests.conftest import _archive_test_config
from tests.conftest import _repository_test_config
from tests.conftest import _sharing_test_config

from ssb_timeseries.config import ENV_VAR_NAME

if TYPE_CHECKING:
    from ssb_timeseries.config import Config

NOTEBOOK_DIR = "notebooks"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def notebook_config(tmp_path):
    """Give each notebook its own repositories and configuration.

    The module-scoped `buildup_and_teardown` fixture shares one temporary tree
    across every notebook in this module, so two notebooks saving the same
    dataset name collide on the catalog and the parquet schema written earlier.
    A function-scoped `tmp_path` keeps each notebook independent, which is what
    lets them be added to this inventory in any order.
    """
    from ssb_timeseries.config import BUILTIN_IO_HANDLERS
    from ssb_timeseries.config import Config

    config_file = str(tmp_path / "config.json")
    log_config = deepcopy(TEST_LOG_CONFIG)
    log_config["handlers"]["file"]["filename"] = str(tmp_path / "notebook.log")

    configuration = Config(
        configuration_file=config_file,
        io_handlers=BUILTIN_IO_HANDLERS,
        repositories=_repository_test_config(tmp_path),
        archives=_archive_test_config(tmp_path),
        sharing=_sharing_test_config(tmp_path),
        bucket=str(tmp_path / "bucket"),
        logging=log_config,
        ignore_file=True,
    )
    configuration.save(config_file)
    yield configuration


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


def test_marimo_quickstart(notebook_config):
    result = subprocess_run_marimo_notebook(
        "quickstart.py",
        notebook_config,
    )

    assert result.returncode == 0


def test_marimo_basic_usage(notebook_config):
    result = subprocess_run_marimo_notebook(
        "basic-usage.py",
        notebook_config,
    )

    assert result.returncode == 0


def test_marimo_calc_basic_arithmetic(notebook_config):
    result = subprocess_run_marimo_notebook(
        "calc-basic-arithmetic.py",
        notebook_config,
    )

    assert result.returncode == 0


def test_marimo_calc_with_time(notebook_config):
    result = subprocess_run_marimo_notebook(
        "calc-with-time.py",
        notebook_config,
    )

    assert result.returncode == 0


def test_marimo_calc_with_metadata(notebook_config):
    result = subprocess_run_marimo_notebook(
        "calc-with-metadata.py",
        notebook_config,
    )

    assert result.returncode == 0


# The two notebooks below are not in any Sphinx toctree: they are commented out
# in docs/guides/toc-other.rst, so neither is reachable from the published docs.
# tools/export_all_guides.py still exports them, so they must keep running once
# fixed. Both have been broken since the marimo migration (029f822) and were
# never noticed for that reason.


# TODO: make the notebook resolve its configuration instead of hardcoding a path
# that only exists on the machine that wrote it, and drop this xfail.
@pytest.mark.xfail(
    reason="Not in the published docs, and activates a hardcoded sharing_config.json "
    "path that overrides the fixture this test injects."
)
def test_marimo_data_archiving_and_sharing(notebook_config):
    result = subprocess_run_marimo_notebook(
        "data-archiving-and-sharing.py",
        notebook_config,
    )

    assert result.returncode == 0


# TODO: implement Versioning.NAMES in pyarrow_simple.py and drop this xfail.
# The dead case at pyarrow_simple.py:143 matches "NAMED", but the member is
# NAMES, and _version_from_file_name expects a "_v<name>" filename that the
# writer never produces. Correcting the literal alone only moves the failure.
@pytest.mark.xfail(
    reason="Not in the published docs, and raises ValueError('Unhandled versioning.') "
    "because Versioning.NAMES is unimplemented in the simple-parquet handler."
)
def test_marimo_data_types_and_storage(notebook_config):
    result = subprocess_run_marimo_notebook(
        "data-types-and-storage.py",
        notebook_config,
    )

    assert result.returncode == 0


def test_marimo_meta_basics(notebook_config):
    result = subprocess_run_marimo_notebook(
        "meta-basics.py",
        notebook_config,
    )

    assert result.returncode == 0


def test_marimo_meta_search_and_filtering(notebook_config):
    result = subprocess_run_marimo_notebook(
        "meta-search-and-filtering.py",
        notebook_config,
    )

    assert result.returncode == 0


def test_marimo_meta_tag_maintenance(notebook_config):
    result = subprocess_run_marimo_notebook(
        "meta-tag-maintenance.py",
        notebook_config,
    )

    assert result.returncode == 0
