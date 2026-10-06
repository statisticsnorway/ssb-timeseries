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

    HOME is redirected to the temporary tree, because quickstart.py saves a configuration
    to the path in the 'default' preset, which is built from the home directory when the
    config module is imported. TIMESERIES_CONFIG cannot redirect that write, since the
    preset sets the path itself.
    """
    environment = os.environ.copy()
    environment[ENV_VAR_NAME] = str(config.configuration_file)
    environment["HOME"] = str(Path(config.configuration_file).parent)
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

    The project root is added to `sys.path` because the module is imported by name,
    and `pytest` as a console script does not put the root on the path the way
    `python -m pytest` does.
    """
    from importlib import import_module

    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    module_name = Path(notebook_name).stem
    notebook = import_module(f"notebooks.{module_name}")

    outputs, definitions = notebook.app.run()
    print(outputs)
    print(definitions)
    return outputs


# ------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason="quickstart.py runs `Config(preset='default').save()`, and the preset hardcodes "
    "`configuration_file` to `$HOME/.config/ssb_timeseries/timeseries_config.json` when the "
    "config module is imported. In a subprocess HOME can be redirected, but in-process the "
    "path is already built, so the notebook would overwrite the user's own configuration.",
)
def test_marimo_quickstart_in_process(buildup_and_teardown):
    result = import_and_run_marimo_app(
        "quickstart.py",
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


def test_marimo_data_archiving_and_sharing(notebook_config):
    result = subprocess_run_marimo_notebook(
        "data-archiving-and-sharing.py",
        notebook_config,
    )

    assert result.returncode == 0


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
