"""docs/conftest.py is a reduced copy of tests/conftest.py.

It does set up and tear down for testing of tutorials,
making working configs and temp directories available.
"""

# from __future__ import annotations

from pathlib import Path

import pytest  # deptry: ignore[DEP004]

from ssb_timeseries import config
from ssb_timeseries.io import fs

# mypy: ignore-errors

_ENV_VAR_VALUE_BEFORE_TESTS = config.active_file()


# def pytest_configure(config):
#     """Pytest hook to configure plugins."""
#     try:
#         from typeguard import config as typeguard_config  # deptry: ignore[DEP004]
#
#         # Policy can be 'warn' (default), 'error', or 'ignore'
#         # 'ignore' will suppress the warning and let the tests pass.
#         # fix typeguard.TypeHintWarning: Cannot resolve forward reference 'DataFrame[Any]'
#         typeguard_config.forward_ref_policy = "ignore"
#     except ImportError:
#         pass  # typeguard is not installed

# @pytest.fixture(scope="session")
# def root_dir(tmp_path_factory):
#     root = tmp_path_factory.mktemp("tests")
#     yield root


def _repository_test_config(path: Path) -> dict[str, str]:
    """Configure repositories based on temp dir root path."""
    return {
        "test_1": {
            "name": "test_1",
            "directory": {
                "options": {
                    "path": str(path / "series_test_1"),
                },
                "handler": "simple-parquet",
            },
            "catalog": {
                "handler": "json",
                "options": {
                    "path": str(path / "metadata_test_1"),
                },
            },
            "default": True,
        },
        "test_2": {
            "name": "test_2",
            "directory": {
                "handler": "simple-parquet",
                "options": {
                    "path": str(path / "series_test_2"),
                },
            },
            "catalog": {
                "handler": "json",
                "options": {"path": str(path / "metadata_test_2")},
            },
        },
    }


def _snapshot_test_config(path: Path) -> dict[str, str]:
    """Configure snapshots based on temp dir root path."""
    return {
        "default": {
            "name": "snapshot-archive",
            "directory": {
                "handler": "snapshots",
                "options": {"path": str(path / "snapshots")},
            },
        },
    }


def _sharing_test_config(path: Path) -> dict[str, str]:
    """Return a sharing test configuration based on temp dir root path."""
    return {
        "default": {
            "directory": {
                "handler": "snapshots",
                "options": {"path": str(path / "shared" / "default")},
            }
        },
        "s123": {
            "directory": {
                "handler": "snapshots",
                "options": {"path": str(path / "shared" / "s123")},
            }
        },
        "s234": {
            "directory": {
                "handler": "snapshots",
                "options": {"path": str(path / "shared" / "s234")},
            }
        },
    }


@pytest.fixture(scope="module", autouse=True)
def buildup_and_teardown(
    tmp_path_factory,
) -> config.Config:
    """Reset config and logging between modules."""
    before_tests = config.CONFIG
    root_dir = tmp_path_factory.mktemp("tests")
    config_file_for_testing = str(
        fs.touch(root_dir / "config" / "config_for_tests.json")
    )
    assert config_file_for_testing != ""

    log_file_for_testing = fs.touch(root_dir / "logs" / "log_for_tests.log")
    log_config = {}

    config.active_file(config_file_for_testing)
    temp_configuration = config.Config(
        configuration_file=str(config_file_for_testing),
        log_file=str(log_file_for_testing),
        io_handlers=config.BUILTIN_IO_HANDLERS,
        repositories=_repository_test_config(root_dir),
        snapshots=_snapshot_test_config(root_dir),
        sharing=_sharing_test_config(root_dir),
        bucket=str(root_dir / "bucket"),
        logging=log_config,
        ignore_file=True,
    )
    temp_configuration.save()
    assert fs.exists(temp_configuration.configuration_file)

    yield temp_configuration

    if before_tests.configuration_file:
        before_tests.save()
    else:
        config.unset_env_var()

    active_config_after = config.active_file()
    assert active_config_after == _ENV_VAR_VALUE_BEFORE_TESTS
