import uuid
from pathlib import Path
from sys import platform

import pytest

from ssb_timeseries import fs
from ssb_timeseries.logging import ts_logger

# mypy: ignore-errors

BUCKET = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/"
JOVYAN = "/home/jovyan/series_data/"
HOME = str(Path.home())
IS_DAPLA = HOME == "/home/jovyan"


@pytest.mark.skip("Fix this later")
def test_bucket_exists_if_running_on_dapla() -> None:
    ts_logger.warning(f"Home directory is {HOME}")
    assert fs.exists(BUCKET)


def test_remove_prefix() -> None:
    assert (
        fs.remove_prefix("gs://ssb-prod-dapla-felles-data-delt")
        == "ssb-prod-dapla-felles-data-delt"
    )
    assert fs.remove_prefix("/home/jovyan") == "/home/jovyan"


def test_is_gcs() -> None:
    assert fs.is_gcs("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/")
    assert not fs.is_gcs("/home/jovyan")


def test_is_local() -> None:
    assert not fs.is_local("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/")
    assert fs.is_local("/home/jovyan")


def test_fs_type() -> None:
    assert fs.fs_type("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/") == "gcs"
    assert fs.fs_type("/home/jovyan") == "local"


def test_fs_path() -> None:
    assert fs.path(BUCKET, "a", "b", "c") == fs.path_to_str(
        "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/a/b/c"
    )
    assert fs.path(JOVYAN, "a", "b", "c") == fs.path_to_str(
        "/home/jovyan/series_data/a/b/c"
    )


@pytest.mark.skipif(platform != "linux", reason="Can not see GCS.")
def test_same_path() -> None:
    assert (
        fs.same_path(BUCKET, "/home/jovyan")
        == fs.path_to_str("/")
        # or fs.same_path(BUCKET, "/home/jovyan") == "\\"
    )
    assert fs.same_path(
        fs.path("/home/jovyan/a"),
        fs.path("/home/jovyan"),
    ) == fs.path("/home/jovyan")
    assert (
        fs.same_path(
            "/ssb-prod-dapla-felles-data-delt/poc-tidsserier",
            "/ssb-prod-dapla-felles-data-delt/poc-tidsserier/a",
        )
        == "/ssb-prod-dapla-felles-data-delt/poc-tidsserier"
    )


def test_home_exists() -> None:
    assert fs.exists(HOME)


def test_existing_subpath() -> None:
    long_path = Path(HOME) / f"this-dir-does-not-to-exist-{uuid.uuid4()}"
    assert fs.exists(HOME)
    assert not fs.exists(long_path)
    assert str(fs.existing_subpath(long_path)) == str(HOME)
