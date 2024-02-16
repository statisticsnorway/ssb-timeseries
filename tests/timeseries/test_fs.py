import os
import pytest

from timeseries.io import CONFIG
from timeseries import fs
from timeseries.logging import ts_logger

BUCKET = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/"
JOVYAN = "/home/jovyan/sample-data/series/"
HOME = os.environ.get("HOME")
IS_DAPLA = HOME == "/home/jovyan"


def test_remove_prefix() -> None:

    assert fs.remove_prefix(BUCKET) == "ssb-prod-dapla-felles-data-delt/poc-tidsserier/"
    assert fs.remove_prefix(JOVYAN) == JOVYAN


def test_is_gcs() -> None:

    assert fs.is_gcs(BUCKET)
    assert not fs.is_gcs(JOVYAN)


def test_is_local() -> None:

    assert not fs.is_local(BUCKET)
    assert fs.is_local(JOVYAN)


def test_fs_type() -> None:

    assert fs.fs_type(BUCKET) == "gcs"
    assert fs.fs_type(JOVYAN) == "local"


def test_same_path() -> None:
    ts_logger.warning(fs.same_path(JOVYAN, JOVYAN, os.path.join(JOVYAN, "b")))
    assert fs.same_path(BUCKET, JOVYAN) == "/"
    assert fs.same_path(os.path.join(JOVYAN, "a"), JOVYAN) == os.path.normpath(JOVYAN)
    assert (
        fs.same_path(BUCKET, os.path.join(BUCKET, "a"))
        == "/ssb-prod-dapla-felles-data-delt/poc-tidsserier"
    )


def test_root_and_home_exists() -> None:
    assert fs.exists("/")
    assert fs.exists(HOME)


def test_existing_subpath() -> None:
    long_path = os.path.join(HOME, "a", "b", "c", "d")
    if fs.exists(HOME) and not fs.exists(long_path):
        ts_logger.warning(fs.existing_subpath(long_path))

    assert fs.existing_subpath(long_path) == HOME


@pytest.mark.skipif(not IS_DAPLA, reason="I do not think we are in Kansas anymore.")
def test_mkdir_dapla() -> None:

    ts_logger.warning(BUCKET)
    fs.mkdir(os.path.join(BUCKET, "tests", "a", "b", "c"))
    assert fs.exists(os.path.join(BUCKET, "tests", "a", "b", "c"))


@pytest.mark.skipif(IS_DAPLA, reason="... now we are in Kansas!")
def test_mkdir_local() -> None:
    short_path = os.path.join(HOME, "a")
    long_path = os.path.join(HOME, "a", "b", "c", "d")
    if fs.exists(HOME) and not fs.exists(short_path):
        fs.mkdir
    ts_logger.warning(CONFIG.bucket)

    assert False
