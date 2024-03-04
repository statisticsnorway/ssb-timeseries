import os
import uuid
import pytest

from timeseries.io import CONFIG
from timeseries import fs
from timeseries.logging import ts_logger

BUCKET = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/"
JOVYAN = "/home/jovyan/series_data/"
HOME = os.getenv("HOME")
# GCS_VISIBLE = fs.exists(BUCKET)
IS_DAPLA = HOME == "/home/jovyan"


@pytest.mark.skipif(HOME == "/home/bernhard", reason="known other location")
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


def test_same_path() -> None:
    # ts_logger.warning(fs.same_path("/home/jovyan", "/home/jovyan/a", "/home/jovyan/b"))
    assert fs.same_path(BUCKET, "/home/jovyan") == "/"
    assert fs.same_path(
        os.path.join("/home/jovyan/a"), "/home/jovyan"
    ) == os.path.normpath("/home/jovyan")
    assert (
        fs.same_path(
            "/ssb-prod-dapla-felles-data-delt/poc-tidsserier",
            "/ssb-prod-dapla-felles-data-delt/poc-tidsserier/a",
        )
        == "/ssb-prod-dapla-felles-data-delt/poc-tidsserier"
    )


def test_root_and_home_exists() -> None:
    assert fs.exists("/")
    assert fs.exists(HOME)


def test_existing_subpath() -> None:
    long_path = os.path.join(HOME, f"this-dir-does-not-to-exist-{uuid.uuid4()}")
    assert fs.exists(HOME)
    assert not fs.exists(long_path)
    assert fs.existing_subpath(long_path) == HOME


# @pytest.mark.skipif(not GCS_VISIBLE, reason="Can not see GCS.")
# def test_mkdir_dapla() -> None:

#     ts_logger.warning(f"Can see{BUCKET}")
#     a = f"temp-dir-while-running-tests-{uuid.uuid4()}"
#     fs.mkdir(os.path.join(BUCKET, "tests", a, "b", "c"))
#     assert fs.exists(os.path.join(BUCKET, "tests", "a", "b", "c"))


# # @pytest.mark.skipif(IS_DAPLA, reason="... now we are in Kansas!")
# def test_mkdir_local() -> None:
#     a = f"temp-dir-while-running-tests-{uuid.uuid4()}"
#     short_path = os.path.join(HOME, a)
#     if fs.exists(short_path):
#         ts_logger.warning(f"The directory {short_path} already existed!")
#         assert False
#     else:
#         long_path = os.path.join(HOME, a, "b", "c", "d")
#         ts_logger.warning(f"Root: {CONFIG.bucket}")
#         ts_logger.warning(f"Attempting to create local fs directory: {long_path}")
#         fs.mkdir(long_path)

#         assert fs.exists(long_path)
