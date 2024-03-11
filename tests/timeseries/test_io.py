import pytest

from timeseries import io
from timeseries.properties import SeriesType


def test_dataset_dirs() -> None:
    dirs = io.FileSystem(
        set_name="test-1", set_type=SeriesType.simple(), as_of_utc=None
    )
    assert isinstance(dirs, io.FileSystem)


def test_dataset_datadir_path_as_expected() -> None:
    dirs = io.FileSystem(
        set_name="test-2", set_type=SeriesType.simple(), as_of_utc=None
    )
    expected: str = f"{dirs.type_path}/test-2"
    assert dirs.data_dir == expected


def test_dataset_metadir_path_as_expected() -> None:
    set_type = SeriesType.simple()
    dirs = io.FileSystem(set_name="test-3", set_type=set_type, as_of_utc=None)
    expected: str = f"{dirs.type_path}/test-3"
    assert dirs.metadata_dir == expected
