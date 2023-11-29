import os.path

from timeseries import io
from timeseries.properties import SeriesType
from timeseries.dates import date_utc


def test_dataset_dirs() -> None:
    dirs = io.DatasetDirectory(
        set_name="test-1", set_type=SeriesType.simple(), as_of_utc=None
    )
    assert isinstance(dirs, io.DatasetDirectory)


def test_dataset_datadir_path_as_expected() -> None:
    dirs = io.DatasetDirectory(
        set_name="test-2", set_type=SeriesType.simple(), as_of_utc=None
    )
    expected: str = f"{dirs.type_path}/test-2"
    assert dirs.data_dir == expected


def test_dataset_metadir_path_as_expected() -> None:
    set_type = SeriesType.simple()
    dirs = io.DatasetDirectory(set_name="test-3", set_type=set_type, as_of_utc=None)
    expected: str = f"{dirs.type_path}/test-3"
    assert dirs.metadata_dir == expected


def test_dataset_directories_created_() -> None:
    set_name = "test-mkdir"
    dirs = io.DatasetDirectory(
        set_name=set_name, set_type=SeriesType.simple(), as_of_utc=None
    )
    dirs.makedirs()
    assert os.path.isdir(dirs.data_dir)
    assert os.path.isdir(dirs.metadata_dir)


def cleanup(dir: io.DatasetDirectory) -> None:
    pass
    # os.path.del(f"{dirs.type_path}/{set_type}/{set_name}")
