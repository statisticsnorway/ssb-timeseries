import os.path

from timeseries import io
from timeseries.properties import SeriesType as type


def test_dataset_dirs() -> None:
    dirs = io.DatasetDirectory(set_name="test-1", set_type=type("SIMPLE"))
    assert isinstance(dirs, io.DatasetDirectory)


def test_dataset_datadir_path_as_expected() -> None:
    set_name = "test-2"
    set_type = "SIMPLE"
    dirs = io.DatasetDirectory(set_name=set_name, set_type=set_type)
    expected: str = f"{dirs.root_dir}/{set_name}"
    print(expected)
    print(dirs.data_dir)
    assert dirs.data_dir == expected


def test_dataset_metadir_path_as_expected() -> None:
    set_name = "test-3"
    set_type = "simple"
    dirs = io.DatasetDirectory(set_name=set_name, set_type=set_type)
    expected: str = f"{dirs.root_dir}/{set_name}"
    print(expected)
    print(dirs.metadata_dir)
    assert dirs.metadata_dir == expected


def test_dataset_directories_created_() -> None:
    set_name = "test-mkdir"
    set_type = "simple"
    dirs = io.DatasetDirectory(set_name=set_name, set_type=set_type)
    dirs.makedirs()
    assert os.path.isdir(dirs.data_dir)
    assert os.path.isdir(dirs.metadata_dir)


def cleanup() -> None:
    pass
    # os.path.del(f"{dirs.root_dir}/{set_type}/{set_name}")
