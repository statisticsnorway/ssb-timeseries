from pathlib import Path

# mypy: ignore-errors
from ssb_timeseries import io
from ssb_timeseries.properties import SeriesType


def test_dataset_dirs() -> None:
    dirs = io.FileSystem(
        set_name="test-1", set_type=SeriesType.simple(), as_of_utc=None
    )
    assert isinstance(dirs, io.FileSystem)


def test_dataset_datadir_path_as_expected(conftest) -> None:
    test_name = conftest.function_name()
    test_io = io.FileSystem(
        set_name=test_name, set_type=SeriesType.simple(), as_of_utc=None
    )
    expected: str = Path(test_io.metadata_dir) / test_io.type_path / test_name
    assert str(test_io.data_dir) == str(expected)


def test_dataset_metadir_path_as_expected(conftest) -> None:
    test_name = conftest.function_name()
    set_type = SeriesType.simple()
    test_io = io.FileSystem(set_name=test_name, set_type=set_type, as_of_utc=None)
    # expected: str = Path(test_io.data_dir) / test_io.type_path / test_name
    expected: str = Path(test_io.root) / "metadata"
    assert str(test_io.metadata_dir) == str(expected)
