"""Unit tests for the `simple` I/O handler."""

import logging
import time
from pathlib import Path

import pyarrow
import pytest
from pytest import LogCaptureFixture

# from ssb_timeseries.io import json_metadata
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import now_utc
from ssb_timeseries.fs import file_count
from ssb_timeseries.io import simple as io
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

test_logger = logging.getLogger(__name__)
# test_logger = logging.getLogger()
# test_logger = ts.logger

# =============================== HELPERS ===============================


def check_file_count_change(
    directory: Path,
    initial_count: int,
    expected_increment: int = 1,
    timeout_seconds: float = 5.0,
    poll_interval: float = 0.1,
) -> None:
    """Polls a directory until the file count reaches the expected increment or the check times out."""
    start_time = time.monotonic()
    while time.monotonic() - start_time < timeout_seconds:
        if file_count(directory) >= initial_count + expected_increment:
            return True  # Success!
        time.sleep(poll_interval)

    return False


# ================================ TESTS ================================


def test_versioning_as_of_creates_new_file(
    one_new_set_for_each_versioned_type, caplog: LogCaptureFixture
) -> None:
    """Verify that saving an AS_OF dataset always creates a new file."""
    caplog.set_level(logging.DEBUG)
    x: Dataset = one_new_set_for_each_versioned_type
    io_handler = io.FileSystem(
        repository=x.repository,
        set_name=x.name,
        set_type=x.data_type,
        as_of_utc=x.as_of_utc,
    )
    data_dir = io_handler.directory

    files_before = file_count(data_dir)
    x.data = (x * 1.1).data
    time.sleep(1)  # so `now_utc()` does not get too close to old x.as_of_utc
    io_handler.as_of_utc = now_utc()
    io_handler.write(data=x.data, tags=x.tags)
    assert check_file_count_change(
        directory=data_dir,
        initial_count=files_before,
        timeout_seconds=20,
    ), f"File count did not increase for type {x.data_type}."


def test_versioning_none_appends_to_existing_file(
    one_existing_set_for_each_unversioned_type, caplog: LogCaptureFixture
) -> None:
    """Verify that saving a NONE dataset merges data into the existing file."""
    caplog.set_level(logging.DEBUG)
    a: Dataset = one_existing_set_for_each_unversioned_type
    io_handler = io.FileSystem(
        repository=a.repository,
        set_name=a.name,
        set_type=a.data_type,
        as_of_utc=a.as_of_utc,
    )

    # Create new data that overlaps partially with the existing data
    # Original data is for 12 months of 2022. New data is for 12 months starting July 2022.
    # This creates a 6-month overlap.
    new_data = create_df(
        a.series,
        start_date="2022-07-01",
        end_date="2023-06-30",  # 12 months
        freq="MS",
        temporality=a.data_type.temporality,
    )
    io_handler.write(data=new_data, tags=a.tags)
    # Read the data back and verify the merge logic
    c = io_handler.read()
    test_logger.debug(
        f"First write {len(a.data)} rows, second write {len(new_data)} rows --> combined {len(c)} rows."
    )
    # Expected: 12 (original) + 12 (new) - 6 (overlap) = 18
    assert len(c) == 18


def test_io_dirs(conftest) -> None:
    dirs = io.FileSystem(
        repository=conftest.repo,
        set_name="test-1",
        set_type=SeriesType.simple(),
        as_of_utc=None,
    )
    assert isinstance(dirs, io.FileSystem)


def test_io_data_directory_path_as_expected(
    conftest,
    caplog,
) -> None:
    test_name = conftest.function_name()
    test_io = io.FileSystem(
        repository=conftest.repo,
        set_name=test_name,
        set_type=SeriesType.simple(),
        as_of_utc=None,
    )
    repo_base_dir = Path(conftest.repo["directory"]["options"]["path"])
    expected: str = repo_base_dir / "NONE_AT" / test_name
    assert str(test_io.directory) == str(expected)


def test_write_new_dataset_creates_file_with_correct_schema(
    one_new_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that writing a new dataset creates a file with the correct schema."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.FileSystem(
        repository=dataset.repository,
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )

    assert not io_handler.exists
    io_handler.write(data=dataset.data, tags=dataset.tags)
    assert io_handler.exists

    schema = pyarrow.parquet.read_schema(io_handler.fullpath)
    expected_schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert schema.equals(expected_schema)


def test_simple_write_with_none_data_raises_type_error(
    one_new_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that calling write with data=None raises a TypeError."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.FileSystem(
        repository=dataset.repository,
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )

    with pytest.raises(TypeError):
        io_handler.write(data=None, tags=dataset.tags)
