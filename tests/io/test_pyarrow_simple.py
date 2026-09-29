"""Unit tests for the `simple` I/O handler."""

import logging
import time
from pathlib import Path

import pyarrow
import pytest
from pytest import LogCaptureFixture

# from ssb_timeseries.io import json_metadata
from ssb_timeseries.config import Config
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import now_utc
from ssb_timeseries.io import pyarrow_simple as io
from ssb_timeseries.io.dataset_ref import DatasetRef
from ssb_timeseries.io.fs import file_count
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

test_logger = logging.getLogger(__name__)
# test_logger = logging.getLogger()
# test_logger = ts.logger

# =============================== HELPERS ===============================


def data_path(repository: str | dict) -> str:
    """Get the data path configured for a repository, as the dispatcher would."""
    repo = (
        repository
        if isinstance(repository, dict)
        else Config.active().repositories[repository]
    )
    return str(repo["directory"]["options"]["path"])


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
        path=data_path(x.repository),
    )
    data_dir = io_handler._directory(x.ref)

    files_before = file_count(data_dir)
    x.data = (x * 1.1).data
    time.sleep(1)  # so `now_utc()` does not get too close to old x.as_of_utc
    x.as_of_utc = now_utc()
    io_handler.write(x.ref, data=x.data, tags=x.tags)
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
        path=data_path(a.repository),
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
    io_handler.write(a.ref, data=new_data, tags=a.tags)
    # Read the data back and verify the merge logic
    c = io_handler.read(a.ref)
    test_logger.debug(
        f"First write {len(a.data)} rows, second write {len(new_data)} rows --> combined {len(c)} rows."
    )
    # Expected: 12 (original) + 12 (new) - 6 (overlap) = 18
    assert len(c) == 18


def test_io_dirs(conftest) -> None:
    dirs = io.FileSystem(
        repository=conftest.repo,
        path=data_path(conftest.repo),
    )
    assert isinstance(dirs, io.FileSystem)


def test_io_data_directory_path_as_expected(
    conftest,
    caplog,
) -> None:
    test_name = conftest.function_name()
    data_type = SeriesType.simple()
    test_io = io.FileSystem(
        repository=conftest.repo,
        path=data_path(conftest.repo),
    )
    repo_base_dir = Path(data_path(conftest.repo))
    expected: str = repo_base_dir / "NONE_AT" / test_name
    ref = DatasetRef(name=test_name, data_type=data_type)
    assert str(test_io._directory(ref)) == str(expected)


def test_write_new_dataset_creates_file_with_correct_schema(
    one_new_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that writing a new dataset creates a file with the correct schema."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.FileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )

    assert not io_handler.exists(dataset.ref)
    io_handler.write(dataset.ref, data=dataset.data, tags=dataset.tags)
    assert io_handler.exists(dataset.ref)

    schema = pyarrow.parquet.read_schema(io_handler.fullpath(dataset.ref))
    expected_schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert schema.equals(expected_schema)


def test_a_versioned_dataset_cannot_be_written_without_an_as_of(
    one_new_set_for_each_versioned_type: Dataset,
) -> None:
    """Writing a versioned dataset needs to know which version to write.

    Reading and listing are fine without one, since there is simply
    nothing to return, so the requirement is checked when it first matters.
    """
    dataset = one_new_set_for_each_versioned_type
    ref = DatasetRef(
        name=dataset.name,
        data_type=dataset.data_type,
        as_of_utc=None,
    )

    assert not ref.is_identified
    # A read finds no version, and yields an empty frame rather than failing.
    read_result = io.FileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    ).read(ref)
    assert read_result.num_rows == 0

    # A write would have to invent a version, so it must fail.
    with pytest.raises(ValueError, match="An 'as of' datetime must be specified"):
        io.FileSystem(
            repository=dataset.repository,
            path=data_path(dataset.repository),
        ).write(ref, data=dataset.data, tags=dataset.tags)


def test_read_non_existent_dataset_returns_empty_frame(
    one_new_set_for_each_data_type: Dataset,
) -> None:
    """Verify that reading a non-existent dataset returns an empty dataframe."""
    dataset = one_new_set_for_each_data_type
    io_handler = io.FileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )
    # Ensure the dataset doesn't exist
    ref = DatasetRef(
        name="non_existent_dataset",
        data_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )
    assert not io_handler.exists(ref)
    read_data = io_handler.read(ref)
    assert read_data.shape[0] == 0


def test_write_propagates_filesystem_errors(
    one_new_set_for_each_data_type: Dataset,
    monkeypatch,
    caplog: LogCaptureFixture,
) -> None:
    """Filesystem failures must be raised instead of reported as successful writes."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.FileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )

    def fail_write(*_args, **_kwargs) -> None:
        raise OSError("forced write failure")

    monkeypatch.setattr(io.fs, "write_parquet", fail_write)

    with pytest.raises(OSError, match="forced write failure"):
        io_handler.write(dataset.ref, data=dataset.data, tags=dataset.tags)

    assert "DATASET.write.error" in caplog.text
    assert "DATASET.write.success" not in caplog.text


def test_simple_write_with_none_data_raises_type_error(
    one_new_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that calling write with data=None raises a TypeError."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.FileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )

    with pytest.raises(TypeError):
        io_handler.write(dataset.ref, data=None, tags=dataset.tags)
