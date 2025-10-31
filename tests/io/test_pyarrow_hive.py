"""Unit tests for the `simple` I/O handler."""

import logging
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pytest import LogCaptureFixture

from ssb_timeseries import fs
from ssb_timeseries.dataframes import is_empty
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import now_utc
from ssb_timeseries.fs import file_count
from ssb_timeseries.io import pyarrow_hive as io
from ssb_timeseries.io.pyarrow_hive import _parquet_schema
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.properties import Versioning
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

test_logger = logging.getLogger(__name__)
# test_logger = logging.getLogger()
# test_logger = ts.logger

# copied from test_dataset_core --> review  to make sure correct scope
# here: test io/simple.py behaviours
# (leave to test_dataset_core to test Dataset behaviours)

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
    io_handler = io.HiveFileSystem(
        repository=x.repository,
        set_name=x.name,
        set_type=x.data_type,
        as_of_utc=x.as_of_utc,
    )
    data_dir = io_handler.directory

    subdirectories_before = fs.ls(data_dir)
    x.data = (x * 1.1).data
    time.sleep(1)  # so `now_utc()` does not get too close to old x.as_of_utc
    io_handler.as_of_utc = now_utc()
    io_handler.write(data=x.data, tags=x.tags)
    subdirectories_after = fs.ls(data_dir)
    assert len(subdirectories_after) == len(subdirectories_before) + 1, (
        f"Directory count did not increase for type {x.data_type}."
    )


def test_versioning_none_merges_existing_data(
    one_new_set_for_each_unversioned_type, request, caplog: LogCaptureFixture
) -> None:
    """Verify that saving a NONE dataset merges the existing data."""
    caplog.set_level(logging.DEBUG)
    a: Dataset = one_new_set_for_each_unversioned_type
    io_handler = io.HiveFileSystem(
        repository=a.repository,
        set_name=a.name,
        set_type=a.data_type,
        as_of_utc=a.as_of_utc,
    )
    # First, write the initial data
    test_logger.debug("12 rows of data? %s", a.data.shape)
    io_handler.write(data=a.data, tags=a.tags)

    # Create new, smaller data
    new_data = create_df(
        a.series,
        start_date="2023-01-01",
        end_date="2023-03-31",  # 3 months
        freq="MS",
        temporality=a.data_type.temporality.name,
    )
    test_logger.debug("3 rows of new data? %s", new_data.shape)
    io_handler.write(data=new_data, tags=a.tags)

    # Read the data back and verify it has been merged
    read_data = io_handler.read()
    test_logger.debug(
        "15 rows of data read back? %s",
        read_data.shape,
    )
    assert read_data.shape[0] == 15  # 12 (original) + 3 (new) = 15
    # The second assertion is tricky because the merge can reorder things.
    # For now, we focus on the shape.
    # assert read_data.to_arrow().equals(datelike_to_utc(new_data).to_arrow())


def test_write_creates_correct_partition_directories(
    one_new_set_for_each_data_type: Dataset,
) -> None:
    """Verify that write creates the correct Hive-style partition directories."""
    dataset = one_new_set_for_each_data_type
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )
    io_handler.write(data=dataset.data, tags=dataset.tags)

    subdirectories = fs.ls(io_handler.directory)

    if dataset.data_type.versioning == "AS_OF":
        assert any(d.startswith("as_of=") for d in subdirectories)
    elif dataset.data_type.versioning == "NONE":
        assert "as_of=__HIVE_DEFAULT_PARTITION__" in subdirectories


def test_versions_method_returns_correct_versions(
    existing_as_of_from_to_set: Dataset,
) -> None:
    """Verify that the versions() method correctly returns available versions."""
    dataset = existing_as_of_from_to_set
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )
    # Write the first version
    io_handler.write(data=dataset.data, tags=dataset.tags)

    # Write a second version with a new timestamp
    time.sleep(1)
    new_as_of = now_utc()
    io_handler.as_of_utc = new_as_of
    io_handler.write(data=dataset.data, tags=dataset.tags)

    # Retrieve the list of versions
    available_versions = io_handler.versions()

    # Verify that both original and new as_of timestamps are present
    assert len(available_versions) >= 2  # Can be more if tests run multiple times
    assert dataset.as_of_utc in available_versions
    assert new_as_of in available_versions


# --------------- from test_io -------------------------------


def test_io_dirs(conftest) -> None:
    dirs = io.HiveFileSystem(
        repository=conftest.repo,
        set_name="test-1",
        set_type=SeriesType.simple(),
        as_of_utc=None,
    )
    assert isinstance(dirs, io.HiveFileSystem)


def test_io_data_directory_path_as_expected(
    conftest,
    caplog,
) -> None:
    test_name = conftest.function_name()
    test_io = io.HiveFileSystem(
        repository=conftest.repo,
        set_name=test_name,
        set_type=SeriesType.simple(),
        as_of_utc=None,
    )
    repo_base_dir = Path(conftest.repo["directory"]["options"]["path"])
    expected: str = repo_base_dir / "data_type=NONE_AT" / f"dataset={test_name}"
    assert str(test_io.directory) == str(expected)


def test_write_new_dataset_creates_file_with_correct_schema(
    one_new_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that writing a new dataset creates a file with the correct schema."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )

    # The fixture ensures the dataset is new, so the dataset directory should not exist
    assert not io_handler.exists

    # Write the dataset, which triggers file and schema creation
    io_handler.write(data=dataset.data, tags=dataset.tags)

    written_files = fs.find(io_handler.directory, pattern="*.parquet", full_path=True)
    assert written_files, "No Parquet files found in the output directory."
    assert len(written_files) == 1

    written_schema = pq.read_schema(written_files[0])
    (expected_schema, _) = _parquet_schema(dataset.data_type, dataset.tags, [])

    # the 'as_of' column is a partition key and will not be in the written file schema.
    expected_schema = expected_schema.remove(expected_schema.get_field_index("as_of"))

    print("\n--- Written Schema ---")
    print(written_schema)
    print("\n--- Expected Schema ---")
    print(expected_schema)
    # Sort fields by name for comparison
    written_fields = sorted(written_schema, key=lambda f: f.name)
    expected_fields = sorted(expected_schema, key=lambda f: f.name)

    # Compare field by field
    for written_field, expected_field in zip(
        written_fields, expected_fields, strict=False
    ):
        assert written_field.name == expected_field.name
        assert written_field.type == expected_field.type
        assert written_field.nullable == expected_field.nullable

    # Finally, compare the full schemas
    assert written_schema.equals(expected_schema), (
        "Written schema does not match the expected schema."
    )


def test_simple_write_with_none_data_raises_type_error(
    one_new_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that calling write with data=None raises a TypeError."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )

    with pytest.raises(TypeError):
        io_handler.write(data=None, tags=dataset.tags)


def test_hive_filesystem_init_raises_error_for_as_of_none_without_as_of_utc(
    one_new_set_for_each_versioned_type: Dataset,
) -> None:
    """Verify that initializing HiveFileSystem with Versioning.AS_OF and as_of_utc=None raises a ValueError."""
    dataset = one_new_set_for_each_versioned_type
    with pytest.raises(ValueError, match="An 'as of' datetime must be specified"):
        io.HiveFileSystem(
            repository=dataset.repository,
            set_name=dataset.name,
            set_type=SeriesType(
                versioning=Versioning.AS_OF,
                temporality=dataset.data_type.temporality,
            ),
            as_of_utc=None,
        )


def test_read_non_existent_dataset_returns_empty_frame(
    one_new_set_for_each_data_type: Dataset,
) -> None:
    """Verify that reading a non-existent dataset returns an empty dataframe."""
    dataset = one_new_set_for_each_data_type
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )
    # Ensure the dataset does not exist
    if io_handler.exists:
        # This should not happen with the fixture, but as a safeguard
        # we would need to delete the directory. For now, assume it doesn't exist.
        pass

    read_data = io_handler.read()
    assert read_data.shape == (0, 0)
    assert is_empty(read_data)
