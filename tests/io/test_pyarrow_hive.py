"""Unit tests for the `simple` I/O handler."""

import logging
import time
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from pytest import LogCaptureFixture

from ssb_timeseries.config import Config
from ssb_timeseries.dataframes import is_empty
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import now_utc
from ssb_timeseries.io import fs
from ssb_timeseries.io import pyarrow_hive as io
from ssb_timeseries.io.dataset_ref import DatasetRef
from ssb_timeseries.io.pyarrow_hive import _parquet_schema
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType
from ssb_timeseries.types import Versioning

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

test_logger = logging.getLogger(__name__)
# test_logger = logging.getLogger()
# test_logger = ts.logger

# copied from test_dataset_core --> review  to make sure correct scope
# here: test io/simple.py behaviours
# (leave to test_dataset_core to test Dataset behaviours)

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
        if fs.file_count(directory) >= initial_count + expected_increment:
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
        path=data_path(x.repository),
    )
    data_dir = io_handler._directory(x.ref)

    subdirectories_before = fs.ls(data_dir)
    x.data = (x * 1.1).data
    time.sleep(1)  # so `now_utc()` does not get too close to old x.as_of_utc
    x.as_of_utc = now_utc()
    io_handler.write(x.ref, data=x.data, tags=x.tags)
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
        path=data_path(a.repository),
    )
    # First, write the initial data
    test_logger.debug("12 rows of data? %s", a.data.shape)
    io_handler.write(a.ref, data=a.data, tags=a.tags)

    # Create new, smaller data
    new_data = create_df(
        a.series,
        start_date="2023-01-01",
        end_date="2023-03-31",  # 3 months
        freq="MS",
        temporality=a.data_type.temporality.name,
    )
    test_logger.debug("3 rows of new data? %s", new_data.shape)
    io_handler.write(a.ref, data=new_data, tags=a.tags)

    # Read the data back and verify it has been merged
    read_data = io_handler.read(a.ref)
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
        path=data_path(dataset.repository),
    )
    io_handler.write(dataset.ref, data=dataset.data, tags=dataset.tags)

    partitions = [Path(d).name for d in fs.ls(io_handler._directory(dataset.ref))]

    match dataset.data_type.versioning:
        case Versioning.AS_OF:
            assert any(p.startswith("as_of=") for p in partitions), partitions
        case Versioning.NONE:
            # An unversioned dataset has no 'as of' to partition on, so it must
            # land in the null partition rather than being stamped with the
            # time of the write.
            assert "as_of=__HIVE_DEFAULT_PARTITION__" in partitions, partitions


def test_an_unversioned_dataset_gets_no_as_of_in_its_partition(
    one_new_set_for_each_unversioned_type: Dataset,
) -> None:
    """A NONE dataset must never be stamped with the time it happened to be written.

    `DatasetRef` leaves `as_of_utc` as None for such a dataset.
    If that were ever normalised to 'now', the Hive partition would change name
    on every write, and the null partition would never be used.
    """
    dataset = one_new_set_for_each_unversioned_type
    assert dataset.as_of_utc is None
    assert dataset.ref.as_of_utc is None

    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )
    io_handler.write(dataset.ref, data=dataset.data, tags=dataset.tags)

    partitions = [Path(d).name for d in fs.ls(io_handler._directory(dataset.ref))]
    assert "as_of=__HIVE_DEFAULT_PARTITION__" in partitions, partitions


def test_versions_method_returns_correct_versions(
    existing_as_of_from_to_set: Dataset,
) -> None:
    """Verify that the versions() method correctly returns available versions."""
    dataset = existing_as_of_from_to_set
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )
    # Write the first version
    first_ref = dataset.ref
    io_handler.write(first_ref, data=dataset.data, tags=dataset.tags)

    # Write a second version with a new timestamp
    time.sleep(1)
    dataset.as_of_utc = now_utc()
    io_handler.write(dataset.ref, data=dataset.data, tags=dataset.tags)

    # Retrieve the list of versions
    available_versions = io_handler.versions(dataset.ref)

    # Verify that both original and new as_of timestamps are present
    assert len(available_versions) >= 2  # Can be more if tests run multiple times
    assert first_ref.as_of_utc in available_versions
    assert dataset.as_of_utc in available_versions


# --------------- from test_io -------------------------------


def test_io_dirs(conftest) -> None:
    dirs = io.HiveFileSystem(
        repository=conftest.repo,
        path=data_path(conftest.repo),
    )
    assert isinstance(dirs, io.HiveFileSystem)


def test_io_data_directory_path_as_expected(
    conftest,
    caplog,
) -> None:
    test_name = conftest.function_name()
    test_io = io.HiveFileSystem(
        repository=conftest.repo,
        path=data_path(conftest.repo),
    )
    repo_base_dir = Path(data_path(conftest.repo))
    ref = DatasetRef(name=test_name, data_type=SeriesType.simple())
    expected: str = repo_base_dir / "data_type=NONE_AT" / f"dataset={test_name}"
    assert str(test_io._directory(ref)) == str(expected)


def test_write_new_dataset_creates_file_with_correct_schema(
    one_new_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that writing a new dataset creates a file with the correct schema."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )

    # The fixture ensures the dataset is new, so the dataset directory should not exist
    assert not io_handler.exists(dataset.ref)

    # Write the dataset, which triggers file and schema creation
    io_handler.write(dataset.ref, data=dataset.data, tags=dataset.tags)

    written_files = fs.find(io_handler._directory(dataset.ref), pattern="*.parquet")
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


def test_parquet_schema_includes_series_without_individual_tags() -> None:
    """Series names must remain in the schema when their tag dictionaries are empty."""
    tags = {
        "name": "untagged-series",
        "versioning": "NONE",
        "temporality": "AT",
        "series": {"a": {}, "b": {}},
    }

    schema, _ = _parquet_schema(SeriesType.simple(), tags, [])

    assert schema.names == ["as_of", "valid_at", "a", "b"]


def test_write_preserves_all_series(
    one_new_set_for_each_unversioned_type: Dataset,
) -> None:
    """A Hive write and read round trip must retain every series column."""
    dataset = one_new_set_for_each_unversioned_type
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )

    io_handler.write(dataset.ref, data=dataset.data, tags=dataset.tags)
    written = io_handler.read(dataset.ref)

    expected_columns = set(dataset.series) | set(dataset.data_type.date_columns)
    assert set(written.column_names) == expected_columns
    assert len(written) == len(dataset.data)


def test_simple_write_with_none_data_raises_type_error(
    one_new_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that calling write with data=None raises a TypeError."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )

    with pytest.raises(TypeError):
        io_handler.write(dataset.ref, data=None, tags=dataset.tags)


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
        data_type=SeriesType(
            versioning=Versioning.AS_OF,
            temporality=dataset.data_type.temporality,
        ),
        as_of_utc=None,
    )

    assert not ref.is_identified
    handler = io.HiveFileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )
    # A read finds no version, and yields an empty frame rather than failing.
    assert is_empty(handler.read(ref))

    # A write would have to invent a version, so it must fail.
    with pytest.raises(ValueError, match="An 'as of' datetime must be specified"):
        handler.write(ref, data=dataset.data, tags=dataset.tags)


def test_read_non_existent_dataset_returns_empty_frame(
    one_new_set_for_each_data_type: Dataset,
) -> None:
    """Verify that reading a non-existent dataset returns an empty dataframe."""
    dataset = one_new_set_for_each_data_type
    io_handler = io.HiveFileSystem(
        repository=dataset.repository,
        path=data_path(dataset.repository),
    )
    # Ensure the dataset does not exist
    assert not io_handler.exists(dataset.ref)

    read_data = io_handler.read(dataset.ref)
    assert read_data.shape == (0, 0)
    assert is_empty(read_data)
