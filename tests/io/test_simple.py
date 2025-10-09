import logging
import uuid
from pathlib import Path
from time import sleep

import pandas
import polars
import pyarrow
import pytest
from pytest import LogCaptureFixture

# from ssb_timeseries.io import json_metadata
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import datelike_to_utc
from ssb_timeseries.dates import now_utc
from ssb_timeseries.fs import file_count
from ssb_timeseries.io import DataIO
from ssb_timeseries.io import simple as io
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

test_logger = logging.getLogger(__name__)
# test_logger = logging.getLogger()
# test_logger = ts.logger

# copied from test_dataset_core --> review  to make sure correct scope
# here: test io/simple.py behaviours
# (leave to test_dataset_core to test Dataset behaviours)


@pytest.mark.skip("TO DO: test the right thing")
def test_read_versioned_data_partitions_by_as_of(
    caplog: LogCaptureFixture,
    existing_estimate_set: Dataset,
    conftest,
):
    caplog.set_level(logging.DEBUG)

    new_set_name = conftest.function_name_hex()
    new_set = existing_estimate_set.copy(new_set_name)
    versions_before_saving = new_set.versions()
    assert isinstance(new_set, Dataset)
    assert len(versions_before_saving) == 0

    as_of_dates = [
        "2024-01-01",
        "2024-02-01",
        "2024-03-01",
        "2024-04-01",
        "2024-05-01",
        "2024-06-01",
    ]
    for d in as_of_dates:
        new_set.as_of_utc = date_utc(d)
        new_set.data = create_df(
            new_set.series, start_date="2023-01-01", end_date=d, freq="MS"
        )
        new_set.save()

    # TO DO: this works, but we should check stored returned columns / Hive partitioning, not just the versions!
    versions_after_saving = new_set.versions()
    assert len(versions_after_saving) > len(versions_before_saving)


def test_versioning_as_of_creates_new_file(
    existing_estimate_set: Dataset, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    x = existing_estimate_set
    y = x * 1.1
    files_before = file_count(DataIO(x).dh.directory)
    x.as_of_utc = now_utc()
    x.data = y.data
    x.save()
    sleep(2)
    files_after = file_count(DataIO(x).dh.directory)
    assert files_after == files_before + 1


def test_versioning_none_appends_to_existing_file(
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    dataset_name = f"test-merge-{uuid.uuid4().hex}"
    dataset_type = SeriesType.simple()
    a = Dataset(
        name=dataset_name,
        data_type=dataset_type,
        data=create_df(
            ["x", "y", "z"], start_date="2022-01-01", end_date="2023-12-03", freq="MS"
        ),
    )
    a.save()

    b = Dataset(
        name=dataset_name,
        data_type=dataset_type,
        data=create_df(
            ["x", "y", "z"], start_date="2023-01-01", end_date="2024-12-03", freq="MS"
        ),
    )
    b.save()

    c = Dataset(name=dataset_name, data_type=dataset_type)
    test_logger.debug(
        f"DATASET: {a.name}: First write {len(a.data)} rows, second write {len(b.data)} rows (50% new) --> combined {len(c.data)} rows."
    )
    test_logger.debug(f"{a.data}\n{b.data}\n{c.data}\n{len(c.data)}")
    assert len(c.data) > len(a.data)
    assert len(c.data) > len(b.data)
    assert len(c.data) < len(a.data) + len(b.data)


# --------------- from test_io -------------------------------


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
    repo_base_dir = Path(conftest.repo["directory"]["path"])
    expected: str = repo_base_dir / "NONE_AT" / test_name
    assert str(test_io.directory) == str(expected)


def test_io_parquet_schema_as_of_at(
    new_dataset_none_at,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_none_at
    schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns)
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = io.tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


def test_io_parquet_schema_none_from_to(
    new_dataset_none_from_to,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_none_from_to
    schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns)
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = io.tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


def test_io_parquet_schema_none_at(
    new_dataset_as_of_at,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_as_of_at
    schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns)
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = io.tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


def test_io_parquet_schema_as_of_from_to(
    new_dataset_as_of_from_to,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_as_of_from_to
    schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns)
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = io.tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


def test_io_merge_data_with_arrow_tables(
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    x1 = pyarrow.Table.from_pandas(
        create_df(
            ["x", "y", "z"],
            start_date="2022-01-01",
            end_date="2022-09-03",
            freq="MS",
        )
    )
    x2 = pyarrow.Table.from_pandas(
        create_df(
            ["x", "y", "z"],
            start_date="2022-07-01",
            end_date="2022-12-03",
            freq="MS",
        )
    )
    assert isinstance(x1, pyarrow.Table) and isinstance(x2, pyarrow.Table)
    df = io.merge_data(x1, x2, {"valid_at"})
    logging.debug(
        f"merge arrow tables:\nOLD\n{x1.to_pandas()}\n\nNEW\n{x2.to_pandas()}\n\nRESULT\n{df.to_pandas()}"
    )
    assert isinstance(df, pyarrow.Table)
    assert df.shape == (12, 4)


def test_io_merge_data_with_pandas_dataframes(
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    x1 = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    x2 = create_df(
        ["x", "y", "z"],
        start_date="2022-07-01",
        end_date="2022-12-03",
        freq="MS",
    )
    assert isinstance(x1, pandas.DataFrame) and isinstance(x2, pandas.DataFrame)
    df = io.merge_data(x1, x2, ["valid_at"])
    logging.debug(f"merge pandas dataframes:\nOLD\n{x1}\n\nNEW\n{x2}\n\nRESULT\n{df}")
    assert isinstance(df, pyarrow.Table)
    # assert isinstance(df, pandas.DataFrame)
    assert df.shape == (12, 4)


def test_io_merge_data_with_polars_dataframes(
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    x1 = polars.from_pandas(
        create_df(
            ["x", "y", "z"],
            start_date="2022-01-01",
            end_date="2022-08-03",
            freq="MS",
        )
    ).sort("valid_at")
    x1 = datelike_to_utc(x1)
    x2 = polars.from_pandas(
        create_df(
            ["x", "y", "z"],
            start_date="2022-08-01",
            end_date="2022-12-03",
            freq="MS",
        )
    ).sort("valid_at")
    x2 = datelike_to_utc(x2)
    assert isinstance(x1, polars.DataFrame) and isinstance(x2, polars.DataFrame)
    df = io.merge_data(x1, x2, {"valid_at"})
    logging.debug(
        f"merge polars dataframes:\nOLD\n{x1.to_pandas()}\n\nNEW:\n{x2.to_pandas()}\n\nRESULT:\n{df.to_pandas()}"
    )
    #   assert isinstance(df, polars.DataFrame)
    assert isinstance(df, pyarrow.Table)
    assert len(df) > len(x1)
    assert len(df) > len(x2)
    assert len(df) < len(x1) + len(x2)
    assert df.shape == (12, 4)
