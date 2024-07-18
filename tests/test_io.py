import logging
from pathlib import Path

import pandas
import polars
import pyarrow

from ssb_timeseries import io
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors


def test_io_dirs() -> None:
    dirs = io.FileSystem(
        set_name="test-1", set_type=SeriesType.simple(), as_of_utc=None
    )
    assert isinstance(dirs, io.FileSystem)


def test_io_datadir_path_as_expected(
    conftest,
    caplog,
) -> None:
    test_name = conftest.function_name()
    test_io = io.FileSystem(
        set_name=test_name, set_type=SeriesType.simple(), as_of_utc=None
    )
    expected: str = Path(test_io.metadata_dir) / test_io.type_path / test_name
    assert str(test_io.data_dir) == str(expected)


def test_io_metadir_path_as_expected(
    conftest,
    caplog,
) -> None:
    test_name = conftest.function_name()
    set_type = SeriesType.simple()
    test_io = io.FileSystem(set_name=test_name, set_type=set_type, as_of_utc=None)
    # expected: str = Path(test_io.data_dir) / test_io.type_path / test_name
    expected: str = Path(test_io.root) / "metadata"
    assert str(test_io.metadata_dir) == str(expected)


# @pytest.mark.xfail
def test_io_parquet_schema_as_of_at(
    new_dataset_none_at,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_none_at
    test_io = io.FileSystem(
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )
    schema = test_io.parquet_schema(dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns())
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = io.tags_from_json(schema.field(key).metadata)
        ts_logger.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


# @pytest.mark.xfail
def test_io_parquet_schema_none_from_to(
    new_dataset_none_from_to,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_none_from_to
    test_io = io.FileSystem(
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )
    schema = test_io.parquet_schema(dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns())
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = io.tags_from_json(schema.field(key).metadata)
        ts_logger.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


# @pytest.mark.xfail
def test_io_parquet_schema_none_at(
    new_dataset_as_of_at,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_as_of_at
    test_io = io.FileSystem(
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )
    schema = test_io.parquet_schema(dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns())
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = io.tags_from_json(schema.field(key).metadata)
        ts_logger.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


# @pytest.mark.xfail
def test_io_parquet_schema_as_of_from_to(
    new_dataset_as_of_from_to,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_as_of_from_to
    test_io = io.FileSystem(
        set_name=dataset.name,
        set_type=dataset.data_type,
        as_of_utc=dataset.as_of_utc,
    )
    schema = test_io.parquet_schema(dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns())
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = io.tags_from_json(schema.field(key).metadata)
        ts_logger.debug(f"{key=}:\n{tags=}")
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
    ts_logger.debug(
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
    df = io.merge_data(x1, x2, {"valid_at"})
    ts_logger.debug(f"merge pandas dataframes:\nOLD\n{x1}\n\nNEW\n{x2}\n\nRESULT\n{df}")
    assert isinstance(df, pandas.DataFrame)
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
    x2 = polars.from_pandas(
        create_df(
            ["x", "y", "z"],
            start_date="2022-08-01",
            end_date="2022-12-03",
            freq="MS",
        )
    ).sort("valid_at")
    assert isinstance(x1, polars.DataFrame) and isinstance(x2, polars.DataFrame)
    df = io.merge_data(x1, x2, {"valid_at"})
    ts_logger.debug(
        f"merge polars dataframes:\nOLD\n{x1.to_pandas()}\n\nNEW:\n{x2.to_pandas()}\n\nRESULT:\n{df.to_pandas()}"
    )
    assert isinstance(df, polars.DataFrame)
    assert df.shape == (12, 4)
