import logging
import uuid
from pathlib import Path
from sys import platform

import polars
import pyarrow
import pytest

from ssb_timeseries import fs
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors

BUCKET = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/"
JOVYAN = "/home/jovyan/series_data/"
HOME = str(Path.home())
IS_DAPLA = HOME == "/home/jovyan"


@pytest.fixture(scope="function", autouse=True)
def df():
    simple_data = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-06-03",
        freq="MS",
    )
    yield simple_data


@pytest.mark.skip("Fix this later")
def test_bucket_exists_if_running_on_dapla() -> None:
    ts_logger.warning(f"Home directory is {HOME}")
    assert fs.exists(BUCKET)


def test_remove_prefix() -> None:
    assert (
        fs.remove_prefix("gs://ssb-prod-dapla-felles-data-delt")
        == "ssb-prod-dapla-felles-data-delt"
    )
    assert fs.remove_prefix("/home/jovyan") == "/home/jovyan"


def test_is_gcs() -> None:
    assert fs.is_gcs("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/")
    assert not fs.is_gcs("/home/jovyan")


def test_is_local() -> None:
    assert not fs.is_local("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/")
    assert fs.is_local("/home/jovyan")


def test_fs_type() -> None:
    assert fs.fs_type("gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/") == "gcs"
    assert fs.fs_type("/home/jovyan") == "local"


def test_fs_path() -> None:
    assert fs.path(BUCKET, "a", "b", "c") == fs.path_to_str(
        "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier/a/b/c"
    )
    assert fs.path(JOVYAN, "a", "b", "c") == fs.path_to_str(
        "/home/jovyan/series_data/a/b/c"
    )


@pytest.mark.skipif(platform != "linux", reason="Can not see GCS.")
def test_same_path() -> None:
    assert (
        fs.same_path(BUCKET, "/home/jovyan")
        == fs.path_to_str("/")
        # or fs.same_path(BUCKET, "/home/jovyan") == "\\"
    )
    assert fs.same_path(
        fs.path("/home/jovyan/a"),
        fs.path("/home/jovyan"),
    ) == fs.path("/home/jovyan")
    assert (
        fs.same_path(
            "/ssb-prod-dapla-felles-data-delt/poc-tidsserier",
            "/ssb-prod-dapla-felles-data-delt/poc-tidsserier/a",
        )
        == "/ssb-prod-dapla-felles-data-delt/poc-tidsserier"
    )


def test_home_exists() -> None:
    assert fs.exists(HOME)


def test_existing_subpath() -> None:
    long_path = Path(HOME) / f"this-dir-does-not-to-exist-{uuid.uuid4()}"
    assert fs.exists(HOME)
    assert not fs.exists(long_path)
    assert str(fs.existing_subpath(long_path)) == str(HOME)


def test_touch_creates_file_rm_removes_it(tmp_path) -> None:
    long_path = tmp_path / f"this-dir-does-not-to-exist-{uuid.uuid4()}/file.txt"
    assert not fs.exists(long_path)
    fs.touch(long_path)
    assert fs.exists(long_path)
    fs.rm(long_path)
    assert not fs.exists(long_path)


def test_to_arrow(caplog, tmp_path, df) -> None:
    caplog.set_level(logging.DEBUG)
    assert not isinstance(df, pyarrow.Table)
    table = fs.to_arrow(df)
    assert isinstance(table, pyarrow.Table)


def test_write_parquet_with_no_schema_creates_a_file(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    assert not fs.exists(temp_file)
    fs.write_parquet(
        path=temp_file,
        data=fs.to_arrow(df),
        schema=None,
    )
    assert fs.exists(temp_file)
    xyz = fs.pandas_read_parquet(path=temp_file)
    assert all(df == xyz)


def test_write_parquet_with_hardcoded_schema_creates_a_file(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    schema = pyarrow.schema(
        [
            pyarrow.field("valid_at", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
            pyarrow.field("z", pyarrow.float64(), nullable=True),
        ]
    )
    assert not fs.exists(temp_file)
    fs.write_parquet(
        path=temp_file,
        data=fs.to_arrow(df),
        schema=schema,
    )
    assert fs.exists(temp_file)


def test_write_parquet_fails_if_date_columns_does_not_match_schema(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    # df has valid_at datecolumn
    # --> create schema with valid_from and valid_to
    schema_with_wrong_date_columns = pyarrow.schema(
        [
            pyarrow.field("valid_from", pyarrow.date64(), nullable=False),
            pyarrow.field("valid_to", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
            pyarrow.field("z", pyarrow.float64(), nullable=True),
        ]
    )
    with pytest.raises(KeyError):
        fs.write_parquet(
            path=temp_file,
            data=fs.to_arrow(df),
            schema=schema_with_wrong_date_columns,
        )


#def test_write_parquet_raises_key_error_if_df_contains_column_not_defined_in_schema(
def test_write_parquet_handles_that_df_contains_column_not_defined_in_schema_but_writes_only_the_defined_ones(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    data_with_z = df
    # df has columns valid_at, x, y, z
    # --> create schema with only valid_at, x, y
    schema_without_z = pyarrow.schema(
        [
            pyarrow.field("valid_at", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
        ]
    )
    # with pytest.raises(KeyError):
    #     fs.write_parquet(
    #         path=temp_file,
    #         data=data_with_z,
    #         schema=schema_without_z,
    #     )
    fs.write_parquet(
        path=temp_file,
        data=data_with_z,
        schema=schema_without_z,
    )
    assert fs.exists(temp_file)
    read_back = fs.pandas_read_parquet(path=temp_file)
    assert sorted(read_back.columns) == sorted(schema_without_z.names)
    #assert sorted(read_back.columns) == sorted(df.columns)


def test_write_parquet_handles_that_df_and_schema_columns_are_not_in_same_order(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    # df has columns valid_at, x, y, z
    # --> create schema with same columns in different order: valid_at, x, z, y
    schema_with_columns_out_of_order = pyarrow.schema(
        [
            pyarrow.field("valid_at", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("z", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
        ]
    )
    # writing will reorder the data columns to match the schema
    fs.write_parquet(
        path=temp_file,
        data=fs.to_arrow(df),
        schema=schema_with_columns_out_of_order,
    )
    assert fs.exists(temp_file)


def test_write_parquet_fails_if_df_does_not_contain_all_schema_columns(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)
    temp_file = tmp_path / "no_schema_sample.parquet"
    # df has columns x, y, z
    # --> create schema with x and y
    schema_with_extra_columns = pyarrow.schema(
        [
            pyarrow.field("valid_at", pyarrow.date64(), nullable=False),
            pyarrow.field("x", pyarrow.float64(), nullable=True),
            pyarrow.field("y", pyarrow.float64(), nullable=True),
            pyarrow.field("z", pyarrow.float64(), nullable=True),
            pyarrow.field("æ", pyarrow.float64(), nullable=True),
            pyarrow.field("ø", pyarrow.float64(), nullable=True),
            pyarrow.field("å", pyarrow.float64(), nullable=True),
        ]
    )
    with pytest.raises(KeyError):
        fs.write_parquet(
            path=temp_file,
            data=fs.to_arrow(df),
            schema=schema_with_extra_columns,
        )


def test_write_parquet_supports_pandas_df_input(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)

    # df is already pandas
    temp_file = tmp_path / "pandas_df.parquet"
    fs.write_parquet(
        path=temp_file,
        data=df,
        schema=None,
    )
    assert fs.exists(temp_file)


def test_write_parquet_supports_polars_df_input(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)

    # polars
    temp_file = tmp_path / "polars_df.parquet"
    fs.write_parquet(
        path=temp_file,
        data=polars.from_pandas(df),
        schema=None,
    )
    assert fs.exists(temp_file)


def test_write_parquet_supports_arrow_table_input(
    caplog,
    tmp_path,
    df,
) -> None:
    caplog.set_level(logging.DEBUG)

    # arrow
    temp_file = tmp_path / "arrow_table.parquet"
    fs.write_parquet(
        path=temp_file,
        data=fs.to_arrow(df),
        schema=None,
    )
    assert fs.exists(temp_file)
