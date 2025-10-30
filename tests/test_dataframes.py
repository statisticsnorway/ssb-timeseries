import logging

import narwhals as nw
import pandas
import polars
import pyarrow as pa
import pytest
from pandas import DataFrame as PdDf
from polars import DataFrame as PlDf
from pyarrow import Table as PaTbl

from ssb_timeseries.dataframes import are_equal
from ssb_timeseries.dataframes import empty_frame
from ssb_timeseries.dataframes import is_df_like
from ssb_timeseries.dataframes import is_empty
from ssb_timeseries.dataframes import merge_data
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import datelike_to_utc
from ssb_timeseries.sample_data import create_df


def test_empty_frame_call_with_no_parameters_returns_df_with_shape_0_0() -> None:
    empty_df = empty_frame()
    assert is_df_like(empty_df)
    assert empty_df.shape == (0, 0)


@pytest.mark.parametrize(
    "implementation,object_type",
    [
        ("polars", PlDf),
        ("pl", PlDf),
        ("arrow", PaTbl),
        ("pyarrow", PaTbl),
        ("arrow", PaTbl),
        ("pa", PaTbl),
        ("pandas", PdDf),
        ("pd", PdDf),
    ],
)
def test_empty_frame_returns_empty_df_of_correct_type(
    implementation, object_type
) -> None:
    empty_df = empty_frame(implementation=implementation)
    assert is_df_like(empty_df)
    assert isinstance(empty_df, object_type)
    assert empty_df.shape == (0, 0)
    assert is_empty(empty_df)


@pytest.mark.parametrize(
    "implementation,object_type",
    [
        # ("pa",PaTbl),
        # ("arrow",PaTbl),
        # ("pyarrow",PaTbl),
        ("pl", PlDf),
        ("polars", PlDf),
        ("pd", PdDf),
        ("pandas", PdDf),
    ],
)
def test_empty_frame_call_with_list_of_column_names_returns_empty_dataframe_with_defined_columns(
    implementation, object_type
) -> None:
    empty_df = empty_frame(columns=["a", "b"], implementation=implementation)
    print(empty_df)
    assert is_df_like(empty_df)
    assert isinstance(empty_df, object_type)
    assert list(empty_df.columns) == ["a", "b"]


@pytest.mark.parametrize(
    "implementation,object_type",
    [
        # ("pa",PaTbl),
        # ("arrow",PaTbl),
        # ("pyarrow",PaTbl),
        ("pl", PlDf),
        ("polars", PlDf),
        ("pd", PdDf),
        ("pandas", PdDf),
    ],
)
def test_empty_frame_call_with_schema_returns_empty_dataframe_with_defined_columns(
    implementation, object_type
) -> None:
    schema = pa.schema([("a", "int64"), ("b", "string")])
    empty_df = empty_frame(schema=schema, implementation=implementation)
    print(empty_df)
    assert is_df_like(empty_df)
    assert isinstance(empty_df, object_type)
    assert list(empty_df.columns) == ["a", "b"]
    # TODO: test column datatypes


@pytest.mark.parametrize("implementation", ["pyarrow", "polars", "pandas"])
def test_are_equal_returns_true_for_multiple_dataframes_of_same_type_with_equal_column_names_and_values(
    implementation,
) -> None:
    a = {"foo": [1, 2], "bar": [6.0, 7.0]}
    b = {"foo": [1, 2], "bar": [6.0, 7.0]}
    c = {"foo": [1, 2], "bar": [6.0, 7.0]}
    a_nw = nw.from_dict(a, backend=implementation)
    b_nw = nw.from_dict(b, backend=implementation)
    c_nw = nw.from_dict(c, backend=implementation)

    assert are_equal(a_nw, b_nw, c_nw)


def test_are_equal_returns_true_for_multiple_dataframes_of_different_type_with_equal_column_names_and_values() -> (
    None
):
    a = {"foo": [1, 2], "bar": [6.0, 7.0]}
    a_pa = pa.Table.from_pydict(a)
    a_pl = nw.from_native(a_pa).to_polars()
    a_pd = nw.from_native(a_pl).to_pandas()

    assert are_equal(a_pa, a_pd, a_pl)


def test_are_equal_returns_false_if_shapes_are_different() -> None:
    a = {"foo": [1, 2, 3], "bar": [6.0, 7.0, 8.0]}
    b = {"foo": [1, 2], "bar": [6.1, 7.0]}
    a_pa = pa.Table.from_pydict(a)
    b_pa = pa.Table.from_pydict(b)

    assert not are_equal(a_pa, b_pa)


def test_are_equal_returns_false_if_any_values_are_different() -> None:
    a = {"foo": [1, 2], "bar": [6.0, 7.0]}
    b = {"foo": [1, 2], "bar": [6.1, 7.0]}
    a_pa = pa.Table.from_pydict(a)
    b_pa = pa.Table.from_pydict(b)

    assert not are_equal(a_pa, b_pa)


def test_are_equal_returns_false_if_columnn_names_are_different() -> None:
    a = {"foo": [1, 2], "bar": [6.0, 7.0]}
    b = {"foooo": [1, 2], "baz": [6.0, 7.0]}
    a_pa = pa.Table.from_pydict(a)
    b_pa = pa.Table.from_pydict(b)

    assert not are_equal(a_pa, b_pa)


def test_are_equal_returns_false_if_columnn_names_are_swapped() -> None:
    a = {"foo": [1, 2], "bar": [6.0, 7.0]}
    b = {"bar": [1, 2], "foo": [6.0, 7.0]}
    a_pa = pa.Table.from_pydict(a)
    b_pa = pa.Table.from_pydict(b)

    assert not are_equal(a_pa, b_pa)


def test_are_equal_returns_true_if_columnn_names_are_out_of_order() -> None:
    a = {"foo": [1, 2], "bar": [6.0, 7.0]}
    b = {"bar": [6.0, 7.0], "foo": [1, 2]}
    a_pa = pa.Table.from_pydict(a)
    b_pa = pa.Table.from_pydict(b)

    assert are_equal(a_pa, b_pa)


def test_io_merge_data_with_arrow_tables(
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    x1 = pa.Table.from_pandas(
        create_df(
            ["x", "y", "z"],
            start_date="2022-01-01",
            end_date="2022-09-03",
            freq="MS",
        )
    )
    x2 = pa.Table.from_pandas(
        create_df(
            ["x", "y", "z"],
            start_date="2022-07-01",
            end_date="2022-12-03",
            freq="MS",
        )
    )
    assert isinstance(x1, pa.Table) and isinstance(x2, pa.Table)
    df = merge_data(x1, x2, {"valid_at"})
    logging.debug(
        f"merge arrow tables:\nOLD\n{x1.to_pandas()}\n\nNEW\n{x2.to_pandas()}\n\nRESULT\n{df.to_pandas()}"
    )
    df = merge_data(x1, x2, {"valid_at"})
    logging.debug(
        f"merge arrow tables:\nOLD\n{x1.to_pandas()}\n\nNEW\n{x2.to_pandas()}\n\nRESULT\n{df.to_pandas()}"
    )
    assert isinstance(df, pa.Table)
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
    df = merge_data(x1, x2, ["valid_at"])
    logging.debug(f"merge pandas dataframes:\nOLD\n{x1}\n\nNEW\n{x2}\n\nRESULT\n{df}")
    assert isinstance(df, pa.Table)
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
    df = merge_data(x1, x2, {"valid_at"})
    logging.debug(
        f"merge polars dataframes:\nOLD\n{x1.to_pandas()}\n\nNEW:\n{x2.to_pandas()}\n\nRESULT:\n{df.to_pandas()}"
    )
    #   assert isinstance(df, polars.DataFrame)
    assert isinstance(df, pa.Table)
    assert len(df) > len(x1)
    assert len(df) > len(x2)
    assert len(df) < len(x1) + len(x2)
    assert df.shape == (12, 4)


@pytest.mark.parametrize(
    "temporality, date_cols, start_dates, end_dates, expected_rows, overlap_date",
    [
        (
            "AT",
            ["valid_at"],
            ("2022-01-01", "2022-02-01"),
            ("2022-03-01", "2022-04-01"),
            4,
            "2022-02-01",
        ),
        (
            "FROM_TO",
            ["valid_from", "valid_to"],
            ("2022-01-01", "2022-07-01"),
            ("2022-12-31", "2023-06-30"),
            18,
            "2022-07-01",
        ),
    ],
)
def test_merge_data_replaces_overlapping_values(
    temporality, date_cols, start_dates, end_dates, expected_rows, overlap_date, caplog
):
    """Verify merge_data correctly replaces values for both AT and FROM_TO temporalities."""
    caplog.set_level(logging.DEBUG)
    old_df = create_df(
        ["x"],
        start_date=start_dates[0],
        end_date=end_dates[0],
        freq="MS",
        temporality=temporality,
        midpoint=100,
    )
    new_df = create_df(
        ["x"],
        start_date=start_dates[1],
        end_date=end_dates[1],
        freq="MS",
        temporality=temporality,
        midpoint=200,
    )

    merged_df = merge_data(old_df, new_df, date_cols, temporality=temporality)

    assert merged_df.shape[0] == expected_rows

    # Verify the value for an overlapping date is from the new_df
    overlap_dt = date_utc(overlap_date)
    key_col = date_cols[0]
    # Convert to Narwhals DataFrame for filtering
    merged_nw_df = nw.from_native(merged_df)
    overlapping_row = merged_nw_df.filter(nw.col(key_col) == overlap_dt)

    assert overlapping_row.shape[0] == 1
    assert (
        overlapping_row["x"].item() > 150
    )  # Value should be from new_df (midpoint 200)


def test_merge_data_raises_error_on_different_temporalities(
    caplog, xyz_at, xyz_from_to
):
    """Verify that merging dataframes with different temporalities raises a ValueError."""
    caplog.set_level(logging.DEBUG)
    at_df = xyz_at
    from_to_df = xyz_from_to

    with pytest.raises(ValueError, match=r"No matching date columns;.*"):
        merge_data(at_df, from_to_df, ["valid_at"], temporality="AT")
