import narwhals as nw
import pyarrow as pa
import pytest
from pandas import DataFrame as PdDf
from polars import DataFrame as PlDf
from pyarrow import Table as PaTbl

from ssb_timeseries.dataframes import are_equal
from ssb_timeseries.dataframes import empty_frame
from ssb_timeseries.dataframes import is_df_like
from ssb_timeseries.dataframes import is_empty


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
