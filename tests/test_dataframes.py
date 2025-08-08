import pytest
from pandas import DataFrame as PdDf
from polars import DataFrame as PlDf
from pyarrow import Table as PaTbl

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
