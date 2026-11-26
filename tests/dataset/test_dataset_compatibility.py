"""Tests conversions of Dataset to other dataframe libraries, an compliance with standards/protocols."""

from __future__ import annotations

import narwhals as nw
import narwhals.selectors as ncs
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.types import NONE_AT
from ssb_timeseries.types import NONE_FROM_TO


def test_dataset_np_returns_numpy_nd_array(existing_simple_set) -> None:
    test_set = existing_simple_set
    assert isinstance(test_set.np, np.ndarray)


def test_dataset_nw_returns_narwhals_frame(existing_simple_set) -> None:
    test_set = existing_simple_set
    assert isinstance(test_set.nw, nw.DataFrame)


def test_dataset_pa_returns_pyarrow_table(existing_simple_set) -> None:
    test_set = existing_simple_set
    assert isinstance(test_set.pa, pa.Table)


def test_dataset_pd_returns_pandas_df(existing_simple_set) -> None:
    test_set = existing_simple_set
    assert isinstance(test_set.pd, pd.DataFrame)


def test_dataset_pl_returns_polars_df(existing_simple_set) -> None:
    test_set = existing_simple_set
    assert isinstance(test_set.pl, pl.DataFrame)


def test_dataset_numeric_array_returns_numpy_nd_array(existing_simple_set) -> None:
    test_set = existing_simple_set
    assert isinstance(test_set.numeric_array(), np.ndarray)


def test_dataset_numeric_array_series_argument_returns_selected_subset(
    existing_simple_set,
) -> None:
    """The series argument selects exactly the named columns, in caller order."""
    test_set = existing_simple_set
    selected = list(reversed(test_set.numeric_columns[:2]))

    subset = test_set.numeric_array(selected)

    assert subset.shape == (test_set.pd.shape[0], 2)
    np.testing.assert_array_equal(subset, test_set.pd[selected].to_numpy())


def test_dataset_init_sorts_series_columns() -> None:
    """New datasets use the same sorted series order as data read from disk."""
    data = pd.DataFrame(
        {
            "valid_at": pd.to_datetime(["2025-01-31"], utc=True),
            "z": [1.0],
            "flag": [True],
            "a": [2.0],
        }
    )

    ds = Dataset(name="sorted", data_type=NONE_AT, data=data, find_existing=False)

    assert list(ds.pd.columns) == ["valid_at", "a", "flag", "z"]
    assert ds._columns(ncs.numeric() | ncs.boolean(), sort=False) == ["a", "flag", "z"]


def test_dataset_boolean_and_datetime_columns_are_sorted() -> None:
    """Public column-name properties return sorted names."""
    data = pd.DataFrame(
        {
            "valid_to": pd.to_datetime(["2025-02-28"], utc=True),
            "valid_from": pd.to_datetime(["2025-01-31"], utc=True),
            "z_flag": [True],
            "a_flag": [False],
        }
    )
    ds = Dataset(
        name="sorted-properties",
        data_type=NONE_FROM_TO,
        data=data,
        find_existing=False,
    )

    assert ds.datetime_columns == ["valid_from", "valid_to"]
    assert ds.boolean_columns == ["a_flag", "z_flag"]


def test_dataset_numeric_array_represents_booleans_as_floats() -> None:
    """Booleans share a numeric dtype with the other value columns."""
    data = pd.DataFrame(
        {
            "valid_at": pd.to_datetime(["2025-01-31"], utc=True),
            "flag": [True],
            "amount": [2],
        }
    )
    ds = Dataset(name="mixed", data_type=NONE_AT, data=data, find_existing=False)

    values = ds.numeric_array()

    assert values.dtype.name == "float64"
    np.testing.assert_array_equal(values, [[2.0, 1.0]])


def test_dataset_numeric_array_rejects_non_value_columns() -> None:
    """Explicit series selections must agree with numeric_array's datatypes."""
    data = pd.DataFrame(
        {
            "valid_at": pd.to_datetime(["2025-01-31"], utc=True),
            "a": [1.0],
        }
    )
    ds = Dataset(name="dates", data_type=NONE_AT, data=data, find_existing=False)

    with pytest.raises(ValueError, match="numeric or boolean series only"):
        ds.numeric_array("valid_at")


def test_dataset_np_returns_numeric_array_plus_datetime_columns(
    existing_estimate_set,
) -> None:
    test_set = existing_estimate_set
    c = len(test_set.datetime_columns)

    date_cols = test_set.np[:, :c]
    num_cols = test_set.np[:, c:]
    num_arr = test_set.numeric_array()

    assert np.all(num_cols == num_arr)

    # we would expect this to hold:
    # assert num_cols.dtype == num_arr.dtype

    # numeric_array() returns numeric data, as one would expect
    assert num_arr.dtype.name == "float64"
    # .np = .nw.to_numpy() --> 'object' type(s) returned is not datetime + numeric
    assert date_cols.dtype.name == "object"
    assert num_cols.dtype.name == "object"
    # potential drag on performance if not coerced into datetime / numeric
    # ... but that cannot be done for np.ndarray
    # ... hence __array__ uses .numeric_array()
    # ... and is different from .np


def test_init_np_returns_numpy_nd_array(existing_simple_set) -> None:
    test_set = existing_simple_set
    np_array = np.array(test_set)
    assert isinstance(np_array, np.ndarray)
    assert np_array.dtype.name == "float64"
    assert np.all(np_array == test_set.numeric_array())


def test_init_pa_returns_pyarrow_table(existing_simple_set) -> None:
    from pyarrow.interchange import from_dataframe

    test_set = existing_simple_set
    pa_table = from_dataframe(test_set)
    assert isinstance(pa_table, pa.Table)
    assert pa_table == test_set.pa


def test_init_pd_returns_pandas_df(existing_simple_set) -> None:
    from pandas.api.interchange import from_dataframe

    test_set = existing_simple_set
    pd_df = from_dataframe(test_set)
    assert isinstance(pd_df, pd.DataFrame)
    assert all(pd_df == test_set.pd)


def test_init_pl_returns_polars_df(existing_simple_set) -> None:
    test_set = existing_simple_set
    pl_df = pl.DataFrame(test_set)
    assert isinstance(pl_df, pl.DataFrame)
    assert pl_df.equals(test_set.pl)
