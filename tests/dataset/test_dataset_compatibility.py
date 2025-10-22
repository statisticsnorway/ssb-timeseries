"""Tests conversions of Dataset to other dataframe libraries, an compliance with standards/protocols."""

from __future__ import annotations

import narwhals as nw
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa


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
