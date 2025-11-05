import logging
from collections.abc import Generator

import narwhals as nw
import numpy as np
import pytest

from ssb_timeseries.dataframes import is_empty
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dataset import is_df_like
from ssb_timeseries.dates import date_utc
from ssb_timeseries.types import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: disable-error-code="arg-type,attr-defined,no-untyped-def,index,no-untyped-def"
# ruff: noqa

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def xyz_w_ones_seq_quad(
    caplog,
    xyz_at,
) -> Generator:
    """Generate sequential data (convenient when looking at tests for moving averages)."""
    caplog.set_level(logging.DEBUG)

    ds = Dataset(
        name="test-xyz-ones-sequence-squares",
        data_type=SeriesType.simple(),
        data=xyz_at,
    )
    rows, cols = ds.data[ds.numeric_columns].shape
    ds.data["x"] = 1
    ds.data["y"] = np.array(range(rows)) + 1
    ds.data["z"] = ds.data["y"] ** 2
    yield ds


# ================================ HELPERS ====================================

T = SeriesType.simple()

df_a = create_df(
    ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
)
df_b = create_df(
    ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
)
df_c = create_df(
    ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
)

a = Dataset(name="A", data_type=T, data=df_a)
b = Dataset(name="B", data_type=T, data=df_b)
c = Dataset(name="C", data_type=T, data=df_c)


def to_pandas(df: nw.typing.IntoDataFrame):
    return nw.from_native(df).to_pandas()


def to_polars(df: nw.typing.IntoDataFrame):
    return nw.from_native(df).to_polars()


def to_arrow(df: nw.typing.IntoDataFrame):
    return nw.from_native(df).to_arrow()


# ================================ THE TESTS =================================


# @pytest.mark.skipif(True, reason="dataset.identity needs rethinking/fixing")
def test_dataset_instance_identity_vs_equality(caplog) -> None:
    caplog.set_level("DEBUG")
    """Test SAMENESS as opposed to .data euality in terms of the == operator.
    Two Dataset are the same SET if name, type and storage location are the same.
    Two Dataset instances are the same INSTANCE if name, type and datafile are the same.
    (For versioned sets, this implies the same version identifier.)
    Related stuff:
    https://stackoverflow.com/questions/34599953/are-objects-with-the-same-id-always-equal-when-comparing-them-with
    """

    assert a is a
    assert a is not b
    assert a is not c

    d = a == a  # new dataset, with boolean values, all True
    e = a == df_a  # same as d, new dataset, boolean values, all True
    f = a == b  # new dataset, boolean values, NOT all True

    assert a is not d
    assert a is not e
    assert d is not e

    assert d.all()
    assert e.all()
    assert not f.all()

    for s in [a, b, c, d, e, f]:
        assert isinstance(s, Dataset)
        assert is_df_like(s.data)

    # BUT: We should also be able to do something like (pseudo code):
    # a' = a.copy(no_rename=True) # no_rename param is not implemented >> TODO?
    # a'.data.append(<somedata>)
    # assert a'.same_set_as(a) # TODO implement this?
    # assert a' is not a #because they are different objects/ have different
    # assert a == a # ERR on shape mismatch

    # TBD: when should two instances of a dataset be considered the same?
    # ... name and type + how many more attributes?


@pytest.mark.parametrize(
    "var_type, var, expected",
    [
        ("scalar", 1000, False),
        ("df", df_a, True),
        ("dataset", a, True),
        ("dataset", b, False),
    ],
)
def test_dataset_equals(caplog, var_type, var, expected) -> None:
    res = a == var
    assert isinstance(res, Dataset)
    assert res.data.shape == a.data.shape
    print(f"{res.nw=}\n{res.numeric_array()=}")
    print(f"now ok:\n\t {res.all()=}")

    # tautology - this is essentially the internal representation
    res_all = np.all(res.numeric_array())
    assert res_all == res.all()

    assert res.all() == expected


def test_dataset_math_with_two_datasets_returns_new_dataset(caplog) -> None:
    caplog.set_level("DEBUG")

    d = a + b
    e = a - b
    f = a * b
    g = a / b

    for s in [d, e, f, g]:
        assert isinstance(s, Dataset)
        assert is_df_like(s.data)
        assert not (s == 0).all()


def test_dataset_math_with_dataset_and_dataframe_returns_new_dataset(caplog) -> None:
    caplog.set_level("DEBUG")

    # test __add__,__sub__,__mul__,__div__ implementations
    assert isinstance(a, Dataset)
    assert is_df_like(df_b)
    d = a + df_b
    e = a - df_b
    f = a * df_b
    g = a / df_b

    for s in [
        d,
        e,
        f,
        g,
    ]:
        assert isinstance(s, Dataset)
        assert is_df_like(s.data)
        assert not (s == 0).all()


@pytest.mark.parametrize(
    "implementation, df_aa, df_bb",
    [
        pytest.param(
            "pandas", to_pandas(df_a), to_pandas(df_b), marks=pytest.mark.xfail
        ),
        pytest.param(
            "polars", to_polars(df_a), to_polars(df_b), marks=pytest.mark.xfail
        ),
        ("pyarrow", to_arrow(df_a), to_arrow(df_b)),
    ],
)
def test_dataset_math_with_dataset_and_dataframe_and_vice_vers_returns_new_dataset(
    caplog, implementation, df_aa, df_bb
) -> None:
    caplog.set_level("DEBUG")

    # test __add__,__sub__,__mul__,__div__ implementations
    df_bb = nw.from_native(df_bb).to_polars()
    assert isinstance(a, Dataset)
    assert is_df_like(df_bb)
    d = a + df_bb
    e = a - df_bb
    f = a * df_bb
    g = a / df_bb

    # test __radd__,__rsub__,__rmul__,__rdiv__ implementations
    assert isinstance(b, Dataset)
    assert is_df_like(df_aa)
    h = df_aa + b
    i = df_aa - b
    j = df_aa * b
    k = df_aa / b

    assert (d == h).all()
    assert (e == i).all()
    assert (f == j).all()
    # assert (g == k).all()
    assert g.isclose(k)

    for s in [d, e, f, g, h, i, j, k]:
        assert isinstance(s, Dataset)
        assert is_df_like(s.data)
        assert not (s == 0).all()


def test_dataset_math_with_dataset_and_scalar_returns_new_dataset(caplog) -> None:
    caplog.set_level("DEBUG")

    scalar = 1000

    # test __add__,__sub__,__mul__,__div__ implementations
    d = a + scalar
    e = a - scalar
    f = a * scalar
    g = a / scalar

    # test __radd__,__rsub__,__rmul__,__rdiv__ implementations
    h = scalar + a
    i = scalar - a
    j = scalar * a
    k = scalar / a

    assert (d == h).all()
    assert (e != i).all()
    assert (f == j).all()
    assert (g != k).all()

    for s in [d, e, f, g, h, i, j, k]:
        assert isinstance(s, Dataset)
        assert is_df_like(s.data)
        assert not (s == 0).all()


def test_dataset_math_with_dataset_and_df_returns_new_dataset(caplog) -> None:
    caplog.set_level("DEBUG")

    # TO DO: add test cases for numpy arrays
    # col_vector = np.ones((1, 3)) * scalar
    # row_vector = np.ones((4, 1)) * scalar
    # matrix = np.ones((4, 3)) * scalar

    d = a + b
    e = (a + df_b) + 100
    f = a - b
    g = (a - df_b) + 100
    h = a * b
    i = (a * df_b) + 100
    j = a / b
    k = (a / df_b) + 100

    for s in [d, e, f, g]:
        assert isinstance(s, Dataset)
        assert is_df_like(s.data)
        assert not (s == 0).all()


def test_dataset_math_with_two_sets_gives_same_result_as_set_and_df(caplog) -> None:
    caplog.set_level("DEBUG")

    # TO DO: add test cases for numpy arrays
    # col_vector = np.ones((1, 3)) * scalar
    # row_vector = np.ones((4, 1)) * scalar
    # matrix = np.ones((4, 3)) * scalar

    assert ((a + a) == (a + df_a)).all()
    assert ((a - a) == (a - df_a)).all()
    assert ((a * a) == (a * df_a)).all()
    assert ((a / a) == (a / df_a)).all()


def test_dataset_add_multiply_and_power_are_consistent(caplog) -> None:
    caplog.set_level("DEBUG")

    # test relies on both what is tested and scalar multiplication
    # -> not perfect, but let us just move on
    d = a + a
    e = a * 2
    f = 2 * a
    assert (d == e).all()
    assert (e == f).all()

    f = d * e
    g = d**2
    assert (f == g).all()

    assert isinstance(d, Dataset) and isinstance(e, Dataset)
    assert is_df_like(d.data) and is_df_like(e.data)
    assert (d == e).all()
    print(a.data.shape)
    print(b.data.shape)
    print(c.data.shape)
    print(d.data.shape)
    f = d + b  # f = (a + b) + c

    assert isinstance(f, Dataset) and is_df_like(f.data)
    assert ((e + e + e) == (3 * e)).all()


# @pytest.mark.parametrize("implementation", ["pandas", "polars", "pyarrow"])?
def test_dataset_add_dataframe(caplog) -> None:
    caplog.set_level("DEBUG")

    # test relies on both what is subject to being tested and scalar multiplication
    # -> not perfect, but let us just move on
    b = a + df_a
    c = a * 2
    print("a\n", a.nw)
    print(b.nw)
    print(c.nw)
    assert isinstance(b, Dataset) and isinstance(c, Dataset)
    assert is_df_like(b.data) and is_df_like(c.data)
    assert a.datetime_columns == b.datetime_columns
    assert b.nw.to_arrow() == c.nw.to_arrow()


def test_dataset_subtract_dataset(caplog) -> None:
    caplog.set_level("DEBUG")

    c = a - b
    d = a - a
    print(c.nw)
    print(c.numeric_array().shape)
    print(c.nw.schema)
    e = c - a

    # assert all(c == 0)
    # to be supplemented by
    assert not (c == 0).all()
    assert (d == 0).all()
    assert (e == -b).all()


def test_dataset_subtract_dataframe(caplog) -> None:
    caplog.set_level("DEBUG")

    data1 = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )
    d = b - df_b

    assert isinstance(d, Dataset) and is_df_like(d.data)
    assert (d == 0).all()
    # assert all(b == 0)
    # to be supplemented by


def test_algebra_expression_with_multiple_dataset(caplog) -> None:
    caplog.set_level("DEBUG")

    e = (a**3) + (2 * (b**2)) - (3 * c)
    d = a * a * a + b * b + b * b - c - c - c

    assert (d.numeric_array() == e.numeric_array()).all()


def test_dataset_vectors(caplog):
    caplog.set_level("DEBUG")

    xyz = Dataset(
        name="test-vectors",
        data_type=SeriesType.estimate(),
        load_data=False,
        as_of_tz=date_utc("2022-01-01"),
        # series_tags={},
    )
    assert is_empty(xyz.data)
    xyz.data = df_a
    xyz.vectors()
    # logging.debug(f"matrix:\n{eval('x') == xyz['x']}")

    # variables should be defined for all columns
    assert "valid_at" in locals()
    assert "x" in locals()
    assert "y" in locals()
    assert "z" in locals()

    # and should evaluate to x.data[column_name]
    assert all(eval("valid_at") == xyz.data["valid_at"])
    assert all(eval("x") == xyz.data["x"])
    assert all(eval("y") == xyz.data["y"])
    assert all(eval("z") == xyz.data["z"])


def test_dataset_vectors_with_filter(caplog):
    caplog.set_level("DEBUG")

    xyz = Dataset(
        name="test-vectors",
        data_type=SeriesType.estimate(),
        load_data=False,
        as_of_tz=date_utc("2022-01-01"),
        series_tags={},
    )
    assert is_empty(xyz.data)
    tag_values = [["px", "qx", "r"]]
    xyz.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-10-03", freq="MS"
    )
    xyz.vectors("x")

    # variables should be defined only for some columns
    assert "valid_at" not in locals()
    assert "px" in locals()
    assert "qx" in locals()
    assert "r" not in locals()

    # and should evaluate to xyz.data[column_name] for the defined ones
    assert all(eval("px") == xyz.data["px"])
    assert all(eval("qx") == xyz.data["qx"])


def test_dataset_calc_mov_avg_centered_window_length_of_one_equals_input(
    caplog,
    xyz_w_ones_seq_quad,
) -> None:
    """Tests the special case .moving_average(0,0)."""
    caplog.set_level(logging.DEBUG)
    raw = xyz_w_ones_seq_quad
    logging.debug("Input:\n%s", raw.data)

    calc = raw.moving_average(0, 0)
    logging.debug("Output:\n%s", calc.data)
    check = raw == calc
    logging.debug("Check:\n%s", check)
    assert check.all()


def test_dataset_calc_xyz_mov_avg_lag_3_to_lag_1_returns_expected_result(
    caplog,
    xyz_w_ones_seq_quad,
) -> None:
    caplog.set_level(logging.DEBUG)
    raw = xyz_w_ones_seq_quad
    logging.debug("Input:\n%s", raw.data)
    calc = raw.moving_average(-3, -1)
    logging.debug("Output:\n%s", calc.data)

    expected = np.array(
        [
            [np.nan, np.nan, np.nan],
            [np.nan, np.nan, np.nan],
            [np.nan, np.nan, np.nan],
            [1, 2, 14 / 3],
            [1, 3, 29 / 3],
            [1, 4, 50 / 3],
            [1, 5, 77 / 3],
            [1, 6, 110 / 3],
            [1, 7, 149 / 3],
            [1, 8, 194 / 3],
        ]
    )

    assert np.array_equal(
        calc.numeric_array(),
        expected,
        equal_nan=True,
    )


def test_dataset_calc_xyz_mov_avg_lag_1_to_leap_2_returns_expected_result(
    caplog,
    xyz_w_ones_seq_quad,
) -> None:
    caplog.set_level(logging.DEBUG)
    raw = xyz_w_ones_seq_quad
    logging.debug("Input:\n%s", raw.data)
    calc = raw.moving_average(-1, 2)
    logging.debug("Output:\n%s", calc.data)

    expected = np.array(
        [
            [np.nan, np.nan, np.nan],
            [1, 2.5, 7.5],
            [1, 3.5, 13.5],
            [1, 4.5, 21.5],
            [1, 5.5, 31.5],
            [1, 6.5, 43.5],
            [1, 7.5, 57.5],
            [1, 8.5, 73.5],
            [np.nan, np.nan, np.nan],
            [np.nan, np.nan, np.nan],
        ]
    )

    assert np.array_equal(
        calc.numeric_array(),
        expected,
        equal_nan=True,
    )


def test_dataset_calc_xyz_mov_avg_lag_2_to_leap_2_removing_nans_returns_numeric(
    caplog,
    xyz_w_ones_seq_quad,
) -> None:
    caplog.set_level(logging.DEBUG)
    raw = xyz_w_ones_seq_quad
    logging.debug("Input:\n%s", raw.data)
    b = raw.moving_average(-2, 2, nan_rows="remove")
    c = raw.moving_average(-2, 2, nan_rows="remove_end")
    d = raw.moving_average(-2, 2, nan_rows="remove_beginning")
    logging.debug("Output:\n%s", b.data)

    expected = np.array(
        [
            [np.nan, np.nan, np.nan],
            [np.nan, np.nan, np.nan],
            [1, 3, 11],
            [1, 4, 18],
            [1, 5, 27],
            [1, 6, 38],
            [1, 7, 51],
            [1, 8, 66],
            [np.nan, np.nan, np.nan],
            [np.nan, np.nan, np.nan],
        ]
    )

    print(f"{b.numeric_array()=}\n----\n{expected[2:-2, :]=}\n==========")
    assert np.array_equal(
        b.numeric_array(),
        expected[2:-2, :],
        # equal_nan=True,
    )
    print(f"{c.numeric_array()=}\n----\n{expected[:7, :]=}\n==========")
    assert np.array_equal(
        c.numeric_array(),
        expected[:7, :],
        equal_nan=True,
    )
    print(f"{d.numeric_array()=}\n----\n{expected[2:, :]=}\n==========")
    assert np.array_equal(
        d.numeric_array(),
        expected[2:, :],
        equal_nan=True,
    )


def test_dataset_calc_mov_avg_illegal_nanrows_param_raises_value_error(
    caplog,
    xyz_w_ones_seq_quad,
) -> None:
    caplog.set_level(logging.DEBUG)
    a = xyz_w_ones_seq_quad

    with pytest.raises(ValueError):
        _ = a.moving_average(-2, 2, nan_rows="not_a_valid_param")
