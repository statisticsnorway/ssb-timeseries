# type: ignore
# ruff: noqa
import logging

import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df


@log_start_stop
@pytest.mark.skipif(True, reason="dataset.identity needs rethinking/fixing")
def test_dataset_instance_identity(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    """Test SAMENESS as opposed to .data euality in terms of the == operator.
    Two Dataset are the same SET if name, type and storage location are the same.
    Two Dataset instances are the same INSTANCE if name, type and datafile are the same.
    (For versioned sets, this implies the same version identifier.)
    Related stuff:
    https://stackoverflow.com/questions/34599953/are-objects-with-the-same-id-always-equal-when-comparing-them-with
    """

    a = Dataset(
        name="test-no-dir-need-to-be-created",
        data_type=SeriesType.simple(),
        as_of_tz="2022-01-01",
    )
    b = Dataset(
        name="test-no-dir-need-to-be-created",
        data_type=SeriesType.simple(),
        as_of_tz="2022-01-01",
    )
    c = Dataset(
        name="test-no-dir-created-different",
        data_type=SeriesType.simple(),
        as_of_tz="2022-12-01",
    )

    # TEMPORARY DISABLED skip_<name>
    # TBD: when should two instances of a dataset be considered the same?
    # ... name and type + how many more attributes?
    assert a.identical(a)
    assert a.identical(b)
    assert not a.identical(c)


def test_dataset_math(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    a_data = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=a_data,  # , index="valid_at"
    )

    scalar = 1000

    # TO DO: improve coverage by adding asserts for scalar cases
    ts_logger.debug(f"matrix:\n{a + scalar}")
    ts_logger.debug(f"matrix:\n{a - scalar}")
    ts_logger.debug(f"matrix:\n{a * scalar}")
    ts_logger.debug(f"matrix:\n{a / scalar}")

    # TO DO: add test cases for numpy arrays
    # col_vector = np.ones((1, 3)) * scalar
    # row_vector = np.ones((4, 1)) * scalar
    # matrix = np.ones((4, 3)) * scalar

    # peek into the asserts below:

    # ts_logger.debug(f"matrix:\n{a + a_data}")
    # ts_logger.debug(f"matrix:\n{a - a_data}")
    # ts_logger.debug(f"matrix:\n{a * a_data}")
    # ts_logger.debug(f"matrix:\n{a / a_data}")

    # ts_logger.debug(f"matrix:\n{a + a}")
    # ts_logger.debug(f"matrix:\n{a - a}")
    # ts_logger.debug(f"matrix:\n{a * a}")
    # ts_logger.debug(f"matrix:\n{a / a}")

    assert all((a + a).data == (a + a_data).data)
    assert all((a - a).data == (a - a_data).data)
    assert all((a * a).data == (a * a_data).data)
    assert all((a / a).data == (a / a_data).data)

    # TO DO: add method dataset.__iter__ for these assertss to go though
    # assert all((a + a) == (a + a_data))
    # assert all((a - a) == (a - a_data))
    # assert all((a * a) == (a * a_data))
    # assert all((a / a) == (a / a_data))


@log_start_stop
def test_dataset_add_dataset(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=df,
    )
    b = Dataset(
        name="test-temp-b",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=df,
    )

    # test relies on both what is tested and scalar multiplication
    # -> not perfect, but let us just move on
    c = a + b
    d = a * 2

    ts_logger.debug(d)
    ts_logger.debug(all(d == c))

    assert all(c.data == d.data)
    # TO DO: redefine equals for datasets? add __iter__
    # assert all(c == d)


@log_start_stop
def test_algebra_expression_with_multiple_dataset(
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name="A",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=create_df(
            ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )
    b = Dataset(
        name="B",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=create_df(
            ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )
    c = Dataset(
        name="C",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=create_df(
            ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )

    d = a * a * a + b * b + b * b - c - c - c
    e = a**3 + 2 * b**2 - 3 * c

    # ts_logger.warning(f"d: {d.name}\n{d.data}")
    # ts_logger.warning(f"e: {e.name}\n{e.data}")
    ts_logger.warning(
        f"d.data[numeric] == e.data[numeric]: \n{d.data[d.numeric_columns()] == e.data[e.numeric_columns()]} --> all = {all(d.data[d.numeric_columns()] == e.data[e.numeric_columns()])}"
    )
    assert all(d.data[d.numeric_columns()] == e.data[e.numeric_columns()])

    # must redefine equals for datasets
    # ts_logger.warning(f"e == d\n{(e == d).data}\n\t--> {all(e == d)}")
    # assert all(e == d)


@log_start_stop
def test_dataset_add_dataframe(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=df,
    )

    # test relies on both what is tested and scalar multiplication
    # -> not perfect, but let us just move on
    b = a + df
    c = a * 2

    assert all(b.data == c.data)
    # must redefine equals for datasets
    # assert all(b == c)


@log_start_stop
def test_dataset_subtract_dataset(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=df,
    )
    b = Dataset(
        name="test-temp-b",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=df,
    )

    c = a - b

    # assert all(c == 0)
    # to be supplemented by
    assert all(c.data == 0)


@log_start_stop
def test_dataset_subtract_dataframe(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    data1 = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=data1,
    )
    b = a - data1

    # assert all(b == 0)
    # assert all(b == 0)
    # to be supplemented by
    assert all(b.data == 0)


@log_start_stop
def test_dataset_vectors(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-vectors",
        data_type=SeriesType.estimate(),
        load_data=False,
        as_of_tz=date_utc("2022-01-01"),
        # series_tags={},
    )
    assert x.data.empty
    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-10-03", freq="MS"
    )
    x.vectors()
    ts_logger.debug(f"matrix:\n{eval('p') == x.data['p']}")

    # variables should be defined for all columns
    assert "valid_at" in locals()
    assert "p" in locals()
    assert "q" in locals()
    assert "r" in locals()

    # and should evaluate to x.data[column_name]
    assert all(eval("valid_at") == x.data["valid_at"])
    assert all(eval("p") == x.data["p"])
    assert all(eval("q") == x.data["q"])
    assert all(eval("r") == x.data["r"])


@log_start_stop
def test_dataset_vectors_with_filter(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-vectors",
        data_type=SeriesType.estimate(),
        load_data=False,
        as_of_tz=date_utc("2022-01-01"),
        series_tags={},
    )
    assert x.data.empty
    tag_values = [["px", "qx", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-10-03", freq="MS"
    )
    x.vectors("x")

    # variables should be defined only for some columns
    assert "valid_at" not in locals()
    assert "px" in locals()
    assert "qx" in locals()
    assert "r" not in locals()

    # and should evaluate to x.data[column_name] for the defined ones
    assert all(eval("px") == x.data["px"])
    assert all(eval("qx") == x.data["qx"])
