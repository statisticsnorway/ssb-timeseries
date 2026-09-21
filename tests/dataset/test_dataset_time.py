import logging
import uuid

import pytest

import ssb_timeseries as ts
from ssb_timeseries.dataframes.sampling import FILL_METHODS
from ssb_timeseries.dataframes.sampling import SIMPLE_AGGS
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import EUROPE
from ssb_timeseries.dates import date_eur_no
from ssb_timeseries.dates import date_utc
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType

# mypy: ignore-errors


@pytest.fixture(params=["pandas", "polars", "pyarrow"])
def monthly_data(request, tag_values=None):
    implementation = request.param
    if not tag_values:
        tag_values = [["p", "q", "r"]]
    df = create_df(
        *tag_values,
        start_date=date_eur_no("2022-01-01"),
        end_date=date_eur_no("2022-12-31"),
        freq="MS",
        tz=EUROPE,
        implementation=implementation,
    )
    if input == "pandas":
        # monthly_data.set_index(temporal_columns(df))
        # monthly_data.reset_index()
        ...
    yield df  # datetime_convert_timezone(df, EUROPE)


# ------------------------------------------------------------------


def test_correct_datetime_columns_valid_at(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name=f"test-datetimecols-{uuid.uuid4().hex}",
        data_type=SeriesType.simple(),
        data=create_df(
            ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )
    ts.logger.debug(f"test_datetime_columns: {a.datetime_columns}")
    assert a.datetime_columns == ["valid_at"]


def test_correct_datetime_columns_valid_from_to(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name=f"test-datetimecols-{uuid.uuid4().hex}",
        data_type=SeriesType.as_of_from_to(),
        as_of_tz=date_utc("2024-05-01"),
        data=create_df(
            ["x", "y", "z"],
            start_date="2022-01-01",
            end_date="2022-04-03",
            freq="MS",
            temporality="FROM_TO",
        ),
    )
    ts.logger.debug(f"test_datetime_columns: {a.datetime_columns}")
    assert a.datetime_columns.sort() == ["valid_from", "valid_to"].sort()


def test_dataset_groupby_sum(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-groupby-sum", data_type=SeriesType.simple(), load_data=False)

    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2023-02-28", freq="D"
    )
    assert x.data.shape == (424, 4)
    y = x.groupby("M", "sum")
    ts.logger.debug(f"groupby:\n{y.data}")
    assert y.data.shape == (14, 3)


def test_dataset_groupby_mean(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-groupby-mean", data_type=SeriesType.simple(), load_data=False
    )

    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2023-02-28", freq="D"
    )
    assert x.data.shape == (424, 4)
    y = x.groupby("M", "mean")
    ts.logger.debug(f"groupby:\n{y.data}")
    assert y.data.shape == (14, 3)


@pytest.mark.skip(reason="Not ready yet.")
def test_dataset_groupby_auto(monthly_data, caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-groupby-auto", data_type=SeriesType.simple(), load_data=False
    )

    tag_values = [["p_pris", "q_pris", "r_pris", "p_volum", "q_volum", "r_volum"]]
    x.data = monthly_data(tag_values=tag_values)
    assert x.data.shape == (424, 7)
    df = x.groupby("M", "auto")
    df_mean = x.groupby("M", "mean")
    df_sum = x.groupby("M", "sum")
    ts.logger.debug(f"groupby:\n{df}")
    # use of period index means 'valid_at' is not counted in columns
    assert df.shape == (14, 6)
    assert ~all(df == df_mean)
    assert ~all(df == df_sum)


@pytest.mark.parametrize("method", FILL_METHODS)
@pytest.mark.parametrize(
    "calculation, freq",
    [
        ("pd", "D"),
        ("pl", "1d"),
    ],
)
def test_dataset_resample_upsampling(
    monthly_data,
    calculation,
    method,
    freq,
    caplog,
):
    caplog.set_level(logging.DEBUG)
    x = Dataset(
        name="test-resample",
        data_type=SeriesType.simple(),
        load_data=False,
        data=monthly_data,
    )
    assert x.data.shape == (12, 4)

    y = x.resample(
        freq,
        method,  # not used , replaced by interpretation of freq
        implementation=calculation,
    )  # , closed="s")
    ts.logger.debug(f"resample:\n{x.data}\n{y.name}\n{y.data}")
    # beware of index column!
    # double check behaviour for lat period
    # verify / create test cases per Temporality
    # (might want to rethink )
    print(x.nw, "\n", y.nw)
    assert y.data.shape == (335, 4)


@pytest.mark.parametrize(
    "method",
    SIMPLE_AGGS,
)
@pytest.mark.parametrize(
    "calculation, freq, expected_shape",
    [
        ("pd", "QS", (4, 4)),
        ("pd", "QE", (4, 4)),
        ("pd", "YS", (1, 4)),
        ("pd", "YE", (1, 4)),
        ("pl", "3mo", (4, 4)),
        ("pl", "1y", (1, 4)),
    ],
)
def test_dataset_resample_downsampling(
    calculation,
    freq,
    method,
    monthly_data,
    caplog,
    expected_shape,
):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-resample",
        data_type=SeriesType.simple(),
        load_data=False,
        data=monthly_data,
    )
    assert x.data.shape == (12, 4)
    y = x.resample(
        freq,
        method,
        implementation=calculation,
    )
    ts.logger.debug(f"resample:\n{x.data}\n{y.name}\n{y.data}")
    print(x.pd, "\n", y.data)
    assert y.data.shape == expected_shape
