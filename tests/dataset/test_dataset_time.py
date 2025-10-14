import logging
import uuid

import pytest

import ssb_timeseries as ts
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors


@log_start_stop
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


@log_start_stop
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


@log_start_stop
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


@log_start_stop
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
@log_start_stop
def test_dataset_groupby_auto(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-groupby-auto", data_type=SeriesType.simple(), load_data=False
    )

    tag_values = [["p_pris", "q_pris", "r_pris", "p_volum", "q_volum", "r_volum"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2023-02-28", freq="D"
    )
    assert x.data.shape == (424, 7)
    df = x.groupby("M", "auto")
    df_mean = x.groupby("M", "mean")
    df_sum = x.groupby("M", "sum")
    ts.logger.debug(f"groupby:\n{df}")
    # use of period index means 'valid_at' is not counted in columns
    assert df.shape == (14, 6)
    assert ~all(df == df_mean)
    assert ~all(df == df_sum)


@log_start_stop
def test_dataset_resample_upsampling_ffil(caplog):
    caplog.set_level(logging.DEBUG)

    tag_values = [["p", "q", "r"]]
    x = Dataset(
        name="test-resample",
        data_type=SeriesType.simple(),
        load_data=False,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="MS"
        ),
    )
    assert x.data.shape == (12, 4)

    y = x.resample("D", "ffill", closed="s")
    ts.logger.debug(f"resample:\n{x.data}\n{y.name}\n{y.data}")
    # beware of index column!
    # double check behaviour for lat period
    # verify / create test cases per Temporality
    # (might want to rethink )
    assert y.data.shape == (335, 3)


@log_start_stop
def test_dataset_resample_downsampling_w_mean(caplog):
    caplog.set_level(logging.DEBUG)

    tag_values = [["p", "q", "r"]]
    x = Dataset(
        name="test-resample",
        data_type=SeriesType.simple(),
        load_data=False,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="MS"
        ),
    )
    assert x.data.shape == (12, 4)
    y = x.resample("QE", "mean")
    ts.logger.debug(f"resample:\n{x.data}\n{y.name}\n{y.data}")
    assert y.data.shape == (4, 3)
