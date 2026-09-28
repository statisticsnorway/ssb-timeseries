import logging

import pytest

import ssb_timeseries as ts
from ssb_timeseries.dataframes.sampling import FILL_METHODS
from ssb_timeseries.dataframes.sampling import SIMPLE_AGGS
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.types import SeriesType

# mypy: ignore-errors


@pytest.mark.parametrize("method", FILL_METHODS)
@pytest.mark.parametrize(
    "freq, expected_shape",
    [
        # pandas frequency dialect, dispatched on PANDAS_TO_POLARS_FREQ keys
        ("D", (335, 4)),
        # polars frequency dialect, dispatched on PANDAS_TO_POLARS_FREQ values
        ("1d", (335, 4)),
    ],
)
def test_resample_with_fill_method_fills_a_year_of_monthly_data_up_to_every_day(
    monthly_data,
    method,
    freq,
    expected_shape,
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
    )  # , closed="s")
    ts.logger.debug(f"resample:\n{x.data}\n{y.name}\n{y.data}")
    # beware of index column!
    # double check behaviour for last period
    assert y.data.shape == expected_shape


@pytest.mark.parametrize(
    "method",
    SIMPLE_AGGS,
)
@pytest.mark.parametrize(
    "freq, expected_shape",
    [
        # pandas frequency dialect, dispatched on PANDAS_TO_POLARS_FREQ keys
        ("QS", (4, 4)),
        ("QE", (4, 4)),
        ("YS", (1, 4)),
        ("YE", (1, 4)),
        # polars frequency dialect, dispatched on PANDAS_TO_POLARS_FREQ values
        ("3mo", (4, 4)),
        ("1y", (1, 4)),
    ],
)
def test_resample_with_aggregation_method_reduces_monthly_data_to_the_expected_number_of_periods(
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
    )
    ts.logger.debug(f"resample:\n{x.data}\n{y.name}\n{y.data}")
    assert y.data.shape == expected_shape
