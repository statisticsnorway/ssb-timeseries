"""Tests the Series class."""

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType


def test_iterating_a_dataset_yields_one_series_per_series_name() -> None:
    """Each Series carries only its own data column, plus the time column."""
    dataset_name = "test-series-iteration"
    dataset = Dataset(
        name=dataset_name,
        data_type=SeriesType.simple(),
        data=create_df(
            ["x", "y"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )

    series = list(dataset)

    assert [one.name for one in series] == ["x", "y"]
    for one in series:
        assert one.dataset == dataset_name
        assert one.long_name == f"{dataset_name}::{one.name}"
        assert one.name in one.pa.column_names
        assert len(one.pa.column_names) == 2
        assert one.pa.num_rows == len(dataset.data)
