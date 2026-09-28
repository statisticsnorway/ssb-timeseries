import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType


def _auto_on_hold_dataset() -> Dataset:
    """Build a dataset for exercising the held-back `auto` aggregation."""
    return Dataset(
        name="test-group-by-auto-on-hold",
        data_type=SeriesType.simple(),
        data=create_df(
            ["x", "y"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )


def test_group_by_with_the_default_func_warns_and_raises_not_implemented_error():
    x = _auto_on_hold_dataset()

    with (
        pytest.warns(FutureWarning, match="unavailable"),
        pytest.raises(NotImplementedError, match="unavailable"),
    ):
        x.group_by("q")


def test_group_by_with_auto_passed_explicitly_warns_and_raises_not_implemented_error():
    x = _auto_on_hold_dataset()

    with (
        pytest.warns(FutureWarning, match="unavailable"),
        pytest.raises(NotImplementedError, match="unavailable"),
    ):
        x.group_by("q", "auto")


def test_group_by_with_explicit_func_aggregates_and_emits_no_future_warning(recwarn):
    x = Dataset(
        name="test-group-by-explicit",
        data_type=SeriesType.simple(),
        data=create_df(
            ["x", "y"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )

    result = x.group_by("q", "sum")

    assert result.data.shape[1] == 3
    assert [w for w in recwarn if issubclass(w.category, FutureWarning)] == []
