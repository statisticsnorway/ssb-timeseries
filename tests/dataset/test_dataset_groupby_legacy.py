import logging

import pytest

import ssb_timeseries as ts
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType

# mypy: ignore-errors


def test_legacy_groupby_sums_daily_series_into_one_row_per_month(caplog):
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


def test_legacy_groupby_averages_daily_series_into_one_row_per_month(caplog):
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
def test_legacy_groupby_with_poc_auto_differs_from_both_mean_and_sum(
    monthly_data, caplog
):
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


@pytest.mark.xfail(
    strict=True,
    reason="The proof-of-concept `auto` is on hold and currently broken: it calls the Polars "
    "method select(regex=...) on a pandas frame, so it raises AttributeError.",
)
def test_groupby_with_proof_of_concept_auto_warns_that_it_is_on_hold_but_still_fails_on_polars_api():
    x = Dataset(
        name="test-groupby-auto-warns",
        data_type=SeriesType.simple(),
        data=create_df(
            ["p_pris", "q_pris", "p_volum", "q_volum"],
            start_date="2022-01-01",
            end_date="2022-12-31",
            freq="MS",
        ),
    )

    with pytest.warns(FutureWarning, match="on hold"):
        result = x.groupby("M", "auto")

    assert result.data.shape[0] == 12
