import pytest

from ssb_timeseries.dates import EUROPE
from ssb_timeseries.dates import date_eur_no
from ssb_timeseries.sample_data import create_df


@pytest.fixture(params=["pandas", "polars", "pyarrow"])
def monthly_data(request):
    """A year of monthly data in each supported backend implementation."""
    return create_df(
        ["p", "q", "r"],
        start_date=date_eur_no("2022-01-01"),
        end_date=date_eur_no("2022-12-31"),
        freq="MS",
        tz=EUROPE,
        implementation=request.param,
    )
