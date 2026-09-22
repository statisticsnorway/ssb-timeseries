from datetime import datetime

import narwhals as nw

from ssb_timeseries.dataframes import are_equal
from ssb_timeseries.dataframes import is_df_like
from ssb_timeseries.dataframes.dates import datetime_localize
from ssb_timeseries.dataframes.sampling import resample_pandas
from ssb_timeseries.dates import DEFAULT_TZ
from ssb_timeseries.sample_data import date_ranges


def test_resample_pandas_callable_with_args_and_kwargs():
    x = date_ranges("2024-01-01", "2024-01-04", freq="D")
    x["series_1"] = [1, 2, 3, 4]
    print(x)
    backend = "pandas"
    df = nw.from_dict(x, backend=backend)

    def custom_agg(resampler, multiplier, *, offset):
        result = resampler.sum()
        result["series_1"] = result["series_1"] * multiplier + offset
        return result

    result = resample_pandas(
        df,
        freq="2D",
        func=custom_agg,
        multiplier=2,
        offset=10,
    )
    assert is_df_like(result)

    expected = datetime_localize(
        nw.from_dict(
            {
                "valid_at": [datetime(2024, 1, 1), datetime(2024, 1, 3)],
                "series_1": [16, 24],
            },
            backend=backend,
        ),
        DEFAULT_TZ,
    )
    print(result, "\n-----\n", expected)
    assert are_equal(result, expected)
