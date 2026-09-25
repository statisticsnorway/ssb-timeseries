from datetime import datetime

import narwhals as nw
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from ssb_timeseries.dataframes import are_equal
from ssb_timeseries.dataframes import is_df_like
from ssb_timeseries.dataframes.dates import datetime_localize
from ssb_timeseries.dataframes.sampling import group_by
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


# ------------------------------------------------------------------
# group_by
# ------------------------------------------------------------------

BACKENDS = ["pandas", "polars", "pyarrow"]

GROUP_BY_DATES = [
    datetime(2023, 12, 30),
    datetime(2023, 12, 31),
    datetime(2024, 1, 1),
    datetime(2024, 1, 2),
    datetime(2024, 2, 1),
    datetime(2024, 2, 2),
]
SERIES_A = [1, 2, 3, 4, 5, 6]
SERIES_B = [6, 5, 4, 3, 2, 1]

EMPTY_FRAMES = {
    "pandas": lambda: pd.DataFrame(
        {
            "valid_at": pd.Series([], dtype="datetime64[ns, UTC]"),
            "a": pd.Series([], dtype="float64"),
        }
    ),
    "polars": lambda: pl.DataFrame(
        {"valid_at": [], "a": []},
        schema={"valid_at": pl.Datetime("us", "UTC"), "a": pl.Float64},
    ),
    "pyarrow": lambda: pa.table(
        {
            "valid_at": pa.array([], type=pa.timestamp("us", tz="UTC")),
            "a": pa.array([], type=pa.float64()),
        }
    ),
}

EXPECTED_LABELS_AND_SUMS = {
    "y": [("2023", 3), ("2024", 18)],
    "yr": [("2023", 3), ("2024", 18)],
    "year": [("2023", 3), ("2024", 18)],
    "m": [("2023-12", 3), ("2024-01", 7), ("2024-02", 11)],
    "mth": [("2023-12", 3), ("2024-01", 7), ("2024-02", 11)],
    "month": [("2023-12", 3), ("2024-01", 7), ("2024-02", 11)],
    "q": [("2023-Q4", 3), ("2024-Q1", 18)],
    "quarter": [("2023-Q4", 3), ("2024-Q1", 18)],
    "w": [("2023-52", 3), ("2024-01", 7), ("2024-05", 11)],
    "wk": [("2023-52", 3), ("2024-01", 7), ("2024-05", 11)],
    "week": [("2023-52", 3), ("2024-01", 7), ("2024-05", 11)],
}


def group_by_frame(implementation, tz=None):
    frame = nw.from_dict(
        {"valid_at": GROUP_BY_DATES, "a": SERIES_A, "b": SERIES_B},
        backend=implementation,
    )
    if tz:
        return datetime_localize(frame, tz)
    return frame.to_native()


def labels_and_sums(result, value_column):
    frame = nw.from_native(result)
    key_column = frame.columns[0]
    rows = [
        (str(key), value)
        for key, value in zip(
            frame[key_column].to_list(), frame[value_column].to_list(), strict=True
        )
    ]
    return sorted(rows)


@pytest.mark.parametrize("implementation", BACKENDS)
@pytest.mark.parametrize("freq", sorted(EXPECTED_LABELS_AND_SUMS))
def test_group_by_sums_daily_series_into_expected_labels_and_sums_for_every_frequency_and_alias(
    implementation, freq
):
    result = group_by(group_by_frame(implementation), freq, "sum")
    assert labels_and_sums(result, f"a_{freq}_sum") == EXPECTED_LABELS_AND_SUMS[freq]


@pytest.mark.parametrize("freq", ["y", "m", "q", "w"])
def test_group_by_returns_identical_labels_and_sums_on_pandas_polars_and_pyarrow(freq):
    results = [group_by(group_by_frame(backend), freq, "sum") for backend in BACKENDS]
    reference = labels_and_sums(results[0], f"a_{freq}_sum")
    for result in results[1:]:
        assert labels_and_sums(result, f"a_{freq}_sum") == reference
    assert reference == EXPECTED_LABELS_AND_SUMS[freq]


@pytest.mark.parametrize("implementation", BACKENDS)
def test_group_by_with_agg_mapping_applies_sum_to_a_and_mean_to_b_per_quarter_and_renames_columns(
    implementation,
):
    result = group_by(
        group_by_frame(implementation), "q", agg_mapping={"sum": ["a"], "mean": ["b"]}
    )
    frame = nw.from_native(result)
    assert frame.columns == ["valid_at", "a_q_sum", "b_q_mean"]
    rows = [
        (str(key), a_sum, b_mean)
        for key, a_sum, b_mean in zip(
            frame["valid_at"].to_list(),
            frame["a_q_sum"].to_list(),
            frame["b_q_mean"].to_list(),
            strict=True,
        )
    ]
    assert sorted(rows) == [("2023-Q4", 3, 5.5), ("2024-Q1", 18, 2.5)]


@pytest.mark.parametrize("implementation", BACKENDS)
def test_group_by_with_list_of_functions_applies_sum_and_mean_to_every_series(
    implementation,
):
    result = group_by(group_by_frame(implementation), "y", ["sum", "mean"])
    frame = nw.from_native(result)
    assert set(frame.columns) == {
        "valid_at",
        "a_y_sum",
        "a_y_mean",
        "b_y_sum",
        "b_y_mean",
    }
    assert labels_and_sums(result, "a_y_sum") == [("2023", 3), ("2024", 18)]
    assert labels_and_sums(result, "b_y_mean") == [("2023", 5.5), ("2024", 2.5)]


@pytest.mark.parametrize("implementation", BACKENDS)
def test_group_by_with_series_names_limits_the_aggregation_to_the_named_series_only(
    implementation,
):
    result = group_by(group_by_frame(implementation), "y", "sum", series_names=["a"])
    assert nw.from_native(result).columns == ["valid_at", "a_y_sum"]


@pytest.mark.parametrize("implementation", BACKENDS)
def test_group_by_with_raw_frequency_keeps_one_row_per_timestamp_and_the_time_column_name(
    implementation,
):
    result = group_by(group_by_frame(implementation), "raw", "sum")
    frame = nw.from_native(result)
    assert frame.columns == ["valid_at", "a_raw_sum", "b_raw_sum"]
    assert frame.shape == (len(GROUP_BY_DATES), 3)


@pytest.mark.parametrize("implementation", BACKENDS)
def test_group_by_converts_to_the_requested_timezone_before_forming_the_month_labels(
    implementation,
):
    result = group_by(
        group_by_frame(implementation, "UTC"), "m", "sum", tz="Europe/Oslo"
    )
    assert labels_and_sums(result, "a_m_sum") == [
        ("2023-12", 3),
        ("2024-01", 7),
        ("2024-02", 11),
    ]


@pytest.mark.parametrize("implementation", BACKENDS)
def test_group_by_of_empty_frame_returns_zero_rows_and_the_aggregated_column_names(
    implementation,
):
    result = group_by(EMPTY_FRAMES[implementation](), "m", "sum")
    frame = nw.from_native(result)
    assert frame.shape == (0, 2)
    assert frame.columns == ["valid_at", "a_m_sum"]


@pytest.mark.parametrize("implementation", BACKENDS)
def test_group_by_accepts_a_narwhals_frame_and_returns_the_same_labels_and_sums(
    implementation,
):
    frame = nw.from_dict(
        {"valid_at": GROUP_BY_DATES, "a": SERIES_A}, backend=implementation
    )
    result = group_by(frame, "m", "sum")
    assert labels_and_sums(result, "a_m_sum") == [
        ("2023-12", 3),
        ("2024-01", 7),
        ("2024-02", 11),
    ]


@pytest.mark.parametrize("implementation", BACKENDS)
def test_group_by_with_time_col_groups_by_the_requested_column_not_the_first_temporal_column(
    implementation,
):
    frame = nw.from_dict(
        {
            "valid_at": GROUP_BY_DATES,
            "published": GROUP_BY_DATES,
            "a": SERIES_A,
        },
        backend=implementation,
    ).to_native()
    result = group_by(frame, "y", "sum", time_col="published")
    assert nw.from_native(result).columns == ["published", "a_y_sum"]


def test_group_by_with_unsupported_frequency_raises_value_error_naming_the_frequency():
    with pytest.raises(ValueError, match="Unsupported frequency type: 2h"):
        group_by(group_by_frame("pandas"), "2h", "sum")


def test_group_by_without_func_and_without_agg_mapping_raises_value_error():
    with pytest.raises(ValueError, match="Either agg_mapping or func_name"):
        group_by(group_by_frame("pandas"), "m")
