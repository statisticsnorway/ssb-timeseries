import logging

import ssb_timeseries as ts
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.sample_data import series_names
from ssb_timeseries.sample_data import xyz_at
from ssb_timeseries.sample_data import xyz_from_to

# mypy: ignore-errors


def test_series_names_from_string_args_joins_with_separator_and_returns_list_of_single_string(
    caplog,
):
    names = series_names("a", "b", "c")
    assert names == ["a_b_c"]


def test_series_names_from_string_and_none_args_joins_with_separator_and_returns_list_of_single_string(
    caplog,
):
    names = series_names("a", "b", None, "c")
    assert names == ["a_b__c"]


def test_series_names_from_one_list_of_strings_returns_same_list(caplog):
    names = series_names(["a", "b", "c"])
    assert names == ["a", "b", "c"]


def test_series_names_from_one_tuple_of_strings_returns_list_of_same_strings(caplog):
    names = series_names(("a", "b", "c"))
    assert names == ["a", "b", "c"]


def test_series_names_from_two_lists_of_strings_returns_list_of_permutations(caplog):
    names = series_names(["a", "b", "c"], ["1", "2"])
    assert sorted(names) == ["a_1", "a_2", "b_1", "b_2", "c_1", "c_2"]


def test_series_names_from_dict_returns_dict_values(caplog):
    """Test that series_names returns the values of a dictionary argument."""
    names = series_names({"a": "x", "b": "y", "c": "z"})
    assert sorted(list(names)) == ["x", "y", "z"]


def test_series_names_from_two_tuples_of_strings_returns_list_of_permutations(caplog):
    names = series_names(("a", "b", "c"), ("1", "2"))
    assert sorted(names) == ["a_1", "a_2", "b_1", "b_2", "c_1", "c_2"]


def test_series_names_from_list_and_tuple_and_string_returns_list_of_permutations(
    caplog,
):
    names = series_names(["a", "b", "c"], ("1", "2"), "xyz")
    assert sorted(names) == [
        "a_1_xyz",
        "a_2_xyz",
        "b_1_xyz",
        "b_2_xyz",
        "c_1_xyz",
        "c_2_xyz",
    ]


def test_create_sample_from_single_string() -> None:
    df = create_df("a", start_date="2022-01-01", end_date="2022-01-03", freq="D")
    assert df.size == 6


def test_create_sample_from_single_multichar_string(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df("abc", start_date="2022-01-01", end_date="2022-01-03", freq="D")
    assert df.size == 6


def test_create_sample_from_one_list(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df(
        ["a", "bb", "cc"], start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    assert df.size == 4
    assert df.valid_at.values[0] == df.valid_at.values[-1]


def test_create_df_from_two_lists(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df(
        ["a", "b"],
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-05-05",
        freq="ME",
    )
    assert df.size == 28


def test_create_df_without_specifying_dates_returns_one_year_back() -> None:
    df = create_df(
        ["a", "b", "c", "d"],
        freq="MS",
    )
    assert df.size == 60


def test_create_df_from_mix_of_lists_and_string() -> None:
    df = create_df(
        ["a", "b"],
        "Q",
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-01-05",
        freq="D",
        separator="___",
        midpoint=50,
        variance=5,
    )
    assert df.size == 35


def test_create_dataset_with_correct_data_size() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x", "y", "z"]}
    tag_values = [value for value in tags.values()]
    x = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    assert x.size == 280


def test_create_df_twice_returns_different_data(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    x = create_df(
        ["a", "b"],
        "Q",
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-01-05",
        freq="D",
        separator="___",
        midpoint=50,
        variance=5,
    )
    y = create_df(
        ["a", "b"],
        "Q",
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-01-05",
        freq="D",
        separator="___",
        midpoint=50,
        variance=5,
    )
    ts.logger.debug(f"{x=}, \n{y=}")
    assert any(x != y)


def test_xyz_at() -> None:
    df = xyz_at()
    assert df.size == 48
    assert list(df.columns) == ["valid_at", "x", "y", "z"]


def test_xyz_from_to() -> None:
    df = xyz_from_to()
    assert df.size == 60
    assert list(df.columns) == ["valid_from", "valid_to", "x", "y", "z"]


def test_create_df_at_temporality_creates_correct_dates():
    """Verify that create_df with AT temporality generates the correct date sequence."""
    df = create_df(
        start_date="2023-01-01", end_date="2023-03-01", freq="MS", temporality="AT"
    )
    expected_dates = ["2023-01-01", "2023-02-01", "2023-03-01"]
    # Convert to datetime objects for comparison
    expected_datetimes = [ts.dates.date_utc(d) for d in expected_dates]
    # Convert dataframe column to list of datetime objects
    actual_datetimes = [ts.dates.date_utc(d) for d in df["valid_at"].to_list()]
    assert actual_datetimes == expected_datetimes


def test_create_df_from_to_temporality_creates_correct_periods():
    """Verify that create_df with FROM_TO temporality generates correct and valid periods."""
    from dateutil.relativedelta import relativedelta

    df = create_df(
        start_date="2023-01-01", end_date="2023-03-01", freq="MS", temporality="FROM_TO"
    )

    # 1. Check that valid_from is always before valid_to
    assert all(df["valid_from"] < df["valid_to"])

    # 2. Check that the period is exactly one month
    expected_delta = relativedelta(months=1)
    for _index, row in df.iterrows():
        actual_delta = relativedelta(row["valid_to"], row["valid_from"])
        assert actual_delta == expected_delta
