import logging

from ssb_timeseries.logging import ts_logger
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.sample_data import xyz_at
from ssb_timeseries.sample_data import xyz_from_to

# mypy: ignore-errors


def test_create_sample_from_single_string() -> None:
    df = create_df("a", start_date="2022-01-01", end_date="2022-01-03", freq="D")
    assert df.size == 6
    # expected 3 days x (1 date column + 1 variable columns) = 6 values


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
    ts_logger.debug(f"{x=}, \n{y=}")
    assert any(x != y)


def test_xyz_at() -> None:
    df = xyz_at()
    assert df.size == 48
    assert list(df.columns) == ["valid_at", "x", "y", "z"]


def test_xyz_from_to() -> None:
    df = xyz_from_to()
    assert df.size == 60
    assert list(df.columns) == ["valid_from", "valid_to", "x", "y", "z"]
