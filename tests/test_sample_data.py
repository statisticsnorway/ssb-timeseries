import logging

from ssb_timeseries.sample_data import create_df


def test_create_sample_from_single_string() -> None:
    df = create_df("a", start_date="2022-01-01", end_date="2022-01-03", freq="D")
    assert df.size == 6
    # expected 3 days x (1 date column + 1 variable columns) = 6 values


def test_create_sample_from_single_multichar_string(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df("abc", start_date="2022-01-01", end_date="2022-01-03", freq="D")
    # FIXED known issue: if passing strings rather than lists, permutes over chars in string"
    assert df.size == 6
    # expected 3 days x (1 date column + 1 variable columns) = 6 values


def test_create_sample_from_one_list(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df(
        ["a", "bb", "cc"], start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    assert df.size == 4
    assert df.valid_at.values[0] == df.valid_at.values[-1]
    # expected 1 years x (1 date column + 3 variable columns) = 4 values


def test_create_sample_from_two_lists(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    df = create_df(
        ["a", "b"],
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-05-05",
        freq="M",
    )
    # expected 4 months x (1 date column + 6 variable columns) = 28 values
    assert df.size == 28


def test_create_df() -> None:
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
    # TO DO: update assert to check range of data


# test parameters
