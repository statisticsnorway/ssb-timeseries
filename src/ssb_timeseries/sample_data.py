"""The :py:mod:`ssb_timeseries.sample_data` module provides tools for creating sample timeseries data. These are convenience functions for tests and demos."""

import itertools
from datetime import datetime
from datetime import timedelta
from functools import partial
from typing import Any

import narwhals as nw
import numpy as np
from dateutil.relativedelta import relativedelta
from dateutil.rrule import DAILY
from dateutil.rrule import HOURLY
from dateutil.rrule import MINUTELY
from dateutil.rrule import MONTHLY
from dateutil.rrule import SECONDLY
from dateutil.rrule import WEEKLY
from dateutil.rrule import YEARLY
from dateutil.rrule import rrule

from ssb_timeseries.dates import date_round
from ssb_timeseries.dates import ensure_datetime

# mypy: disable-error-code="arg-type, type-arg, import-untyped, unreachable, attr-defined"


def series_names(*args: dict | str | list[str] | tuple, **kwargs: str) -> list[str]:
    """Return all permutations of the elements of multiple groups of strings.

    Args:
        *args (str | list | tuple | dict): Each arg in args should be a collection of names to be combined with the other.

        **kwargs (str): One option: 'separator' defines a character sequence inserted between name elements. Defaults to '_'.

    The choice of '_' as default separator is not arbitrary: Some functionality,notably Dataset.vectors(), use series names to create Python variables.
    A default separator that is valid in a variable name simnplifies that.

    Returns:
        list[str]: List of names to be used as series names.

    Raises:
        ValueError: If an argument of an invalid type is passed.

    """
    # The real dataseries uses the sign . as separator
    # "Real dataseries" = "The most used naming convention for series in the legacy FAME databases of Statistics Norway"
    separator = kwargs.get("separator", "_")

    if isinstance(args, dict):
        return args.values()

    final_args = []

    for arg in args:
        if arg is None:
            final_args.append([""])
        elif isinstance(arg, str):
            final_args.append([arg])
        elif isinstance(arg, list):
            final_args.append(arg)
        elif isinstance(arg, tuple):
            final_args.append(list(arg))
        else:
            raise ValueError(f"Invalid argument type: {type(arg)}")

    names = [
        separator.join(combination) for combination in itertools.product(*final_args)
    ]

    return names


def create_df(
    *lists: dict | list[str] | tuple | str,
    start_date: datetime | str = "",
    end_date: datetime | str = "",
    freq: str = "D",
    interval: int = 1,
    separator: str = "_",
    midpoint: int | float = 100,
    variance: int | float = 10,
    temporality: str = "AT",
    decimals: int = 0,
    implementation: str = "pandas",
) -> Any:
    """Generate sample data for specified date range and permutations over lists.

    Args:
        start_date (datetime): The start date of the date range. Optional, default is today - 365 days.
        end_date (datetime): The end date of the date range. Optional, default is today.
        *lists (list[str]): Lists of values to generate combinations from.
        freq (str): The frequency of date generation.
            'Y' for yearly at last day of year,
            'YS' for yearly at first day of year,
            'M' for monthly at last day of month,
            'MS' for monthly at first day of month,
            'W' for weekly on Sundays,
            'D' for daily,
            'H' for hourly,
            'T' for minutely,
            'S' for secondly,
            etc.
            Optional, default is 'D'.
        interval: The interval between dates. ; optionalDefault is 1.
        separator: The separator used to join combinations. Optional, default is '_'.
        midpoint: The midpoint value for generating random data. Optional, default is 100.
        variance: The variance value for generating random data. Optional, default is 10.
        temporality: The temporality of the data. Default is 'AT'.
        decimals: The number of decimal places to round to. Optional, default is 0.
        implementation: Narwhals supported dataframe library or object type.

    Returns:
        A DataFrame or similar object (Numpy array, Arrow table, dict) containing sample data.

    Example:
    ```
    # Generate sample data with no specified start or end date (defaults to +/- infinity)
    sample_data = generate_sample_df(List1, List2, freq='D')
    ```
    """
    if not start_date:
        start_date = date_round(datetime.now()) - timedelta(days=364)
    if not end_date:
        end_date = date_round(datetime.now())

    series = series_names(*lists, separator=separator)
    dates = date_ranges(
        start_date=ensure_datetime(start_date),
        end_date=ensure_datetime(end_date),
        freq=freq,
        interval=interval,
        temporality=temporality,
    )

    rows = len(dates.get("valid_at", dates.get("valid_from")))
    cols = len(series)
    numbers = random_numbers(
        rows, cols, midpoint=midpoint, variance=variance, decimals=decimals
    )
    data_dict = {**dates, **{name: numbers[:, i] for i, name in enumerate(series)}}

    if implementation == "dict":
        return data_dict
    else:
        nw_df = nw.from_dict(data_dict, backend=implementation)
        match implementation.lower():
            case "pyarrow" | "arrow" | "pa":
                return nw_df.to_arrow()
            case "numpy" | "np":
                return nw_df.to_numpy()
            case "polars" | "pl":
                return nw_df.to_polars()
            case "narwhals" | "nw":
                return nw_df
            case "pandas" | "pd" | _:
                return nw_df.to_pandas()


def date_ranges(
    start_date: datetime,
    end_date: datetime,
    freq: str,
    interval: int = 1,
    temporality: str = "AT",
) -> dict[str, list[datetime]]:
    """Generate a list of dates with a specified frequency."""
    freq_map = {
        "Y": YEARLY,
        "YS": YEARLY,
        "YE": YEARLY,
        "M": MONTHLY,
        "MS": MONTHLY,
        "ME": MONTHLY,
        "W": WEEKLY,
        "D": DAILY,
        "H": HOURLY,
        "T": MINUTELY,
        "S": SECONDLY,
    }
    if freq[0] == "Q":
        interval *= 3
        freq = freq.replace("Q", "M")

    if freq[0] == "Y":
        bymonth = 1
        bymonthday = 1
        if freq[-1] == ("E"):
            bymonthday = -1
    elif freq in ("M", "MS"):
        bymonth = None
        bymonthday = 1
    elif freq in ("ME"):
        bymonth = None
        bymonthday = -1
    else:
        bymonth = None
        bymonthday = None

    r = partial(
        rrule,
        freq=freq_map[freq.upper()],
        interval=interval,
        bymonth=bymonth,
        bymonthday=bymonthday,
    )
    d = r(
        dtstart=start_date,
        until=end_date,
    )
    if temporality == "AT":
        return {"valid_at": list(d)}
    else:
        delta = relativedelta(d[0], d[1])
        d_to = r(dtstart=start_date + delta, until=end_date + delta)
        return {"valid_from": list(d), "valid_to": list(d_to)}


def random_numbers(
    rows: int,
    cols: int,
    decimals: int = 0,
    midpoint: int | float = 100,
    variance: int | float = 10,
) -> np.ndarray:
    """Generate sample dataframe of specified dimensions."""
    generator = np.random.default_rng()
    random_matrix = generator.standard_normal(size=(rows, cols))
    return midpoint + variance * random_matrix.round(decimals)


def xyz_at(implementation: str = "pandas") -> Any:
    """Return a :py:class:`Temporality.AT` compliant dataframe with a year of monthly data for series 'x', 'y' and 'z'."""
    df = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-12-31",
        freq="MS",
        temporality="AT",
        implementation=implementation,
    )
    return df


def xyz_from_to(implementation: str = "pandas") -> Any:
    """Return a :py:class:`Temporality.FROM_TO` compliant dataframe with a year of monthly data for series 'x', 'y' and 'z'."""
    df = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-12-31",
        freq="MS",
        temporality="FROM_TO",
        implementation=implementation,
    )
    return df
