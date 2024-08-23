import itertools
from datetime import datetime

import numpy as np
import pandas as pd

from ssb_timeseries.dates import date_utc

# mypy: disable-error-code="arg-type, type-arg, import-untyped, unreachable, attr-defined"


def series_names(*args: dict | str | list[str] | tuple, **kwargs: str) -> list[str]:
    """Return all permutations of the elements of multiple groups of strings.

    Args:
        *args (str | list | tuple | dict): Each arg in args should be a collection of names to be combined with the other.
        **kwargs (str): One option: 'separator' defines a character sequence inserted between name elements. Defaults to '_'.

    Returns:
        list[str]: List of names to be used as series names.

    Raises:
        ValueError: If an argument of an invalid type is passed.

    """
    separator = kwargs.get("separator", "_")

    if isinstance(args, dict):
        # TODO: fix mypy error: Statement is unreachable  [unreachable]
        return [value for value in args.values()]

    final_args = []

    for arg in args:
        if arg is None:
            final_args.append([""])
        if isinstance(arg, str):
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
    start_date: datetime | str | None = None,
    end_date: datetime | str | None = None,
    freq: str = "D",
    interval: int = 1,
    separator: str = "_",
    midpoint: int | float = 100,
    variance: int | float = 10,
    temporality: str = "AT",
    decimals: int = 0,
) -> pd.DataFrame:
    """Generate sample data for specified date range and permutations over lists.

    Args:
        start_date (datetime): The start date of the date range. Optional, default is negative infinity.
        end_date (datetime): The end date of the date range. Optional, default is positive infinity.
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
        interval (int): The interval between dates. ; optionalDefault is 1.
        separator (str): The separator used to join combinations. Optional, default is '_'.
        midpoint (float): The midpoint value for generating random data. Optional, default is 100.
        variance (float): The variance value for generating random data. Optional, default is 10.
        temporality (str): The temporality of the data. Default is 'AT'.
        decimals (int): The number of decimal places to round to. Optional, default is 0.

    Returns:
        DataFrame: A DataFrame containing sample data.

    Example:
    ```
    # Generate sample data with no specified start or end date (defaults to +/- infinity)
    sample_data = generate_sample_df(List1, List2, freq='D')
    ```
    """
    # Handle start_date and end_date defaults
    if start_date is None:
        start_date = datetime.min  # Representing negative infinity
    if end_date is None:
        end_date = datetime.max  # Representing positive infinity

    # Add other frequencies as needed
    freq_lookup = {
        "Y": "years",
        "YS": "years",
        "M": "months",
        "ME": "months",
        "MS": "months",
        "D": "days",
        "H": "hours",
        "T": "minutes",
        "S": "seconds",
    }
    offset = pd.DateOffset(**{freq_lookup[freq]: interval})
    valid_at = pd.date_range(start=start_date, end=end_date, freq=f"{interval}{freq}")
    valid_from = valid_at
    valid_to = pd.date_range(
        start=date_utc(start_date) + offset,
        end=date_utc(end_date) + offset,
        freq=f"{interval}{freq}",
    )
    # ... valid_to = valid_from + pd.DateOffset(**{freq_lookup[freq]: interval})  # type: ignore

    # BUGFIX: If *lists receives strings, permutations will be over chars by chars
    # Kombiner listene til en enkelt liste av lister
    # list = list(lists)

    # name_parts = series_names(*lists)
    # Generer alle mulige kombinasjoner av listene med separator
    # series = [
    #     separator.join(combination) for combination in itertools.product(*name_parts)
    # ]
    series = series_names(*lists, separator=separator)

    # Opprett DataFrame med tilfeldige tall
    rows = len(valid_at)
    cols = len(series)
    some_numbers = random_numbers(
        rows, cols, midpoint=midpoint, variance=variance, decimals=decimals
    )
    df = pd.DataFrame(
        # (midpoint + variance * np.random.randn(rows, cols)).round(decimals),
        some_numbers,
        columns=series,
        dtype="float32[pyarrow]",
    )

    # Legg til "Dates" som den første kolonnen i "df"
    match temporality:
        case "AT":
            df.insert(0, "valid_at", valid_at)
            df.set_index("valid_at")
        case "FROM_TO":
            df.insert(0, "valid_to", valid_to)
            df.insert(0, "valid_from", valid_from)
            df.set_index(["valid_from", "valid_to"])

    return df


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
