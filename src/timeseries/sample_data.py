import numpy as np
import pandas as pd
import itertools

from datetime import datetime  # , timedelta
from timeseries import dates
from timeseries.dataset import Dataset
from timeseries.properties import SeriesType
from timeseries.logging import ts_logger


def series_names(*args, **kwargs) -> list[str]:
    separator = kwargs.get("separator", "_")

    if isinstance(args, dict):
        return [value for value in args.values()]

    final_args = []

    for arg in args:
        if arg is None:
            return ""
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
    *lists,
    start_date: datetime = None,
    end_date: datetime = None,
    freq: str = "D",
    interval: int = 1,
    separator: str = "_",
    midpoint: int = 100,
    variance: int = 10,
) -> pd.DataFrame:
    """
    Generate sample data for specified date range and permutations over lists.

    Parameters:
    - start_date (datetime, optional): The start date of the date range. Defaults to negative infinity.
    - end_date (datetime, optional): The end date of the date range. Defaults to positive infinity.
    - *lists: Lists of values to generate combinations from.
    - freq (str, optional): The frequency of date generation:
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
      Default is 'D'.
    - interval (int, optional): The interval between dates. Default is 1.
    - separator (str, optional): The separator used to join combinations. Default is '_'.
    - midpoint (float, optional): The midpoint value for generating random data. Default is 100.
    - variance (float, optional): The variance value for generating random data. Default is 10.

    Returns:
    - DataFrame: A DataFrame containing sample data.

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

    # TO DO: support frequency multiplier
    # freq_str = f"{interval}{freq}"
    # print(freq_str)
    dates = pd.date_range(start=start_date, end=end_date, freq=freq)

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
    rows = len(dates)
    cols = len(series)
    some_data = pd.DataFrame(
        midpoint + variance * np.random.randn(rows, cols),
        columns=series,
        dtype="float32[pyarrow]",
    )

    # Legg til "Dates" som den første kolonnen i "some_data"
    some_data.insert(0, "valid_at", dates)

    return some_data


# # def create_dataset(name:str, series_tags:dict, dataset_tags:dict={}, **kwargs) -> pd.DataFrame:
# def create_dataset(
#     name: str,
#     series_tags: dict,
#     data_type: SeriesType = SeriesType.simple(),
#     dataset_tags: dict = {},
#     as_of_tz: datetime = dates.now_utc(),
#     start_date=None,
#     end_date=None,
#     **kwargs
#     # freq: str = "D",
#     # interval: int = 1,
#     # separator: str = "_",
#     # midpoint: int = 100,
#     # variance: int = 10,
# ) -> Dataset:
#     """
#     For a specified date range, generate sample data for permutations of series metadata in dictionary.

#     Parameters:
#     - series_tags: dictionary of attribute names and lists of values to generate series metadata and data columns from.
#     - start_date (datetime, optional): The start date of the date range. Defaults to negative infinity.
#     - end_date (datetime, optional): The end date of the date range. Defaults to positive infinity.
#     - freq (str, optional): The frequency of date generation:
#         'Y' for yearly at last day of year,
#         'YS' for yearly at first day of year,
#         'M' for monthly at last day of month,
#         'MS' for monthly at first day of month,
#         'W' for weekly on Sundays,
#         'D' for daily,
#         'H' for hourly,
#         'T' for minutely,
#         'S' for secondly,
#         etc.
#       Default is 'D'.
#     - interval (int, optional): The interval between dates. Default is 1.
#     - separator (str, optional): The separator used to join combinations. Default is '_'.
#     - midpoint (float, optional): The midpoint value for generating random data. Default is 100.
#     - variance (float, optional): The variance value for generating random data. Default is 10.

#     Returns:
#     - DataFrame: A DataFrame containing sample data.

#     Example:
#     ```
#     # Generate sample data with no specified start or end date (defaults to +/- infinity)
#     sample_data = generate_sample_df(List1, List2, freq='D')
#     ```
#     """

#     x = Dataset(name=name, data_type=data_type, as_of_tz=as_of_tz)
#     lists = [value for value in series_tags.values()]

#     x.data = create_df(
#         *lists,
#         start_date=start_date,
#         end_date=end_date,
#         **kwargs
#         # freq=freq,
#         # interval=interval,
#         # separator=separator,
#         # midpoint=midpoint,
#         # variance=variance,
#     )

#     return x
