"""The :py:mod:`ssb_timeseries.sample_data` module provides tools for creating sample timeseries data. These are convenience functions for tests and demos."""

from __future__ import annotations

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

from .dataframes.date_cols import temporal_columns
from .dataframes.dates import datelike_convert_timezone
from .dates import DEFAULT_TZ
from .dates import TimeZone
from .dates import date_round
from .dates import date_tz
from .dates import ensure_datetime

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

    if len(args) == 1 and isinstance(args[0], dict):
        return list(args[0].values())

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
        elif isinstance(arg, dict):
            final_args.append(list(arg.values()))
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
    tz: str | TimeZone = DEFAULT_TZ,
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
        tz: Timezone to convert data to after generation; uses DFAULT_TZ if not specified.

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
        start_date=date_tz(start_date, tz),
        end_date=date_tz(end_date, tz),
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

    return _as_implementation(data_dict, implementation, tz)


def _as_implementation(
    data_dict: dict[str, Any], implementation: str, tz: str | TimeZone
) -> Any:
    """Return a data dictionary converted to the requested dataframe implementation.

    Args:
        data_dict (dict[str, Any]): Column names mapped to column values.
        implementation: Narwhals supported dataframe library or object type.
        tz: Timezone to convert date columns to.

    Returns:
        The data as a dict if implementation is 'dict', otherwise a dataframe of the requested type.

    """
    if implementation == "dict":
        return data_dict

    nw_df = nw.from_dict(data_dict, backend=implementation)
    match implementation.lower():
        case "pyarrow" | "arrow" | "pa":
            out = nw_df.to_arrow()  # type: ignore[assignment]
        case "numpy" | "np":
            out = nw_df.to_numpy()  # type: ignore[assignment]
        case "polars" | "pl":
            out = nw_df.to_polars()  # type: ignore[assignment]
        case "narwhals" | "nw":
            out = nw_df  # type: ignore[assignment]
        case "pandas" | "pd" | _:
            out = nw_df.to_pandas().reset_index(drop=True)  # type: ignore[assignment]
            out.set_index(temporal_columns(nw_df))
    return datelike_convert_timezone(out, tz)


def date_ranges(
    start_date: datetime | str,
    end_date: datetime | str,
    freq: str,
    interval: int = 1,
    temporality: str = "AT",
    tz: TimeZone = DEFAULT_TZ,
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
    dt_start = date_tz(ensure_datetime(start_date), tz)
    dt_end = date_tz(ensure_datetime(end_date), tz)
    d = r(
        dtstart=dt_start,
        until=dt_end,
    )
    d_list = list(d)
    if str(temporality) == "AT":
        return {"valid_at": d_list}
    elif str(temporality) == "FROM_TO":
        delta = relativedelta(d_list[1], d_list[0])
        d_to = r(dtstart=dt_start + delta, until=dt_end + delta)
        return {"valid_from": d_list, "valid_to": list(d_to)}
    else:
        raise ValueError(f"Unhandled temporality: {temporality}")


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


POPU06_SOURCE = (
    "https://pxweb.nordicstatistics.org/api/v1/en/Nordic Statistics"
    "/Demography/Population projections/POPU06.px"
)
"""Table the static :py:const:`POPU06_POPULATION` values are copied from."""

POPU06_MAIN_COUNTRIES = ("Denmark", "Finland", "Iceland", "Norway", "Sweden")
"""The five Nordic countries reported in POPU06.

Every one of them is projected for 2027 to 2046, the longest span shared by all reporting countries.
Denmark, Iceland, Norway and Sweden are projected all the way to 2070.
"""

POPU06_YEARS = tuple(range(2027, 2071))

POPU06_POPULATION: dict[str, tuple[int | None, ...]] = {
    "Denmark": (
        5999093,
        6015949,
        6032459,
        6048419,
        6063759,
        6078384,
        6092268,
        6105380,
        6117690,
        6129258,
        6140055,
        6150065,
        6159310,
        6167755,
        6175400,
        6182235,
        6188367,
        6193777,
        6198609,
        6202891,
        6206735,
        6210124,
        6213100,
        6215670,
        6217831,
        6219556,
        6220859,
        6221808,
        6222540,
        6223157,
        6223827,
        6224663,
        6225758,
        6227268,
        6229222,
        6231720,
        6234763,
        6238385,
        6242630,
        6247434,
        6252770,
        6258573,
        6264738,
        6271185,
    ),
    "Faroe Islands": (
        55754,
        55949,
        56142,
        56341,
        56527,
        56696,
        56862,
        57021,
        57180,
        57323,
        57452,
        57588,
        57719,
        57837,
        57968,
        58085,
        58192,
        58304,
        58388,
        58462,
        58547,
        58612,
        58680,
        58743,
        58803,
        58847,
        58890,
        58946,
        58968,
        58994,
        59022,
        59049,
        59088,
        59124,
        59149,
        59174,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ),
    "Greenland": (
        56553,
        56358,
        56158,
        55938,
        55709,
        55472,
        55205,
        54930,
        54647,
        54347,
        54040,
        53731,
        53419,
        53104,
        52795,
        52476,
        52161,
        51838,
        51520,
        51189,
        50881,
        50560,
        50242,
        49932,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ),
    "Finland": (
        5694785,
        5719048,
        5742996,
        5766603,
        5789834,
        5812631,
        5834984,
        5856950,
        5878470,
        5899586,
        5920285,
        5940582,
        5960452,
        5979959,
        5999124,
        6017961,
        6036526,
        6054829,
        6072912,
        6090802,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ),
    "Åland": (
        30884,
        30990,
        31074,
        31152,
        31219,
        31273,
        31325,
        31370,
        31406,
        31437,
        31462,
        31482,
        31500,
        31515,
        31529,
        31542,
        31560,
        31573,
        31589,
        31596,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ),
    "Iceland": (
        405087,
        412681,
        420191,
        427648,
        434998,
        442187,
        449133,
        455936,
        462605,
        469125,
        475449,
        481565,
        487486,
        493055,
        498312,
        503411,
        508243,
        512868,
        517252,
        521450,
        525422,
        528783,
        532013,
        535058,
        537918,
        540536,
        543007,
        545351,
        547338,
        549085,
        550657,
        552064,
        553397,
        554672,
        555868,
        556840,
        557684,
        558353,
        558803,
        559129,
        559236,
        559059,
        558789,
        558277,
    ),
    "Norway": (
        5666689,
        5694657,
        5722427,
        5749712,
        5776723,
        5803284,
        5829350,
        5855072,
        5880318,
        5905184,
        5928866,
        5951491,
        5973100,
        5993766,
        6013501,
        6032325,
        6050194,
        6067121,
        6083032,
        6097893,
        6111684,
        6124356,
        6135899,
        6146321,
        6155675,
        6164001,
        6171378,
        6177963,
        6183882,
        6189267,
        6194227,
        6198859,
        6203245,
        6207476,
        6211611,
        6215719,
        6219837,
        6224021,
        6228297,
        6232688,
        6237195,
        6241817,
        6246521,
        6251240,
    ),
    "Sweden": (
        10617229,
        10590195,
        10595683,
        10607632,
        10633822,
        10660739,
        10688729,
        10717978,
        10747029,
        10776335,
        10806015,
        10836448,
        10867945,
        10900804,
        10935214,
        10970081,
        11005383,
        11041168,
        11077299,
        11113669,
        11150098,
        11186370,
        11222294,
        11257590,
        11292060,
        11325460,
        11357521,
        11388169,
        11417275,
        11444857,
        11470900,
        11495591,
        11519087,
        11541712,
        11563698,
        11585398,
        11607075,
        11629002,
        11651376,
        11674358,
        11698073,
        11722553,
        11747835,
        11773818,
    ),
    "EU": (
        453274061,
        453059422,
        452876789,
        452700101,
        452518492,
        452333305,
        452162740,
        451999756,
        451991345,
        451961872,
        451909715,
        451834154,
        451724179,
        451592188,
        451422951,
        451213715,
        450962160,
        450665270,
        450322278,
        449939037,
        449500166,
        449013584,
        448468711,
        447877407,
        447240312,
        446560342,
        445840783,
        445095614,
        444312273,
        443507120,
        442668664,
        441811204,
        440936678,
        440054110,
        439173683,
        438307890,
        437459259,
        436629071,
        435822969,
        435051043,
        434296631,
        433567057,
        432868315,
        432202794,
    ),
}


def popu06(
    countries: list[str] | tuple[str, ...] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    implementation: str = "pandas",
    tz: str | TimeZone = DEFAULT_TZ,
) -> Any:
    """Return Nordic population projections from the static POPU06 table.

    Unlike the randomly generated helpers in this module, the values are real and reproducible.
    One series is returned per reporting country, so the series names are country names.

    Projections are not published for every country for every year.
    Countries with a shorter projection than the requested period yield None for the missing years.

    Args:
        countries: Reporting countries to include, one series each.
            Defaults to every country in :py:const:`POPU06_POPULATION`.
        start_year: First projection year to include.
            Defaults to the first year in :py:const:`POPU06_YEARS`.
        end_year: Last projection year to include.
            Defaults to the last year in :py:const:`POPU06_YEARS`.
        implementation: Narwhals supported dataframe library or object type.
        tz: Timezone of the generated date column.

    Returns:
        A dataframe with a 'valid_at' column and one column per country.

    Raises:
        ValueError: If a requested country is not in :py:const:`POPU06_POPULATION`.
        ValueError: If the requested period contains no projection year.

    Example:
        ```
        from ssb_timeseries.sample_data import POPU06_MAIN_COUNTRIES, popu06

        nordic = popu06(countries=POPU06_MAIN_COUNTRIES, start_year=2030)
        ```
    """
    if countries is None:
        selected = list(POPU06_POPULATION)
    else:
        selected = list(countries)
        unknown = [name for name in selected if name not in POPU06_POPULATION]
        if unknown:
            raise ValueError(f"Unknown POPU06 countries: {', '.join(unknown)}")

    years = [
        year
        for year in POPU06_YEARS
        if (start_year is None or year >= start_year)
        and (end_year is None or year <= end_year)
    ]
    if not years:
        raise ValueError("The requested period contains no POPU06 projection year.")

    first = POPU06_YEARS.index(years[0])
    last = POPU06_YEARS.index(years[-1]) + 1

    data_dict: dict[str, Any] = {
        "valid_at": [date_tz(datetime(year, 1, 1), tz) for year in years]
    }
    for name in selected:
        data_dict[name] = list(POPU06_POPULATION[name][first:last])

    return _as_implementation(data_dict, implementation, tz)
