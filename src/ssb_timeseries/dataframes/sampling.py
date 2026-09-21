"""Dataframe date and time operations."""

from typing import Any

import narwhals as nw
import polars as pl
from narwhals.typing import IntoFrameT

from ..dates import DEFAULT_TZ
from ..dates import TimeZone
from ..types import F
from . import eager
from .date_cols import temporal_column_schema
from .dates import datelike_convert_naive
from .dates import datelike_convert_timezone
from .dates import datelike_to_default_tz

SIMPLE_AGGS = {
    "min",
    "max",
    "sum",
    "mean",
    "median",
    "std",
    "var",
    "count",
    "first",
    "last",
}
"""All works with bot Pandas and Polars, but first/last aren't quite equivalent.
Pandas' resample().first() skips NaNs by default,
Polars' .first()/.last() return whatever's literally first/last, null or not.
"""
FILL_METHODS = {"ffill", "bfill"}
PANDAS_TO_POLARS_FREQ = {
    "D": "1d",
    "W": "1w",
    "M": "1mo",
    "ME": "1mo",
    "MS": "1mo",
    "Q": "3mo",
    "QE": "3mo",
    "QS": "3mo",
    "Y": "1y",
    "YE": "1y",
    "YS": "1y",
    "H": "1h",
    "h": "1h",
    "T": "1m",
    "min": "1m",
    "S": "1s",
    "s": "1s",
    "L": "1ms",
    "ms": "1ms",
    "U": "1us",
    "us": "1us",
    "N": "1ns",
    "ns": "1ns",
}


def group_by(
    df: IntoFrameT,
    *,
    series_names: str | list[str] = "",
    tz: TimeZone = DEFAULT_TZ,
    **kwargs,
) -> IntoFrameT:
    """Check if dataframes are equal."""
    df = datelike_convert_timezone(df, tz)
    temporal = temporal_column_schema(df)
    nw_df = eager(df).sort()  # type: ignore[arg-type]
    p_df = datelike_convert_naive(nw_df.to_polars())
    result = p_df.group_by_dynamic(
        temporal.keys(),
        every=kwargs.get("every", "1y"),
        period=kwargs.get("period", "1y"),
        closed=kwargs.get("closed", "left"),
    ).agg(pl.col(series_names).sum())
    naive = nw.from_native(result.reset_index())
    tz = str({v.time_zone for v in temporal.values()}.unique)  # type: ignore[attr-defined]
    return datelike_convert_timezone(naive, tz)  # ... to_native() # of nw_df!


def resample_pandas(
    df: IntoFrameT,
    freq: str,
    func: F | str,
    *args: Any,
    **kwargs: Any,
) -> IntoFrameT:
    """Alter frequency of dataset data using Pandas syntax and conventions."""
    df_in_default_tz = datelike_to_default_tz(df)
    temporal = temporal_column_schema(df_in_default_tz)

    naive = datelike_convert_naive(df_in_default_tz)
    pd_df = eager(naive).to_pandas()  # type: ignore[arg-type]

    # TODO: have a closer look at dates returned for last period when upsampling
    resampler = pd_df.set_index(list(temporal.keys())).resample(freq)
    if isinstance(func, str) and func in SIMPLE_AGGS | FILL_METHODS:
        out = getattr(resampler, func)()
    else:
        out = pd_df.resample(freq, *args, **kwargs).apply(func)

    return nw.from_native(out.reset_index())


def resample_polars(
    df: IntoFrameT,
    freq: str,
    func: F | str,  # Literal[SIMPLE_AGGS] | Literal[FILL_METHODS],
    /,
    **kwargs: Any,
) -> pl.DataFrame:
    """Alter frequency of dataset data using Polars syntax and conventions.

    These are largely overlapping with, but not identical to Pandas'.
    Notable different behaviours are NaN handling for first/last.
    See respective documentations for more detail.
    """
    df = datelike_to_default_tz(df)
    temporal = temporal_column_schema(df)

    timezones = {v.time_zone for v in temporal.values()}  # type: ignore[attr-defined]
    if len(timezones) == 1:
        tz = next(iter(timezones))
    else:
        raise ValueError(
            "Can not resample dataframe where temporal columns have different time zones."
        )  # TODO: relax to allow differnet as_of from valid_from, valid_to / valid_at?

    pl_df = eager(datelike_convert_naive(df)).to_polars()  # type: ignore[arg-type]

    if len(temporal.keys()) != 1:
        msg = (
            f"resample_polars needs exactly one datetime column, got {temporal.keys()}"
        )
        raise ValueError(msg)
    time_col = next(iter(temporal.keys()))

    pl_df = pl_df.sort(time_col)
    other_cols = pl.all().exclude(time_col)

    if isinstance(func, str) and func in FILL_METHODS:
        strategy = "forward" if func == "ffill" else "backward"
        return pl_df.upsample(
            time_column=time_col,
            every=kwargs.pop("every", freq),
            **kwargs,
        ).fill_null(strategy=strategy)

    if isinstance(func, str) and func in SIMPLE_AGGS:
        agg_expr = getattr(other_cols, func)()
        return pl_df.group_by_dynamic(
            time_col,
            every=kwargs.pop("every", freq),
            **kwargs,
        ).agg(agg_expr)

    # Arbitrary callable: applied per-column within each dynamic window.
    # Note this is a per-column apply, not a per-group-dataframe apply
    # like pandas' `.resample().apply(func)` — if `func` needs to see
    # multiple columns of the same group at once, this won't match
    # pandas semantics and needs a different construct
    # (e.g. `.map_groups`).
    naive = pl_df.group_by_dynamic(time_col, every=freq, **kwargs).agg(
        other_cols.map_batches(func)  # type: ignore[arg-type]
    )
    return naive.with_columns(pl.col(temporal.keys()).dt.replace_time_zone(tz))
