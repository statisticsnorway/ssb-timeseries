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


def group_by_pl(
    df: IntoFrameT,
    *,
    series_names: str | list[str] = "",
    tz: TimeZone = DEFAULT_TZ,
    **kwargs,
) -> IntoFrameT:
    """Aggregate over time axes."""
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


def group_by(
    df_raw: IntoFrameT,
    freq: str,
    func: str | list[str] = "",
    /,
    agg_mapping: dict | None = None,
    *,
    time_col: str = "",
    series_names: str | list[str] = "",
    tz: TimeZone = DEFAULT_TZ,
    **kwargs,
) -> IntoFrameT:
    """Aggregate over time axes."""
    df = nw.from_native(df_raw)
    if not time_col:
        temporal = temporal_column_schema(df)
        if len(temporal.keys()) > 1:
            time_col = temporal.keys()
        else:
            time_col = next(iter(temporal.keys()))

    if agg_mapping is None and not func:
        raise ValueError("Either agg_mapping or func_name must be specified.")

    # Define a temporary name for our grouping key
    group_key = f"{time_col}_{freq}"

    time_expr = nw.col(time_col)
    if tz is not None:
        time_expr = time_expr.dt.convert_time_zone(tz)

    match freq.lower():
        case "y" | "yr" | "year":
            year_expr = time_expr.dt.to_string("%Y")
            df = df.with_columns(year_expr.alias(group_key))
        case "m" | "mth" | "month":
            month_expr = time_expr.dt.to_string("%Y-%m")
            df = df.with_columns(month_expr.alias(group_key))
        case "q" | "quarter":
            # not available: ... nw.col(time_expr).dt.quarter()
            # --> math derivation: (month - 1) // 3 + 1
            y_expr = time_expr.dt.to_string("%Y")
            m_expr = time_expr.dt.month()
            q_expr = ((m_expr - 1) // 3) + 1
            # quarter_expr = f"{time_expr.dt.year()}-Q{((month_expr - 1) // 3) + 1}"
            # quarter_expr = time_expr.dt.to_string("%Y-%Q-qq").str.replace("qq", q_expr)
            quarter_expr = nw.concat_str(
                [y_expr, q_expr.cast(nw.String)], separator="-Q"
            )
            df = df.with_columns(quarter_expr.alias(group_key))
        case "w" | "wk" | "week":
            # week_expr = f"{time_expr.dt.year()}-W{nw.col(time_expr).dt.week():02}"
            week_expr = time_expr.dt.to_string("%Y-%v")
            # %v weeknum - ISO week
            # %u weeknum - Sun first day of week
            # %w weeknum - Mon first day of week
            df = df.with_columns(week_expr.alias(group_key))
        case "raw":
            # If column already contains pre-formatted strings:
            # '2026-Q1', '2026-w34', ...
            group_key = time_col
        case _:
            raise ValueError(f"Unsupported frequency type: {freq}")

    if not series_names:
        series_names = [
            name for name, dtype in df.schema.items() if not dtype.is_temporal()
        ]
    # list(set(df.columns) - set(temporal.keys()))
    if not agg_mapping:
        if func and isinstance(func, str):
            agg_mapping = {func: series_names}
        elif func and isinstance(func, list):
            agg_mapping = {f: [series_names] for f in func}

    expressions = []
    for func, cols in agg_mapping.items():
        for col in cols:
            if col not in series_names or col == group_key:
                continue
            expr = getattr(nw.col(col), func)()
            expr = expr.alias(f"{col}_{freq}_{func}")
            expressions.append(expr)

    result = df.group_by(group_key).agg(*expressions)
    if isinstance(time_col, list):
        # time_col = time_col[0]
        result = result.rename({group_key: time_col[0]})
    else:
        # if group_key != time_col:
        result = result.rename({group_key: time_col})

    return nw.to_native(result)


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

    timezones = {v.time_zone for v in temporal.values()}  # type: ignore[attr-defined]
    if len(timezones) == 1:
        tz = next(iter(timezones))
    else:
        raise ValueError(
            "Can not resample dataframe where temporal columns have different time zones."
        )  # TODO: relax to allow differnet as_of from valid_from, valid_to / valid_at?

    naive = datelike_convert_naive(df_in_default_tz)
    pd_df = eager(naive).to_pandas()  # type: ignore[arg-type]

    # TODO: have a closer look at dates returned for last period when upsampling
    resampler = pd_df.set_index(list(temporal.keys())).resample(freq)
    if isinstance(func, str) and func in SIMPLE_AGGS | FILL_METHODS:
        out = getattr(resampler, func)()
    else:
        # Arbitrary callable: "resampler" as first arg --> TODO: document!
        out = func(resampler, *args, **kwargs)  # type: ignore[operator]

    nw_out = nw.from_native(out.reset_index()).to_native()
    return datelike_convert_timezone(nw_out, tz)


def resample_polars(
    df: IntoFrameT,
    freq: str,
    func: F | str,
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
        )  # TODO: relax to allow different as_of from valid_from, valid_to / valid_at?

    pl_df = eager(datelike_convert_naive(df)).to_polars()  # type: ignore[arg-type]

    time_col = next(iter(temporal.keys()))
    pl_df = pl_df.sort(time_col)
    other_cols = pl.all().exclude(time_col)

    if isinstance(func, str) and func in FILL_METHODS:
        strategy = "forward" if func == "ffill" else "backward"
        naive = pl_df.upsample(
            time_column=time_col,
            every=kwargs.pop("every", freq),
            **kwargs,
        ).fill_null(strategy=strategy)

    elif isinstance(func, str) and func in SIMPLE_AGGS:
        agg_expr = getattr(other_cols, func)()
        naive = pl_df.group_by_dynamic(
            time_col,
            every=kwargs.pop("every", freq),
            **kwargs,
        ).agg(agg_expr)

    else:
        # Arbitrary callable: applied per-column within each dynamic window.
        # Note this is a per-column apply, not a per-group-dataframe apply
        # like pandas' `.resample().apply(func)` — if `func` needs to see
        # multiple columns of the same group at once, this won't match
        # pandas semantics and needs a different construct
        # (e.g. `.map_groups`).
        naive = pl_df.group_by_dynamic(time_col, every=freq, **kwargs).agg(
            other_cols.map_batches(func)  # type: ignore[arg-type]
        )
    return datelike_convert_timezone(naive, tz)
    # .with_columns(pl.col(temporal.keys()).dt.replace_time_zone(tz))
