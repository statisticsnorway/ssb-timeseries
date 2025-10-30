"""A collection of helper functions to work with dataframes."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from typing import Literal
from typing import cast

import narwhals as nw
import polars as pl
import pyarrow
from narwhals.typing import Frame
from narwhals.typing import FrameT
from narwhals.typing import IntoFrame
from narwhals.typing import IntoFrameT
from numpy.typing import DTypeLike
from numpy.typing import NDArray

from .dates import standardize_dates
from .logging import logger
from .properties import SeriesType
from .properties import Temporality
from .properties import Versioning

# mypy: disable-error-code="union-attr"


def _coalesce(*args):
    """Return first value that evaluates to True."""
    return next((bool(arg) for arg in args if arg is not None), None)


def copy(df: FrameT) -> Any:
    """Return an (eager) copy of the dataframe."""
    return eager(df).clone().to_native()


def eager(df: Frame) -> Any:
    """Collect or compute for lazy implementations."""
    return nw.from_native(df).lazy().collect()


def empty_frame(
    *,
    columns: list[str] | None = None,
    schema: pyarrow.Schema | None = None,
    implementation: str = "arrow",
) -> Any:
    """Return a dataframe or Arrow table with no data."""
    if schema:
        pa_tbl = pyarrow.Table.from_pylist([], schema=schema)
        df = nw.from_native(pa_tbl)
    elif columns:
        # Pandas dependency to be removed,
        # but Pyarrow implementsation below does not create columns :(
        import pandas

        pd_df = pandas.DataFrame(columns=columns)
        df = nw.from_native(pd_df)  # type: ignore
    elif columns and False:
        pa_schema = pyarrow.schema([])
        for c in columns:
            if c in ["as_of", "valid_at", "valid_from", "valid_to"]:
                pa_schema.append(pyarrow.field(c, pyarrow.date64()))
            else:
                pa_schema.append(pyarrow.field(c, pyarrow.float64()))
        pa_tbl = pyarrow.Table.from_pylist([], schema=pa_schema)
        df = nw.from_native(pa_tbl)
    else:
        df = nw.from_native(pyarrow.Table.from_pylist([]))

    match implementation.lower():
        case "arrow" | "pyarrow" | "pa":
            return df.to_arrow()
        case "pandas" | "pd":
            return df.to_pandas()
        case "polars" | "pl":
            return df.to_polars()


def are_equal(*frames: IntoFrame) -> bool:
    """Check if dataframes are equal."""
    first_df = nw.from_native(frames[0]).to_polars()
    for df in frames[1:]:
        df = nw.from_native(df).to_polars()
        if df.shape != first_df.shape:
            return False
        elif set(df.columns) != set(first_df.columns):
            return False
        elif not df.select(sorted(df.columns)).equals(
            first_df.select(sorted(first_df.columns))
        ):
            return False
    return True


def infer_datatype(df: IntoFrame, **kwargs) -> SeriesType:
    """Checks dataframe columns and kwargs to identify SeriesType.

    Args:
        df: The dataframe to check.
        **kwargs: Override values for keys ' versioning', 'temporality'.

    Returns:
        SeriesType if the dataframe and/or kwargs provides sufficient information to infer Versioning and Temporality.

    `Versioning` is determined by assessing in order of (priority):
    - the kwarg value for the key 'versioning', if provided.
    - if dataframe columns contain 'as_of', 'as_of_tz' or 'as_of_utc'.
    - if keys of kwargs contain 'as_of', 'as_of_tz' or 'as_of_utc'.

    `Temporality` is determined by assessing in order of (priority):
    - the kwarg value for the key 'temporality', if provided.
    - if the dataframe columns contain 'valid_from' and 'valid_to'.

    """
    nw_df = nw.from_native(df)
    vs_markers = {"as_of", "as_of_tz", "as_of_utc"}
    from_to = {"valid_from", "valid_to"}

    columns = set(nw_df.columns)

    vs_explicit = kwargs.get("versioning")
    vs_from_kwargs = Versioning.AS_OF if vs_markers & set(kwargs.keys()) else None
    vs_from_column = Versioning.AS_OF if vs_markers & columns else None
    versioning = _coalesce(vs_explicit, vs_from_column, vs_from_kwargs, Versioning.NONE)

    t_from_columns = (
        Temporality.FROM_TO if from_to & columns == from_to else Temporality.AT
    )
    temporality = _coalesce(kwargs.get("temporality"), t_from_columns)

    return SeriesType(versioning, temporality)


def is_df_like(obj: Any) -> bool:
    """Checks if an object is "dataframe-like" for Narwhals compatibility.

    This is a robust, duck-typing alternative to `isinstance(obj, IntoFrameT)`,
    which is not possible.

    Args:
        obj: The object to check.

    Returns:
        True if the object has dataframe-like attributes, False otherwise.
    """
    # All supported dataframe objects have a `.shape` tuple (rows, cols)
    # and a `.columns` attribute (a list or index of column names).
    # We also check that the object is not a NumPy array, as arrays also
    # have a .shape but are not dataframes.
    return (
        hasattr(obj, "shape")
        and isinstance(getattr(obj, "shape", None), tuple)
        and hasattr(obj, "columns")
        and "numpy.ndarray" not in str(type(obj))  # A simple way to exclude numpy
    )


def is_empty(df: IntoFrame) -> bool:
    """Check if dataframe is empty."""
    # nox/mypy vs 1.10.1 --> [redundant-cast] | pre-commit --> [no-any-return]
    # (but cast was introduced because of other error with other mypy env)
    # return cast(bool, nw.from_native(df).is_empty())  # nox --> [redundant-cast]
    # return nw.from_native(df).is_empty() # pre-commit --> [no-any-return]
    # ... fix(?):
    nw_df = nw.from_native(df)
    df_is_empty = eager(nw_df).is_empty()
    return cast(bool, df_is_empty)


def rename_columns(
    df: Any,
    substitutions: dict[str, str],
) -> Any:
    """Rename columns of dataframe."""
    nw_df = nw.from_native(df)
    names = nw_df.schema.names()

    mapping = {}
    for n in names:
        for r, w in substitutions.items():
            if r in n:
                mapping[n] = n.replace(r, w)

    return nw_df.rename(mapping).to_native()


def to_arrow(
    df: IntoFrame,
    schema: pyarrow.Schema | None = None,
) -> pyarrow.Table:
    """Convert any Narwhals compatible Data Frame to Pyarrow table, cast schema if provided."""
    table = nw.from_native(df).to_arrow()
    if schema:
        return table.select(schema.names).cast(schema)
    else:
        return table


def to_numpy(
    df: Frame,
    dtype: DTypeLike | None = None,
    *,
    output_type: Literal["homogeneous", "structured"] = "homogeneous",
) -> NDArray:
    """Converts a dataframe to a NumPy ndarray.

    Parameters:
        dtype : type | DTypeLike | None
            The desired dtype for the resulting array. If None and output_type is
            'homogeneous', will upcast to 'object' if columns have mixed types.
            dtype is ignored when output_type='structured' as column dtypes are preserved.
        output_type : "homogeneous" | "structured"
            'homogeneous': Returns a 2D array, possibly with dtype='object' for mixed types.
            'structured': Returns a 1D structured array, preserving individual column dtypes.

    Returns:
        numpy.ndarray
            A NumPy array representation of the data.
    """
    nw_df = nw.from_native(df)
    if output_type == "homogeneous":
        return cast(NDArray, nw_df.to_arrow().to_numpy(dtype=dtype, copy=True))
    elif output_type == "structured":
        return nw_df.to_pandas().to_records(index=False)
    else:
        raise ValueError(
            f"Invalid output_type: {output_type}. Must be 'homogeneous' or 'structured'."
        )


def merge_data(
    old: IntoFrameT,
    new: IntoFrameT,
    date_cols: Iterable[str],
    temporality: Temporality = Temporality.AT,
) -> pyarrow.Table:
    """Merge new data into an existing dataframe, handling overlaps for period-based data.

    For `AT` temporality, it keeps the last entry for duplicates based on date columns.
    For `FROM_TO` temporality, it uses an anti-join to replace rows with matching
    `valid_from` and `valid_to` pairs.
    """
    new_pl = cast(nw.DataFrame, nw.from_native(standardize_dates(new))).to_polars()
    old_pl = cast(nw.DataFrame, nw.from_native(standardize_dates(old))).to_polars()

    # Ensure consistent dtypes, casting float32 to float64
    new_pl = new_pl.with_columns(pl.col(pl.Float32).cast(pl.Float64))
    old_pl = old_pl.with_columns(pl.col(pl.Float32).cast(pl.Float64))

    logger.debug("merge_data schemas \n%s\n%s", old_pl.schema, new_pl.schema)

    if temporality == Temporality.FROM_TO:
        # Polars-specific anti-join
        old_filtered = old_pl.join(new_pl, on=["valid_from", "valid_to"], how="anti")
        merged = nw.concat(
            [
                nw.from_native(old_filtered),
                nw.from_native(new_pl),
            ],
            how="diagonal",
        )
    else:
        # Original logic for 'AT' temporality
        merged = nw.concat(
            [nw.from_native(old_pl), nw.from_native(new_pl)],
            how="diagonal",
        )

    # Deduplicate and sort to ensure integrity
    out = merged.unique(
        subset=list(date_cols),
        keep="last",
    ).sort(by=sorted(date_cols))

    pa_table = out.to_arrow()
    return cast(pyarrow.Table, pa_table)
