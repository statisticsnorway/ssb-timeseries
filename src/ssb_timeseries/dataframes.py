"""A collection of helper functions to work with dataframes."""

from typing import Any

import narwhals as nw
import pyarrow
from narwhals.typing import IntoFrame
from narwhals.typing import IntoFrameT


def copy(df: IntoFrameT) -> IntoFrameT:
    """Check if dataframe is empty."""
    return nw.from_native(df).clone().to_native()


def eager(df: IntoFrameT) -> nw.DataFrame:
    """Collect or compute for lazy implementations."""
    return nw.from_native(df).lazy(backend="polars").collect()


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


def is_empty(df: IntoFrame) -> bool:
    """Check if dataframe is empty."""
    # nox/mypy vs 1.10.1 --> [redundant-cast] | pre-commit --> [no-any-return]
    # (but cast was introduced because of other error with other mypy env)
    # return cast(bool, nw.from_native(df).is_empty())  # nox --> [redundant-cast]
    # return nw.from_native(df).is_empty() # pre-commit --> [no-any-return]
    # ... fix(?):
    nw_df = nw.from_native(df)
    df_is_empty = eager(nw_df).is_empty()
    return df_is_empty


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
