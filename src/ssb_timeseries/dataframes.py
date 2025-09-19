"""A collection of helper functions to work with dataframes."""

from typing import Any
from typing import Literal
from typing import cast

import narwhals as nw
import pyarrow
from narwhals.typing import Frame
from narwhals.typing import FrameT
from narwhals.typing import IntoFrame
from numpy.typing import DTypeLike
from numpy.typing import NDArray

# mypy: disable-error-code="union-attr"


def copy(df: FrameT) -> Any:
    """Return an (eager) copy of the dataframe."""
    # return nw.from_native(df).clone().to_native()
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

    Parameters
    ----------
    dtype : type | DTypeLike | None
        The desired dtype for the resulting array. If None and output_type is
        'homogeneous', will upcast to 'object' if columns have mixed types.
        dtype is ignored when output_type='structured' as column dtypes are preserved.
    output_type : "homogeneous" | "structured"
        'homogeneous': Returns a 2D array, possibly with dtype='object' for mixed types.
        'structured': Returns a 1D structured array, preserving individual column dtypes.

    Returns:
    -------
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
