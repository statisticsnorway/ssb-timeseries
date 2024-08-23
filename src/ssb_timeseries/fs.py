import functools
import glob
import json
import logging
import os
import shutil
from pathlib import Path

import pandas
import polars
import pyarrow
import pyarrow.dataset
import pyarrow.parquet as pq
from dapla import FileClient

from ssb_timeseries.types import F
from ssb_timeseries.types import PathStr

"""
This module allows file based io regardless of whether involved file systems are local or GCS.
"""

# ruff: noqa: ANN002, ANN003
# mypy: disable-error-code="arg-type, type-arg, no-any-return, no-untyped-def, import-untyped, attr-defined, type-var, index, return-value"


def path_to_str(path: PathStr) -> PathStr:
    """Normalise as strings.

    This is a trick to make automated tests pass on Windows.
    """
    return str(Path(path)).replace("gs:/", "gs://")


def wrap_return_as_str(func: F) -> F:
    """Decorator to normalise outputs using path_to_str()."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        out = func(*args, **kwargs)
        return path_to_str(out)

    return wrapper


def remove_prefix(path: PathStr) -> str:
    """Helper function to compensate for some os.* functions shorten gs://<path> to gs:/<path>."""
    return str(path).replace("//", "/").replace("gs:/", "")


def is_gcs(path: PathStr) -> bool:
    """Check if path is on GCS."""
    return str(path).startswith("gs:/")


def is_local(path: PathStr) -> bool:
    """Check if path is local."""
    return not str(path).startswith("gs:/")


def fs_type(path: PathStr) -> str:
    """Check filesystem type (local or GCS) for a given path."""
    out = ""
    types = {"gcs": is_gcs(path), "local": is_local(path)}
    out = list(types.keys())[list(types.values()).index(True)]
    return out


def exists(path: PathStr) -> bool:
    """Check if a given (local or GCS) path exists."""
    if not path:
        return False
    elif is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        return fs.exists(path)
    else:
        return Path(path).exists()


@wrap_return_as_str
def existing_subpath(path: PathStr) -> PathStr:
    """Return the existing part of a path on local or GCS file system."""
    if Path(path).exists():
        return str(path)
    else:
        p = Path(path).parent
        while not p.exists():
            p = Path(p).parent
        return p


def touch(path: PathStr) -> None:
    """Touch file regardless of wether the filesystem is local or GCS."""
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        fs.touch(path)
    else:
        mk_parent_dir(path)
        Path(path).touch()


@wrap_return_as_str
def path(*args: PathStr) -> str:
    """Join args to form path. Make sure that gcs paths are begins with double slash: gs://..."""
    p = Path(args[0]).joinpath(*args[1:])
    return p
    # .replace("gs:/", "gs://")
    # Feels dirty. Could instead do something like:
    # str(Path(args[0]).joinpath(*args[1:])).replace("gs:/{[a-z]}", "gs://{1}")


def mkdir(path: PathStr) -> None:
    """Make directory regardless of filesystem is local or GCS."""
    # not good enough .. it is hard to distinguish between dirs and files that do not exist yet
    if is_local(path):
        # os.makedirs(path, exist_ok=True)
        Path(path).mkdir(parents=True, exist_ok=True)
    else:
        ...


def mk_parent_dir(path: PathStr) -> None:
    """Ensure a parent directory exists. ... regardless of wether fielsystem is local or GCS."""
    # wanted a mkdir that could work seamlessly with both file and directory paths,
    # but it is hard to distinguish between dirs and files that do not exist yet
    # --> use this to create parent directory for files, mkdir() when the last part of path is a directory
    if is_local(path):
        # os.makedirs(os.path.dirname(path), exist_ok=True)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    else:
        ...


def file_count(path: PathStr, create: bool = False) -> int:
    """Count files in path. Should work regardless of wether source and target location is local fs or GCS to local."""
    return len(ls(path, create=create))


def ls(path: str, pattern: str = "*", create: bool = False) -> list[str]:
    """List files. Should work regardless of wether the filesystem is local or GCS."""
    search = os.path.join(path, pattern)
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        return fs.glob(search)
    else:
        if create:
            mkdir(path)
        return glob.glob(search)


def cp(from_path: PathStr, to_path: PathStr) -> None:
    """Copy file ... regardless of source and target location is local fs or GCS to local."""
    from_type = fs_type(from_path)
    to_type = fs_type(to_path)
    if is_gcs(from_path) | is_gcs(to_path):
        fs = FileClient.get_gcs_file_system()
    if is_local(to_path):
        mk_parent_dir(to_path)

    match (from_type, to_type):
        case ("local", "local"):
            shutil.copy2(from_path, to_path)
        case ("local", "gcs"):
            fs.put(from_path, to_path)
        case ("gcs", "local"):
            fs.get(from_path, to_path)
        case ("gcs", "gcs"):
            fs.copy(from_path, to_path)


def mv(from_path: PathStr, to_path: PathStr) -> None:
    """Move file ... regardless of source and target location is local fs or GCS to local."""
    from_type = fs_type(from_path)
    to_type = fs_type(to_path)

    if is_gcs(from_path) | is_gcs(to_path):
        fs = FileClient.get_gcs_file_system()
    if is_local(to_path):
        mk_parent_dir(to_path)

    match (from_type, to_type):
        case ("local", "local"):
            shutil.move(from_path, to_path)
        case ("local", "gcs"):
            fs.put(from_path, to_path)
        case ("gcs", "local"):
            fs.get(from_path, to_path)
        case ("gcs", "gcs"):
            fs.move(from_path, to_path)


def rm(path: PathStr) -> None:
    """Remove file from local or GCS filesystem. Nonrecursive. For a recursive variant, see rmtree()."""
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        fs.rm(path)
    else:
        os.remove(path)


def rmtree(
    path: str,
) -> None:
    """Recursively remove a directory and all its subdirectories and files regardless of local or GCS filesystem."""
    if is_gcs(path):
        ...
        # TO DO: implement this (but recursive)
        # fs = FileClient.get_gcs_file_system()
        # fs.rm(path)
    else:
        shutil.rmtree(path)


@wrap_return_as_str
def same_path(*args) -> PathStr:
    """Return common part of path, for two or more files. Files must be on same file system, but the file system can be either local or GCS."""
    # TO DO: add support for Windows style paths?
    # ... regex along the lines of: [A-Z\:|\\\\]
    paths = [a.replace("gs:/", "") for a in args]
    return os.path.commonpath(paths)


def find(
    search_path: PathStr,
    equals: str = "",
    contains: str = "",
    pattern: str = "",
    search_sub_dirs: bool = True,
    full_path: bool = False,
    replace_root: bool = False,
) -> list[str]:
    """Find files and subdirectories with names matching pattern. Should work for both local and GCS filesystems."""
    if contains:
        pattern = f"*{pattern}*"
    elif equals:
        pattern = equals
    elif not pattern:
        pattern = "*"

    if search_sub_dirs:
        search_str = path(search_path, "*", pattern)
    else:
        search_str = path(search_path, pattern)

    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        found = fs.glob(search_str)
    else:
        found = glob.glob(search_str)

    if replace_root:
        # may be necessary if not returning full path? -> TODO: add tests
        found = [f.replace(path, "root").split(os.path.sep) for f in found]

    if full_path:
        return found
    else:
        return [f[-1] for f in found]


def read_parquet(
    path: PathStr, returntype: str = "pandas"
) -> tuple[pyarrow.table, pyarrow.Schema]:
    """TODO: Add faster pyarrrow implementations enforcing type based schemas."""
    table = pq.read_table(path)
    match returntype:
        case "pandas":
            data = table.to_pandas()
        case "polars":
            data = table.to_pandas()
        case _:
            data = table
    return (data, table.schema)


def write_parquet(
    data: pyarrow.Table | pandas.DataFrame | polars.DataFrame,
    path: PathStr,
    schema: pyarrow.Schema | None = None,
    **kwargs,
) -> None:
    """TODO: Add faster pyarrrow implementations enforcing type based schemas."""
    table = to_arrow(data, schema)
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
    else:
        fs = pyarrow.fs.LocalFileSystem()
        mk_parent_dir(path)

    pq.write_table(
        table,
        where=path,
        filesystem=fs,
        **kwargs,
    )
    # pyarrow.dataset.write_dataset(
    #     data,
    #     path,
    #     filesystem=fs,
    #     format="parquet",
    #     schema=schema,
    #     # partitioning=["as_of_utc"],
    #     # partitioning_flavor="hive",
    #     **kwargs,
    # )


# def update_parquet_metadata(
#     path: PathStr,
#     tags: dict | None = None,
#     schema: pyarrow.Schema = None,
# ) -> None:
#     """TODO: Add faster pyarrrow implementations enforcing type based schemas."""
#     table = pq.read_table(path)
#     existing_metadata = table.schema.metadata

#     if schema:
#         table = table.cast(schema)

#     byte_encoded_tags = json.dumps(tags).encode("utf8")
#     merged_metadata = {
#         **existing_metadata,
#         **{"metadata": byte_encoded_tags},
#     }

#     # is this covered by cast(schema) and replace_schema_metadata?
#     # convert_data = table.cast(table.schema)
#     pq.write_table(
#         table,
#         table.replace_schema_metadata(merged_metadata),
#         path,
#     )


def pandas_read_parquet(
    path: PathStr,
) -> pandas.DataFrame:
    """Quick and dirty --> replace later."""
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(path, "rb") as file:
            df = pandas.read_parquet(file)
    else:
        if exists(path):
            df = pandas.read_parquet(path)
        else:
            df = pandas.DataFrame()

    return df


def pandas_write_parquet(df: pandas.DataFrame, path: PathStr) -> None:
    """Quick and dirty --> replace later."""
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(path, "wb") as file:
            df.to_parquet(file)
    else:
        mk_parent_dir(path)
        df.to_parquet(path)


def read_json(path: PathStr) -> dict:
    """Read json file from path on either local fs or GCS."""
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(path, "r") as file:
            return json.load(file)
    else:
        with open(path) as file:
            return json.load(file)


def write_json(path: PathStr, content: str | dict) -> None:
    """Write json file to path on either local fs or GCS."""
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(path, "w") as file:
            json.dump(content, file, indent=4, ensure_ascii=False)
    else:
        mk_parent_dir(path)
        with open(path, "w") as file:
            json.dump(content, file, indent=4, ensure_ascii=False)


# nosonar: disable comment
# from pyarrow import fs
# local = fs.LocalFileSystem()
# with local.open_output_stream('/tmp/pyarrowtest.dat') as stream:
#         stream.write(b'data')
# 4
# with local.open_input_stream('/tmp/pyarrowtest.dat') as stream:warning
#         print(stream.readall())
# b'data'

# # creating an fsspec-based filesystem object for Google Cloud Storage
# import gcsfs
# fs = gcsfs.GCSFileSystem(project='my-google-project')

# # using this to read a partitioned dataset
# import pyarrow.dataset as ds
# ds.dataset("data/", filesystem=fs)


# def metadata_from_parquet(filename: PathStr) -> dict:
#     """Read metadata from parquet file."""
#     meta = pq.read_metadata(filename)
#     decoded_schema = base64.b64decode(meta.metadata[b"ARROW:schema"])
#     return pyarrow.ipc.read_schema(pyarrow.BufferReader(decoded_schema))


# def metadata_to_parquet(metadata: dict, filename: PathStr) -> None:
#     """Write metadata to parquet file."""
#     schema = pyarrow.schema(metadata)
#     table = pyarrow.Table.from_pandas(metadata, schema=schema)
#     pq.write_table(table, filename)


def to_arrow(
    df: pyarrow.Table | polars.DataFrame | pandas.DataFrame,
    schema: pyarrow.Schema | None = None,
) -> pyarrow.Table:
    """Convert a Pandas or Polars dataframe to Pyarrow table, cast schema if provided."""
    if isinstance(df, pyarrow.Table):
        table = df
    elif isinstance(df, polars.DataFrame):
        logging.debug(f"Polars Dataframe will be converted to Arrow Table.\n{df}")
        table = df.to_arrow()
    elif isinstance(df, pandas.DataFrame):
        logging.debug(f"Pandas Dataframe will be converted to Arrow Table.\n{df}")
        table = pyarrow.Table.from_pandas(df, schema=schema)
    else:
        raise ValueError

    if schema:
        return table.select(schema.names).cast(schema)
    else:
        return table
