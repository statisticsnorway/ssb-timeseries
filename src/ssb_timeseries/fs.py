import glob
import json
import os
import shutil
from pathlib import Path

import pandas
import pyarrow
from dapla import FileClient

from ssb_timeseries.types import PathStr

# from ssb_timeseries.logging import ts_logger  # , log_start_stop

"""This is an abstraction that allows file based io regardless of whether involved file systems are local or gcs.
"""

# ruff: noqa: ANN002, ANN003
# mypy: disable-error-code="arg-type, type-arg, no-any-return, no-untyped-def, import-untyped, attr-defined, type-var, index"


def remove_prefix(path: PathStr) -> str:
    """Helper function to compensate for some os.* functions shorten gs://<path> to gs:/<path>."""
    return path.replace("//", "/").replace("gs:/", "")


def is_gcs(path: PathStr) -> bool:
    """Check if path is on GCS."""
    return str(path)[:4] == "gs:/"


def is_local(path: PathStr) -> bool:
    """Check if path is local."""
    return str(path)[:4] != "gs:/"


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


# def existing_subpath(path: PathStr) -> str:
#     """Return the existing part of a path on local or GCS file system."""
#     out = ""
#     # TODO: redo this with pathlib
#     parts = str(path).split(os.sep)
#     pp = ""
#     for p in parts:
#         if p:
#             pp = os.sep.join([pp, p])
#         if exists(pp):
#             out = pp

#     return out


# redo above w pathlib
def existing_subpath(path: PathStr) -> str:
    """Return the existing part of a path on local or GCS file system."""
    if Path(path).exists():
        return str(path)
    else:
        p = Path(path).parent
        while not p.exists():
            p = Path(p).parent
        return str(p)


def touch(path: PathStr) -> None:
    """Touch file regardless of wether the filesystem is local or GCS."""
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        fs.touch(path)
    else:
        mk_parent_dir(path)
        Path(path).touch()


# def dir_name(path: PathStr):
#     basename = os.path.basename(path)
#     parts = os.path.splitext(basename)
#     if len(parts) > 1:
#         d = os.path.dirname(path)
#     else:
#         d = path

#     return d  # os.path.normpath(d)


def mkdir(path: PathStr) -> None:
    """Make directory regardless of filesystem is local or GCS."""
    # not good enough .. it is hard to distinguish between dirs and files that do not exist yet
    if is_local(path):
        os.makedirs(path, exist_ok=True)
    else:
        pass


def mk_parent_dir(path: PathStr) -> None:
    """Ensure a parent directory exists. ... regardless of wether fielsystem is local or GCS."""
    # wanted a mkdir that could work with both file and directory paths,
    # but it is hard to distinguish between dirs and files that do not exist yet
    # --> use this to create parent directory for files, mkdir() when the last part of path is a directory
    if is_local(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    else:
        pass


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
        os.makedirs(os.path.dirname(to_path), exist_ok=True)

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
        os.makedirs(os.path.dirname(to_path), exist_ok=True)

    match (from_type, to_type):
        case ("local", "local"):
            shutil.move(from_path, to_path)
        case ("local", "gcs"):
            fs.put(from_path, to_path)
        case ("gcs", "local"):
            fs.get(from_path, to_path)
        case ("gcs", "gcs"):
            fs.move(from_path, to_path)


def rm(path: PathStr, *args) -> None:
    """Remove file from local or GCS filesystem."""
    if is_gcs(path):
        pass
        # TO DO: implement this (but recursive)
        # fs = FileClient.get_gcs_file_system()
        # fs.rm(path)
    else:
        os.remove(path)


def rmtree(path: str, *args) -> None:
    """Remove all directory and all its files and subdirectories regardless of local or GCS filesystem."""
    if is_gcs(path):
        pass
        # TO DO: implement this (but recursive)
        # fs = FileClient.get_gcs_file_system()
        # fs.rm(path)
    else:
        shutil.rmtree(path)


def same_path(*args) -> PathStr:
    """Return common part of path, for two or more files. Files must be on same file system, but the file system can be either local or GCS."""
    # TO DO: add support for Windows style paths?
    # ... regex along the lines of: [A-Z\:|\\\\]
    paths = [a.replace("gs:/", "") for a in args]
    return os.path.commonpath(paths)


def find(path: PathStr, pattern: str = "", *args, **kwargs) -> list[str]:
    """Find files and subdirectories with names matching pattern. Should work for both local and GCS filesystems."""
    if pattern:
        pattern = f"*{pattern}*"
    else:
        pattern = "*"

    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        return fs.glob(os.path.join(path, pattern))
    else:
        search_str = os.path.join(path, "*", pattern)
        dirs = glob.glob(search_str)
        search_results = [d.replace(path, "root").split(os.path.sep) for d in dirs]

        return [f[2] for f in search_results]


def read_parquet(path: PathStr) -> None:
    """TODO: Add faster pyarrrow implementations enforcing type based schemas."""
    pass


def write_parquet(
    data: pyarrow.Table,  # or pd./pl.dataframe
    path: PathStr,
    tags: dict | None = None,
    schema: pyarrow.Schema = None,
) -> None:
    """TODO: Add faster pyarrrow implementations enforcing type based schemas."""
    pass


def pandas_read_parquet(
    path: PathStr,
    *args,
    **kwargs,
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
        os.makedirs(os.path.dirname(path), exist_ok=True)
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
    # Code does not make sense, but is not invoked:
    # if not isinstance(path, str):
    #     path = json.loads(path)
    # more reasonable would be somehting like -->
    # if not isinstance(content, str):
    #     content = json.loads(content)
    # ... but that caauses an error. --> Code is "too clever"?

    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(path, "w") as file:
            json.dump(content, file, indent=4, ensure_ascii=False)
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as file:
            json.dump(content, file, indent=4, ensure_ascii=False)


# from pyarrow import fs
# local = fs.LocalFileSystem()
# with local.open_output_stream('/tmp/pyarrowtest.dat') as stream:
#         stream.write(b'data')
# 4
# with local.open_input_stream('/tmp/pyarrowtest.dat') as stream:
#         print(stream.readall())
# b'data'

# # creating an fsspec-based filesystem object for Google Cloud Storage
# import gcsfs
# fs = gcsfs.GCSFileSystem(project='my-google-project')

# # using this to read a partitioned dataset
# import pyarrow.dataset as ds
# ds.dataset("data/", filesystem=fs)


# @staticmethod
# def funcname(parameter_list):
#     """Docstring"""
#     pass
