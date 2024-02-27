# from abc import ABC, abstractmethod
import contextlib
import os
from dapla import FileClient
import shutil
import glob
import json
from timeseries.logging import ts_logger, log_start_stop

# import pyarrow
import pandas

"""This is an abstraction that allows file based io regardless of whether involved file systems are local or gcs.
"""


def remove_prefix(path: str):
    return path.replace("gs://", "")


def is_gcs(path: str):
    return path[:5] == "gs://"


def is_local(path: str):
    return path[:5] != "gs://"


def fs_type(path: str):
    out = ""
    types = {"gcs": is_gcs(path), "local": is_local(path)}
    out = list(types.keys())[list(types.values()).index(True)]
    return out


def exists(path: str):
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        return fs.exists(path)
    else:
        return os.path.exists(path)


def existing_subpath(path: str):
    out = ""
    parts = path.split(os.sep)
    pp = ""
    for p in parts:
        if p:
            pp = os.sep.join([pp, p])
        if exists(pp):
            out = pp

    return out


def dir_name(path: str):
    basename = os.path.basename(path)
    parts = os.path.splitext(basename)
    ts_logger.warning(f".FS: base:{basename} split into parts{parts}")
    if len(parts) > 1:
        d = os.path.dirname(path)
    else:
        d = path
    return d  # os.path.normpath(d)


def mkdir(path):
    if is_local(path):
        os.makedirs(dir_name(path), exist_ok=True)
    else:
        pass


def file_count(path: str, create=False):
    return len(ls(path, create=create))


def ls(path, pattern="*", create=False):
    search = os.path.join(path, pattern)
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        return fs.glob(search)
    else:
        if create:
            mkdir(path)
        return glob.glob(search)


def cp(from_path, to_path):
    """Copy file ... regardless of source and target location is local fs or GCS to local.

    Args:
        from_path (to__ty): _description_
        path_to (_type_): _description_
    """

    from_type = fs_type(from_path)
    to_type = fs_type(to_path)
    if is_gcs(from_path) | is_gcs(to_path):
        fs = FileClient.get_gcs_file_system()
    mkdir(to_path)

    match (from_type, to_type):
        case ("local", "local"):
            shutil.copy2(from_path, to_path)
        case ("local", "gcs"):
            fs.put(from_path, to_path)
        case ("gcs", "local"):
            fs.get(from_path, to_path)
        case ("gcs", "gcs"):
            fs.copy(from_path, to_path)


def mv(from_path, to_path):
    """Move file ... regardless of source and target location is local fs or GCS to local.

    Args:
        from_path (to__ty): _description_
        path_to (_type_): _description_
    """
    from_type = fs_type(from_path)
    to_type = fs_type(to_path)

    if is_gcs(from_path) | is_gcs(to_path):
        fs = FileClient.get_gcs_file_system()

    match (from_type, to_type):
        case ("local", "local"):
            shutil.move(from_path, to_path)
        case ("local", "gcs"):
            fs.put(from_path, to_path)
        case ("gcs", "local"):
            fs.get(from_path, to_path)
        case ("gcs", "gcs"):
            fs.move(from_path, to_path)


def rm(path):
    pass


def same_path(*args):
    # TO DO: add support for Windows style paths?
    # ... regex along the lines of: [A-Z\:|\\\\]
    paths = [a.replace("gs:/", "") for a in args]
    return os.path.commonpath(paths)


def find(path, pattern="", *args, **kwargs):
    if is_gcs(path):
        pass
    else:
        if pattern:
            pattern = f"*{pattern}*"
        else:
            pattern = "*"

        search_str = os.path.join(path, "*", pattern)
        dirs = glob.glob(search_str)
        search_results = [d.replace(path, "root").split(os.path.sep) for d in dirs]

        return [f[2] for f in search_results]


@contextlib.contextmanager
def create_temp_directory_and_delete_after_use(path):
    """Create a temporary directory (ONLY if not exists), to be used like so:

    with create_temp_directory_and_delete_after_use(path):
        do_stuff()

    """
    target_exists = exists(path)

    CWD = os.getcwd()

    try:
        if os.path.isdir(path):
            os.chdir(path)
        else:
            os.makedirs(path, exist_ok=True)
            os.chdir(path)

        yield
    finally:
        os.chdir(CWD)


def read_parquet(path):
    pass


def write_parquet(data, path, tags={}, schema=None):
    pass


def pandas_read_parquet(
    path,
    *args,
    **kwargs,
) -> pandas.DataFrame:

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


def pandas_write_parquet(df: pandas.DataFrame, path):

    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(path, "wb") as file:
            df.to_parquet(file)
    else:
        mkdir(path)
        df.to_parquet(path)


def read_json(path) -> dict:
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(path, "r") as file:
            return json.load(file)
    else:
        with open(path, "r") as file:
            return json.load(file)


def write_json(path, content) -> None:
    if is_gcs(path):
        fs = FileClient.get_gcs_file_system()
        with fs.open(path, "w") as file:
            json.dump(content, file, indent=4, ensure_ascii=False)
    else:
        mkdir(path)
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
