"""The IO module provides abstractions for READ and WRITE operations so that `Dataset` does not have to care avbout the mechanics.

TO DO: turn Dataset.io into a Protocol class?

Essential configs:
    TIMESERIES_CONFIG: str = os.environ.get("TIMESERIES_CONFIG")
    CONFIG = config.Config(configuration_file=TIMESERIES_CONFIG)

Default configs may be created by running
    `poetry run timeseries-config {home | jovyan | gcs}`

See `config` module docs for details.
"""

import glob
import os
import re
from datetime import datetime
from typing import NamedTuple

import pandas

from ssb_timeseries import config
from ssb_timeseries import fs
from ssb_timeseries import properties
from ssb_timeseries.dates import Interval
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.types import PathStr

# from ssb_timeseries.types import F
# from abc import ABC, abstractmethod
# from typing import Protocol
# import contextlib

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "


TIMESERIES_CONFIG: str = os.environ.get("TIMESERIES_CONFIG")
CONFIG = config.Config(configuration_file=TIMESERIES_CONFIG)


class SearchResult(NamedTuple):
    """Result item for search."""

    name: str
    type_directory: str


class FileSystem:
    """A filesystem abstraction for Dataset IO."""

    def __init__(
        self,
        set_name: str,
        set_type: properties.SeriesType,
        as_of_utc: datetime,
        process_stage: str = "statistikk",
        sharing: dict | None = None,
    ) -> None:
        """Initialise filesystem abstraction for dataset.

        Calculate directory structure based on dataset type and name.

        """
        self.set_name = set_name
        self.data_type = set_type
        self.process_stage = process_stage
        self.sharing = sharing

        if as_of_utc is None:
            pass
            # ecxception if not
        else:
            # rounded_utc = as_of_utc
            self.as_of_utc: datetime = as_of_utc.isoformat().replace(":", "")

    # def __new__(
    #     cls,
    #     set_name: str,
    #     set_type: properties.SeriesType,
    #     as_of_utc: datetime,
    #     process_stage: str = "statistikk",
    #     sharing: dict = {},
    #     type_name="local",
    #     *args,
    #     **kwargs,
    # ):

    #     subclass_map = {
    #         subclass.type_name: subclass for subclass in cls.__subclasses__()
    #     }
    #     subclass = subclass_map[type_name]
    #     instance = super(FileSystem, subclass).__new__(subclass)
    #     instance.init_fs()
    #     return instance

    @property
    def root(self) -> str:
        """The root path is the basis for all other paths."""
        ts_root = CONFIG.timeseries_root
        return ts_root

    @property
    def set_type_dir(self) -> str:
        """Under the time series root there is a directory for each data type. Names concatenate the contituents of the type: temporality and versioning."""
        return f"{self.data_type.versioning}_{self.data_type.temporality}"

    @property
    def type_path(self) -> str:
        """All sets of the same data type are stored in the same sub directory under the timeseries root."""
        return os.path.join(self.root, self.set_type_dir)

    @property
    def metadata_file(self) -> str:
        """The name of the metadata file for the dataset."""
        return f"{self.set_name}-metadata.json"

    @property
    def data_file(self) -> str:
        """The name of the data file for the dataset."""
        if "AS_OF" in self.set_type_dir:
            file_name = f"{self.set_name}-as_of_{self.as_of_utc}-data.parquet"
        elif "NONE" in self.set_type_dir:
            file_name = f"{self.set_name}-latest-data.parquet"
        elif "NAMED" in self.set_type_dir:
            file_name = f"{self.set_name}-NAMED-data.parquet"
        else:
            raise Exception("Unhandled versioning.")

        ts_logger.debug(file_name)
        return file_name

    @property
    def data_dir(self) -> str:
        """The data directory for the dataset. This is a subdirectory under the type path."""
        return os.path.join(self.type_path, self.set_name)

    @property
    def data_fullpath(self) -> str:
        """The full path to the data file."""
        return os.path.join(self.data_dir, self.data_file)

    @property
    def metadata_dir(self) -> str:
        """The location of the metadata file for the dataset.

        In the inital implementation with data and metadata in separate files it made sense for this to be the same as the data directory. However, Most likely, in a future version we will change this apporach and store metadata as header information in the data file, and the same information in a central meta data directory.
        """
        return os.path.join(self.type_path, self.set_name)

    @property
    def metadata_fullpath(self) -> str:
        """The full path to the metadata file."""
        return os.path.join(self.metadata_dir, self.metadata_file)

    def read_data(
        self,
        interval: Interval = Interval.all,
    ) -> pandas.DataFrame:
        """Read data from the filesystem. Return empty dataframe if not found."""
        ts_logger.debug(interval)
        if fs.exists(self.data_fullpath):
            ts_logger.debug(
                f"DATASET {self.set_name}: Reading data from file {self.data_fullpath}"
            )
            try:
                df = fs.pandas_read_parquet(self.data_fullpath)
                ts_logger.info(f"DATASET {self.set_name}: Read data.")
            except FileNotFoundError:
                ts_logger.exception(
                    f"DATASET {self.set_name}: Read data failed. File not found: {self.data_fullpath}"
                )
                df = pandas.DataFrame()

        else:
            df = pandas.DataFrame()

        ts_logger.debug(f"DATASET {self.set_name}: read data:\n{df}")
        return df

    def write_data(self, new: pandas.DataFrame) -> None:
        """Write data to the filesystem. If versioning is AS_OF, write to new file. If versioning is NONE, write to existing file."""
        if self.data_type.versioning == properties.Versioning.AS_OF:
            df = new
        else:
            old = self.read_data(self.set_name)
            if old.empty:
                df = new
            else:
                date_cols = list(
                    set(new.columns)
                    & set(old.columns)
                    & {"valid_at", "valid_from", "valid_to"}
                )
                df = pandas.concat(
                    [old, new],
                    axis=0,
                    ignore_index=True,
                ).drop_duplicates(date_cols, keep="last")

        ts_logger.info(
            f"DATASET {self.set_name}: starting writing data to file {self.data_fullpath}."
        )
        try:
            ts_logger.debug(df)
            fs.pandas_write_parquet(df, self.data_fullpath)
        except Exception as e:
            ts_logger.exception(
                f"DATASET {self.set_name}: failed writing data to {self.data_fullpath}, exception returned: {e}."
            )
        ts_logger.info(
            f"DATASET {self.set_name}: done writing data to file {self.data_fullpath}."
        )

    def read_metadata(self) -> dict:
        """Read tags from the metadata file."""
        meta: dict = {"name": self.set_name}
        if fs.exists(self.metadata_fullpath):
            ts_logger.info(
                f"DATASET {self.set_name}: START: Reading metadata from file {self.metadata_fullpath}."
            )
            meta = fs.read_json(self.metadata_fullpath)
        return meta

    def write_metadata(self, meta: dict) -> None:
        """Write tags to the metadata file."""
        os.makedirs(self.metadata_dir, exist_ok=True)
        try:
            fs.write_json(self.metadata_fullpath, meta)
            ts_logger.info(
                f"DATASET {self.set_name}: Writing metadata to file {self.metadata_fullpath}."
            )
        except Exception as e:
            ts_logger.exception(
                f"DATASET {self.set_name}: ERROR: Writing metadata to file {self.metadata_fullpath} returned exception {e}."
            )

    def datafile_exists(self) -> bool:
        """Check if the data file exists."""
        return fs.exists(self.data_fullpath)

    def metadatafile_exists(self) -> bool:
        """Check if the metadata file exists."""
        return fs.exists(self.metadata_fullpath)

    def save(self, meta: dict, data: pandas.DataFrame = None) -> None:
        """Save data and metadata to disk."""
        if meta:
            self.write_metadata(meta)
        else:
            ts_logger.warning(
                f"DATASET {self.set_name}: Metadata is empty. Nothing to write."
            )

        if not data.empty:
            self.write_data(data)
        else:
            ts_logger.warning(
                f"DATASET {self.set_name}: Data is empty. Nothing to write."
            )

    def last_version(self, directory: str, pattern: str = "*.parquet") -> str:
        """Check directory and get max version number from files matching regex pattern."""
        files = fs.ls(directory, pattern=pattern)
        number_of_files = len(files)

        # TODO: mypy --> error: Item "None" of "Match[str] | None" has no attribute "group"  [union-attr]
        vs = sorted(
            [int(re.search("(_v)([0-9]+)(.parquet)", f).group(2)) for f in files]
        )
        ts_logger.debug(
            f"DATASET {self.set_name}: io.last_version regex identified versions {vs} in {directory}."
        )
        if vs:
            read_from_filenames = max(vs)
            out = read_from_filenames
        else:
            read_from_filenames = 0
            out = number_of_files

        ts_logger.debug(
            f"DATASET {self.set_name}: io.last_version searched directory: \n\t{directory}\n\tfor '{pattern}' found {number_of_files!s} files, regex identified version {read_from_filenames!s} --> vs {out!s}."
        )
        return out

    def snapshot_directory(
        self, product: str, process_stage: str = "statistikk"
    ) -> PathStr:
        """Get name of snapshot directory.

        Uses dataset parameters, configuration, product and process stage.
        """
        return os.path.join(
            CONFIG.bucket,
            product,
            process_stage,
            "series",  # to distinguish from other data types
            self.set_type_dir,
            self.set_name,
        )

    def snapshot_filename(
        self,
        product: str,
        process_stage: str,
        as_of_utc: datetime | None = None,
        period_from: str = "",
        period_to: str = "",
    ) -> PathStr:
        """Get full path of snapshot file.

        Uses dataset parameters, configuration, product, process stage and as-of time.
        Relying on snapshot_directory() first to get the directory name.
        """
        directory = self.snapshot_directory(
            product=product, process_stage=process_stage
        )
        next_vs = self.last_version(directory=directory, pattern="*.parquet") + 1

        def iso_no_colon(dt: datetime) -> str:
            return dt.isoformat().replace(":", "")

        if as_of_utc:
            out = f"{self.set_name}_p{iso_no_colon(period_from)}_p{iso_no_colon(period_to)}_v{iso_no_colon(as_of_utc)}_v{next_vs}"
        else:
            out = f"{self.set_name}_p{iso_no_colon(period_from)}_p{iso_no_colon(period_to)}_v{next_vs}"

            #  to comply with the naming standard we need to stuff about the data
            ts_logger.debug(
                f"DATASET last version {next_vs} from {period_from} to {period_to}.')"
            )
        return out

    def sharing_directory(
        self,
        team: str,
        bucket: str,
    ) -> PathStr:
        """Get name of sharing directory based on dataset parameters and configuration.

        Creates the directory if it does not exist.
        """
        # if team:
        #     dir = os.path.join(bucket, team, self.set_name)
        # else:
        #     dir = os.path.join(bucket, self.set_name)

        directory = os.path.join(bucket, self.set_name)
        ts_logger.warning(f"DATASET.IO.SHARING_DIRECTORY: {directory}")
        fs.mkdir(directory)
        return directory

    def snapshot(
        self,
        product: str,
        process_stage: str,
        sharing: dict | None = None,
        as_of_tz: datetime | None = None,
        period_from: datetime | None = None,
        period_to: datetime | None = None,
    ) -> None:
        """Copies snapshots to bucket(s) according to processing stage and sharing configuration.

        For this to work, .stage and sharing configurations should be set for the dataset, eg::

            .sharing = [
                {'team': 's123', 'path': '<s1234-bucket>'},
                {'team': 's234', 'path': '<s234-bucket>'},
                {'team': 's345': 'path': '<s345-bucket>'}
            ]
            .stage = 'statistikk'

        """
        directory = self.snapshot_directory(
            product=product, process_stage=process_stage
        )
        snapshot_name = self.snapshot_filename(
            product=product,
            process_stage=process_stage,
            as_of_utc=as_of_tz,
            period_from=period_from,
            period_to=period_to,
        )

        data_publish_path = os.path.join(directory, f"{snapshot_name}.parquet")
        meta_publish_path = os.path.join(directory, f"{snapshot_name}.json")

        fs.cp(self.data_fullpath, data_publish_path)
        fs.cp(self.metadata_fullpath, meta_publish_path)

        if sharing:
            ts_logger.debug(f"Sharing configs: {sharing}")
            for s in sharing:
                ts_logger.debug(f"Sharing: {s}")
                fs.cp(
                    data_publish_path,
                    self.sharing_directory(bucket=s["path"], team=s["team"]),
                )
                fs.cp(
                    meta_publish_path,
                    self.sharing_directory(bucket=s["path"], team=s["team"]),
                )
                ts_logger.warning(
                    f"DATASET {self.set_name}: sharing with {s['team']}, snapshot copied to {s['path']}."
                )

    @classmethod
    def search(
        cls, pattern: str | PathStr = "", as_of: datetime | None = None
    ) -> list[SearchResult]:
        """Search for files in under timeseries root."""
        if pattern:
            pattern = f"*{pattern}*"
        else:
            pattern = "*"

        search_str = os.path.join(CONFIG.timeseries_root, "*", pattern)
        dirs = glob.glob(search_str)
        ts_logger.debug(f"DATASET.IO.SEARCH: {search_str} dirs{dirs}")
        search_results = [
            d.replace(CONFIG.timeseries_root, "root").split(os.path.sep) for d in dirs
        ]
        ts_logger.debug(f"DATASET.IO.SEARCH: search_results{search_results}")

        return [SearchResult(f[2], f[1]) for f in search_results]

    @classmethod
    def dir(cls, *args: str, **kwargs: bool) -> str:
        """Check that target directory is under BUCKET. If so, create it if it does not exist."""
        ts_logger.debug(f"{args}:")
        path = os.path.join(*args)
        ts_root = str(CONFIG.bucket)

        # hidden feature: also for kwarg 'force' == True
        if ts_root in path or kwargs.get("force", False):
            fs.mkdir(path)
        else:
            raise DatasetIoException(
                f"Directory {path} must be below {ts_root} in file tree."
            )
        return path


def find_datasets(
    pattern: str | PathStr = "", as_of: datetime | None = None
) -> list[SearchResult]:
    # ) -> list[str | PathStr]:
    """Search for files in under timeseries root."""
    if pattern:
        pattern = f"*{pattern}*"
    else:
        pattern = "*"

    search_str = os.path.join(CONFIG.timeseries_root, "*", pattern)
    dirs = glob.glob(search_str)
    ts_logger.debug(f"DATASET.IO.SEARCH: {search_str} dirs{dirs}")
    search_results = [
        d.replace(CONFIG.timeseries_root, "root").split(os.path.sep) for d in dirs
    ]
    ts_logger.debug(f"DATASET.IO.SEARCH: search_results{search_results}")

    return [SearchResult(f[2], f[1]) for f in search_results]


class DatasetIoException(Exception):
    """Exception for dataset io errors."""

    pass
