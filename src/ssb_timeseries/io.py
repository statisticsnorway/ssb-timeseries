"""The IO module provides abstractions for READ and WRITE operations so that `Dataset` does not have to care avbout the mechanics.

TO DO: turn Dataset.io into a Protocol class?

Essential configs:
    TIMESERIES_CONFIG: str = os.environ.get("TIMESERIES_CONFIG")
    CONFIG = config.Config(configuration_file=TIMESERIES_CONFIG)

Default configs may be created by running
    `poetry run timeseries-config {home | jovyan | gcs}`

See `config` module docs for details.
"""

import json
import os
import re
from copy import deepcopy
from datetime import datetime
from typing import Any
from typing import NamedTuple
from typing import TypeAlias

import pandas
import polars
import pyarrow
import pyarrow.compute

import ssb_timeseries as ts
from ssb_timeseries import fs
from ssb_timeseries import properties
from ssb_timeseries.config import Config
from ssb_timeseries.config import FileBasedRepository
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import utc_iso_no_colon
from ssb_timeseries.meta import DatasetTagDict
from ssb_timeseries.meta import TagDict
from ssb_timeseries.types import PathStr

# consider:
# from ssb_timeseries.types import F
# from abc import ABC, abstractmethod
# from typing import Protocol
# import contextlib

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "
# ruff: noqa: D202

Data: TypeAlias = pyarrow.Table | pandas.DataFrame | polars.DataFrame

active_config = Config.active


def version_from_file_name(
    file_name: str, pattern: str | properties.Versioning = "as_of", group: int = 2
) -> str:
    """For known name patterns, extract version marker."""
    if isinstance(pattern, properties.Versioning):
        pattern = str(pattern)

    match pattern.lower():
        case "persisted":
            regex = r"(_v)(\d+)(.parquet)"
        case "as_of":
            regex = "(as_of_)(.*)(-data.parquet)"
        case "names":
            # type is not implemented
            regex = "(_v)(*)(-data.parquet)"
        case "none":
            regex = "(.*)(latest)(-data.parquet)"
        case _:
            regex = pattern

    vs = re.search(regex, file_name).group(group)
    ts.logger.debug(
        "file: %s pattern:%s, regex%s \n--> version: %s ",
        file_name,
        pattern,
        regex,
        vs,
    )
    return vs


class SearchResult(NamedTuple):
    """Result item for search."""

    name: str
    type_directory: str


class FileSystem:
    """A filesystem abstraction for Dataset IO."""

    def __init__(
        self,
        repository: str | FileBasedRepository,
        set_name: str,
        set_type: properties.SeriesType,
        as_of_utc: datetime | None = None,
        process_stage: str = "statistikk",
        sharing: dict | None = None,
    ) -> None:
        """Initialise filesystem abstraction for dataset.

        Calculate directory structure based on dataset type and name.

        """
        if isinstance(repository, dict):
            self.repository = repository
        else:
            cfg = Config.active()
            self.repository = cfg.repositories.get(repository)

        self.set_name = set_name
        self.data_type = set_type
        self.process_stage = process_stage
        self.sharing = sharing

        # consider:
        # if as_of_utc is None:
        #     ...
        #     # exception if type is AS_OF?
        # else:
        self.as_of_utc: datetime = utc_iso_no_colon(as_of_utc)

    @property
    def root(self) -> str:
        """The root path is the basis for all other paths."""
        ts_root = self.repository["directory"]
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
        match str(self.data_type.versioning):
            case "AS_OF":
                file_name = f"{self.set_name}-as_of_{self.as_of_utc}-data.parquet"
            case "NONE":
                file_name = f"{self.set_name}-latest-data.parquet"
            case "NAMED":
                file_name = f"{self.set_name}-NAMED-data.parquet"
            case _:
                raise ValueError("Unhandled versioning.")

        ts.logger.debug(file_name)
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
        return self.repository["catalog"]
        # replaces: return os.path.join(self.type_path, self.set_name)

    @property
    def metadata_fullpath(self) -> str:
        """The full path to the metadata file."""
        return os.path.join(self.metadata_dir, self.metadata_file)

    def read_data(
        self, interval: str = ""
    ) -> pandas.DataFrame:  #: Interval = Interval.all,
        """Read data from the filesystem. Return empty dataframe if not found."""
        ts.logger.debug(interval)
        if fs.exists(self.data_fullpath):
            ts.logger.debug(
                "DATASET.read.start %s: Reading data from file %s",
                self.set_name,
                self.data_fullpath,
            )
            try:
                df = fs.pandas_read_parquet(self.data_fullpath)
                ts.logger.info("DATASET.read.success %s: Read data.", self.set_name)
            except FileNotFoundError:
                ts.logger.exception(
                    "DATASET.read.error %s: Read data failed. File not found: %s",
                    self.set_name,
                    self.data_fullpath,
                )
                df = pandas.DataFrame()

        else:
            df = pandas.DataFrame()
        return df

    def write_data(self, new: pandas.DataFrame, tags: dict | None = None) -> None:
        """Write data to the filesystem. If versioning is AS_OF, write to new file. If versioning is NONE, write to existing file."""
        if self.data_type.versioning == properties.Versioning.AS_OF:
            df = new
        else:
            old = self.read_data(self.set_name)
            if old.empty:
                df = new
            else:
                df = merge_data(old, new, self.data_type.temporality.date_columns)

        ts.logger.info(
            "DATASET.write.start %s: writing data to file\n\t%s\nstarted.",
            self.set_name,
            self.data_fullpath,
        )
        try:
            if tags:
                schema = self.parquet_schema(tags)
            else:
                raise ValueError("Tags can not be empty.")
            #     schema = self.parquet_schema_from_df(df)

            # test logs show test-merge- has many NANs in oldest data
            if schema:
                ts.logger.debug("Pyarrow schema defined: \n%s\n%s.", schema, df)
                fs.write_parquet(
                    data=df,
                    path=self.data_fullpath,
                    schema=schema,
                    # existing_data_behavior="overwrite_or_ignore",
                )

            else:
                ts.logger.warning(
                    "Arrow schema is not defined: %s.\nFalling back to writing with Pandas.",
                    self.set_name,
                )
                fs.pandas_write_parquet(df, self.data_fullpath)
        except Exception as e:
            ts.logger.exception(
                "DATASET.write.error %s: writing data to file\n\t%s\nreturned exception: %s.",
                self.set_name,
                self.data_fullpath,
                e,
            )
        ts.logger.info(
            "DATASET.write.success %s: writing data to file\n\t%s\nended.",
            self.set_name,
            self.data_fullpath,
        )

    def read_metadata(self) -> dict:
        """Read tags from the metadata file."""
        meta: dict = {"name": self.set_name}
        if fs.exists(self.metadata_fullpath):
            ts.logger.info(
                "DATASET.read.success %s: reading metadata from file %s\nended.",
                self.set_name,
                self.metadata_fullpath,
            )
            meta = fs.read_json(self.metadata_fullpath)
        else:
            ts.logger.debug("Metadata file %s was not found.", self.metadata_fullpath)
        return meta

    def write_metadata(self, meta: dict) -> None:
        """Write tags to the metadata file."""
        try:
            fs.write_json(self.metadata_fullpath, meta)
            ts.logger.info(
                "DATASET %s: Writing metadata to file %s.",
                self.set_name,
                self.metadata_fullpath,
            )
        except Exception as e:
            ts.logger.exception(
                "DATASET %s: Writing metadata to file %s returned exception %s.",
                self.set_name,
                self.metadata_fullpath,
                e,
            )

    def parquet_schema(
        self,
        meta: dict[str, Any],
    ) -> pyarrow.Schema | None:
        """Dataset specific helper: translate tags to parquet schema metadata before the generic call 'write_parquet'."""
        return parquet_schema(self.data_type, meta)

    def parquet_schema_from_df(self, df: pandas.DataFrame) -> pyarrow.Schema | None:
        """Dataset specific helper: translate tags to parquet schema metadata before the generic call 'write_parquet'."""
        schema = pyarrow.schema(df.columns, metadata=df.dtypes.to_dict())
        return schema

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
            ts.logger.warning(
                "DATASET %s: Metadata is empty. Nothing to write.",
                self.set_name,
            )

        if not data.empty:
            self.write_data(data, tags=meta)
        else:
            ts.logger.warning(
                "DATASET %s: Data is empty. Nothing to write.",
                self.set_name,
            )

    def last_version_number_by_regex(self, directory: str, pattern: str = "*") -> str:
        """Check directory and get max version number from files matching regex pattern."""
        files = fs.ls(directory, pattern=pattern)
        number_of_files = len(files)

        vs = sorted(
            [int(version_from_file_name(fname, "persisted")) for fname in files]
        )
        ts.logger.debug(
            "DATASET %s: io.last_version regex identified versions %s in %s.",
            self.set_name,
            vs,
            directory,
        )
        if vs:
            read_from_filenames = max(vs)
            out = read_from_filenames
        else:
            read_from_filenames = 0
            out = number_of_files

        ts.logger.debug(
            "DATASET %s: io.last_version searched directory: \n\t%s\n\tfor '%s' found %s files, regex identified version %s --> vs %s.",
            self.set_name,
            directory,
            pattern,
            f"{number_of_files!s}",
            f"{read_from_filenames!s}",
            f"{out!s}",
        )
        return out

    def list_versions(
        self, file_pattern: str = "*", pattern: str | properties.Versioning = "as_of"
    ) -> list[str | datetime]:
        """Check data directory and list version marker ('as-of' or 'name') of data files."""
        files = fs.ls(self.data_dir, pattern=file_pattern)

        if files:
            vs_strings = [
                version_from_file_name(str(fname), pattern, group=2) for fname in files
            ]
            match pattern:
                case properties.Versioning.AS_OF:
                    return sorted([date_utc(as_of) for as_of in vs_strings])
                case properties.Versioning.NAMES:
                    return sorted([name for name in vs_strings])
                case properties.Versioning.NONE:
                    return [late for late in vs_strings]
                case _:
                    raise ValueError(f"pattern '{pattern}' not recognized.")
        else:
            return []

    def snapshot_directory(
        self, bucket: str = "", product: str = "", process_stage: str = "statistikk"
    ) -> PathStr:
        """Get name of snapshot directory.

        Uses dataset parameters, configuration, product and process stage.
        """
        if not bucket:
            bucket = Config.active().bucket

        return os.path.join(
            bucket,
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
        next_vs = (
            self.last_version_number_by_regex(directory=directory, pattern="*.parquet")
            + 1
        )

        def iso_no_colon(dt: datetime) -> str:
            return dt.isoformat().replace(":", "")

        if as_of_utc:
            out = f"{self.set_name}_p{iso_no_colon(period_from)}_p{iso_no_colon(period_to)}_v{iso_no_colon(as_of_utc)}_v{next_vs}"
        else:
            out = f"{self.set_name}_p{iso_no_colon(period_from)}_p{iso_no_colon(period_to)}_v{next_vs}"

            #  to comply with the naming standard we need to know some things about the data
            ts.logger.debug(
                "DATASET last version %s from %s to %s.')",
                next_vs,
                period_from,
                period_to,
            )
        return out

    def sharing_directory(self, bucket: str) -> PathStr:
        """Get name of sharing directory based on dataset parameters and configuration.

        Creates the directory if it does not exist.
        """
        directory = os.path.join(bucket, self.set_name)

        ts.logger.debug(
            "DATASET.IO.SHARING_DIRECTORY: %s",
            directory,
        )
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
            ts.logger.debug("Sharing configs: %s", sharing)
            for s in sharing:
                ts.logger.debug("Sharing: %s", s)
                if "team" not in s.keys():
                    s["team"] = "no team specified"
                fs.cp(
                    data_publish_path,
                    self.sharing_directory(bucket=s["path"]),
                )
                fs.cp(
                    meta_publish_path,
                    self.sharing_directory(bucket=s["path"]),
                )
                ts.logger.debug(
                    "DATASET %s: sharing with %s, snapshot copied to %s.",
                    self.set_name,
                    s["team"],
                    s["path"],
                )

    @classmethod
    def dir(cls, *args: str, **kwargs: bool) -> str:
        """Check that target directory is under BUCKET. If so, create it if it does not exist."""
        path = os.path.join(*args)
        ts_root = str(active_config().bucket)

        # hidden feature: also for kwarg 'force' == True
        if ts_root in path or kwargs.get("force", False):
            fs.mkdir(path)
        else:
            # this should be relaxed when support for multiple shared buckets is added
            raise DatasetIoException(
                f"Directory {path} must be below {ts_root} in file tree."
            )
        return path


def find_datasets(
    pattern: str | PathStr = "",  # as_of: datetime | None = None
    exclude: str = "metadata",
    repository: list[PathStr] | PathStr = "",
) -> list[SearchResult]:
    """Search for files in data directories of all configured repositories."""
    if pattern:
        pattern = f"*{pattern}*"
    else:
        pattern = "*"

    if repository:
        search_directories = [repository]
        repo_names = ["root"]
        ts.logger.debug("IO.find_dataset pattern %s in repo %s", pattern, repository)
    else:
        search_directories = [
            v["directory"] for k, v in active_config().repositories.items()
        ]
        repo_names = [k for k in active_config().repositories.keys()]

    data_dirs = []
    for search_dir in search_directories:
        data_dirs.extend(fs.find(search_dir, pattern, full_path=True))

    ts.logger.debug("%s %s", pattern, data_dirs)
    if exclude:
        dirs = [d for d in data_dirs if exclude not in d]
        ts.logger.debug(
            "DATASET.IO.find_datasets: exclude '%s' eliminated:\n%s",
            exclude,
            [d for d in dirs if exclude in d],
        )
    print(data_dirs)
    search_results = []
    for search_dir, repo in zip(search_directories, repo_names, strict=False):
        ts.logger.debug("%s | %s", search_dir, repo)
        search_results.extend(
            [
                # d.replace(active_config().timeseries_root, "root").split(os.path.sep)
                # d.replace(search_dir, "root").split(os.path.sep)
                # VERIFY that identifying multiple repos by name works?
                d.replace(search_dir, repo).split(os.path.sep)
                for d in dirs
            ]
        )
    ts.logger.debug("search results: %s", search_results)
    return [SearchResult(f[2], f[1]) for f in search_results]


def find_metadata_files(
    repository: list[PathStr] | PathStr | None = None,
    pattern: str = "",
    contains: str = "",  # as_of: datetime | None = None
    equals: str = "",
) -> list[str]:
    """Search for metadata json files in the 'catalog' directory.

    Only one of the arguments 'contains' or 'equals' can be provided at the same time. If none is provided, all files are returned.
    """
    ts.logger.debug("find_metadata_files in repo(s) %s.", repository)
    if contains:
        pattern = f"*{contains}*"
    elif equals:
        pattern = equals
    elif not pattern:
        pattern = "*"

    def find_in_repo(repo: str) -> list[str]:
        return fs.find(
            search_path=repo,
            pattern=pattern,
            full_path=True,
            search_sub_dirs=False,
        )

    if not repository:
        ts.logger.debug(
            "find_metadata_files in default repo:\n%s.",
            repository,
        )
        result = find_in_repo(active_config())
    elif isinstance(repository, str):
        ts.logger.debug(
            "find_metadata_files in repo by str:\n%s.",
            repository,
        )
        result = find_in_repo(repository)
    elif isinstance(repository, "Path"):  # type: ignore
        ts.logger.debug(
            "find_metadata_files in repo by Path:\n%s.",
            repository,
        )
        result = find_in_repo(repository)
    elif isinstance(repository, list):
        ts.logger.debug(
            "find_metadata_files in multiple repos:\n%s",
            repository,
        )
        result = []
        for r in repository:
            result.append(find_in_repo(r))
    else:
        raise TypeError("Invalid repository type.")

    return result


def list_datasets(
    # pattern: str | PathStr = "",  # as_of: datetime | None = None
) -> list[SearchResult]:
    """List all datasets under timeseries root."""
    return find_datasets(pattern="")


class DatasetIoException(Exception):
    """Exception for dataset io errors."""

    pass


def for_all_datasets_move_metadata_files(
    pattern: str | PathStr = "",  # as_of: datetime | None = None
) -> list[SearchResult]:
    """Search for files in under timeseries root."""
    if pattern:
        pattern = f"*{pattern}*"
    else:
        pattern = "*"

    dirs = fs.find(active_config().timeseries_root, pattern, full_path=True)
    search_results = [
        d.replace(active_config().timeseries_root, "root").split(os.path.sep)
        for d in dirs
    ]
    ts.logger.debug("DATASET.IO.SEARCH: results: %s", search_results)

    return [SearchResult(f[2], f[1]) for f in search_results]


def merge_data(old: Data, new: Data, date_cols: set[str]) -> Data:
    """Merge new data into old data."""

    if isinstance(new, pandas.DataFrame) and isinstance(old, pandas.DataFrame):
        df = pandas.concat(
            [old, new],
            axis=0,
            ignore_index=True,
        ).drop_duplicates(list(date_cols), keep="last")

    elif isinstance(new, pyarrow.Table) and isinstance(old, pyarrow.Table):
        sort_order = [(d, "ascending") for d in date_cols]
        arrow_table = pyarrow.concat_tables([old, new]).sort_by(sort_order)
        polars_df = polars.from_arrow(arrow_table).unique(date_cols, keep="last")  # type: ignore [call-arg,misc]
        df = polars_df.to_arrow().sort_by(sort_order)

    elif isinstance(new, polars.DataFrame) and isinstance(old, polars.DataFrame):
        df = polars.concat([old, new]).unique(date_cols, keep="last").sort(date_cols)

    return df


def parquet_schema(
    data_type: properties.SeriesType,
    meta: dict[str, Any],
) -> pyarrow.Schema | None:
    """Dataset specific helper: translate tags to parquet schema metadata before the generic call 'write_parquet'."""

    if not meta:
        # return None
        # better?
        raise ValueError("Tags can not be empty.")

    dataset_meta = deepcopy(meta)
    series_meta = dataset_meta.pop("series")

    if not series_meta:
        return None

    date_col_fields = [
        pyarrow.field(
            d,
            "date64",
            nullable=False,
        )
        for d in data_type.temporality.date_columns
    ]

    num_col_fields = [
        pyarrow.field(
            series_key,
            "float64",
            nullable=True,
            metadata=tags_to_json(series_tags),
        )
        for series_key, series_tags in series_meta.items()
    ]
    num_col_fields.sort(key=lambda x: x.name)

    schema = pyarrow.schema(
        date_col_fields + num_col_fields,
        metadata=tags_to_json(dataset_meta),
    )
    return schema


def tags_to_json(x: TagDict) -> dict[str, str]:
    """Turn tag dict into a dict where keys and values are coercible to bytes.

    See: https://arrow.apache.org/docs/python/generated/pyarrow.schema.html

    The simple solution is to put it all into a single field: {json: <json-string>}
    """
    j = {"json": json.dumps(x).encode("utf8")}
    return j


def tags_from_json(
    dict_with_json_string: dict,
    byte_encoded: bool = True,
) -> dict:
    """Reverse 'tags_to_json()': return tag dict from dict that has been coerced to bytes.

    Mutliple dict fields into a single field: {json: <json-string>}. May or may not have been byte encoded.
    """
    if byte_encoded:
        return json.loads(dict_with_json_string[b"json"].decode())  # type: ignore [no-any-return]
    else:
        return json.loads(dict_with_json_string["json"])  # type: ignore [no-any-return]


def tags_from_json_file(
    file_or_files: PathStr | list[PathStr],
) -> DatasetTagDict | list[DatasetTagDict]:
    """Read one or more json files."""

    if isinstance(file_or_files, list):
        result = []
        for f in file_or_files:
            j = fs.read_json(f)
            result.append(json.loads(j))
        return result
    else:
        t = fs.read_json(file_or_files)
        # t = json.loads(j)
        return DatasetTagDict(t)
