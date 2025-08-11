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
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Any
from typing import NamedTuple
from typing import cast

import narwhals as nw
import pyarrow
import pyarrow.compute
from narwhals.typing import FrameT
from narwhals.typing import IntoFrameT

import ssb_timeseries as ts
from ssb_timeseries import fs
from ssb_timeseries import properties
from ssb_timeseries.config import Config
from ssb_timeseries.config import FileBasedRepository
from ssb_timeseries.dataframes import empty_frame
from ssb_timeseries.dataframes import is_empty
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import datelike_to_utc
from ssb_timeseries.dates import prepend_as_of
from ssb_timeseries.dates import standardize_dates
from ssb_timeseries.dates import utc_iso_no_colon
from ssb_timeseries.meta import DatasetTagDict
from ssb_timeseries.meta import TagDict
from ssb_timeseries.types import PathStr

if TYPE_CHECKING:
    from pathlib import Path

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "
# ruff: noqa: D202


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
            date_part = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}[+-][0-9]{4}"
            regex = f"(as_of_)({date_part})(-data.parquet)"
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
        # if as_of_utc is None and set_type.versioning == properties.Versioning.AS_OF:
        #     raise ValueError('As of data must be specified when data type has Versioning.AS_OF.')
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

        In the inital implementation with data and metadata in separate files this was the same as the data directory.
        Now metadata is included in the data file, but also 'registered' in a central meta data directory.
        """
        return self.repository["catalog"]

    @property
    def metadata_fullpath(self) -> str:
        """The full path to the metadata file."""
        return os.path.join(self.metadata_dir, self.metadata_file)

    def read_data(
        self,
        interval: str = "",  # TODO: Implement use av interval = Interval.all,
    ) -> pyarrow.Table:
        """Read data from the filesystem. Return empty dataframe if not found."""
        ts.logger.debug(interval)
        if fs.exists(self.data_fullpath):
            ts.logger.info(
                "DATASET.read.start %s: Reading data from file %s",
                self.set_name,
                self.data_fullpath,
            )
            try:
                df = fs.read_parquet(self.data_fullpath, implementation="pyarrow")
                ts.logger.info("DATASET.read.success %s: Read data.", self.set_name)
            except FileNotFoundError:
                ts.logger.exception(
                    "DATASET.read.error %s: Read data failed. File not found: %s",
                    self.set_name,
                    self.data_fullpath,
                )
                df = empty_frame()

        else:
            df = empty_frame()
            ts.logger.debug(
                f"No file {self.data_fullpath} - return empty frame instead."
            )
        pa_table = datelike_to_utc(df)
        return cast(pyarrow.Table, pa_table)

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

    def write_data(self, data: FrameT, tags: dict | None = None) -> None:
        """Writes data to the filesystem.

        If versioning is AS_OF, writes to new file.
        If versioning is NONE, writes to existing file.
        """
        new = nw.from_native(data)
        if self.data_type.versioning == properties.Versioning.AS_OF:
            # consider a merge option for versioned writing?
            df = prepend_as_of(new, self.as_of_utc)
        else:
            old = self.read_data(self.set_name)
            if is_empty(old):
                df = datelike_to_utc(new)
            else:
                df = merge_data(
                    new=datelike_to_utc(new),
                    old=datelike_to_utc(old),
                    date_cols=self.data_type.date_columns,
                )

        ts.logger.info(
            "DATASET.write.start %s: writing data to file\n\t%s\nstarted.",
            self.set_name,
            self.data_fullpath,
        )
        try:
            fs.write_parquet(
                data=df,
                path=self.data_fullpath,
                schema=parquet_schema(self.data_type, tags),
            )
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

    # we may need to reintroduce these?
    # def parquet_schema(
    #     self,
    #     meta: dict[str, Any],
    # ) -> pyarrow.Schema | None:
    #     """Dataset specific helper: translate tags to parquet schema metadata before the generic call 'write_parquet'."""
    #     return parquet_schema(self.data_type, meta)
    # def parquet_schema_from_df(self, df) -> pyarrow.Schema | None:
    #     """Dataset specific helper: translate tags to parquet schema metadata before the generic call 'write_parquet'."""
    #     schema = pyarrow.schema(df.columns, metadata=df.dtypes.to_dict())
    #     return schema

    def datafile_exists(self) -> bool:
        """Check if the data file exists."""
        return fs.exists(self.data_fullpath)

    def metadatafile_exists(self) -> bool:
        """Check if the metadata file exists."""
        return fs.exists(self.metadata_fullpath)

    def save(self, meta: dict, data: nw.typing.IntoFrame) -> None:
        """Save data and metadata to disk."""
        if meta:
            self.write_metadata(meta)
        else:
            ts.logger.warning(
                "DATASET %s: Metadata is empty. Nothing to write.",
                self.set_name,
            )
        data = nw.from_native(data)
        if not is_empty(data):
            self.write_data(
                data,
                tags=meta,
            )
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
                    return sorted(vs_strings)
                case properties.Versioning.NONE:
                    return vs_strings
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
        fs.mkdir(path)
        return path


def find_datasets(
    pattern: str | PathStr = "",
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
    search_results = []
    for search_dir, repo in zip(search_directories, repo_names, strict=False):
        ts.logger.debug("%s | %s", search_dir, repo)
        search_results.extend(
            [d.replace(search_dir, repo).split(os.path.sep) for d in dirs]
        )
    ts.logger.debug("search results: %s", search_results)
    return [SearchResult(f[2], f[1]) for f in search_results]


def find_metadata_files(
    repository: list[PathStr] | PathStr | None = None,
    pattern: str = "",
    contains: str = "",
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
    elif isinstance(repository, Path):
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


def list_datasets() -> list[SearchResult]:
    """List all datasets under timeseries root."""
    return find_datasets(pattern="")


class DatasetIoException(Exception):
    """Exception for dataset io errors."""

    pass


def for_all_datasets_move_metadata_files(
    pattern: str | PathStr = "",
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


def normalize_numeric(
    df: IntoFrameT,
) -> IntoFrameT:
    """Normalize datatypes for numeric columns of dataframe."""
    nw_df = nw.from_native(df).lazy(backend="polars")
    expressions = []
    expressions.append(nw.selectors.by_dtype(nw.Float32).cast(nw.Float64))
    return nw_df.with_columns(expressions).collect()


def merge_data(
    old: IntoFrameT, new: IntoFrameT, date_cols: Iterable[str]
) -> pyarrow.Table:
    """Merge new data into old data."""
    new = standardize_dates(new)
    old = standardize_dates(old)
    new = normalize_numeric(new)
    old = normalize_numeric(old)

    ts.logger.debug("merge_data schemas \n%s\n%s", old.schema, new.schema)
    merged = nw.concat(
        [old, new],
        how="diagonal",
    )
    out = merged.unique(
        subset=date_cols,
        keep="last",
    ).sort(by=sorted(date_cols))
    pa_table = nw.from_native(out).to_arrow()
    return cast(pyarrow.Table, pa_table)


def parquet_schema(
    data_type: properties.SeriesType,
    meta: dict[str, Any],
) -> pyarrow.Schema | None:
    """Dataset specific helper: translate tags to parquet schema metadata before the generic call 'write_parquet'."""

    if not meta:
        raise ValueError("Tags can not be empty.")

    dataset_meta = deepcopy(meta)
    series_meta = dataset_meta.pop("series")

    if not series_meta:
        return None

    date_col_fields = [
        pyarrow.field(
            d,
            pyarrow.timestamp(
                "ns", tz="UTC"
            ),  # TODO: get this from config / dataset metadata
            nullable=False,
        )
        for d in data_type.temporality.date_columns
    ]

    num_col_fields = [
        pyarrow.field(
            series_key,
            "float64",  # TODO: get this from config / dataset metadata
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
        return DatasetTagDict(t)
