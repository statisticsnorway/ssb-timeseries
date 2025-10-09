"""Simple file based read and write of dataset data and metadata.

Stores wide format data in a hard coded directory structure:
Repository/Datatype/Dataset/[Version|latest].parquet

(Hard coded directory structure: as opposed to Hive partitioning.)
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from typing import Any
from typing import NamedTuple
from typing import cast

import narwhals as nw
import pyarrow
import pyarrow.compute
from narwhals.typing import FrameT
from narwhals.typing import IntoFrameT

import ssb_timeseries as ts

from .. import fs
from .. import properties
from ..config import Config
from ..config import FileBasedRepository
from ..dataframes import empty_frame
from ..dataframes import is_empty
from ..dates import date_utc
from ..dates import datelike_to_utc
from ..dates import prepend_as_of
from ..dates import standardize_dates
from ..dates import utc_iso_no_colon
from ..meta import TagDict
from ..types import PathStr

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "
# ruff: noqa: D202


active_config = Config.active

# TODO: get this from config / dataset metadata:
PA_TIMESTAMP_UNIT = "ns"
PA_TIMESTAMP_TZ = "UTC"
PA_NUMERIC = "float64"


def _version_from_file_name(
    file_name: str, pattern: str | properties.Versioning = "as_of", group: int = 2
) -> str:
    """For known name patterns, extract version marker.

    For persisted data, a number; for regular versioned series an 'as of' datetime.
    """
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


def last_version_number_by_regex(directory: str, pattern: str = "*") -> str:
    """Check directory and get max version number from files matching regex pattern."""
    files = fs.ls(directory, pattern=pattern)
    number_of_files = len(files)

    vs = sorted([int(_version_from_file_name(fname, "persisted")) for fname in files])
    if vs:
        read_from_filenames = max(vs)
        out = read_from_filenames
    else:
        read_from_filenames = 0
        out = number_of_files

    ts.logger.debug(
        "io/simple.last_version_number_by_regex() search in directory: \n\t%s\n\tfor '%s' found %s files, regex identified version %s --> vs %s.",
        directory,
        pattern,
        f"{number_of_files!s}",
        f"{read_from_filenames!s}",
        f"{out!s}",
    )
    return out


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

        if as_of_utc is None and set_type.versioning == properties.Versioning.AS_OF:
            raise ValueError(
                "An 'as of' datetime must be specified when the type has versioning of type Versioning.AS_OF."
            )

        self.as_of_utc: datetime = utc_iso_no_colon(as_of_utc)

    @property
    def root(self) -> str:
        """The root path is the basis for all other paths."""
        ts_root = self.repository["directory"]["path"]
        return ts_root

    @property
    def filename(self) -> str:
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
    def directory(self) -> str:
        """The data directory for the dataset. This is a subdirectory under the type path."""
        return os.path.join(
            self.root,
            f"{self.data_type.versioning}_{self.data_type.temporality}",
            self.set_name,
        )

    @property
    def fullpath(self) -> str:
        """The full path to the data file."""
        return os.path.join(self.directory, self.filename)

    def read(
        self,
        interval: str = "",  # TODO: Implement use av interval = Interval.all,
    ) -> pyarrow.Table:
        """Read data from the filesystem. Return empty dataframe if not found."""
        ts.logger.debug(interval)
        if fs.exists(self.fullpath):
            ts.logger.info(
                "DATASET.read.start %s: Reading data from file %s",
                self.set_name,
                self.fullpath,
            )
            try:
                df = fs.read_parquet(self.fullpath, implementation="pyarrow")
                ts.logger.info("DATASET.read.success %s: Read data.", self.set_name)
            except FileNotFoundError:
                ts.logger.exception(
                    "DATASET.read.error %s: Read data failed. File not found: %s",
                    self.set_name,
                    self.fullpath,
                )
                df = empty_frame()

        else:
            df = empty_frame()
            ts.logger.debug(f"No file {self.fullpath} - return empty frame instead.")
        pa_table = datelike_to_utc(df)
        return cast(pyarrow.Table, pa_table)

    def write(self, data: FrameT, tags: dict | None = None) -> None:
        """Writes data to the filesystem.

        If versioning is AS_OF, writes to new file.
        If versioning is NONE, writes to existing file.
        """
        new = nw.from_native(data)
        if self.data_type.versioning == properties.Versioning.AS_OF:
            # consider a merge option for versioned writing?
            df = prepend_as_of(new, self.as_of_utc)
        else:
            old = self.read(self.set_name)
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
            self.fullpath,
        )
        try:
            fs.write_parquet(
                data=df,
                path=self.fullpath,
                schema=parquet_schema(self.data_type, tags),
            )
        except Exception as e:
            ts.logger.exception(
                "DATASET.write.error %s: writing data to file\n\t%s\nreturned exception: %s.",
                self.set_name,
                self.fullpath,
                e,
            )
        ts.logger.info(
            "DATASET.write.success %s: writing data to file\n\t%s\nended.",
            self.set_name,
            self.fullpath,
        )

    @property
    def exists(self) -> bool:
        """Check if the data file exists."""
        return fs.exists(self.fullpath)

    def versions(
        self, file_pattern: str = "*", pattern: str | properties.Versioning = "as_of"
    ) -> list[datetime | str]:
        """Check data directory and list version marker ('as-of' or 'name') of data files."""
        files = fs.ls(self.directory, pattern=file_pattern)
        versions: list[str | datetime] = []
        if files:
            vs_strings = [
                _version_from_file_name(str(fname), pattern, group=2) for fname in files
            ]
            match properties.Versioning(pattern):
                case properties.Versioning.AS_OF:
                    versions = sorted([date_utc(as_of) for as_of in vs_strings])
                case properties.Versioning.NAMES:
                    versions = sorted(vs_strings)
                case properties.Versioning.NONE:
                    versions = vs_strings
                case _:
                    raise ValueError(f"pattern '{pattern}' not recognized.")
        return versions


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
            v["directory"]["path"] for k, v in active_config().repositories.items()
        ]
        repo_names = [k for k in active_config().repositories.keys()]

    directories = []
    for search_dir in search_directories:
        directories.extend(fs.find(search_dir, pattern, full_path=True))

    ts.logger.debug("%s %s", pattern, directories)
    if exclude:
        dirs = [d for d in directories if exclude not in d]
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


def merge_data(
    old: IntoFrameT, new: IntoFrameT, date_cols: Iterable[str]
) -> pyarrow.Table:
    """Merge new data into old data."""
    new = standardize_dates(new)
    old = standardize_dates(old)

    expressions = [nw.selectors.by_dtype(nw.Float32).cast(nw.Float64)]
    new = nw.from_native(new).lazy(backend="polars").with_columns(expressions).collect()  # type: ignore[call-arg]
    old = nw.from_native(old).lazy(backend="polars").with_columns(expressions).collect()  # type: ignore[call-arg]

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
            pyarrow.timestamp(unit=PA_TIMESTAMP_UNIT, tz=PA_TIMESTAMP_TZ),  # type: ignore[call-overload]
            nullable=False,
        )
        for d in data_type.temporality.date_columns
    ]

    num_col_fields = [
        pyarrow.field(
            series_key,
            PA_NUMERIC,
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


# def tags_from_json_file(
#    file_or_files: PathStr | list[PathStr],
# ) -> DatasetTagDict | list[DatasetTagDict]:
#    """Read one or more json files."""
#
#    if isinstance(file_or_files, list):
#        result = []
#        for f in file_or_files:
#            j = fs.read_json(f)
#            result.append(json.loads(j))
#        return result
#    else:
#        t = fs.read_json(file_or_files)
#        return DatasetTagDict(t)
