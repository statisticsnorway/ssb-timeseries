"""Provides a simple, file-based I/O handler for Parquet format.

This handler stores datasets in a wide format using a defined directory
structure: `<repository>/<datatype>/<dataset>/[<version>|<latest>].parquet`.

It uses PyArrow for eager reading and writing.
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

from .. import fs
from .. import properties
from ..config import Config
from ..dataframes import empty_frame
from ..dataframes import is_empty
from ..dates import date_utc
from ..dates import datelike_to_utc
from ..dates import prepend_as_of
from ..dates import standardize_dates
from ..dates import utc_iso_no_colon
from ..logging import logger
from ..meta import TagDict
from ..types import PathStr
from .json_metadata import _sanitize_for_json

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "


active_config = Config.active


# TODO: get this from config / dataset metadata:
PA_TIMESTAMP_UNIT = "ns"
PA_TIMESTAMP_TZ = "UTC"
PA_NUMERIC = "float64"


def _version_from_file_name(
    file_name: str, pattern: str | properties.Versioning = "as_of", group: int = 2
) -> str:
    """Extract a version marker from a filename using known patterns."""
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
    logger.debug(
        "file: %s pattern:%s, regex%s \n--> version: %s ",
        file_name,
        pattern,
        regex,
        vs,
    )
    return vs


def last_version_number_by_regex(directory: str, pattern: str = "*") -> str:
    """Return the max version number from files in a directory matching a pattern."""
    files = fs.ls(directory, pattern=pattern)
    number_of_files = len(files)

    vs = sorted([int(_version_from_file_name(fname, "persisted")) for fname in files])
    if vs:
        read_from_filenames = max(vs)
        out = read_from_filenames
    else:
        read_from_filenames = 0
        out = number_of_files

    logger.debug(
        "io/simple.last_version_number_by_regex() search in directory: \n\t%s\n\tfor '%s' found %s files, regex identified version %s --> vs %s.",
        directory,
        pattern,
        f"{number_of_files!s}",
        f"{read_from_filenames!s}",
        f"{out!s}",
    )
    return out


class FileSystem:
    """A filesystem abstraction for reading and writing dataset data."""

    def __init__(
        self,
        repository: Any,  # dict[str,str] | FileBasedRepository,
        set_name: str,
        set_type: properties.SeriesType,
        as_of_utc: datetime | None = None,
        process_stage: str = "statistikk",
        sharing: dict | None = None,
    ) -> None:
        """Initialize the filesystem handler for a given dataset.

        This method calculates the necessary directory structure based on the
        dataset's type and name.
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
        """Return the root path of the configured repository."""
        ts_root = self.repository["directory"]["options"]["path"]
        return str(ts_root)

    @property
    def filename(self) -> str:
        """Construct the standard filename for the dataset's data file."""
        match str(self.data_type.versioning):
            case "AS_OF":
                file_name = f"{self.set_name}-as_of_{self.as_of_utc}-data.parquet"
            case "NONE":
                file_name = f"{self.set_name}-latest-data.parquet"
            case "NAMED":
                file_name = f"{self.set_name}-NAMED-data.parquet"
            case _:
                raise ValueError("Unhandled versioning.")

        logger.debug(file_name)
        return file_name

    @property
    def directory(self) -> str:
        """Return the data directory for the dataset."""
        return os.path.join(
            self.root,
            f"{self.data_type.versioning!s}_{self.data_type.temporality!s}",
            self.set_name,
        )

    @property
    def fullpath(self) -> str:
        """Return the full path to the dataset's data file."""
        return os.path.join(self.directory, self.filename)

    def read(
        self,
        interval: str = "",  # TODO: Implement use av interval = Interval.all,
    ) -> pyarrow.Table:
        """Read data from the filesystem.

        Returns an empty dataframe if the file is not found.
        """
        logger.debug(interval)
        if fs.exists(self.fullpath):
            logger.info(
                "DATASET.read.start %s: Reading data from file %s",
                self.set_name,
                self.fullpath,
            )
            try:
                df = fs.read_parquet(self.fullpath, implementation="pyarrow")
                logger.info("DATASET.read.success %s: Read data.", self.set_name)
            except FileNotFoundError:
                logger.exception(
                    "DATASET.read.error %s: Read data failed. File not found: %s",
                    self.set_name,
                    self.fullpath,
                )
                df = empty_frame()

        else:
            df = empty_frame()
            logger.debug(f"No file {self.fullpath} - return empty frame instead.")
        pa_table = datelike_to_utc(df)
        return cast(pyarrow.Table, pa_table)

    def write(self, data: FrameT, tags: dict | None = None) -> None:
        """Write data to the filesystem.

        If versioning is AS_OF, a new file is always created.
        If versioning is NONE, new data is merged into the existing file.
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

        logger.info(
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
            logger.exception(
                "DATASET.write.error %s: writing data to file\n\t%s\nreturned exception: %s.",
                self.set_name,
                self.fullpath,
                e,
            )
        logger.info(
            "DATASET.write.success %s: writing data to file\n\t%s\nended.",
            self.set_name,
            self.fullpath,
        )

    @property
    def exists(self) -> bool:
        """Check if the data file for the dataset exists."""
        return fs.exists(self.fullpath)

    def versions(
        self, file_pattern: str = "*", pattern: str | properties.Versioning = "as_of"
    ) -> list[datetime | str]:
        """List all available version markers from the data directory."""
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


# ================================ SEARCH: =================================


class SearchResult(NamedTuple):
    """Represents a single item in a search result."""

    name: str
    type_directory: str


def find_datasets(
    pattern: str | PathStr = "",
    exclude: str = "metadata",
    repository: list[PathStr] | PathStr = "",
) -> list[SearchResult]:
    """Search for dataset directories in all configured repositories.

    Args:
        pattern: A glob pattern to match against directory names.
        exclude: A substring to exclude from the search results.
        repository: A specific repository path to search in. If empty,
            searches all configured repositories.

    Returns:
        A list of SearchResult objects for the found datasets.
    """
    if pattern:
        pattern = f"*{pattern}*"
    else:
        pattern = "*"

    if repository:
        search_directories = [repository]
        repo_names = ["root"]
        logger.debug("IO.find_dataset pattern %s in repo %s", pattern, repository)
    else:
        search_directories = [
            v["directory"]["options"]["path"]
            for k, v in active_config().repositories.items()
        ]
        repo_names = list(active_config().repositories.keys())

    directories = []
    for search_dir in search_directories:
        directories.extend(fs.find(search_dir, pattern, full_path=True))

    logger.debug("%s %s", pattern, directories)
    if exclude:
        dirs = [d for d in directories if exclude not in d]
        logger.debug(
            "DATASET.IO.find_datasets: exclude '%s' eliminated:\n%s",
            exclude,
            [d for d in dirs if exclude in d],
        )
    search_results = []
    for search_dir, repo in zip(search_directories, repo_names, strict=False):
        logger.debug("%s | %s", search_dir, repo)
        search_results.extend(
            [d.replace(search_dir, repo).split(os.path.sep) for d in dirs]
        )
    logger.debug("search results: %s", search_results)
    return [SearchResult(f[2], f[1]) for f in search_results]


# ================================ READ/WRITE HELPERS: =================================


def merge_data(
    old: IntoFrameT, new: IntoFrameT, date_cols: Iterable[str]
) -> pyarrow.Table:
    """Merge new data into an existing dataframe, keeping the last entry for duplicates."""
    new = standardize_dates(new)
    old = standardize_dates(old)

    expressions = [nw.selectors.by_dtype(nw.Float32).cast(nw.Float64)]
    new = nw.from_native(new).lazy(backend="polars").with_columns(expressions).collect()  # type: ignore[call-arg]
    old = nw.from_native(old).lazy(backend="polars").with_columns(expressions).collect()  # type: ignore[call-arg]

    logger.debug("merge_data schemas \n%s\n%s", old.schema, new.schema)
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
    """Translate dataset tags into a PyArrow schema with embedded metadata."""
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


def tags_to_json(x: TagDict) -> dict[str, bytes]:
    """Serialize a tag dictionary into a format suitable for Parquet metadata.

    See Also:
        https://arrow.apache.org/docs/python/generated/pyarrow.schema.html
    """
    sanitized_tags = _sanitize_for_json(x)
    j = {"json": json.dumps(sanitized_tags).encode("utf8")}
    return j


def tags_from_json(
    dict_with_json_string: dict[str | bytes, str | bytes],
    byte_encoded: bool = True,
) -> TagDict:  # dict[str, Any]:
    """Deserialize a tag dictionary from the Parquet metadata format.

    This is the reverse of `tags_to_json`.
    """
    if byte_encoded:
        d = json.loads(dict_with_json_string[b"json"].decode())
    else:
        d = json.loads(dict_with_json_string["json"])
    return cast(TagDict, d)


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
