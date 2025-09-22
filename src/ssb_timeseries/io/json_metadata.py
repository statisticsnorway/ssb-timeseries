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
from pathlib import Path
from typing import NamedTuple

import ssb_timeseries as ts
from ssb_timeseries import fs
from ssb_timeseries.config import Config
from ssb_timeseries.config import FileBasedRepository
from ssb_timeseries.meta import DatasetTagDict
from ssb_timeseries.meta import TagDict
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "
# ruff: noqa: D202


active_config = Config.active


class SearchResult(NamedTuple):
    """Result item for search."""

    name: str
    type_directory: str


class MetaIO:
    """A filesystem abstraction for Dataset IO."""

    def __init__(
        self,
        repository: str | FileBasedRepository,
        set_name: str,
        # set_type: properties.SeriesType,
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
        # self.data_type = set_type

    @property
    def metadata_file(self) -> str:
        """The name of the metadata file for the dataset."""
        return f"{self.set_name}-metadata.json"

    @property
    def metadata_dir(self) -> str:
        """The location of the metadata file for the dataset.

        In the inital implementation with data and metadata in separate files this was the same as the data directory.
        Now metadata is included in the data file, but also 'registered' in a central meta data directory.
        """
        return self.repository["catalog"]["path"]

    @property
    def metadata_fullpath(self) -> str:
        """The full path to the metadata file."""
        return os.path.join(self.metadata_dir, self.metadata_file)

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

    def file_exists(self) -> bool:
        """Check if the metadata file exists."""
        return fs.exists(self.metadata_fullpath)


def find_metadata_files(
    repository: PathStr | None = None,
    # repository: list[PathStr] | PathStr | None = None,
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

    def find_in_repo(repo: str | Path) -> list[str]:
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
    elif isinstance(repository, Path | str):
        ts.logger.debug(
            "find_metadata_files in repo by str/Path:\n%s.",
            repository,
        )
        result = find_in_repo(repository)
    elif isinstance(repository, dict):
        ts.logger.debug(
            "find_metadata_files in repo specified as {'path': ..., 'handler': ...}:\n%s",
            repository,
        )
        result = find_in_repo(repository["path"])
        # result = []
        # for r in repository:
        #    result.append(find_in_repo(r))
    else:
        raise TypeError("Invalid repository type.")

    return result


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
