"""Simple file based read and write of metadata in JSON format.

This 'registers' the JSON formatted metadata in a central 'catalog' for easy search andd lookup:
`/<repository catalog path>/<dataset name>-metadata.json`

This will duplicate any metadata stored in Parquet files.
The catalog location is configurable per metadata repository.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import NamedTuple

from .. import fs
from ..config import Config
from ..config import FileBasedRepository
from ..logging import logger
from ..meta import DatasetTagDict
from ..meta import TagDict
from ..types import PathStr

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "
# ruff: noqa: D202


active_config = Config.active


class SearchResult(NamedTuple):
    """Result item for search."""

    name: str
    type_directory: str


class JsonMetaIO:
    """A filesystem abstraction for Dataset IO."""

    def __init__(
        self,
        repository: str | FileBasedRepository,
        set_name: str,
    ) -> None:
        """Initialise filesystem abstraction for dataset.

        Calculate directory structure based on dataset type and name.
        """
        if isinstance(repository, dict):
            self.repository = repository
        else:
            cfg = Config.active()
            self.repository = cfg.repositories.get(repository)
        logger.debug("JsonMetaIO uses repository %s", self.repository)
        self.set_name = set_name

    @property
    def file(self) -> str:
        """The name of the metadata file for the dataset."""
        return f"{self.set_name}-metadata.json"

    @property
    def dir(self) -> str:
        """The location of the metadata file for the dataset.

        For parquet based data storage, the metadata is included in the data file, but is also 'registered' in a central metadata directory (`repository.catalog.path`).
        """
        return self.repository["catalog"]["path"]

    @property
    def fullpath(self) -> str:
        """The full path to the metadata file."""
        return str(Path(self.dir) / self.file)

    def read(self) -> dict:
        """Read tags from the metadata file."""
        meta: dict = {"name": self.set_name}
        logger.info(
            "JsonMetaIO.read.start %s: reading metadata from file %s\n",
            self.set_name,
            self.fullpath,
        )
        if fs.exists(self.fullpath):
            logger.info(
                "JsonMetaIO.read.success %s: reading metadata from file %s\nended.",
                self.set_name,
                self.fullpath,
            )
            meta = fs.read_json(self.fullpath)
        else:
            logger.debug("Metadata file %s was not found.", self.fullpath)
        return meta

    def write(self, meta: dict) -> None:
        """Write tags to the metadata file."""
        try:
            logger.info(
                "JsonMetaIO.write.start %s: writing metadata to file\n\t%s\nstarted.",
                self.set_name,
                self.fullpath,
            )
            fs.write_json(self.fullpath, meta)
            logger.info(
                "JsonMetaIO %s: Writing metadata to file %s.",
                self.set_name,
                self.fullpath,
            )
        except Exception as e:
            logger.exception(
                "JsonMetaIO %s: Writing metadata to file %s returned exception %s.",
                self.set_name,
                self.fullpath,
                e,
            )

    @property
    def exists(self) -> bool:
        """Check if the metadata file exists."""
        return fs.exists(self.fullpath)


def find_metadata_files(
    repository: PathStr | None = None,
    # repository: list[PathStr] | PathStr | None = None,
    pattern: str = "",
    contains: str = "",
    equals: str = "",
) -> list[str]:
    """Search for metadata json files in the 'catalog' directory.

    Only one of the arguments 'pattern', 'contains' or 'equals' can be provided at the same time. If none is provided, all files are returned.
    """
    logger.debug("find_metadata_files in repo(s) %s.", repository)
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
        logger.debug(
            "find_metadata_files in default repo:\n%s.",
            repository,
        )
        result = find_in_repo(active_config())
    elif isinstance(repository, Path | str):
        logger.debug(
            "find_metadata_files in repo by str/Path:\n%s.",
            repository,
        )
        result = find_in_repo(repository)
    elif isinstance(repository, dict):
        logger.debug(
            "find_metadata_files in repo specified as {'path': ..., 'handler': ...}:\n%s",
            repository,
        )
        result = find_in_repo(repository["path"])
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
