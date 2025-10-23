"""Simple file based read and write of metadata in JSON format.

This 'registers' the JSON formatted metadata in a central 'catalog' for easy search and lookup:
`/<repository catalog path>/<dataset name>-metadata.json`

This will duplicate any metadata stored in Parquet files.
The catalog location is configurable per metadata repository.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import NamedTuple

from .. import fs

# from ..config import Config
from ..config import FileBasedRepository
from ..logging import logger
from ..meta import DatasetTagDict
from ..meta import TagDict
from ..meta import matches_criteria
from ..properties import SeriesType
from ..properties import Temporality
from ..properties import Versioning
from ..types import PathStr

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "
# ruff: noqa: D202


# active_config = Config.active


class SearchResult(NamedTuple):
    """Result item for search."""

    name: str
    type_directory: str


def _filename(set_name: str) -> str:
    """The name of the metadata file for the dataset."""
    return f"{set_name}-metadata.json"


def _matches_tags(d: dict, tags: Any) -> bool:
    """Check if a dict includes all tags provided in criteria."""
    if not tags:
        return True
    elif isinstance(tags, dict):
        return matches_criteria(d, tags)
    elif isinstance(tags, list):
        checks = []
        for t in tags:
            checks.append(matches_criteria(d, t))
        return any(checks)
    else:
        raise TypeError(f"Can not check tags of type '{type(tags)}'.")


def _sanitize_for_json(d: dict) -> dict:
    """Recursively convert ssb-timeseries types to strings in a dictionary."""
    if not isinstance(d, dict):
        return d
    sanitized_dict = {}
    for k, v in d.items():
        if isinstance(v, SeriesType | Versioning | Temporality):
            sanitized_dict[k] = str(v)
        elif isinstance(v, datetime):
            sanitized_dict[k] = v.isoformat()
        elif isinstance(v, dict):
            sanitized_dict[k] = _sanitize_for_json(v)
        elif isinstance(v, list):
            sanitized_dict[k] = [
                _sanitize_for_json(item) if isinstance(item, dict) else item
                for item in v
            ]
        else:
            sanitized_dict[k] = v
    return sanitized_dict


def _build_dataset_item(tags_from_file: dict, repo_name: str) -> dict:
    """Construct a dataset catalog item dictionary."""
    return {
        "repository_name": repo_name,
        "object_name": tags_from_file["name"],
        "object_type": "dataset",
        "object_tags": tags_from_file,
        "parent": tags_from_file.get("parent", ""),
    }


def _build_series_items(tags_from_file: dict, repo_name: str) -> list[dict]:
    set_name = tags_from_file["name"]
    return [
        {
            "repository_name": repo_name,
            "object_name": series_key,
            "object_type": "series",
            "object_tags": series_tags,
            "parent": set_name,
        }
        for series_key, series_tags in tags_from_file.get("series", {}).items()
    ]


def _filter_items(items: list[dict], criteria: dict | list[dict]) -> list[dict]:
    """Filter a list of catalog item dictionaries based on tag criteria."""
    if not criteria:
        return items
    return [item for item in items if _matches_tags(item["object_tags"], criteria)]


class JsonMetaIO:
    """File based metadata storage for time series Datasets.

    Data storage solutions will often also handle descriptive metadata.
    For instance, parquet files can include json metadata in the data files.
    That keeps data and metadata together, which is good.

    It also makes sense to 'register' the metadata in a central catalog.
    :JsonMetaIO: puts metadata for a repository in the `repository.catalog.path`.
    """

    def __init__(
        self,
        repository: FileBasedRepository,
        set_name: str = "",
    ) -> None:
        """Initialise filesystem abstraction for dataset.

        Calculate directory structure based on dataset type and name.
        """
        if isinstance(repository, dict | FileBasedRepository):
            self.repository = repository
        else:
            raise TypeError("Repository must be a dict.")
        logger.debug("JsonMetaIO uses repository %s", self.repository)
        self.repo_name = repository.get("name", "unnamed metadata repository")
        self.set_name = set_name

    @property
    def dir(self) -> str:
        """The location of the metadata file for the dataset."""
        return self.repository["catalog"]["options"]["path"]

    def fullpath(self, set_name: str = "") -> str:
        """The full path to the metadata file."""
        if not set_name:
            set_name = self.set_name

        return str(Path(self.dir) / _filename(set_name))

    def read(self, **kwargs) -> dict:
        """Read tags from the metadata file."""
        set_name = kwargs.get("set_name", self.set_name)
        path = self.fullpath(set_name)
        meta: dict = {"name": set_name}
        logger.info(
            "JsonMetaIO.read.start %s: reading metadata from file %s\n",
            set_name,
            path,
        )
        if fs.exists(path):
            logger.info(
                "JsonMetaIO.read.success %s: reading metadata from file %s\nended.",
                set_name,
                path,
            )
            meta = fs.read_json(path)
        else:
            logger.info("JsonMetaIO.read.FileNotFound: %s", path)
        return meta

    def write(
        self,
        tags: dict,
        set_name: str,
    ) -> None:
        """Write tags to the metadata file."""
        try:
            logger.info(
                "JsonMetaIO.write.start %s: writing metadata to file\n\t%s\nstarted.",
                set_name,
                self.fullpath(set_name),
            )
            sanitized_tags = _sanitize_for_json(tags)
            fs.write_json(self.fullpath(set_name), sanitized_tags)
            logger.info(
                "JsonMetaIO.write.success %s: Writing metadata to file %s.",
                set_name,
                self.fullpath(set_name),
            )
        except Exception as e:
            logger.exception(
                "JsonMetaIO.write.error %s: Writing metadata for dataset %s t file %s.",
                e,
                set_name,
                self.fullpath(set_name),
            )

    @property
    def exists(self, set_name: str = "") -> bool:
        """Check if the metadata file exists."""
        if not set_name:
            set_name = self.set_name
        return fs.exists(self.fullpath(set_name))

    def search(self, **kwargs) -> list[dict]:
        """Read all tags from the metadata file and filter them.

        Match dataset name by 'equals', 'contains', OR 'pattern', AND filter by 'tags'.
        If no filters are provided, all files in the path are returned.
        """
        tags_criteria = kwargs.get("tags", {})
        if tags_criteria is None:
            tags_criteria = {}

        do_datasets = kwargs.pop("datasets", True)
        do_series = kwargs.pop("series", False)

        json_files = find_metadata_files(path=self.dir, **kwargs)
        results = []

        for f in json_files:
            tags_from_file = tags_from_json_file(f)
            if not isinstance(tags_from_file, dict):
                raise TypeError(
                    f"Metadata file {f} did not contain a valid dictionary."
                )

            repo_name = tags_from_file.get("repository")
            # no longer a concern?
            # if isinstance(repo_name, dict):
            #    repo_name = repo_name.get("name")

            if do_datasets:
                dataset_item = _build_dataset_item(tags_from_file, repo_name)
                matching_set = _filter_items([dataset_item], tags_criteria)
                results.extend(matching_set)

            if do_series:
                series_in_set = _build_series_items(tags_from_file, repo_name)
                matching_series = _filter_items(series_in_set, tags_criteria)
                results.extend(matching_series)

        # return _filter_items(all_results, tags_criteria)
        return results


def find_metadata_files(
    path: PathStr,
    pattern: str = "",
    contains: str = "",
    equals: str = "",
    **kwargs,
) -> list[str]:
    """Search for metadata json files in the 'catalog' directory.

    Match dataset name by 'equals', 'contains' or 'pattern'.
    If no filter is provided, all files in path are returned.
    """
    if equals:
        pattern = equals
    elif contains:
        pattern = f"*{contains}*"
    elif not pattern:
        pattern = "*"
    search_pattern = _filename(pattern)
    logger.debug(
        "find_metadata_files() searches for %s in repo path\n%s.", search_pattern, path
    )
    found = fs.find(
        search_path=path,
        pattern=search_pattern,
        full_path=True,
        search_sub_dirs=False,
    )
    logger.debug("find_metadata_files() in repo path\n%s.", found)
    return found


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
