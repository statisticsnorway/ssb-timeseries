import importlib.resources as pkg_resources
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Protocol

import duckdb

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.io import find_metadata_files
from ssb_timeseries.io import tags_from_json_file
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.meta import DatasetTagDict  # noqa: F401
from ssb_timeseries.meta import TagDict  # noqa: F401
from ssb_timeseries.meta import TagValue

from . import sql

# mypy: disable-error-code="no-untyped-def"
# ruff: noqa:ANN002 ANN003 D102


SEARCH_OPTIONS = """
Search options:
    pattern (str): Text pattern for search 'like' in names. Defaults to ''.
    regex (str): Expression for regex search in names. Defaults to ''.
    tags (dict): Dictionary with tags to search for. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
        | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
"""


class ObjectType(Enum):
    """ObjectType for data catalog items."""

    DATASET = "dataset"
    SERIES = "series"


@dataclass
class CatalogItem:
    """One item (set or series) in the data catalog."""

    repository_name: str
    object_name: str
    object_type: str
    object_tags: dict[str, Any]
    parent: Any = None
    children: set[Any] | list[Any] | None = None

    def __hash__(self) -> int:
        """Hash function must be provided to be able to make sets of catalog items.

        This implementation requires that for each object_type there is only one item with any given object name.
        """
        return hash(f"{self.repository_name}:{self.object_name}")

    def __eq__(self, other) -> bool:  # noqa: ANN001
        """Catalog items are considered equal if object type and object name are equal."""
        return (
            self.object_type,
            self.object_name,
        ) == (
            other.object_type,
            other.object_name,
        )

    def get(self) -> Dataset:
        """Return the dataset."""
        if self.object_type == "dataset":
            return Dataset(self.object_name)
        elif self.object_type == "series":
            return Dataset(self.parent.object_name).filter(pattern=self.object_name)  # type: ignore[no-any-return]
        else:
            raise TypeError(f"Can not retrieve object of type '{self.object_type}'.")


class _CatalogProtocol(Protocol):
    """Defines required methods + docstrings for catalogs and repositories."""

    def datasets(self) -> list[CatalogItem]:
        """Search in all repositories for datasets that match the criteria.

        Args:
            pattern (str): Text pattern for search 'like' in column names. Defaults to ''.
            regex (str): Expression for regex search in column names. Defaults to ''.
            tags (dict): Dictionary with tags to search for. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
        """
        ...

    def series(self) -> list[CatalogItem]:
        """Search all datasets in all repositories for series that match the criteria.

        Args:
            pattern (str): Text pattern for search 'like' in column names. Defaults to ''.
            regex (str): Expression for regex search in column names. Defaults to ''.
            tags (dict): Dictionary with tags to search for. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
        """
        ...

    def count(self, object_type: str = "") -> int:
        """Search in all repositories for objects (sets or series) that match the criteria.

        Args:
            object_type: 'dataset' or 'series'.
            pattern (str): Text pattern for search 'like' in column names. Defaults to ''.
            regex (str): Expression for regex search in column names. Defaults to ''.
            tags (dict): Dictionary with tags to search for. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
        """
        ...

    def __dict__(self):
        """Aggregate all the information into a single dictionary."""
        ...


class _FileRepositoryProtocol(Protocol):
    """Defines required attributes for file repositories."""

    name: str
    directory: str


class Catalog(_CatalogProtocol):
    """A collection of entities in one or more physical data repositories.

    The catalog searches across all repositories.
    """

    def __init__(self, config: list[_FileRepositoryProtocol]) -> None:
        """Add all registers in config to catatalog object."""
        self.repository: list[Repository] = []
        for filerepo in config:
            # add register info: name, type, owner?
            # begin assuming config is simply a list of directories
            if isinstance(filerepo, Repository):
                self.repository.append(filerepo)
            else:
                self.repository.append(
                    Repository(name=filerepo.name, directory=filerepo.directory)
                )

    def datasets(
        self,
        *args,
        **kwargs,
    ) -> list[CatalogItem]:
        # Inherit detailed docs from porotocol.
        # List all sets matching criteria.
        result: list[CatalogItem] = []
        for r in self.repository:
            for rr in r.datasets(args, kwargs):
                result.append(rr)
            # problem if a dataset occurs in multiple repositories?

        return result

    def series(
        self,
        *args,
        **kwargs,
    ) -> list[CatalogItem]:
        # Inherit detailed docs from porotocol.
        # List all series (across all sets) matching criteria.
        result: list[CatalogItem] = []
        for r in self.repository:
            for rr in r.series(args, kwargs):
                result.append(rr)
            # problem if a dataset occurs in multiple repositories?

        return result

    def count(self, object_type: str = "") -> int:
        # Inherit detailed docs from porotocol.
        # Return number of datasets (default) or series (not yet implemented), or both (to be default).
        result: int = 0
        for r in self.repository:
            result += r.count(
                object_type=str(object_type)
            )  # will count duplicates if same dataset occurs in multiple repositories

        return result

    def __repr__(self) -> str:
        """Return a machine readable string representation that can regenerate the catalog object."""
        return f"Catalog([{','.join([r.__repr__() for r in self.repository])}])"

    def __dict__(self):
        """Aggregate all the information into a single dictionary."""
        ...
        # return {"repository": [r.__dict__() for r in self.repository]}


class Repository(_CatalogProtocol):
    """A physical storage repository for timeseries datasets."""

    def __init__(
        self,
        name: str = "",
        directory: str = "",
        repo_config: _FileRepositoryProtocol | None = None,
    ) -> None:
        """Initiate one repository."""
        if name and directory:
            self.name = name
            self.directory = directory
        elif repo_config:
            self.name = repo_config.name
            self.directory = repo_config.directory
        else:
            raise TypeError(
                "Repository requires name and directory to be provided, either as strings or wrapped in a configuration object."
            )
        self.connection: duckdb.DuckDBPyConnection = duckdb.connect()
        # Load all JSON files into DuckDB

    def datasets(
        self,
        *args,
        **kwargs,
    ) -> list[CatalogItem]:
        # inherit docs from protocol class.
        result: list[CatalogItem] = []
        jsonfiles = find_metadata_files(repository=self.directory)
        for f in jsonfiles:
            tags = tags_from_json_file(f)  # type: ignore
            if isinstance(tags, dict):
                set_name = tags["name"]
                result.append(
                    CatalogItem(
                        repository_name=self.name,
                        object_name=set_name,  # type: ignore
                        object_type="dataset",
                        object_tags=tags,
                        parent=tags.get("parent", ""),
                        children=tags.get("series", {}).keys(),
                    )
                )
            else:
                raise TypeError(
                    f"Expected tags to be returned as a single dictionary. Got {type(tags)}."
                )
            if not result:
                ts_logger.debug(f"Could not read: {f=}\n{tags=}")
        if not result:
            ts_logger.debug(f"Failed to read: {jsonfiles=}")

        return result

    def series(
        self,
        *args,
        **kwargs,
    ) -> list[CatalogItem]:
        # inherit docs from protocol class. List all series (across all sets).
        result: list[CatalogItem] = []
        jsonfiles = find_metadata_files(repository=self.directory)
        for f in jsonfiles:
            tags = tags_from_json_file(f)  # type: ignore
            if isinstance(tags, dict):
                set_name = tags["name"]
                for series_key, series_tags in tags["series"].items():
                    result.append(
                        CatalogItem(
                            repository_name=self.name,
                            object_name=series_key,
                            object_type="series",
                            object_tags=series_tags,  # type: ignore
                            parent=set_name,
                        )
                    )
            else:
                raise TypeError(
                    f"Expected tags to be returned as a single dictionary. Got {type(tags)}."
                )
            if not result:
                ts_logger.debug(f"Could not read: {f=}\n{tags=}")
        if not result:
            ts_logger.debug(f"Failed to read: {jsonfiles=}")

        return result

    def count(self, object_type: str = "", *args, **kwargs) -> int:
        # inherit docs from protocol class.
        # Return number of datasets (default) or series (not yet implemented), or both (to be default).
        sets = self.datasets()
        series = self.series()
        match object_type.lower():
            case "set" | "dataset" | "sets" | "datasets":
                result = len(sets)
            case "series":
                result = len(series)
            case _:
                result = len(sets) + len(series)
                # raise NotImplementedError(f"Count is not supported for {object_type=}.")
        return result

    def __dict__(self):
        """Return one big aggregated dictionary for the entire repository."""
        result = {self.name: {"directory": self.directory, "datasets": {}}}
        jsonfiles = find_metadata_files(repository=self.directory)
        for f in jsonfiles:
            tags = tags_from_json_file(f)  # type: ignore
            ts_logger.debug(f"Found: {tags=}")
            result["datasets"][tags["name"]] = tags
        return result

    def __repr__(self) -> str:
        """Return a machine readable string representation that can regenerate the repository object."""
        return f"Repository(name='{self.name}',directory='{self.directory}')"

    def query(self, queryname: str, **kwargs: TagValue) -> Any:
        """Helper function to make prepared statement queries using .sql files."""
        return execute_prepared_sql(
            connection=self.connection, queryname=queryname, **kwargs
        )


def read_sql_file(filename: str) -> str:
    """Read SQL statement from a .sql file."""
    with pkg_resources.open_text(sql, filename) as file:
        return file.read()


def execute_prepared_sql(connection: Any, queryname: str, **kwargs: Any) -> Any:
    """Pass params to a named prepared statement."""
    if queryname.endswith(".sql"):
        filename = queryname
    else:
        filename = f"{queryname}.sql"
    sql_query = read_sql_file(filename)

    if kwargs:
        return connection.execute(sql_query, kwargs).fetchall()
    else:
        return connection.execute(sql_query).fetchall()
