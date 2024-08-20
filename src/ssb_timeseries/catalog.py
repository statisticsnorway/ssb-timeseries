import importlib.resources as pkg_resources  # noqa: F401
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Protocol

import duckdb

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.io import find_metadata_files
from ssb_timeseries.io import tags_from_json_file
from ssb_timeseries.meta import TagDict
from ssb_timeseries.meta import TagValue
from ssb_timeseries.meta import matches_criteria

# from . import sql

# mypy: disable-error-code="no-untyped-def"
# ruff: noqa:ANN002 ANN003 D102 D417


SEARCH_OPTIONS = """
Search options:
    equals (str): Search within datasets where names are equal to the argument. The default '' searches within all sets.
    contains (str):  Search within datasets where names contain the argument. The default '' searches within all sets.
    tags (dict | list(dict) | None): Filter the sets or series in the result set by a specified tags. Default None retiurns all.
        Defaults to None. All criteria attributes in a dict must be satisfied (tags are combined by AND).
        If a list of values is provided for a criteria attribute, the criteria is satisfied if the tag value matches either of them (OR).
        Alternative sets (dicts) can be provided in a list.
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

    def has_tags(self, tags: Any) -> bool:
        """Check if the catalog item has all tags provided in criteria."""
        # meta.search_by_tags(self.tags["series"], tags)
        if tags is None:
            return True
        elif isinstance(tags, dict):
            check = matches_criteria(self.object_tags, tags)
            return check
        elif isinstance(tags, list):
            checks = []
            check = True
            for t in tags:
                checks.append(matches_criteria(self.object_tags, t))
            return any(checks)
        else:
            raise TypeError(f"Can not check tags of type '{type(tags)}'.")


class _CatalogProtocol(Protocol):
    """Defines required methods + docstrings for catalogs and repositories."""

    def datasets(
        self,
        *,
        equals: str = "",
        contains: str = "",
        tags: TagDict | list[TagDict] | None = None,
    ) -> list[CatalogItem]:
        """Search in all repositories for datasets that match the criteria.

        Args:
            equals (str): Search within datasets where names are equal to the argument. The default '' searches within all sets.
            contains (str):  Search within datasets where names contain the argument. The default '' searches within all sets.
            tags (dict): Filter the sets or series in the result set by the specified tags. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
        """
        ...

    def series(
        self,
        *,
        equals: str = "",
        contains: str = "",
        tags: TagDict | list[TagDict] | None = None,
    ) -> list[CatalogItem]:
        """Search all datasets in all repositories for series that match the criteria.

        Args:
            equals (str): Search within datasets where names are equal to the argument. The default '' searches within all sets.
            contains (str):  Search within datasets where names contain the argument. The default '' searches within all sets.
            tags (dict): Filter the sets or series in the result set by the specified tags. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
        """
        ...

    def count(
        self,
        *,
        object_type: str = "",
        equals: str = "",
        contains: str = "",
        tags: TagDict | list[TagDict] | None = None,
    ) -> int:
        """Search in all repositories for objects (sets or series) that match the criteria.

        Args:
            object_type: 'dataset' or 'series'.
            equals (str): Search within datasets where names are equal to the argument. The default '' searches within all sets.
            contains (str):  Search within datasets where names contain the argument. The default '' searches within all sets.
            tags (dict): Filter the sets or series in the result set by the specified tags. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
        """
        ...

    def items(
        self,
        *,
        datasets: bool = True,
        series: bool = True,
        equals: str = "",
        contains: str = "",
        tags: TagDict | list[TagDict] | None = None,
    ):
        """Aggregate all the information into a single dictionary."""
        ...


class _FileRepositoryProtocol(Protocol):
    """Defines required attributes for file repositories."""

    name: str
    directory: str


class Catalog(_CatalogProtocol):
    """A collection of entities in one or more physical data repositories.

    The catalog will perform searches across all individual repositories.
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
        **kwargs,
    ) -> list[CatalogItem]:
        # Inherit detailed docs from porotocol.
        # List all sets matching criteria.
        result: list[CatalogItem] = []
        for r in self.repository:
            for rr in r.datasets(**kwargs):
                result.append(rr)
            # problem if a dataset occurs in multiple repositories?

        return result

    def series(
        self,
        **kwargs,
    ) -> list[CatalogItem]:
        # Inherit detailed docs from porotocol.
        # List all series (across all sets) matching criteria.
        result: list[CatalogItem] = []
        for r in self.repository:
            for rr in r.series(**kwargs):
                result.append(rr)
            # problem if a dataset occurs in multiple repositories?

        return result

    def count(
        self,
        *,
        object_type: str = "",
        equals: str = "",
        contains: str = "",
        tags: TagDict | list[TagDict] | None = None,
    ) -> int:
        # Inherit detailed docs from porotocol.
        # Return number of datasets (default) or series (not yet implemented), or both (to be default).
        result: int = 0
        for r in self.repository:
            result += r.count(
                object_type=str(object_type),
                equals=equals,
                contains=contains,
                tags=tags,
            )

        return result

    def __repr__(self) -> str:
        """Return a machine readable string representation that can regenerate the catalog object."""
        return f"Catalog([{','.join([r.__repr__() for r in self.repository])}])"

    def items(
        self,
        datasets: bool = True,
        series: bool = True,
        equals: str = "",
        contains: str = "",
        **kwargs,
    ) -> list[CatalogItem]:
        """Aggregate all the information into a single dictionary."""
        result: list[CatalogItem] = []
        for r in self.repository:
            for rr in r.items(
                datasets=datasets,
                series=series,
                equals=equals,
                contains=contains,
                **kwargs,
            ):
                result.append(rr)
            # problem if a dataset occurs in multiple repositories?

        return result


class Repository(_CatalogProtocol):
    """A physical storage repository for timeseries datasets."""

    name: str = ""
    directory: str = ""

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
        *,
        equals: str = "",
        contains: str = "",
        tags: TagDict | list[TagDict] | None = None,
    ) -> list[CatalogItem]:
        # inherit docs from protocol class.
        return self.items(
            datasets=True,
            series=False,
            equals=equals,
            contains=contains,
            tags=tags,
        )

    def series(
        self,
        *,
        equals: str = "",
        contains: str = "",
        tags: TagDict | list[TagDict] | None = None,
    ) -> list[CatalogItem]:
        # inherit docs from protocol class. List all series (across all sets).
        return self.items(
            datasets=False,
            series=True,
            equals=equals,
            contains=contains,
            tags=tags,
        )

    def count(self, *, object_type: str = "", **kwargs) -> int:
        # inherit docs from protocol class.
        # Return number of datasets (default) or series (not yet implemented), or both (to be default).
        match object_type.lower():
            case "set" | "dataset" | "sets" | "datasets":
                items = self.datasets(**kwargs)
            case "series":
                items = self.series(**kwargs)
            case "item" | "items" | _:
                items = self.items(**kwargs)
        return len(items)

    def files(self, *, contains: str = "", equals: str = "") -> list[str]:
        """Return all files in the repository."""
        jsonfiles = find_metadata_files(
            repository=self.directory,
            equals=equals,
            contains=contains,
        )
        return jsonfiles

    def items(
        self,
        datasets: bool = True,
        series: bool = True,
        equals: str = "",
        contains: str = "",
        tags: TagDict | list[TagDict] | None = None,
    ) -> list[CatalogItem]:
        # """Return all catalog items (sets, series or both) matching the search criteria."""
        result: list[CatalogItem] = []
        jsonfiles = self.files(equals=equals, contains=contains)
        for f in jsonfiles:
            # for all json files, read the tags / check against criteria in "tags"
            tags_of_file = tags_from_json_file(f)
            if isinstance(tags_of_file, dict):
                set_name = tags_of_file["name"]
                if datasets:
                    dataset_item = CatalogItem(
                        repository_name=self.name,
                        object_name=set_name,  # type: ignore
                        object_type="dataset",
                        object_tags=tags_of_file,
                        parent=tags_of_file.get("parent", ""),
                        # children=tags.get("series"),
                    )
                    if dataset_item.has_tags(tags):
                        result.append(dataset_item)
                if series:
                    for series_key, series_tags in tags_of_file["series"].items():
                        series_item = CatalogItem(
                            repository_name=self.name,
                            object_name=series_key,
                            object_type="series",
                            object_tags=series_tags,  # type: ignore
                            parent=set_name,
                        )
                        if series_item.has_tags(tags):
                            result.append(series_item)
            else:
                raise TypeError(
                    f"Expected tags from json file to be returned as a single dictionary. For file {f} we got {type(tags_of_file)}."
                )

        return result

    def __repr__(self) -> str:
        """Return a machine readable string representation that can regenerate the repository object."""
        return f"Repository(name='{self.name}',directory='{self.directory}')"

    def query(self, queryname: str, **kwargs: TagValue) -> Any:
        """Helper function to make prepared statement queries using duckdb and .sql files."""
        return execute_prepared_sql(
            connection=self.connection, queryname=queryname, **kwargs
        )


def read_sql_file(filename: str) -> str:
    """Read SQL statement from a .sql file."""
    raise NotImplementedError("pkg_resources.open_text ")
    # with pkg_resources.open_text(sql, filename) as file:
    #     return file.read()


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
