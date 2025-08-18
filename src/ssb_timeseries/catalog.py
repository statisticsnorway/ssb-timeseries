"""The :py:mod:`ssb_timeseries.catalog` module provides several tools for searching for datasets or series in every  :py:class:`Repository` of a :py:class:`Catalog`.

The catalog is essentially just a logical collection of repositories, providing a search interface across all of them.

Searches can list or count sets, series or items (both). The search criteria can be complete names (`equals`), parts of names (`contains`), or metadata attributes (`tags`).

A returned py:class:`CatalogItem` instance is identified by name and descriptive metadate, plus the repository, object type and relationships to parent and child objects are provided. Other information, like lineage and data quality metrics may be added later.

>>> # doctest: +SKIP
>>> from ssb_timeseries.catalog import Catalog
>>> everything = Catalog().items()
>>> # doctest: -SKIP

-----
"""

import importlib.resources as pkg_resources  # noqa: F401
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Protocol
from typing import runtime_checkable

# from . import sql
import duckdb

from ssb_timeseries.config import FileBasedRepository
from ssb_timeseries.io import find_metadata_files
from ssb_timeseries.io import tags_from_json_file
from ssb_timeseries.meta import TagDict
from ssb_timeseries.meta import TagValue
from ssb_timeseries.meta import matches_criteria

# mypy: disable-error-code="no-untyped-def"
# ruff: noqa: ANN002, ANN003, D102


SEARCH_OPTIONS = """
Search options:
    equals (str): Search within datasets where names are equal to the argument. The default '' searches within all sets.
    contains (str):  Search within datasets where names contain the argument. The default '' searches within all sets.
    tags (dict | list(dict) | None): Filter the sets or series in the result set by a specified tags. Default None returns all.
        Defaults to None. All criteria attributes in a dict must be satisfied (tags are combined by AND).
        If a list of values is provided for a criteria attribute, the criteria is satisfied if the tag value matches either of them (OR).
        Alternative sets (dicts) can be provided in a list.
"""


class _DatasetProtocol(Protocol):
    """Specifies the required methods of Datasets."""

    def __init__(self, *args, **kwargs) -> None: ...

    def filter(self, *args, **kwargs) -> Any: ...


class ObjectType(Enum):
    """Supported object types for data catalog items."""

    DATASET = "dataset"
    SERIES = "series"


@dataclass
class CatalogItem:
    """A single item (set or series) in the data catalog."""

    repository_name: str
    """The repository name that contains the object."""

    object_name: str
    """The name of the object."""

    object_type: str
    """The type of the object."""

    object_tags: dict[str, Any]
    """The tags of the object."""

    parent: Any = None
    """The parent of the object."""

    children: set[Any] | list[Any] | None = None
    """The children of the object."""

    def __hash__(self) -> int:
        """Hash function must be provided to be able to make sets of catalog items.

        This implementation requires that for each object_type there is only one item with any given object name in each repository.
        """
        return hash(f"{self.repository_name}:{self.object_type}:{self.object_name}")

    def __eq__(self, other) -> bool:  # noqa: ANN001
        """Catalog items are considered equal if object type and object name are equal."""
        return (
            self.object_type,
            self.object_name,
        ) == (
            other.object_type,
            other.object_name,
        )

    def get(self) -> Any:  # NOSONAR
        """Return the dataset."""
        from ssb_timeseries.dataset import Dataset

        if self.object_type == "dataset":
            return Dataset(self.object_name)
        elif self.object_type == "series":
            return Dataset(self.parent.object_name).select(
                pattern=self.object_name
            )  # ---type: ignore[no-any-return]
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
    """Defines the required methods for catalogs and repositories.

    Catalogs consist of one or more repositories, hence performs the searches across all repositories and accumulates the results.

    Methods:
        series(
            equals:str, contains:str, tags:TagDict | list[TagDict]
        ) --> list[CatalogItem]

        datasets(
            equals:str, contains:str, tags:TagDict | list[TagDict]
        ) --> list[CatalogItem]

        items(
            equals:str, contains:str, tags:TagDict | list[TagDict]
        ) --> list[CatalogItem]

        count(
            object_type:str, equals:str, contains:str, tags:TagDict | list[TagDict]
        ) -> int

    :meta public:
    """

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
        """Search in all datasets in all repositories for series that match the criteria.

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
        """Count items of specified object type that match the criteria.

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
        """Search in all repositories for items (either sets or series) that match the criteria.

        Args:
            datasets (bool): Search for 'datasets'.
            series (bool):  Search for 'series'.
            equals (str):   Search within datasets where names are equal to the argument. The default '' searches within all sets.
            contains (str):  Search within datasets where names contain the argument. The default '' searches within all sets.
            tags (dict): Filter the sets or series in the result set by the specified tags. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
        """
        ...


@runtime_checkable
class RepositoryProtocol(Protocol):
    """Defines required attributes for file repositories."""

    name: str
    catalog: str
    # TODO: consider
    #   IDEA 1:
    #    - renaming 'catalog' to 'metadata' communicate the intent better?
    #   IDEA 2: add properties
    #    - type: enum('db' | 'files')
    #      ... to disinguish between file based and databases, switch interpretation of:
    #    - data: Path | ConnectionString
    #      ... data directory for file based, connection string for database repositories?
    #      (require data and metadata to be same type?)
    #   IDEA 3:
    #   - owner: str


class Catalog(_CatalogProtocol):
    """A data catalog collects metadata from one or more physical data repositories and performs searches across them."""

    def __init__(self, config: list[RepositoryProtocol | FileBasedRepository]) -> None:
        """Add all repositories in the configuration to catalog object.

        Repositories are essentially just named locations.

        Example:
            >>> from ssb_timeseries.config import Config
            >>> my_config = Config(preset='defaults')
            >>> some_directory = my_config.repositories['<teamname>']['catalog']

            >>> repo1 = Repository(name="test_1", catalog=some_directory)
            >>> repo2 = Repository(name="test_2", catalog=some_directory)
            >>> catalog = Catalog(config=[repo1, repo2])

            >>> series_in_repo1 = repo1.series(contains='KOSTRA')
            >>> sets_in_catalog = catalog.datasets()
        """
        self.repositories: list[Repository] = []
        for filerepo in config:
            if isinstance(filerepo, Repository):
                self.repositories.append(filerepo)
            elif isinstance(filerepo, dict):
                self.repositories.append(
                    Repository(name=filerepo["name"], catalog=filerepo["catalog"])
                )
            else:
                self.repositories.append(
                    Repository(name=filerepo.name, catalog=filerepo.catalog)
                )

    def datasets(
        self,
        **kwargs,
    ) -> list[CatalogItem]:
        # Inherit docs from protocol.
        # List all sets matching criteria.
        result: list[CatalogItem] = []
        for r in self.repositories:
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
        for r in self.repositories:
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
        # Inherit docs from protocol.
        # Return number of datasets (default) or series (not yet implemented), or both (to be default).
        result: int = 0
        for r in self.repositories:
            result += r.count(
                object_type=str(object_type),
                equals=equals,
                contains=contains,
                tags=tags,
            )

        return result

    def __repr__(self) -> str:
        """Return a machine readable string representation that can regenerate the catalog object."""
        return f"Catalog([{','.join([r.__repr__() for r in self.repositories])}])"

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
        for r in self.repositories:
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
    catalog: str = ""
    data: str = ""

    def __init__(
        self,
        name: str = "",
        catalog: str = "",
        repo_config: FileBasedRepository | RepositoryProtocol | None = None,
    ) -> None:
        """Initiate one repository."""
        if name and catalog:
            self.name = name
            self.catalog = catalog
        elif repo_config and isinstance(repo_config, RepositoryProtocol):
            self.name = repo_config.name
            self.catalog = repo_config.catalog
        elif repo_config and isinstance(repo_config, dict):
            self.name = repo_config["name"]
            self.catalog = repo_config["catalog"]
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
            repository=self.catalog,
            equals=equals,
            contains=contains,
        )
        return jsonfiles

    def items(
        self,
        *,
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
        return f"Repository(name='{self.name}',catalog='{self.catalog}')"

    def _query(self, queryname: str, **kwargs: TagValue) -> Any:
        """Helper function to make prepared statement queries using duckdb and .sql files."""
        return _execute_prepared_sql(
            connection=self.connection, queryname=queryname, **kwargs
        )


# NOSONAR
# A duckdb approach may be simpler and more efficient than reading all the json files and then filtering
# In that case, it is probably a good idea to use helpers to:
#      * put queries in .sql files so they can be edited with proper syntax highlighting, linting etc:
#      * use prepared statements and pass parameters to the queries to get (depending on target) enhanced performance and security


def _read_sql_file(filename: str) -> str:
    """Read SQL statement from a .sql file."""
    raise NotImplementedError("pkg_resources.open_text ")
    # implement with (something like( this:
    # with pkg_resources.open_text(sql, filename) as file:
    #     return file.read()


def _execute_prepared_sql(connection: Any, queryname: str, **kwargs: Any) -> Any:
    """Pass parameters to a named prepared statement."""
    if queryname.endswith(".sql"):
        filename = queryname
    else:
        filename = f"{queryname}.sql"
    sql_query = _read_sql_file(filename)

    if kwargs:
        return connection.execute(sql_query, kwargs).fetchall()
    else:
        return connection.execute(sql_query).fetchall()
