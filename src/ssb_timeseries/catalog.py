"""The catalog module provides a search in one or more time series repositories for entire datasets and individual series.

>>> from ssb_timeseries.catalog import CONFIG # xdoctest: +SKIP

For each `repository`, the metadata for all catalog items (datasets and series) is registered (ie: a copy is stored in a catalog directory).
The `catalog` provides a single search interface by fanning out searches to all the repositories.

Both the catalog and individual repostories can be searched by names, parts of names or tags. In either case, the returned catalog items, names and descriptive metadate, plus the repository, object type and relationships to parent and child objects are provided. Other information, like lineage and data quality metrics may be added later.

------

Classes:
 *   :py:class:`CatalogItem`
 *   :py:class:`Catalog`
 *   :py:class:`Repository`

------
"""

import importlib.resources as pkg_resources  # noqa: F401
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Protocol

# from . import sql
import duckdb

from ssb_timeseries.io import find_metadata_files
from ssb_timeseries.io import tags_from_json_file
from ssb_timeseries.meta import TagDict
from ssb_timeseries.meta import TagValue
from ssb_timeseries.meta import matches_criteria

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

    def get(self) -> Any:
        """Return the dataset."""
        from ssb_timeseries.dataset import Dataset

        if self.object_type == "dataset":
            return Dataset(self.object_name)
        elif self.object_type == "series":
            return Dataset(self.parent.object_name).filter(
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


class _FileRepositoryProtocol(Protocol):
    """Defines required attributes for file repositories."""

    name: str
    directory: str


class Catalog(_CatalogProtocol):
    """A data catalog collects metadata from one or more physical data repositories and performs searches across them."""

    def __init__(self, config: list[_FileRepositoryProtocol]) -> None:
        """Add all repositories in the configuration to catalog object.

        Repositories are essentially just named locations.

        Example:
            >>> from ssb_timeseries.config import CONFIG
            >>> some_directory = CONFIG.catalog

            >>> repo1 = Repository(name="test_1", directory=some_directory)
            >>> repo2 = Repository(name="test_2", directory=some_directory)
            >>> catalog = Catalog(config=[repo1, repo2])

            >>> series_in_repo1 = repo1.series(contains='KOSTRA')
            >>> sets_in_catalog = catalog.datasets()
        """
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
        # Inherit docs from protocol.
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
        # Inherit docs from protocol.
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

    def _query(self, queryname: str, **kwargs: TagValue) -> Any:
        """Helper function to make prepared statement queries using duckdb and .sql files."""
        return _execute_prepared_sql(
            connection=self.connection, queryname=queryname, **kwargs
        )


# A duckdb approach may be simpler and more efficient than reading all the json files and then filtering
# In that case, it is probably a good idea to use helpers to:
#      * put queries in .sql files so they can be edited with proper syntax highlighting, linting etc:
#      * use prepared statements and pass parameters to the queries to get (depending on target) enhanced performance and security


def _read_sql_file(filename: str) -> str:
    """Read SQL statement from a .sql file."""
    raise NotImplementedError("pkg_resources.open_text ")
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


def _xdoctest() -> None:
    """Tests the code provided in doctstrings.

    Provided as an experiment with simple self contained tests for the library that can be run after an import.

    DISABLED.
    """
    # from nox import session
    # or simply:
    # import xdoctest

    from ssb_timeseries.config import Config
    from ssb_timeseries.fs import exists

    cfg = Config().configuration_file
    if exists(cfg):
        print(f"Configuration file found: {cfg}. What to do with it?")
        # xdoctest.doctest_module(__file__)
    else:
        print("Configuration file not found. Skipping xdoctests.")
        # ... name of script = sys.argv[0]; do something with arguments of the script: sys.argv[1:]?


if __name__ == "__main__":
    """Execute when called directly, ie not via import statements."""
    # run xdoctest
    # _xdoctest()
