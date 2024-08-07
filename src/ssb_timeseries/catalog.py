import importlib.resources as pkg_resources
from dataclasses import dataclass
from enum import Enum
from typing import Any

import duckdb

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.io import SearchResult
from ssb_timeseries.io import find_datasets
from ssb_timeseries.meta import TagDict
from ssb_timeseries.meta import TagValue

from . import sql

# -- mypy: ignore-errors


class ObjectType(Enum):
    """ObjectType for DataCatalog."""

    DATASET = "dataset"
    SERIES = "series"


@dataclass
class CatalogItem:
    """One item (set or series) in the data catalog."""

    object_name: str
    object_type: ObjectType
    object_tags: dict | None = None
    parent: Any = None
    children: set | None = None

    def get(self) -> Dataset:
        """Return the dataset."""
        if self.object_type == ObjectType.DATASET:
            return Dataset(self.object_name)
        elif self.object_type == ObjectType.SERIES:
            return Dataset(self.parent.object_name).filter(pattern=self.object_name)  # type: ignore[no-any-return]


class DataCatalog:
    """All sets and series within a time series repository."""

    def __init__(self, directory: str) -> None:
        """..."""
        self.connection = duckdb.connect()
        self.directory = directory
        # Load all JSON files into DuckDB

    def datasets(self) -> set[SearchResult]:  # set[CatalogItem | None]:
        """Return all datasets."""
        found = find_datasets(exclude="")
        return set(found)

    def series(self) -> set[CatalogItem | None]:
        """List all series (across all sets)."""
        return {None}

    def count(self, object_type: ObjectType | None) -> int:
        """Return number of datasets (default) or series (not yet implemented), or both (to be default)."""
        sets = self.datasets()
        series = self.series()
        match object_type:
            case None:
                count = len(sets) + len(series)
            case ObjectType.DATASET:
                count = len(sets)
            case _:
                raise NotImplementedError(f"Count is not supported for {object_type=}.")
        return count

    def search(self, tags: TagDict) -> Any:
        """Search with duckdb."""
        # datasets:list[str] = []
        # series:list[str] = []
        tag_conditions = " AND ".join(
            [f"{tag} = '{value}'" for tag, value in tags.items()]
        )

        query: str = f"""
            SELECT *
            FROM json_table
            WHERE {tag_conditions}
        """

        return self.connection.execute(query).fetchall()

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
