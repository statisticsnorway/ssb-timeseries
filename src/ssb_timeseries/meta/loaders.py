"""Provides data loaders for the Taxonomy class.

This module defines a protocol for data loaders and provides concrete
implementations for loading taxonomy data from different sources, such as the
KLASS API, local files, and in-memory data structures.
"""

from __future__ import annotations

from collections.abc import Hashable
from datetime import date
from functools import cache
from typing import Any
from typing import Protocol
from typing import TypeAlias

import narwhals as nw
import pyarrow as pa
from klass import get_classification
from narwhals.typing import IntoFrameT

from ssb_timeseries.dataframes import is_df_like
from ssb_timeseries.io import fs
from ssb_timeseries.types import PathStr

# Re-define shared constants and types
KLASS_ITEM_SCHEMA = pa.schema(
    [
        pa.field("code", "string", nullable=False),
        pa.field("parentCode", "string", nullable=True),
        pa.field("name", "string", nullable=False),
        pa.field("level", "string", nullable=True),
        pa.field("shortName", "string", nullable=True),
        pa.field("presentationName", "string", nullable=True),
        pa.field("validFrom", "string", nullable=True),
        pa.field("validTo", "string", nullable=True),
        pa.field("notes", "string", nullable=True),
    ]
)

DEFAULT_ROOT_NODE: dict[str, Any] = {
    "name": "<no name>",
    "code": "0",
    "parentCode": None,
    "level": "0",
    "shortName": "",
    "presentationName": "",
    "validFrom": "",
    "validTo": "",
    "notes": "",
}

KlassTaxonomy: TypeAlias = list[dict[Hashable, Any] | dict[str, str | None]]
"""A list of dictionaries representing KLASS classification items."""


def records_to_arrow(records: list[dict[str, Any]]) -> pa.Table:
    """Creates a PyArrow Table from a list of dictionaries (row/records)."""
    if not records:
        return pa.Table.from_pylist([], schema=KLASS_ITEM_SCHEMA)
    else:
        return pa.Table.from_pylist(records, schema=KLASS_ITEM_SCHEMA)


class TaxonomyLoader(Protocol):
    """A protocol for classes that load taxonomy data."""

    def load(self) -> pa.Table:
        """Load taxonomy data and return it as a PyArrow Table."""
        ...


class KlassLoader:
    """Loads taxonomy data from the KLASS API."""

    def __init__(self, klass_id: int, from_date: str = str(date.today())) -> None:
        """Initialize the KLASS loader with a classification ID."""
        self.klass_id = klass_id
        self.from_date = from_date

    def load(self) -> pa.Table:
        """Fetch data from KLASS and convert it to a PyArrow Table."""
        list_of_items = self._klass_classification(self.klass_id, self.from_date)
        return records_to_arrow(list_of_items)  # type: ignore[arg-type]

    @staticmethod
    @cache
    def _klass_classification(klass_id: int, from_date: str) -> KlassTaxonomy:
        """Get KLASS classification identified by ID as a list of dicts."""
        root_node = DEFAULT_ROOT_NODE.copy()
        root_node["name"] = f"KLASS-{klass_id}"

        classification = get_classification(str(klass_id)).get_codes(from_date)
        klass_data = classification.data.to_dict("records")
        for k in klass_data:
            if not k.get("parentCode"):
                k["parentCode"] = root_node["code"]
        return [root_node, *klass_data]  # type: ignore[misc]


class FileLoader:
    """Loads taxonomy data from a JSON file."""

    def __init__(self, path: PathStr) -> None:
        """Initialize the file loader with a path."""
        self.path = path

    def load(self) -> pa.Table:
        """Read a JSON file and convert it to a PyArrow Table."""
        dict_from_file = fs.read_json(str(self.path))
        return records_to_arrow(dict_from_file)  # type: ignore[arg-type]


class DataLoader:
    """Loads taxonomy data from an in-memory object."""

    def __init__(self, data: list[dict[str, str]] | IntoFrameT) -> None:
        """Initialize the data loader with data."""
        self.data = data

    def load(self) -> pa.Table:
        """Convert in-memory data to a PyArrow Table."""
        if isinstance(self.data, list):
            return records_to_arrow(self.data)
        elif is_df_like(self.data):
            # Convert DataFrame to list of dicts to ensure schema consistency
            df_as_list = nw.from_native(self.data).to_native().to_dict("records")  # type: ignore[union-attr, type-var]
            return records_to_arrow(df_as_list)
        else:
            # This path should ideally not be reached if types are checked
            raise TypeError(f"Unsupported data type for DataLoader: {type(self.data)}")
