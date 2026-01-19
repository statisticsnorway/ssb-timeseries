"""Provides the Taxonomy class for managing hierarchical metadata.

This module defines the ``Taxonomy`` class, which is used to represent
hierarchical classifications, such as those from SSB's KLASS system.
It handles fetching data from KLASS, building tree structures, and
providing methods for navigating and manipulating the taxonomy.
"""

from __future__ import annotations

import itertools
from datetime import date
from itertools import chain
from typing import TYPE_CHECKING
from typing import Any

import matplotlib.pyplot as plt
import narwhals as nw
import networkx as nx

# TODO: Replace with nw in agg_table-method
from narwhals.typing import IntoFrameT

import ssb_timeseries as ts
from ssb_timeseries.dataframes import are_equal
from ssb_timeseries.io import fs
from ssb_timeseries.meta.loaders import DataLoader
from ssb_timeseries.meta.loaders import FileLoader
from ssb_timeseries.meta.loaders import KlassLoader
from ssb_timeseries.types import PathStr

if TYPE_CHECKING:
    pass


class MissingAttributeError(Exception):
    """At least one required attribute was not provided."""

    ...


class Taxonomy:
    """Wraps taxonomies defined in KLASS or json files in a object structure.

    :ivar name: The name of the taxonomy.
    :vartype name: str
    :ivar structure_type: The type of structure, e.g., 'list', 'tree', 'graph'.
    :vartype structure_type: str
    :ivar levels: The number of levels not counting the root node.
    :vartype levels: int
    :ivar entities: Entity definitions, represented as a PyArrow Table.
    :vartype entities: pa.Table
    :ivar structure: The hierarchical structure of the taxonomy.
    :vartype structure: bigtree.Node

    .. note::

        **Structure**:
        Relations between entities of the taxonomy.
        Both lists and trees will be represented as hierarchies, with the root node being the taxonomy.
        Level two will be the first item level, so a flat list will have two levels.
        Hierarchies with a natural top or "root" node should have a single node at level two.

        **Lookups**:
        Listing of supported names for all entities, mapping different categories of names
        of different standards and in different languages to a unique identifier.
    """

    def __init__(
        self,
        *,
        klass_id: int = 0,
        from_date: str = str(date.today()),
        data: list[dict[str, str]] | IntoFrameT | None = None,
        path: PathStr = "",
        name: str = "Taxonomy",
        sep: str = ".",
        **kwargs: Any,
    ) -> None:
        """Create a Taxonomy object from either a `klass_id`, a `data` dictionary or dataframe or a `path` to a JSON file.

        Taxonomy items are listed in .entities and hierarchical relationships mapped in .structure.
        Optional keyword arguments: substitutions (dict): Code values to be replaced: `{'substitute_this': 'with_this', 'and_this': 'as well'}`
        """
        self.name = name
        loader: DataLoader | FileLoader | KlassLoader
        if klass_id:
            loader = KlassLoader(klass_id, from_date)
        elif data is not None:
            loader = DataLoader(data)
        elif path:
            loader = FileLoader(path)
        else:
            raise MissingAttributeError(
                "Either klass_id (int), data (dict|df), or path (str) must be provided."
            )

        tbl = loader.load()

        # TODO: add proper validation - check tbl for root node + duplicates + fill missing
        self.entities = tbl

        self.substitute(kwargs.get("substitutions", {}))

        pandas_df = nw.from_native(self.entities).to_pandas()

        # tbl til networkx-struktur
        edges_df = pandas_df[pandas_df["parentCode"].notna()].copy()
        list_attrs = list(edges_df.columns.difference(["code", "parentCode", "level"]))

        # TODO: Check if we need these in a Taxonomy object
        edges_df["attrs"] = edges_df.apply(
            lambda row: {x: row[x] for x in list_attrs},
            axis=1,
        )

        nx_edges = list(
            zip(
                edges_df["code"],
                edges_df["parentCode"],
                edges_df["attrs"],
                strict=False,
            )
        )

        self.structure: nx.DiGraph = nx.DiGraph(nx_edges)

        # Finding root nodes
        # This could be done in the loaders module
        self.root_nodes = [
            x for x in self.structure.nodes if list(self.structure.successors(x)) == []
        ]
        if len(self.root_nodes) > 1:
            self.root = None
            for x in self.root_nodes:
                if self.agg_dict[x] == self.leaf_nodes:
                    self.root = x
            if self.root is None:
                root_edge_list = [(x, "0") for x in self.root_nodes]
                self.structure.add_edges_from(root_edge_list)
                # TODO: This should be a networkx node
                self.root = "0"
        elif len(self.root_nodes) == 1:
            self.root = self.root_nodes[0]
        else:
            # TODO: Should this raise an error or warning?
            self.root = None

    def __eq__(self, other: object) -> bool:
        """Checks for equality. Taxonomies are considered equal if their codes and hierarchical relations are the same."""
        if not isinstance(other, Taxonomy):
            return NotImplemented

        # TODO: Update with nx
        # tree_diff = get_tree_diff(self.structure, other.structure)
        # if tree_diff:
        #     return False

        # the implementation of are_equal() allows comparing parentCode fields
        # (which have value null for root nodes)
        # fields_to_compare = ["code", "parentCode",]
        fields_to_compare = ["code", "parentCode", "name"]

        self_tbl = self.entities.select(fields_to_compare).sort_by(
            [(f, "ascending") for f in fields_to_compare]
        )
        othr_tbl = other.entities.select(fields_to_compare).sort_by(
            [(f, "ascending") for f in fields_to_compare]
        )
        return are_equal(self_tbl, othr_tbl)

    # def __sub__(self, other: bigtree.Node) -> bigtree.Node:  # type: ignore[name-defined]
    #     """Return the tree difference between the two taxonomy (tree) structures."""
    #     ts.logger.debug("other: %s", other)
    #     if isinstance(other, bigtree.Node):
    #         remove = self.subtree(other.name).asc
    #         return NotImplemented

    def __getitem__(self, key: str) -> str:  # type: ignore[name-defined]
        """Get tree node by name (KLASS code)."""
        # TODO: Return node object
        return self.structure.nodes[key]

    def subtree(self, key: str) -> Any:
        """Get subtree of node identified by code."""
        subgraph_data = []
        for parent, children in nx.bfs_successors(
            nx.reverse_view(self.structure), source=key
        ):
            for child in children:
                subgraph_data.append({"parentCode": parent, "code": child})
        return Taxonomy(data=subgraph_data)

    def print_tree(
        self,
        options: dict | None = None,
        figsize: tuple = (24, 12),
    ) -> None:
        """Graphical representation of directed graph."""
        if options is None:
            options = {
                "font_size": 16,
                "node_size": 8,
                "node_color": "white",
                "edgecolors": "black",
                "linewidths": 1,
                "width": 1,
            }
        plt.figure(figsize=figsize)
        nx.draw_networkx(
            self.structure,
            # pos=nx.bfs_layout(self.structure, start=self.leaf_nodes),
            **options,
        )

    @property
    def all_nodes(self) -> list[str]:
        """Return all nodes in the taxonomy."""
        return list(self.structure.nodes)

    @property
    def leaf_nodes(self) -> list[str]:  # type: ignore[name-defined]
        """Return all leaf nodes in the taxonomy."""
        leaves = [
            x
            for x in self.structure.nodes
            if list(self.structure.predecessors(x)) == []
        ]
        ts.logger.debug("leaves: %s", leaves)
        return leaves

    @property
    def parent_nodes(self) -> list[str]:
        """Return all non-leaf nodes in the taxonomy."""
        parents = [x for x in self.structure.nodes if x not in self.leaf_nodes]
        ts.logger.debug("parents: %s", parents)
        return parents

    @property
    def code_dict(self) -> dict[str, list[str]]:
        """List all aggregates that each leaf node is a part of."""
        c_dict = {}

        for y in self.leaf_nodes:
            c_dict[y] = list(
                chain.from_iterable(
                    [x[1] for x in nx.bfs_successors(self.structure, source=y)]
                )
            )

        return c_dict

    # @property
    # def agg_table(self) -> pd.DataFrame:
    #     # Denne ble tidligere brukt til å utlede agg_dict, men er ikke nødvendig til det
    #     # Mulig den likevel kan brukes til et eller annet
    #     """
    #     Aggregate matrix for directed graph.
    #     """
    #     dict1 = self.code_dict

    #     a1 = self.parent_nodes

    #     return pd.DataFrame(
    #         {col: [col in dict1[key] for key in dict1.keys()]
    #         for col in a1},
    #         index=dict1.keys()
    #     )

    @property
    def agg_dict(self) -> dict[str, list[str]]:
        """Dictionary of aggregate codes as list of leaf nodes."""
        leaves = self.leaf_nodes
        parents = self.parent_nodes
        c_dict = self.code_dict
        return {
            agg: [code for code in leaves if agg in c_dict[code]] for agg in parents
        }

    def save(self, path: PathStr) -> None:
        """Save taxonomy to json file.

        The file can be read using Taxonomy(<path to file>).
        """
        fs.write_json(path, self.entities.to_pylist())  # type: ignore [arg-type]

    def substitute(self, substitutions: dict) -> None:
        """Substitute 'code' and 'parent' values with items in subsitution dictionary."""
        if substitutions:
            df = nw.from_native(self.entities)
            for key, value in substitutions.items():
                df = df.with_columns(
                    nw.col("code").str.replace_all(key, value, literal=True)
                )
                df = df.with_columns(
                    nw.col("parentCode").str.replace_all(key, value, literal=True)
                )

            self.entities = df.to_arrow()


def permutations(
    taxonomies: dict[str, Taxonomy],
    filters: list[str] | str = "",
) -> list[dict]:
    """For a dict on the form {'a': Taxonomy(A), 'b': Taxonomy(B)}, returns permutations of items in A and B, subject to filters.

    Filters are experimental and quite likely to change type / implementation.
    Notably, support for custom functions and include/exclude lists may be considered.
    For now: str | list[str] with length matching the taxonomies identifies Taxonomy tree functions as follows:

        'all' | 'all_nodes' --> .all_nodes()
        'parents' | 'parent_nodes' -- .parent_nodes()
        'leaves' | 'leaf_nodes' | 'children' | 'child_nodes' --> .leaf_nodes()

    If no filters are provided, the default is 'all'.

    Examples:
        >>> from ssb_timeseries.meta import Taxonomy
        >>> tax_a = Taxonomy(data=[{'code': 'a1', 'parentCode': '0'}, {'code': 'a2', 'parentCode': '0'}])
        >>> tax_b = Taxonomy(data=[{'code': 'b1', 'parentCode': '0'}, {'code': 'b2', 'parentCode': '0'}])
        >>> permutations({'A': tax_a, 'B': tax_b})
        [{'A': 'a1', 'B': 'b1'}, {'A': 'a1', 'B': 'b2'}, {'A': 'a2', 'B': 'b1'}, {'A': 'a2', 'B': 'b2'}]
    """
    out = []
    if not filters:
        filters = ["all"] * len(taxonomies)
    elif isinstance(filters, str):
        filters = [filters] * len(taxonomies)

    node_lists = []
    for (_attr, taxonomy), func in zip(taxonomies.items(), filters, strict=False):
        match func.lower():
            case "all" | "all_nodes":
                nodes = [x for x in taxonomy.all_nodes if x not in taxonomy.root_nodes]
            case "parents" | "parent_nodes":
                # TODO: Check if root_nodes should be filtered out.
                nodes = taxonomy.parent_nodes
            case "leaves" | "leaf_nodes" | "children" | "child_nodes":
                nodes = taxonomy.leaf_nodes  # type: ignore[assignment]

        node_lists.append(nodes)

    combinations = itertools.product(*node_lists)
    for c in combinations:
        d = {}
        for k, v in zip(taxonomies.keys(), c, strict=False):
            d[k] = v
        out.append(d)
    ts.logger.debug(out)
    return out
