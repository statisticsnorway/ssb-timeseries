"""Placeholder utility covering meta data functionality used by timeseries.

Ideally, this functionality should live elsewhere, in ssb-python-klass and other meta data libraries. Likely subject to refactoring later.
"""

import io
from typing import Any

import bigtree
import pandas as pd
from bigtree import get_tree_diff
from bigtree import print_tree
from klass import get_classification
from typing_extensions import Self

from ssb_timeseries import fs
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment, override, type-arg, attr-defined, no-untyped-def, import-untyped"


def _df_info_as_string(df: pd.DataFrame) -> str:
    """Returns the content of df.info() as a string."""
    with io.StringIO() as buffer:
        df.info(buf=buffer)
        return buffer.getvalue()


class Taxonomy:
    """Wraps taxonomies defined in KLASS or json files in a object structure.

    Attributes:
        definition (str): Descriptions of the taxonomy.
            name:
            structure_type:     enum:   list | tree | graph
            levels: number of levels not counting the root node
        entities (pd.Dataframe): Entity definitions, represented as a dataframe with columns:
            code: str
            A unique entity identifier within the taxonomy.
            It may very well consist of numeric values, but will be represented as a string.

            parent:  str
            "parentCode"
            The code for the parent entity.

            name: str
            A unique human readable name. Not nullable.

            short:
            "shortName"
            A short version / mnemonic for name, if applicable.

            presentationName
            A "self explanatory" unique name, if applicable.

            validFrom

            validTo

    Notes:
        structure:
            Relations between entities of the taxonomy.
            Both lists and trees will be represented as hierarchies; with the root node being the taxonomy.
            Level two will be the first item level, so a flat list will have two levels.
            Hierarchies with a natural top or "root" node should have a single node at level two.

        lookups:
            Listing of supported names for all entities, mapping different categories of names of different standards and in different languages to a unique identifier.
    """

    def __init__(
        self,
        id_or_path: int | PathStr | str,
        root_name: str = "Taxonomy",
        sep: str = ".",
        substitute: dict | None = None,
    ) -> None:
        """Create Taxonomy object from KLASS id or file name.

        Key attributs: .entities holds the list of values and .structure puts the entitiees in a tree.
        """
        self.definition = {"name": root_name}
        if isinstance(id_or_path, int):
            # TO DO: handle versions of KLASS
            klass = get_classification(str(id_or_path)).get_codes().data
            self.entities = add_root_node(
                klass, {"code": "0", "parentCode": None, "name": root_name}
            )
            if substitute:
                for key, value in substitute.items():
                    self.entities["code"] = self.entities["code"].str.replace(
                        key, value
                    )
                    self.entities["parentCode"] = self.entities[
                        "parentCode"
                    ].str.replace(key, value)
        else:
            df_from_file = pd.DataFrame.from_dict(fs.read_json(str(id_or_path)))
            self.entities = df_from_file

        self.structure = bigtree.dataframe_to_tree_by_relation(
            data=self.entities,
            child_col="code",
            parent_col="parentCode",
            attribute_cols=[
                "name",
                "shortName",
                "presentationName",
                "validFrom",
                "validTo",
                "notes",
            ],
        )

    def __eq__(self, other: Self) -> bool:
        """Checks for equality. Taxonomies are considered equal if their codes and hierarchical relations are the same."""
        tree_diff = get_tree_diff(self.structure, other.structure)
        if tree_diff:
            trees_equal = False
        else:
            trees_equal = True

        fields_to_compare = ["code", "parentCode", "name"]
        s_entities = self.entities[fields_to_compare].reset_index(drop=True)
        o_entities = other.entities[fields_to_compare].reset_index(drop=True)

        ts_logger.debug(
            f"comparing:\n{s_entities.to_string()}\n...and:\n{o_entities.to_string()}"
        )
        ts_logger.debug(
            f".info:\n{_df_info_as_string(s_entities)}\n...and:\n{_df_info_as_string(o_entities)}"
        )
        entities_equal = all(s_entities == o_entities)

        return trees_equal and entities_equal

    def __minus__(self, other: bigtree.Node | Self) -> bigtree.Node:  # type: ignore
        """Return the tree difference between the two taxonomy (tree) structures."""
        if isinstance(other, bigtree.Node):
            return get_tree_diff(self.structure.root, other.root)
        else:
            return get_tree_diff(self.structure.root, other.structure.root)

    def __getitem__(self, key: str) -> bigtree.Node:  # type: ignore
        """Get tree node by name (KLASS code)."""
        return bigtree.find_name(self.structure.root, key)

    def subtree(self, key: str) -> bigtree.tree:  # type: ignore
        """Get subtree of node identified by name (KLASS code)."""
        the_node = bigtree.find_name(self.structure, key)
        return bigtree.get_subtree(the_node)

    def print_tree(self, *args, **kwargs) -> str:  # noqa: ANN002, ANN003
        """Return a string with the tree structure.

        Implementation is ugly! It would be preferable not to print the tree to std out.
        ... but this works.
        """
        import io
        from contextlib import redirect_stdout

        with io.StringIO() as buf, redirect_stdout(buf):
            print_tree(self.structure, *args, **kwargs)
            output = buf.getvalue()
        return output

    def all_nodes(self) -> list[bigtree.node]:  # type: ignore
        """Return all nodes in the taxonomy."""
        return [n for n in self.structure.root.descendants]

    def leaf_nodes(self, name: bigtree.Node | str = "") -> list[bigtree.node]:  # type: ignore
        """Return all leaf nodes in the taxonomy."""
        if name:
            if isinstance(name, bigtree.Node):
                leaves = [n.name for n in name.leaves]
            else:
                leaves = [n.name for n in self.subtree(name).leaves]
                # --- alternative:
                # leaves = [n.name for n in self.__getitem__(name).leaves]
                # tree_node = bigtree.find_name(self.structure.root, name)[0]
                # tree_node = bigtree.get_subtree(self.structure.root, name)[0]

            ts_logger.debug(f"leaves: {leaves}")
            return leaves
        else:
            return [n.name for n in self.structure.leaves]

    def parent_nodes(self) -> list[bigtree.node]:  # type: ignore
        """Return all non-leaf nodes in the taxonomy."""
        return [
            n
            for n in self.structure.root.descendants
            if n not in self.structure.root.leaves
        ]

    def save(self, path: PathStr) -> None:
        """Save taxonomy to json file.

        The file can be read using Taxonomy(<path to file>).
        """
        fs.write_json(path, self.entities.to_dict())


def add_root_node(df: pd.DataFrame, root_node: dict[str, str | None]) -> pd.DataFrame:
    """Prepend root node row to taxonomy dataframe."""
    new_row = {c: None for c in df.columns}
    for k in root_node:
        new_row[k] = root_node[k]
    df.rename(columns={"name": "fullName"})
    df["parentCode"] = df["parentCode"].fillna(value=root_node["code"])
    root_df = pd.DataFrame(root_node, index=[0])
    df = pd.concat([root_df, df], ignore_index=True)
    df.sort_index(inplace=True)
    return df


def matches_criteria(tag: dict[str, Any], criteria: dict[str, str | list[str]]) -> bool:
    """Check if a tag matches the specified criteria.

    Args:
        tag (dict[str, any]): The tag to check.
        criteria (dict[str, str | list[str]]): The criteria to match against.
            Values can be single strings or lists of strings.

    Returns:
        bool: True if the tag matches the criteria, False otherwise.
    """
    for key, value in criteria.items():
        if isinstance(value, list):
            if tag.get(key) not in value:
                return False
        else:
            if tag.get(key) != value:
                return False
    return True


def filter_tags(
    tags: dict[str, dict[str, Any]], criteria: dict[str, str | list[str]]
) -> dict[str, dict[str, Any]]:
    """Filter tags based on the specified criteria.

    Args:
        tags (dict[str, dict[str, any]]): The dictionary of tags to filter.
        criteria (dict[str, str | list[str]]): The criteria to filter by.
            Values can be single strings or lists of strings.

    Returns:
        dict[str, dict[str, any]]: A dictionary of tags that match the criteria.
    """
    return {k: v for k, v in tags.items() if matches_criteria(v, criteria)}


def search_by_tags(
    tags: dict[str, dict[str, Any]], criteria: dict[str, str | list[str]]
) -> list[str]:
    """Filter tags based on the specified criteria.

    Args:
        tags (dict[str, dict[str, any]]): The dictionary of tags to filter.
        criteria (dict[str, str | list[str]]): The criteria to filter by.
            Values can be single strings or lists of strings.

    Returns:
        dict[str, dict[str, any]]: A dictionary of tags that match the criteria.
    """
    return [k for k in filter_tags(tags, criteria).keys()]


def inherited_set_tags(tags: dict) -> dict:
    """Return the tags that are inherited from the set."""
    set_only_tags = ["series", "name"]
    inherit_from_set_tags = {"dataset": tags["name"], **tags}
    [inherit_from_set_tags.pop(key) for key in set_only_tags]
    return inherit_from_set_tags


# A different approach: duckdb to search within tags .
# -------------------
# def duckdb_query(query: str, **kwargs: pa.Table) -> pa.Table:  # -- noqa: E999 #NOSONAR
#     """Run a query on duckdb."""
#     return duckdb.sql(query)
# -------------------
# def duck_filter_by_tags(
#     object_tags: dict[str, str | list[str]], filter_tags: dict[str, str | list[str]]
# ) -> list[str]:
#     """Check object tagss, return keys for which all filter tags are satisfied."""
#     query = """
#     SELECT * FROM objects
#     """
#     #    INTERSECT  SELECT * FROM tags
#     objects = (pa.Table.from_pydict(object_tags),)
#     tags = (pa.Table.from_pydict(filter_tags),)
#     out = duckdb_query(query, objects=objects, tags=tags)
#     return objects


# A different approach for tagging sets and series was considered.
# DatasetTags and SeriesTags classes are currently not used, but kept as the decision may be revisited.

# define class Condition?


# NO SONAR
# @dataclass
# class TaggedObject:
#     """Tags for an object. Methods for manipulating tags."""

#     name: str
#     """The name of the object."""

#     tags: dict[str | list[str]]
#     """The tags of the object."""

#     def __get_item__(self, identifiers: str | list[str]) -> Self:
#         """Get specifed tags object."""
#         ...
#         return self.tags[identifiers]

#     def add(
#         self,
#         tags: dict[str, str | list[str]] | None = None,
#         condition: dict | None = None,
#         **kwargs: str | list[str],
#     ) -> None:
#         """Add tags if they are not already present.

#         Tags may be specified as kwargs or as a tags dict.
#         """
#         ...
#         # -- if value not in self[attribute]:
#         #    self[attribute].append(value)

#     def remove(
#         self,
#         tags: dict[str, str | list[str]] | None = None,
#         condition: dict | None = None,
#         **kwargs: str | list[str],
#     ) -> None:
#         """Remove tags if they are present.

#         Tags may be specified as kwargs or as a tags dict.
#         """
#         ...
#         # -- if value  in self[attribute]:
#         #    self[attribute].remove(value)

#     def replace(
#         self,
#         tags: dict[str, str | list[str]] | None = None,
#         **kwargs: str | list[str],
#     ) -> None:
#         """Remove tags if they are present.

#         Tags may be specified as kwargs or as a tags dict.
#         """
#         ...
#         # -- if value1  in self[attribute]:
#         #    self[attribute].remove(value1)
#         #    self[attribute].add(value2)

#     def propagate(
#         self,
#         operation: str = "add",
#         fields: str | list[str] = "",
#         **kwargs: str | list[str],
#     ) -> None:
#         """Propagate tagging operation to <field>.

#         Series may be identified by series names or index, a list of series names or list of series indexes, or a dict of tags.
#         Tags may be specified as kwargs or as a tags dict.
#         Operation (optional: 'add' (default) | 'replace' | 'remove') specifies the action to perform on the identified series.
#         """
#         for f in fields:
#             for k, v in kwargs.items():
#                 match operation:
#                     case "add":
#                         self.__getattribute__(f)[k].add(v)
#                     case "replace":
#                         self.__getattribute__(f)[k].replace(v)
#                     case "remove":
#                         self.__getattribute__(f)[k].remove(v)
#                     case _:
#                         raise ValueError(f"Invalid operation: {operation}.")


# NO SONAR
# @dataclass
# class SeriesTags(TaggedObject):
#     """Series Tags: key value pairs with series metadata."""

#     dataset: str
#     name: str
#     index: int
#     tags: dict[str, str | list[str]]

#     def __get_item__(self, identifiers: str | list[str]) -> str | list[str]:
#         """Get tags for one or more series of the dataset tag object."""
#         ...
#         return self.tags[identifiers]


# NO SONAR
# # @dataclass
# class DatasetTags(TaggedObject):
#     """Dataset Tags: key value pairs with dataset metadata. Methods for manipulating tags."""

#     name: str
#     versioning: str
#     temporality: str
#     series: dict[str, SeriesTags]
