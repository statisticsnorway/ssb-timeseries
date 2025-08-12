"""The :py:mod:`ssb_timeseries.meta` module is responsible for metadata maintenance.

Dataset and series tags are handled by Python dictionaries and stored in JSON files and Parquet headers. The is a :py:mod:`meta` module takes care of the mechanics of manipulating the dictionaries.

It also consumes taxonomies. That is functionality that should live in the ssb-python-klass or other meta data libraries. Likely subject to refactoring later.
"""

import itertools
from copy import deepcopy
from functools import cache
from typing import Any

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self  # noqa: UP035 #backport to 3.10
import re
from collections.abc import Hashable
from typing import TypeAlias
from typing import no_type_check

import bigtree
import bigtree.node
import bigtree.tree
import narwhals as nw
import pyarrow as pa
from bigtree import get_tree_diff
from bigtree import print_tree
from klass import get_classification
from narwhals.typing import IntoFrameT

import ssb_timeseries as ts
from ssb_timeseries import fs
from ssb_timeseries.dataframes import are_equal
from ssb_timeseries.dataframes import is_df_like
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment,override,type-arg,attr-defined,no-untyped-def,import-untyped,union-attr,call-overload,arg-type,index,no-untyped-call,operator,valid-type,no-any-return"

TagValue: TypeAlias = str | list[str]
TagDict: TypeAlias = dict[str, TagValue]
SeriesTagDict: TypeAlias = dict[str, TagDict]
DatasetTagDict: TypeAlias = dict[str, TagDict | SeriesTagDict]


def camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case, handling acronyms.

    Example:
        'HTTPConnection' -> 'http_connection'
    """
    # Insert underscore before uppercase letter preceded by lowercase letter or digit.
    #    e.g., 'CamelCase' -> 'Camel_Case', 'MyValue1' -> 'My_Value1'
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)

    # Insert an underscore before any uppercase letter that is followed
    #    by a lowercase letter, but only if it's preceded by another
    #    uppercase letter.
    #    e.g., 'HTTPConnection' -> 'HTTP_Connection'
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return name.lower()


class MissingAttributeError(Exception):
    """At least one required attribute was not provided."""

    ...


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

DEFAULT_ROOT_NODE = {
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


@cache
def klass_classification(klass_id: int) -> KlassTaxonomy:
    """Get KLASS classification identified by ID as a list of dicts."""
    root_node = DEFAULT_ROOT_NODE
    root_node["name"] = f"KLASS-{klass_id}"

    classification = get_classification(str(klass_id)).get_codes()
    klass_data = classification.data.to_dict("records")
    for k in klass_data:
        if not k["parentCode"]:
            k["parentCode"] = root_node["code"]
    list_of_items = [dict(record) for record in [root_node, *klass_data]]
    return list_of_items


def records_to_arrow(records: list[dict[str, Any]]) -> pa.Table:
    """Creates a PyArrow Table from a list of dictionaries (row/records).

    Args:
        records: A list where each element is a dictionary representing a row.

    Returns:
        A pyarrow.Table representing the data.
    """
    if not records:
        return pa.Table.from_pylist([], schema=KLASS_ITEM_SCHEMA)
    else:
        return pa.Table.from_pylist(records, schema=KLASS_ITEM_SCHEMA)


class Taxonomy:
    """Wraps taxonomies defined in KLASS or json files in a object structure.

    Attributes:
        name  (str):
        structure_type:     enum:   list | tree | graph
        levels: number of levels not counting the root node
        entities (pa.Table): Entity definitions, represented as a dataframe with columns as defined by ::py:class:`KlassItem`.
        structure (bigtree.tree):

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
        *,
        klass_id: int = 0,
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
        if klass_id:
            # TO DO: handle versions of KLASS
            list_of_items = klass_classification(klass_id)
            tbl = records_to_arrow(list_of_items)
        elif data and isinstance(data, list):
            tbl = records_to_arrow(data)
        elif data and not isinstance(data, list) and is_df_like(data):
            tbl = nw.from_native(data).to_arrow()  # ignore: [type-var]
        elif path:
            dict_from_file = fs.read_json(str(path))
            tbl = records_to_arrow(dict_from_file)
        else:
            raise MissingAttributeError(
                "Either klass_id (int), data (dict|df), or path (str) must be provided."
            )

        # TODO: add proper validation - check tbl for root node + duplicates + fill missing
        self.entities = tbl

        self.substitute(kwargs.get("substitutions", {}))

        pandas_df = nw.from_native(self.entities).to_pandas()
        self.structure: bigtree.tree = bigtree.dataframe_to_tree_by_relation(
            data=pandas_df,
            child_col="code",
            parent_col="parentCode",
        )

    def __eq__(self, other: Self) -> bool:
        """Checks for equality. Taxonomies are considered equal if their codes and hierarchical relations are the same."""
        tree_diff = get_tree_diff(self.structure, other.structure)
        if tree_diff:
            return False

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

    def __sub__(self, other: bigtree.Node) -> bigtree.Node:  # type: ignore[name-defined]
        """Return the tree difference between the two taxonomy (tree) structures."""
        ts.logger.debug("other: %s", other)
        if isinstance(other, bigtree.Node):
            remove = self.subtree(other.name).asc  # noqa: F841
            return NotImplemented

    def __getitem__(self, key: str) -> bigtree.Node:  # type: ignore[name-defined]
        """Get tree node by name (KLASS code)."""
        return bigtree.find_name(self.structure.root, key)

    def subtree(self, key: str) -> Any:
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

    def all_nodes(self) -> list[bigtree.node]:
        """Return all nodes in the taxonomy."""
        return list(self.structure.root.descendants)

    def leaf_nodes(self, name: bigtree.Node | str = "") -> list[bigtree.Node]:  # type: ignore[name-defined]
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

            ts.logger.debug("leaves: %s", leaves)
            return leaves
        else:
            return [n.name for n in self.structure.leaves]

    def parent_nodes(self) -> list[bigtree.node]:
        """Return all non-leaf nodes in the taxonomy."""
        parents = [
            n
            for n in self.structure.root.descendants
            if n not in self.structure.root.leaves
        ] + [self.structure.root]
        ts.logger.debug("parents: %s", parents)
        return parents

    def save(self, path: PathStr) -> None:
        """Save taxonomy to json file.

        The file can be read using Taxonomy(<path to file>).
        """
        pandas_df = nw.from_native(
            self.entities
        ).to_pandas()  # TO DO: reactor not to use pandas
        fs.write_json(path, pandas_df.to_dict("records"))

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
    return list(filter_tags(tags, criteria).keys())


def inherit_set_tags(
    tags: DatasetTagDict | SeriesTagDict,
) -> dict[str, Any]:  # -> TagDict:
    """Return the tags that are inherited from the set."""
    if "dataset" in tags:
        tags["series"] = inherit_set_tags(tags)
        return tags
    else:
        set_only_tags = ["series", "name"]
        inherited_from_set_tags = deepcopy(
            {
                "dataset": tags["name"],
                **tags,
            }
        )
        [inherited_from_set_tags.pop(key) for key in set_only_tags]
        return inherited_from_set_tags


def series_tag_dict_edit(
    existing: SeriesTagDict,
    replace: TagDict,
    new: TagDict,
) -> SeriesTagDict:
    """Alter selected attributes in a Dataset.tag['series'] dictionary.

    Either 'replace' or 'new' (or both) must be specified.
    If 'replace == {}', new tags are appended (aka 'tag_series').
    If 'new == {}', 'replace' tags are deleted (aka 'detag_series').
    If both are specified, 'replace' are deleted before 'new' are appended (aka 'retag_series').
    """
    if replace == new:
        return existing
    elif replace == {} and new == {}:
        raise ValueError("Either 'replace' or 'new' must be specified.")

    out = deepcopy(existing)

    if replace == {}:
        # update all
        for o in out.values():
            o.update(new)
    else:
        # update matching
        matching = search_by_tags(out, replace)

        for series, tags in out.items():
            if series in matching and new != {}:
                tags.update(new)
            elif series in matching and new == {}:
                tags = delete_series_tags(tags, **replace)
                # tags.update(inherited)

    return out


@no_type_check
def add_tag_values(
    old: TagDict,
    additions: TagDict,
    recursive: bool = False,
) -> TagDict:
    """Add tag values to a tag dict.

    Will append new tags as a list if any values already exist. With parameters recursive=True, nested dicts are also traversed.
    """
    ts.logger.debug(
        "add_tag_values - to existing tags:\n\t%s, \nadd value(s): %s.",
        old,
        additions,
    )
    new = deepcopy(old)
    for attr, new_value in additions.items():
        old_value = old.get(attr)
        if old_value is None:
            new[attr] = new_value
        else:
            o = unique_tag_values(old_value)
            n = unique_tag_values(new_value)
            new[attr] = to_tag_value(set(o).union(set(n)))

    if recursive and "series" in new:
        for series_key, tags in new["series"].items():
            new["series"][series_key] = add_tag_values(tags, additions, False)
    return new


@no_type_check
def rm_tag_values(
    existing: TagDict,
    tags_to_remove: TagDict,
    recursive: bool | list[str] = False,
) -> TagDict:
    """Remove tag value from tag dict.

    Values to remove and in tags can be string or list of strings.
    """
    ts.logger.debug(
        "rm_tag - from tag:\n\t%s, \nremove value(s): %s.",
        existing,
        tags_to_remove,
    )
    new = deepcopy(existing)
    for attr, val in existing.items():
        for rm_key, rm_value in tags_to_remove.items():
            if (rm_key, rm_value) == (attr, val):
                new.pop(attr)
            elif rm_key == attr and rm_value is None:
                new.pop(attr)
            elif isinstance(val, list) and rm_value in val:
                match len(val):
                    case 2:
                        new[attr].remove(rm_value)
                        new[attr] = to_tag_value(new[attr])
                    case 1:
                        new.pop(attr)
                    case _:
                        new[attr].remove(rm_value)

    if recursive and "series" in new:
        ts.logger.debug(
            "rm_tag - from tag:\n\t%s, \nrecursively remove value(s): %s.",
            existing,
            tags_to_remove,
        )
        for series_key, tags in new["series"].items():
            new["series"][series_key] = rm_tag_values(tags, tags_to_remove, False)

    return new


def rm_tags(
    existing: TagDict,
    *args: str,
    recursive: bool | list[str] = False,
) -> TagDict:
    """Remove attribute from tag dict regardless of value."""
    new = deepcopy(existing)
    for attribute in args:
        new.pop(attribute)

    if recursive and "series" in new:
        for series_key, tags in new["series"].items():
            new["series"][series_key] = rm_tags(tags, *args, False)

    return new


@no_type_check  # "no any return
def replace_dataset_tags(
    existing: TagDict,
    old: TagDict,
    new: TagDict,
    recursive: bool = False,
) -> DatasetTagDict:
    """Alter selected attributes value pairs in a tag dictionary."""
    if old == new:
        return existing

    if existing.items() < old.items():
        return existing

    out = rm_tag_values(existing, old, recursive=False)
    out = add_tag_values(out, new, recursive=False)

    if recursive and "series" in out:
        for series_key, tags in out["series"].items():
            if tags.items() >= old.items():
                tags = rm_tag_values(tags, old, recursive=False)
                tags = add_tag_values(tags, new, recursive=False)
            out["series"][series_key] = tags
    return out


@no_type_check  # "comparison-overlap
def delete_dataset_tags(
    dictionary: DatasetTagDict,
    *args: str,
    **kwargs: SeriesTagDict | bool,
) -> DatasetTagDict:
    """Remove selected attributes from dataset tag dictionary."""
    remove_all = kwargs.pop("all", False)
    propagate = kwargs.pop("propagate", False)
    if remove_all:
        return inherit_set_tags(dictionary)

    out = deepcopy(dictionary)
    out = rm_tags(out, *args)
    for k, v in kwargs.items():
        out = rm_tag_values(out, {k: v}, recursive=False)

    if propagate:
        out["series"] = delete_series_tags(
            out["series"],
            *args,
            all=remove_all,
            **kwargs,
        )

    return out


def delete_series_tags(
    dictionary: SeriesTagDict | DatasetTagDict,
    *args: str,
    **kwargs: TagValue,  # | bool,
) -> SeriesTagDict | DatasetTagDict:
    """Remove selected series attributes from series or dataset tag dictionary."""
    remove_all: bool = kwargs.pop("all", False)

    if remove_all:
        return inherit_set_tags(dictionary)

    output_tags = deepcopy(dictionary)
    if "series" in dictionary:
        output_tags["series"] = delete_series_tags(
            output_tags["series"], *args, all=remove_all, **kwargs
        )
        return output_tags
    else:
        for series_key, series_tags in output_tags.items():
            if args:
                series_tags.pop(*args)
            if kwargs:
                for k, v in kwargs.items():
                    series_tags = rm_tag_values(series_tags, {k: v}, recursive=False)
            output_tags[series_key] = series_tags
        return output_tags


# helpers:
def to_tag_value(tag: TagValue | set) -> TagValue:
    """If input is a list of unique strings."""
    if isinstance(tag, str):
        return tag

    if isinstance(tag, set):
        tag = list(tag)
    elif isinstance(tag, list):
        tag = list(set(tag))

    if len(tag) == 1:
        return str(tag[0])
    else:
        return sorted(tag)


def unique_tag_values(arg: Any) -> list[str]:
    """Wraps string input in list, and ensure the list is unique."""
    if isinstance(arg, str):
        lst = [arg]
    elif isinstance(arg, set) or isinstance(arg, list):
        lst = sorted([str(s) for s in set(arg) if isinstance(s, str)])
    else:
        raise ValueError(f"Unsupported type: {type(arg)}")

    return lst


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
        TODO: add some. See code for dataset.aggregate() for a notable use case.
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
                nodes = taxonomy.all_nodes()
            case "parents" | "parent_nodes":
                nodes = taxonomy.parent_nodes()
            case "leaves" | "leaf_nodes" | "children" | "child_nodes":
                nodes = taxonomy.leaf_nodes()

        node_lists.append([node.name for node in nodes])

    combinations = itertools.product(*node_lists)
    for c in combinations:
        d = {}
        for k, v in zip(taxonomies.keys(), c, strict=False):
            d[k] = v
        out.append(d)
    ts.logger.debug(out)
    return out


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
