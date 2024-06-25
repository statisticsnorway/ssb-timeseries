"""Placeholder utility covering meta data functionality used by timeseries.

Ideally, this functionality should live elsewhere, in ssb-python-klass and other meta data libraries. Likely subject to refactoring later.
"""

import io
from copy import deepcopy
from typing import Any
from typing import TypeAlias
from typing import no_type_check

import bigtree
import pandas as pd
from bigtree import get_tree_diff
from bigtree import print_tree
from klass import get_classification
from typing_extensions import Self

from ssb_timeseries import fs
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment,override,type-arg,attr-defined,no-untyped-def,import-untyped,union-attr,call-overload,arg-type,index,no-untyped-call,operator"

TagValue: TypeAlias = str | list[str]
TagDict: TypeAlias = dict[str, TagValue]
SeriesTagDict: TypeAlias = dict[str, TagDict]
DatasetTagDict: TypeAlias = dict[str, TagDict | SeriesTagDict]


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
        parents = [
            n
            for n in self.structure.root.descendants
            if n not in self.structure.root.leaves
        ] + [self.structure.root]
        ts_logger.debug(f"parents: {parents}")
        return parents

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


def inherit_set_tags(tags: DatasetTagDict) -> dict[str, Any]:  # -> TagDict:
    """Return the tags that are inherited from the set."""
    set_only_tags = ["series", "name"]
    inherit_from_set_tags = deepcopy(
        {
            "dataset": tags["name"],
            **tags,
        }
    )
    [inherit_from_set_tags.pop(key) for key in set_only_tags]
    return inherit_from_set_tags


def series_tag_dict_edit(
    existing: SeriesTagDict,
    replace: TagDict,
    new: TagDict,
    # dataset_tags: DatasetTagDict = None,
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
def rm_tag(
    input_tags: TagDict,
    tags_to_remove: TagDict,
    recursive: bool = False,
) -> TagDict:
    """Remove tag value from tag dict.

    Values to remove and in tags can be string or list of strings.
    """
    ts_logger.debug(
        f"rm_tag - from tag:\n\t{input_tags}, \nremove value(s): {tags_to_remove}."
    )
    tags = deepcopy(input_tags)
    for attr, val in tags.items():
        for rm_key, rm_value in tags_to_remove.items():
            if (rm_key, rm_value) == (attr, val):
                tags.pop(attr)
            elif rm_key == attr and rm_value is None:
                tags.pop(attr)
            elif isinstance(val, list) and rm_value in val:
                match len(val):
                    case 2:
                        tags[attr].remove(rm_value)
                        tags[attr] = tags[attr][0]
                    case 1:
                        tags.pop(attr)
                    case _:
                        tags[attr].remove(rm_value)
            elif isinstance(val, dict) and recursive:
                tags[attr] = rm_tag(tags[attr], tags_to_remove, recursive)
            else:
                continue

    return tags


@no_type_check  # "no any return
def replace_dataset_tags(
    existing: DatasetTagDict,
    replace: TagDict,
    new: TagDict,
    propagate: bool = False,
) -> DatasetTagDict:
    """Alter selected attributes in dataset tag dictionary.

    Either 'replace' or 'new' (or both) must be specified.
    If 'replace == {}', new tags are appended (aka 'tag_dataset').
    If 'new == {}', 'replace' tags are deleted (aka 'detag_dataset').
    If both are specified, 'replace' are deleted before 'new' are appended.
    """
    if new and not existing:
        tags: DatasetTagDict = new
        if propagate:
            tags["series"] = series_tag_dict_edit({}, replace, new)
        return tags

    if replace == new:
        if replace == {} and new == {}:
            raise ValueError("Either 'replace' or 'new' must be specified.")
        return existing

    out = deepcopy(existing)
    # for tags in existing.values():
    if replace == {}:
        out.update(new)
    elif new == {}:
        return delete_dataset_tags(dictionary=out, remove=replace, propagate=propagate)
    elif replace <= existing:
        ts_logger.debug(
            f"alter_dataset_tags: ... replace:\t{replace} -->  update with:\n\t{new}"
        )
        out.update(new)

    if propagate:
        out["series"] = series_tag_dict_edit(
            existing=out["series"],
            replace=replace,
            new=new,
        )

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
    else:
        out = deepcopy(dictionary)
        ts_logger.debug(
            f"meta.edit_dataset_tags_delete: remove {kwargs}\nfrom existing {out}"
        )
        if args:
            out.pop(*args)
        for k, v in kwargs.items():

            if out[k] == v:
                out.pop(k)
            elif v in out[k]:
                match len(out[k]):
                    case 1:
                        ts_logger.debug(
                            f"pop attribute {k} after removing last value {v}"
                        )
                        out.pop(k)
                    case 2:
                        out[k].remove(v)
                        out[k] = out[k][0]
                        ts_logger.debug(
                            f"attribute {k}: single value {out[k]} remain after removing {v} --> converted to string."
                        )
                    case _:
                        out[k].remove(v)
                        ts_logger.debug(
                            f"attribute {k}: values {out[k]} remain after removing {v}"
                        )

        if propagate:
            out["series"] = delete_series_tags(
                out["series"],
                *args,
                all=remove_all,
                **kwargs,
            )

        return out


@no_type_check
def delete_series_tags(
    dictionary: SeriesTagDict | DatasetTagDict,
    *args: str,
    **kwargs: TagValue,  # | bool,
) -> SeriesTagDict:
    """Remove selected series attributes from series or dataset tag dictionary."""
    remove_all: bool = kwargs.pop("all", False)

    is_dataset = "series" in dictionary
    if is_dataset:
        output_tags = deepcopy(dictionary["series"])
    else:
        output_tags = deepcopy(dictionary)

    if remove_all:
        for k in output_tags.keys():
            if k not in ["dataset", "name", "versioning", "temporality"]:
                output_tags.pop(k)

    elif args or kwargs:
        ts_logger.debug(
            f"meta.delete_series_tags: remove {kwargs}\nfrom series tags: {output_tags}"
        )
        # remove attribute, regardless of values
        if args:
            for o in output_tags.values():
                o.pop(*args)
        ts_logger.debug(f"meta.delete_series_tags: preliminary out {output_tags}")

        # remove matching values and empty attributes
        for k, v in kwargs.items():
            for tags in output_tags.values():
                if tags[k] == v:
                    tags.pop(k)
                elif v in tags[k]:
                    match len(tags[k]):
                        case 1:
                            tags.pop(k)
                        case 2:
                            tags[k].remove(v)
                            tags[k] = tags[k][0]
                        case _:
                            tags[k].remove(v)

        # refactor - something like this?
        # for tags in output_tags.values():
        #     tags = rm_tag(tags, {**kwargs})
    else:
        ts_logger.debug(f"Nothing to remove: {args} {kwargs}")

    return output_tags


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
