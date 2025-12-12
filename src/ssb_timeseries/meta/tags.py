"""Provides functions for manipulating dataset and series metadata tags.

Tags are stored in dictionaries, and this module contains the core logic
for creating, updating, deleting, and transforming those dictionaries.
It defines the main type aliases used for metadata, such as ``TagDict``
and ``DatasetTagDict``.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from typing import TypeAlias
from typing import no_type_check

import ssb_timeseries as ts

# mypy: disable-error-code="assignment,override,type-arg,attr-defined,no-untyped-def,import-untyped,union-attr,call-overload,arg-type,index,no-any-return"

TagValue: TypeAlias = str | list[str]
"""A tag value can be a single string or a list of strings."""

TagDict: TypeAlias = dict[str, TagValue]
"""A dictionary of tags, where keys are tag names and values are TagValues."""

SeriesTagDict: TypeAlias = dict[str, TagDict]
"""A dictionary mapping series names to their TagDict."""

# The more specific type hint below is too restrictive for runtime type checkers like typeguard,
# which fail on the complex, nested structure of the tag dictionaries.
# DatasetTagDict: TypeAlias = dict[str, TagDict | SeriesTagDict]
DatasetTagDict: TypeAlias = dict[str, Any]
"""A dictionary representing the full dataset metadata, including 'series'."""


def matches_criteria(tag: dict[str, Any], criteria: dict[str, str | list[str]]) -> bool:
    """Check if a tag matches the specified criteria.

    Args:
        tag: The tag to check.
        criteria: The criteria to match against.
            Values can be single strings or lists of strings.

    Returns:
        True if the tag matches the criteria, False otherwise.
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
        tags: The dictionary of tags to filter.
        criteria: The criteria to filter by.
            Values can be single strings or lists of strings.

    Returns:
        A dictionary of tags that match the criteria.
    """
    return {k: v for k, v in tags.items() if matches_criteria(v, criteria)}


def search_by_tags(
    tags: dict[str, dict[str, Any]], criteria: dict[str, str | list[str]]
) -> list[str]:
    """Filter tags based on the specified criteria and return the keys.

    Args:
        tags: The dictionary of tags to filter.
        criteria: The criteria to filter by.
            Values can be single strings or lists of strings.

    Returns:
        A list of keys for tags that match the criteria.
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
