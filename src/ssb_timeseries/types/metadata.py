"""Metadata type aliases: repository, dataset, series, and tag names and values."""

from __future__ import annotations

from typing import TypeAlias

from typing_extensions import TypedDict

from .series import Temporality
from .series import Versioning

RepositoryName: TypeAlias = str
"""Repository name can be any string that match a key in the repositories section of the configuration."""

DatasetName: TypeAlias = str
"""`Dataset` names must be unique within a `Repository`."""

SeriesName: TypeAlias = str
"""Series names are strings identifying data columns and tag keys, ie they must be unique within the `Dataset`."""

TagKey: TypeAlias = str
"""Tag keys are attribute names represented as strings."""

TagValue: TypeAlias = str | list[str]
"""Tag values are attribute values represented as strings or lists of strings."""

Tags: TypeAlias = dict[TagKey, TagValue]
"""A dictionary with tag key-value pairs."""

SeriesTags: TypeAlias = dict[SeriesName, Tags]
"""A dictionary mapping series names to their tags."""


class DatasetTags(TypedDict, extra_items=TagValue):  # type: ignore[call-arg]  # PEP 728, not yet known to mypy
    """A dictionary representing the full dataset metadata, including 'series'.

    The five declared keys are mandatory; additional tags are allowed, but their
    values must be tag values.
    """

    name: DatasetName
    repository: RepositoryName
    versioning: Versioning | str
    temporality: Temporality | str
    series: SeriesTags
