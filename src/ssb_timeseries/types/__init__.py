"""Type aliases and series types used across `ssb_timeseries`.

The definitions live in the submodules `series` (versioning, temporality and
`SeriesType`) and `metadata` (name and tag aliases).
They are re-exported here so that `from ssb_timeseries.types import ...` keeps
working for both internal and external code.
"""

from __future__ import annotations

from collections.abc import Callable
from os import PathLike
from typing import TypeAlias

from .metadata import DatasetName
from .metadata import DatasetTags
from .metadata import RepositoryName
from .metadata import SeriesName
from .metadata import SeriesTags
from .metadata import TagKey
from .metadata import Tags
from .metadata import TagValue
from .series import AS_OF_AT
from .series import AS_OF_FROM_TO
from .series import NONE_AT
from .series import NONE_FROM_TO
from .series import SeriesType
from .series import SuperEnum
from .series import Temporality
from .series import Versioning
from .series import estimate_types
from .series import seriestype_from_str

# mypy: disable-error-code="type-arg"

PathStr: TypeAlias = str | PathLike[str]
F: TypeAlias = Callable

__all__ = [
    "AS_OF_AT",
    "AS_OF_FROM_TO",
    "NONE_AT",
    "NONE_FROM_TO",
    "DatasetName",
    "DatasetTags",
    "F",
    "PathStr",
    "RepositoryName",
    "SeriesName",
    "SeriesTags",
    "SeriesType",
    "SuperEnum",
    "TagKey",
    "TagValue",
    "Tags",
    "Temporality",
    "Versioning",
    "estimate_types",
    "seriestype_from_str",
]
