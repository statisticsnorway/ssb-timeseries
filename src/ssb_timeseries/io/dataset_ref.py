"""Carries the identity of a single dataset to an I/O handler.

A handler is constructed from a repository and its options.
Everything that identifies *which* dataset an operation concerns is carried
here instead, and passed to each operation as its first argument.
A single handler instance can therefore serve any number of datasets.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime

from ..types import SeriesType
from ..types import Versioning

# mypy: disable-error-code="no-untyped-def"


@dataclass(frozen=True)
class DatasetRef:
    """A read-only reference to one dataset, as needed by I/O operations.

    This is a snapshot, not a live view.
    A `Dataset` mutates its name when renamed and its `as_of_utc` when saved,
    so a ref built before such a change will not reflect it.
    Build a ref on demand for each operation rather than holding one.
    """

    name: str
    """The name of the dataset."""

    data_type: SeriesType
    """The series type of the dataset."""

    as_of_utc: datetime | None = None
    """The 'as of' datetime, required when the series type has versioning of type `Versioning.AS_OF`."""

    process_stage: str = ""
    """The process stage the dataset belongs to."""

    product: str = ""
    """The product the dataset belongs to."""

    sharing: dict[str, str] = field(default_factory=dict)
    """Access control tags for the dataset."""

    def __post_init__(self) -> None:
        """Check that the reference identifies a storable dataset.

        A dataset with `Versioning.AS_OF` is identified by the time it was
        valid, so it cannot be written or read without one.
        This is a property of the dataset, not of any particular handler,
        so it is checked once here rather than in every handler.
        """
        if self.as_of_utc is None and self.data_type.versioning == Versioning.AS_OF:
            raise ValueError(
                "An 'as of' datetime must be specified when the type has versioning of type Versioning.AS_OF."
            )
