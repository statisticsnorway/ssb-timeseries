"""Series types: the versioning and temporality combinations that define a dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# mypy: disable-error-code="type-arg, override"


class Versioning(Enum):
    """Versioning refers to how revisions of data are identified (named)."""

    NONE = 0
    """Version control only. Versions are not accessible through API."""
    AS_OF = 1
    """Version identified by dates allows date arithemetic ('equals' | 'greater than'  | 'smaller than' | 'between')."""
    NAMES = 2
    """Versions identified by free text names.

    This functionality is under consideration, hence preparepared in the type system, but with not all the way through IO and core modules."""

    @property
    def date_columns(self) -> list[str]:
        """Returns the date columns for versioning."""
        return ["as_of"] if self == Versioning.AS_OF else []


class Temporality(Enum):
    """Temporality describes the time dimensionality of each data point; notably duration or lack thereof."""

    NONE = 0
    """No temporal dimension."""
    AT = 1
    """Single points in time, expressed with (exact) 'valid_at' dates."""
    FROM_TO = 2
    """Duration periods, expressed with 'valid_from' (inclusive) and 'valid_to' (exclusive) dates."""

    @property
    def date_columns(self) -> list[str]:
        """Returns the date columns of the temporality."""
        if self == Temporality.AT:
            return ["valid_at"]
        if self == Temporality.FROM_TO:
            return ["valid_from", "valid_to"]
        return []


@dataclass(frozen=True)
class SeriesType:
    """SeriesTypes are defined by combinations of attributes that have technical implications for time series datasets.

    Notable examples are Versioning and Temporality, but a few more may be added later.
    """

    versioning: Versioning
    temporality: Temporality

    @classmethod
    def from_name(cls, name: str) -> SeriesType:
        """Return the SeriesType with the given canonical name, ignoring case.

        The canonical names are the persisted spellings, for example 'NONE_AT'.
        """
        try:
            return _CANONICAL_NAMES[name.strip().lower()]
        except KeyError:
            raise ValueError(f"Invalid SeriesType name: {name!r}") from None

    @classmethod
    def from_tag_values(
        cls, versioning: Versioning | str, temporality: Temporality | str
    ) -> SeriesType:
        """Return the SeriesType stored as the 'versioning' and 'temporality' tag values."""
        return cls(
            versioning=versioning
            if isinstance(versioning, Versioning)
            else Versioning[versioning],
            temporality=temporality
            if isinstance(temporality, Temporality)
            else Temporality[temporality],
        )

    @property
    def date_columns(self) -> list[str]:
        """Returns the date columns for the series type."""
        return list(
            dict.fromkeys(
                [*self.versioning.date_columns, *self.temporality.date_columns]
            )
        )

    def __str__(self) -> str:
        """Canonical spelling, also used as the persisted directory name."""
        return f"{self.versioning.name}_{self.temporality.name}"

    def __repr__(self) -> str:
        """Reconstructable representation, sufficient to recreate the value."""
        return (
            f"SeriesType(versioning=Versioning.{self.versioning.name}, "
            f"temporality=Temporality.{self.temporality.name})"
        )


NONE_AT = SeriesType(versioning=Versioning.NONE, temporality=Temporality.AT)
"""Instance of `SeriesType` for non-versioned data with a single point in time per observation."""

NONE_FROM_TO = SeriesType(versioning=Versioning.NONE, temporality=Temporality.FROM_TO)
"""Instance of `SeriesType` for non-versioned data with a validity period per observation."""

AS_OF_AT = SeriesType(versioning=Versioning.AS_OF, temporality=Temporality.AT)
"""Instance of `SeriesType` for estimates at points in time, versioned by 'as of' dates."""

AS_OF_FROM_TO = SeriesType(versioning=Versioning.AS_OF, temporality=Temporality.FROM_TO)
"""Instance of `SeriesType` for estimates over validity periods, versioned by 'as of' dates."""

_CANONICAL_NAMES: dict[str, SeriesType] = {
    str(series_type).lower(): series_type
    for series_type in (NONE_AT, NONE_FROM_TO, AS_OF_AT, AS_OF_FROM_TO)
}
