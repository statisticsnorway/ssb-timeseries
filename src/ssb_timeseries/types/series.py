"""Series types: the versioning and temporality combinations that define a dataset."""

from __future__ import annotations

from enum import Enum

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self  # noqa: UP035 #backport to 3.10

# mypy: disable-error-code="type-arg, override"


class SuperEnum(Enum):
    """Generic enumeration helper."""

    def __repr__(self) -> str:
        """Machine readable string representation, ideally sufficient to recreate object."""
        return f"{self.__class__.__name__}.{self.name}"

    def __str__(self) -> str:
        """Human readable string representation of object."""
        return self.name


class Versioning(SuperEnum):
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


class Temporality(SuperEnum):
    """Temporality describes the time dimensionality of each data point; notably duration or lack thereof."""

    NONE = 0
    """No temporal dimension."""
    AT = 1
    """Single points in time, expressed with (exact) 'valid_at' dates."""
    FROM_TO = 2
    """Duration periods, expressed with 'valid_from' (inclusive) and 'valid_to' (exclusive) dates."""

    @property
    def date_columns(self) -> list[str]:
        """Returns the data columns of the temporality."""
        if self == Temporality.AT:
            return ["valid_at"]
        if self == Temporality.FROM_TO:
            return ["valid_from", "valid_to"]
        return []


class SeriesType:
    """SeriesTypes are defined by combinations of attributes that have technical implications for time series datasets.

    Notable examples are Versioning and Temporality, but a few more may be added later.
    """

    def __init__(
        self,
        *type_def: object,
        versioning: Versioning | None = None,
        temporality: Temporality | None = None,
    ) -> None:
        """SeriesType constructor.

        Args:
            *type_def: Keyword strings identifying versioning and/or temporality.
            versioning: The versioning of the series.
            temporality: The temporality of the series.

        """
        # is_sparse (bool): Whether the series is sparse. Defaults to False.
        # dtype: numeric
        #   (without further specification for float/int/decimal)
        # partial support: bool
        # consider later: text
        words = {str(t).lower() for t in type_def}

        # Initialize with defaults
        _versioning = Versioning.NONE
        _temporality = Temporality.FROM_TO

        # Handle high-level keywords first
        if "simple" in words:
            _versioning = Versioning.NONE
            _temporality = Temporality.AT
        elif "estimate" in words:
            _versioning = Versioning.AS_OF
            _temporality = Temporality.AT
        else:
            # Fallback for other versioning keywords if no high-level keyword was found
            if {"asof", "as_of"} & words:
                _versioning = Versioning.AS_OF
            elif {"names"} & words:
                _versioning = Versioning.NAMES

            # Fallback for other temporality keywords if no high-level keyword was found
            if {"at"} & words:
                _temporality = Temporality.AT

        # Explicit arguments always override
        self.versioning = versioning if versioning is not None else _versioning
        self.temporality = temporality if temporality is not None else _temporality

    @classmethod
    def none_at(cls) -> Self:
        """Same as SeriesType.simple(): Shorthand for SeriesType(versioning=Versioning.NONE, temporality=Temporality.AT)."""
        return cls(versioning=Versioning.NONE, temporality=Temporality.AT)

    @classmethod
    def simple(cls) -> Self:
        """Same as SeriesType.none_at(): Shorthand for SeriesType(versioning=Versioning.NONE, temporality=Temporality.AT)."""
        return cls.none_at()

    @classmethod
    def from_to(cls) -> Self:
        """Shorthand for SeriesType(versioning=Versioning.NONE, temporality=Temporality.FROM_TO)."""
        return cls(versioning=Versioning.NONE, temporality=Temporality.FROM_TO)

    @classmethod
    def as_of_at(cls) -> Self:
        """Same as SeriesType.estimate(): Shorthand for SeriesType(versioning=Versioning.AS_OF, temporality=Temporality.AT)."""
        return cls(versioning=Versioning.AS_OF, temporality=Temporality.AT)

    @classmethod
    def as_of_from_to(cls) -> Self:
        """Shorthand for SeriesType(versioning=Versioning.AS_OF, temporality=Temporality.FROM_TO)."""
        return cls(versioning=Versioning.AS_OF, temporality=Temporality.FROM_TO)

    @classmethod
    def estimate(cls) -> Self:
        """Same as SeriesType.as_of_at(): Shorthand for SeriesType(versioning=Versioning.AS_OF, temporality=Temporality.AT)."""
        return cls.as_of_at()

    @property
    def date_columns(self) -> list[str]:
        """Returns the data columns corresponding to the series type temporality."""
        return list(
            set(self.versioning.date_columns) | set(self.temporality.date_columns)
        )

    def __str__(self) -> str:
        """Helper; returns '<versioning>_<temporality>'."""
        return f"{self.versioning!s}_{self.temporality!s}"

    def __repr__(self) -> str:
        """Helper, returns code with required parameters to initialise given SeriesType."""
        return f"SeriesType({self.versioning!r},{self.temporality!r})"

    def __eq__(self, other: Self | None) -> bool:
        """Equality test."""
        if other is None:
            return False
        else:
            return repr(self) == repr(other)


NONE_AT = SeriesType(versioning=Versioning.NONE, temporality=Temporality.AT)
"""Instance of `SeriesType` for non-versioned data with a single point in time per observation."""

NONE_FROM_TO = SeriesType(versioning=Versioning.NONE, temporality=Temporality.FROM_TO)
"""Instance of `SeriesType` for non-versioned data with a validity period per observation."""

AS_OF_AT = SeriesType(versioning=Versioning.AS_OF, temporality=Temporality.AT)
"""Instance of `SeriesType` for estimates at points in time, versioned by 'as of' dates."""

AS_OF_FROM_TO = SeriesType(versioning=Versioning.AS_OF, temporality=Temporality.FROM_TO)
"""Instance of `SeriesType` for estimates over validity periods, versioned by 'as of' dates."""
