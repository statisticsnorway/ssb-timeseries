"""Properties are type definitions used for series and datasets."""

from enum import Enum
from itertools import product

from typing_extensions import Self

# mypy: disable-error-code="type-arg, override"


class SuperEnum(Enum):
    """Generic enumeration helper."""

    @classmethod
    def to_dict(cls) -> dict:
        """Returns a dictionary representation of the enum."""
        return {e.name: e.value for e in cls}

    @classmethod
    def keys(cls) -> list:
        """Returns a list of all the enum keys."""
        return [item.name for item in cls]

    @classmethod
    def values(cls) -> list:
        """Returns a list of all the enum values."""
        return [item.value[0] for item in cls]

    @classmethod
    def descriptions(cls) -> list:
        """Returns a list of descriptions for all enum values."""
        return [item.value[1] for item in cls]

    def __eq__(self, other: Self) -> bool:
        """Equality test."""
        return repr(self) == repr(other)

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
    """Consider adding support for: Versions identified by free text names."""
    SEMANTIC = 3
    """Consider adding support for: Versions identified by numbers on form X.Y.Z, ie. Major.Minor.Patch."""


class Temporality(SuperEnum):
    """Temporality describes the time dimensionality of each data point; notably duration or lack thereof."""

    NONE = 0
    """No temporal dimension."""
    AT = 1
    """Single point in time expressed with 'valid_at' date."""
    FROM_TO = 2
    """Duration from-to expressed with 'valid_from' and 'valid_to' dates."""

    @property
    def date_columns(self) -> set[str]:
        """Returns the data columns of the temporality."""
        return {"valid_at"} if self == Temporality.AT else {"valid_from", "valid_to"}


class SeriesType:
    """SeriesTypes are defined by combinations of attributes that have technical implications for time series datasets.

    Notable examples are Versioning and Temporality, but a few more may be added later.
    """

    def __init__(
        self,
        versioning: Versioning,
        temporality: Temporality,
        is_sparse: bool = False,
    ) -> None:
        """SeriesType constructor.

        Args:
            versioning (Versioning): The versioning of the series.
            temporality (Temporality): The temporality of the series.
            is_sparse (bool): Whether the series is sparse. Defaults to False.

        """
        self.versioning = versioning
        self.temporality = temporality
        self.is_sparse = is_sparse

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

    @classmethod
    def permutations(cls) -> list[str]:
        """Helper; returns ['<versioning>_<temporality>', ...] ."""
        return ["_".join(c) for c in product(Versioning.keys(), Temporality.keys())]

    @property
    def date_columns(self) -> set[str]:
        """Returns the data columns corresponding to the series type temporality."""
        return self.temporality.date_columns

    def __str__(self) -> str:
        """Helper; returns '<versioning>_<temporality>'."""
        return f"{self.versioning!s}_{self.temporality!s}"

    def __repr__(self) -> str:
        """Helper, returns code with required parameters to initialise given SeriesType."""
        return f"SeriesType({self.versioning!r},{self.temporality!r})"

    def __eq__(self, other: Self) -> bool:
        """Equality test."""
        return repr(self) == repr(other)


def estimate_types() -> list[str]:
    """Helper; returns list of SeriesTypes for which Versioning is not NONE."""
    return ["_".join(c) for c in product(["AS_OF"], Temporality.keys())]


def seriestype_from_str(dir_name: str) -> SeriesType:
    """Helper; returns SeriesType from directory name."""
    match dir_name.lower():
        case "none_at":
            return SeriesType.none_at()
        case "simple":
            return SeriesType.simple()
        case "from_to":
            return SeriesType.from_to()
        case "as_of_at":
            return SeriesType.as_of_at()
        case "as_of_from_to":
            return SeriesType.as_of_from_to()
        case "estimate":
            return SeriesType.estimate()
        case _:
            raise ValueError(f"Invalid dir_name: {dir_name}")
