from enum import Enum
from itertools import product

from typing_extensions import Self


class SuperEnum(Enum):
    """Generic enumeration helper."""

    @classmethod
    def to_dict(cls) -> dict:
        """Returns a dictionary representation of the enum."""
        return {e.name: e.value for e in cls}

    @classmethod
    def keys(cls) -> list:
        """Returns a list of all the enum keys."""
        # return cls._member_names_
        return [item.name for item in cls]

    @classmethod
    def values(cls) -> list:
        """Returns a list of all the enum values."""
        # return list(cls._value2member_map_.keys)
        return [item.value[0] for item in cls]

    @classmethod
    def descriptions(cls) -> list:
        """Returns a list of descriptions for all enum values."""
        # return list(cls._value2member_map_.keys())
        return [item.value[1] for item in cls]

    def __repr__(self) -> str:
        """Machine readable string representation, ideally sufficient to recreate object."""
        return f"{self.__class__.__name__}.{self.name}"

    def __str__(self) -> str:
        """Human readable string representation of object."""
        return self.name


class Versioning(SuperEnum):
    """Versioning refers to how revisions of data are identified (named)."""

    NONE = 0
    # ("NONE", "Version control only. Versions are not accessible through API.")
    AS_OF = 1
    # (
    #     "AS_OF",
    #     "Version identified by dates allows date arithemetic ('equals' | 'greater than'  | 'smaller than' | 'between').",
    # )
    NAMES = 2
    # (
    #     "NAMES",
    #     "Consider adding support for: Versions identified by free text names.",
    # )
    SEMANTIC = 3
    # (
    #     "SEMANTIC",
    #     "Consider adding support for: Versions identified by numbers on form X.Y.Z, ie. Major.Minor.Patch.",
    # )


class Temporality(SuperEnum):
    """Temporality describes the time dimensionality of each data point; notably duration or lack thereof."""

    NONE = 0
    AT = 1  # (1, "Single points in time expressed with 'valid_at' dates.")
    FROM_TO = (
        2  # (2, "Duration from-to expressed with 'valid_from' and 'valid_to' dates.")
    )


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
        """SeriesType constructor."""
        self.versioning = versioning
        self.temporality = temporality
        self.is_sparse = is_sparse

        """match type.lower():
            case "simple":
                self.versioning = Versioning.NONE
                self.temporality = Temporality.AT
            case "from_to":
                self.versioning = Versioning.NONE
                self.temporality = Temporality.FROM_TO
            case "estimate":
                self.versioning = Versioning.AS_OF
                self.temporality = Temporality.AT
            case "as_of_from_to":
                self.versioning = Versioning.AS_OF
                self.temporality = Temporality.FROM_TO
            case _:
                pass
        """

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

    # def describe(self) -> str:
    #     """Helper for testing/logging; returns '<versioning>\n<temporality>'. Do not use in production code."""
    #     return f"{self.versioning[1]}\n{self.temporality[1]}"

    def __str__(self) -> str:
        """Helper; returns '<versioning>_<temporality>'."""
        return f"{self.versioning!s}_{self.temporality!s}"

    @classmethod
    def permutations(cls) -> list[str]:
        """Helper; returns ['<versioning>_<temporality>', ...] ."""
        return ["_".join(c) for c in product(Versioning.keys(), Temporality.keys())]

    def __repr__(self) -> str:
        """Helper, returns code with required parameters to initialise given SeriesType."""
        return f"SeriesType({self.versioning!r},{self.temporality!r})"


def estimate_types() -> list[str]:
    """Helper; returns list of SeriesTypes for which Versioning is not NONE."""
    return ["_".join(c) for c in product(["AS_OF"], Temporality.keys())]
