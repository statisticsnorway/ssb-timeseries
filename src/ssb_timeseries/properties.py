from enum import Enum
from itertools import product


class SuperEnum(Enum):
    @classmethod
    def to_dict(cls):
        """Returns a dictionary representation of the enum."""
        return {e.name: e.value for e in cls}

    @classmethod
    def keys(cls):
        """Returns a list of all the enum keys."""
        # return cls._member_names_
        return [item.name for item in cls]

    @classmethod
    def values(cls):
        """Returns a list of all the enum values."""
        # return list(cls._value2member_map_.keys)
        return [item.value[0] for item in cls]

    @classmethod
    def descriptions(cls):
        """Returns a list of descriptions for all enum values."""
        # return list(cls._value2member_map_.keys())
        return [item.value[1] for item in cls]

    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"

    def __str__(self):
        return self.name


class Versioning(SuperEnum):
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
    NONE = 0
    AT = 1  # (1, "Single points in time expressed with 'valid_at' dates.")
    FROM_TO = (
        2  # (2, "Duration from-to expressed with 'valid_from' and 'valid_to' dates.")
    )


class SeriesType:
    def __init__(
        self,
        versioning: Versioning,
        temporality: Temporality,
        is_sparse: bool = False,
    ) -> None:
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
    def none_at(cls):
        return cls(versioning=Versioning.NONE, temporality=Temporality.AT)

    @classmethod
    def simple(cls):
        return cls.none_at()

    @classmethod
    def from_to(cls):
        return cls(versioning=Versioning.NONE, temporality=Temporality.FROM_TO)

    @classmethod
    def as_of_at(cls):
        return cls(versioning=Versioning.AS_OF, temporality=Temporality.AT)

    @classmethod
    def as_of_from_to(cls):
        return cls(versioning=Versioning.AS_OF, temporality=Temporality.FROM_TO)

    @classmethod
    def estimate(cls):
        return cls.as_of_at()

    def describe(self) -> str:
        return f"{self.versioning[1]}\n{self.temporality[1]}"

    def __str__(self):
        # return f"{self.versioning}_{self.temporality}"
        return f"{self.versioning!s}_{self.temporality!s}"

    @classmethod
    def permutations(cls) -> list[str]:
        return ["_".join(c) for c in product(Versioning.keys(), Temporality.keys())]

    def __repr__(self):
        return f"SeriesType({self.versioning!r},{self.temporality!r})"


def estimate_types() -> list[str]:
    return ["_".join(c) for c in product(["AS_OF"], Temporality.keys())]
