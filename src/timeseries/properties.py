from enum import Enum

"""
class Versions(Enum):
    SIMPLE = (
        "Simple series. Although edits are tracked, only the last version is visible."
    )
    AS_OF = "Date versioned series. Any version can easily be retrieved."


class Valid(Enum):
    AT = "Series values are valid AT a single point in time."
    FROM_TO = "Series values are valid FROM a point in time TO another."
"""


class SeriesVersioning(Enum):
    NONE = (1, "Version control only. Versions are not accessible through API.")
    AS_OF = (
        2,
        "Version dates in the data model make versions accessible by way of date arithemetic ('equals' | 'greater than'  | 'smaller than' | 'between').",
    )
    NAMES = (3, "Versions are identified by and accessible through textual names.")
    SEMANTIC = (4, "Version number on form X.Y.Z Major.Minor.Patch.")

    def __str__(self):
        return self.name


class SeriesTemporality(Enum):
    AT = (1, "Single points in time expressed with 'valid_at' dates.")
    FROM_TO = (2, "Duration from-to expressed with 'valid_from' and 'valid_to' dates.")

    def __str__(self):
        return self.name


class SeriesType:
    def __init__(
        self,
        type: str = "",
        versioning: SeriesVersioning = None,
        temporality: SeriesTemporality = None,
    ) -> None:
        self.versioning = versioning
        self.temporality = temporality

        match type.lower():
            case "simple":
                self.versioning = SeriesVersioning.NONE
                self.temporality = SeriesTemporality.AT
            case "from_to":
                self.versioning = SeriesVersioning.NONE
                self.temporality = SeriesTemporality.FROM_TO
            case "estimate":
                self.versioning = SeriesVersioning.AS_OF
                self.temporality = SeriesTemporality.AT
            case "as_of_from_to":
                self.versioning = SeriesVersioning.AS_OF
                self.temporality = SeriesTemporality.FROM_TO
            case _:
                pass

        match versioning:
            case None:
                pass
            case _:
                self.versioning = versioning

        match temporality:
            case None:
                pass
            case _:
                self.temporality = temporality

    def describe(self) -> str:
        return f"{self.versioning[1]}\n{self.temporality[1]}"

    def __str__(self):
        # return f"{self.versioning}_{self.temporality}"
        return f"{str(self.versioning)}_{str(self.temporality)}"


def series_type(type_name: str) -> SeriesType:
    return series_type
