from enum import Enum


class SeriesVersioning(Enum):
    NONE = (1, "Version control only. Versions are not accessible through API.")
    AS_OF = (
        2,
        "Version dates in the data model make versions accessible by way of date arithemetic ('equals' | 'greater than'  | 'smaller than' | 'between').",
    )
    NAMES = (3, "Versions are identified by and accessible through textual names.")


class SeriesTemporality(Enum):
    AT = (1, "Single points in time expressed with 'valid_at' dates.")
    FROM_TO = (2, "Duration from-to expressed with 'valid_from' and 'valid_to' dates.")


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
        return f"{self.versioning}\n{self.temproality}"


def series_type(type_name: str) -> SeriesType:
    return series_type
