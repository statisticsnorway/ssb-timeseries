from enum import Enum

# from typing import Self
# from timeseries import io
# import create_sample_data

from timeseries import logging as log


# import json
class Versions(Enum):
    SIMPLE = (
        "Simple series. Although edits are tracked, only the last version is visible."
    )
    AS_OF = "Date versioned series. Any version can easily be retrieved."


class Valid(Enum):
    AT = "Series values are valid AT a single point in time."
    FROM_TO = "Series values are valid FROM a point in time TO another."


class MetaTemplate:
    def __init__(self, name: str, versioning: Versions, vaild: Valid, **kwargs):
        self.name: str = name
        self.type: str = type
        self.tags: dict = kwargs.get("tags", {})
        self.items = kwargs.get("items", [])

    def __eq__(self, other) -> bool:
        return (self.name, self.type) == (other.name, other.type)


class SimpleSeriesSet(MetaTemplate):
    def __init__(self, name: str, **kwargs):
        self.name: str = name
        self.type: str = "simple series"
