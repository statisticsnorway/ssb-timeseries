# from typing import Self
# import datetime
import pandas as pd

from timeseries import io
from timeseries import properties as prop
from timeseries import logging as log

# import create_sample_data


class Dataset:
    def __init__(self, name: str, datatype: str = "SIMPLE", **kwargs) -> None:
        self.name: str = name
        self.type: prop.SeriesType = prop.SeriesType(type=datatype)
        self.series: dict = kwargs.get("series", {})
        self.io = io.DatasetDirectory(set_name=self.name, set_type=self.type)

        """  included in type
        match type.versioning:
            case prop.SeriesVersioning.AS_OF:
                self.version = kwargs.get("as_of")
            case prop.SeriesVersioning.NAME:
                self.version = kwargs.get("version")
            case _:
                self.version = None"""

        kwarg_data: pd.DataFrame = kwargs.get("data", pd.DataFrame())
        if not kwarg_data.empty:
            self.data = kwarg_data
        else:
            log.debug(f"Dataset {self.name}: Init without data. Reading from file.")
            self.data = self.io.read_data(init=True)

        kwarg_tags: dict = kwargs.get("tags", {})
        if kwarg_tags:
            self.tags = kwarg_tags
        else:
            self.tags = self.io.read_metadata()

        log.debug(self.data)
        log.debug(self.tags)

    def save(self) -> None:
        log.info(f"dataset {self.name}: save")
        self.io.save(data=self.data, meta=self.tags)

    # def update_metadata(self, column_name, metadata_tag) -> None:
    #    self.tags[column_name] = metadata_tag

    def get_series_tags(self, series_name=None):
        return self.tags.get(series_name, "No metadata found")

    def list_series(self):
        return self.data.columns

    def __eq__(self, other) -> bool:
        return (self.name, self.data) == (other.name, other.data)

    def __repr__(self):
        return {"name": self.name, "tags": self.tags, "data": self.data.size}

    def __str__(self) -> str:
        return str(
            {
                "name": self.name,
                "series": self.data.columns,
                "tags": self.tags,
                "data": self.data.size,
            }
        )


"""
# not needed?
class Series:
    def __init__(self, name: str):
        self.name = name
        self.type: str
        self.tags = {}
        self.index: int
        self.datafile: str

    def __eq__(self, other) -> bool:
        return (self.name, self.data) == (other.name, other.data)

    def __repr__(self):
        return {"name": self.name, "meta": self.tags, "data": self.data}

    def __str__(self) -> str:
        return str({"name": self.name, "meta": self.tags, "data": self.data})
"""
