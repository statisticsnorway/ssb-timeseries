import pandas as pd
import datetime

from timeseries import dates
from timeseries import io
from timeseries import properties as prop
from timeseries.logging import ts_logger

# import create_sample_data


class Dataset:
    def __init__(
        self,
        name: str,
        datatype: str = "SIMPLE",
        as_of_tz: datetime = dates.now_utc(),
        **kwargs,
    ) -> None:
        self.name: str = name
        set_type: prop.SeriesType = prop.SeriesType(type=datatype)
        self.as_of_utc = dates.date_utc(as_of_tz)
        self.series: dict = kwargs.get("series", {})

        self.io = io.DatasetDirectory(
            set_name=self.name, set_type=set_type, as_of_utc=self.as_of_utc
        )

        kwarg_data: pd.DataFrame = kwargs.get("data", pd.DataFrame())
        if not kwarg_data.empty:
            self.data = kwarg_data
        else:
            ts_logger.debug(
                f"Dataset {self.name}: Init without data. Reading from file."
            )
            self.data = self.io.read_data(init=True)

        default_tags = {
            "name": name,
            "versioning": str(set_type.versioning),
            "temporality": str(set_type.temporality),
        }
        kwarg_tags: dict = kwargs.get("tags", {})
        stored_tags: dict = self.io.read_metadata()

        ts_logger.debug(
            f"DATASET {self.name}: default_tags {default_tags} kwarg_tags {kwarg_tags} stored_tags {stored_tags} "
        )
        self.tags = {**default_tags, **stored_tags, **kwarg_tags}

        if not self.tags:
            ts_logger.warning(f"DATASET {self.name}: tags = {self.tags}.")

    def save(self) -> None:
        ts_logger.debug(f"DATASET {self.name}: saving tags: \n{self.tags}")
        self.io.save(data=self.data, meta=self.tags)

    def get_series_tags(self, series_name=None):
        return self.tags[series_name]

    def list_series(self):
        return self.data.columns

    def __eq__(self, other) -> bool:
        return (self.name, self.data) == (other.name, other.data)

    def __repr__(self):
        # return f"DATASET: '{self.name}' TYPE: {set_type} | TAGS | {self.tags} SERIES: {self.data.columns}"
        return str(
            {
                "name": self.name,
                "tags": self.tags,
                "data": self.data.size,
            }
        )

    def __str__(self) -> str:
        return str(
            {
                "name": self.name,
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
