import pandas as pd
import numpy as np
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
        data_type: prop.SeriesType,
        as_of_tz: datetime = None,
        load_data: bool = True,
        **kwargs,
    ) -> None:
        """
        Load existing dataset or create a new one of specified type.
        The type defines
         * versioning (NONE, AS_OF, NAMED) and
         * temporality (Valid AT point in time, or FROM and TO for duration).
        If data_type versioning is specified as AS_OF, a datetime with timezone should be provided.
        If no AS_OF-date is provided, but data is passed, AS_OF defaults to current time.
        When loading existing sets, load_data = false can be set in order to suppress reading large amounts of data.
        For data_types with AS_OF versioning, not providing the AS_OF date will have the same effect.

        Metadata will always be read.
        Data is represented as pandas (or polars?) dataframes.
        Data is stored to parquet files (or database?).
        Support for addittional "type" features/flags behaviours like sparse data may be added later.

        Files are created / data is stored to database on .save, but not before.
        """
        self.name: str = name
        self.data_type = data_type
        self.as_of_utc = dates.date_utc(as_of_tz)
        self.series: dict = kwargs.get("series", {})

        self.io = io.DatasetDirectory(
            set_name=self.name, set_type=self.data_type, as_of_utc=self.as_of_utc
        )

        # metadata: defaults overwritten by stored is overwritten kwargs
        default_tags = {
            "name": name,
            "versioning": str(self.data_type.versioning),
            "temporality": str(self.data_type.temporality),
        }
        stored_tags: dict = self.io.read_metadata()
        kwarg_tags: dict = kwargs.get("tags", {})
        self.tags = {**default_tags, **stored_tags, **kwarg_tags}

        # ts_logger.debug(f"DATASET {self.name}: .......... coalesce:\n\tdefault_tags {default_tags}\n\tkwarg_tags {kwarg_tags}\n\tstored_tags {stored_tags}\n\t--> {self.tags} "        )

        # data scenarios:
        #   - IF versioning = NONE
        #      ... simple, just load everything
        #   - IF versioning = AS_OF
        #      ... if as_of_tz is provided, load that, otherwise the latest
        # c) load_data = False and versioning = AS_OF -->
        #      ... if as_of_tz is provided, use it, otherwise set to utc_now()

        # THEN, if kwarg_data, append/merge
        #   if as_of_tz is not provided, set set to utc_now()

        if load_data and self.data_type.versioning == prop.Versioning.NONE:
            ts_logger.debug(f"Dataset {self.name}: Reading from file.")
            self.data = self.io.read_data()
        elif load_data and self.data_type.versioning == prop.Versioning.AS_OF:
            self.data = self.io.read_data(self.as_of_utc)
        else:
            self.data = pd.DataFrame()
        ts_logger.debug(self.data)

        kwarg_data: pd.DataFrame = kwargs.get("data", pd.DataFrame())
        if not kwarg_data.empty:
            self.data = kwarg_data
            ts_logger.info(
                f"DATAWSET {self.name}: Merged {kwarg_data.size} datapoints:\n{self.data}"
            )

    def save(self, as_of_tz: datetime = None) -> None:
        """Persist the Dataset.

        Args:
            as_of_tz (datetime, optional): Provide a timezone sensitive as_of date in order to create another version. The default is None, which will save with Dataset.as_of._utc (utc dates under the hood).
        """
        if as_of_tz is not None:
            self.as_of_utc = dates.date_utc(as_of_tz)

        self.io = io.DatasetDirectory(self.name, self.data_type, self.as_of_utc)
        ts_logger.debug(f"DATASET {self.name}: SAVE. Tags:\n\t{self.tags}.")
        if not self.tags:
            ts_logger.warning(
                f"DATASET {self.name}: attempt to save empty tags = {self.tags}."
            )
        self.io.save(meta=self.tags, data=self.data)

    def series(self):
        return self.data.columns

    def series_tags(self, series_name=None):
        return self.tags[series_name]

    def plot(self, *args, **kwargs):
        return self.data.plot(
            "valid_at",
            legend=len(x.data.columns) < 9,
            title=x.name,
            figsize=(12, 4),
            *args,
            **kwargs,
        )

    # @property
    def series_names(self):
        return self.data.select_dtypes(include=np.number).columns

    def _perform_operation(self, other, operation_func):
        if isinstance(other, Dataset):
            common_columns = set(self.data.columns) & set(other.data.columns)
            result_data = self.data.copy()

            for col in common_columns:
                if pd.api.types.is_numeric_dtype(
                    self.data[col]
                ) and pd.api.types.is_numeric_dtype(other.data[col]):
                    result_data[col] = operation_func(result_data[col], other.data[col])

            return result_data
        elif isinstance(other, (int, float)):
            numeric_columns = self.series_names()
            result_data = self.data.copy()

            for col in numeric_columns:
                result_data[col] = operation_func(result_data[col], other)

            return result_data
        elif isinstance(other, np.ndarray):
            if other.ndim == 1 and (
                other.shape[0] == len(self.data)
                or other.shape[0] == len(self.data.columns)
            ):
                result_data = self.data.copy()

                for col in self.series_names():
                    result_data[col] = operation_func(result_data[col], other)

                return result_data
            else:
                raise ValueError(
                    f"Incompatible shapes for element-wise {operation_func.__name__}"
                )
        else:
            raise ValueError("Unsupported operand type")

    def __add__(self, other):
        return self._perform_operation(other, np.add)

    def __sub__(self, other):
        return self._perform_operation(other, np.subtract)

    def __mul__(self, other):
        return self._perform_operation(other, np.multiply)

    def __truediv__(self, other):
        return self._perform_operation(other, np.divide)

    def __eq__(self, other):
        # return self._perform_comparison(other, np.equal)
        return self._perform_operation(other, np.equal)

    def __gt__(self, other):
        # return self._perform_comparison(other, np.greater)
        return self._perform_operation(other, np.greater)

    def __lt__(self, other):
        # return self._perform_comparison(other, np.less)
        return self._perform_operation(other, np.less)

    def __identity__(self, other) -> bool:
        check_defs = (self.name, self.data_type, self.tags) == (
            other.name,
            other.data_type,
            other.tags,
        )
        check_data = self.__eq__(other)

        return all(check_defs) and check_data.all()

    def __repr__(self) -> str:
        # return f"DATASET: '{self.name}' TYPE: {set_type} | TAGS | {self.tags} SERIES: {self.data.columns}"
        return f"Dataset(name = {self.name}, data_type = {self.data_type}, as_of_utc = {self.as_of_utc.isoformat()} )"

    def __str__(self) -> str:
        return str(
            {
                "name": self.name,
                "data_type": str(self.data_type),
                "as_of_utc": self.as_of_utc,
                "series": str(self.series),
                "data": self.data.size,
            }
        )


"""
# not needed?
class Series:
    def __init__(self, name: str):
        self.name = name
        self.data_type: str
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
