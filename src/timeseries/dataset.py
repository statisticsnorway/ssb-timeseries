# from typing import Optional, Dict
# from dataclasses import dataclass, field

import pandas as pd
import numpy as np
import datetime

from timeseries import dates
from timeseries import io
from timeseries import properties as prop
from timeseries.logging import ts_logger


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
         * versioning (NONE, AS_OF, NAMED)
         * temporality (Valid AT point in time, or FROM and TO for duration)
         * value (for now only scalars)

        If data_type versioning is specified as AS_OF, a datetime *with timezone* should be provided.
        If it is not, but data is passed, AS_OF defaults to current time. Providing an AS_OF date has no effect if versioning is NONE.

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
        # self.series: dict = kwargs.get("series", {})

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
            self.data = self.io.read_data()
        elif load_data and self.data_type.versioning == prop.Versioning.AS_OF:
            self.data = self.io.read_data(self.as_of_utc)
        else:
            self.data = pd.DataFrame()

        kwarg_data: pd.DataFrame = kwargs.get("data", pd.DataFrame())
        if not kwarg_data.empty:
            # TO DO: if kwarg_data overlap data from file, overwrite with kwarg_data
            # ... for all versioning types? Think it through!
            self.data = kwarg_data
            # self.data.set_index(self.datetime_columns())
            # will not work for valid_from_to
            # self.data.set_index(self.datetime_columns()).to_period()
            ts_logger.debug(
                f"DATASET {self.name}: Merged {kwarg_data.size} datapoints:\n{self.data}"
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

    def publish(self, as_of_tz: datetime = None) -> None:
        """Copy data snapshot to immutable processing stage bucket and shared buckets.

        Args:
            as_of_tz (datetime, optional): Provide a timezone sensitive as_of date in order to create another version. The default is None, which will save with Dataset.as_of._utc (utc dates under the hood).
        """
        self.save(as_of_tz=as_of_tz)
        self.io.publish()

    def series(self):
        return self.data.columns

    def series_tags(self, series_name=None):
        return self.tags[series_name]

    def search(self, pattern: str = "*", *args, **kwargs):
        return self.io.search(pattern=pattern, *args, **kwargs)

    def filter(self, pattern: str = "", tags: dict = {}, regex: str = "", **kwargs):
        """Filter dataset.data by textual pattern, regex or metadata tag dictionary. Or a combination.

        Args:
            pattern (str, optional): Text pattern for search 'like' in column names. Defaults to ''.
            regex (str, optional): Expression for regex search in column names. Defaults to ''.
            tags (dict, optional): Dictionary with tags to search for. Defaults to {}.
            **kwargs: if provided, goes into the init of a new Dataset.

        Returns: A new Dataset if kwargs are provided to initialise it, otherwise, a dataframe.
        """

        df = self.data
        if regex:
            df = df.filter(regex=regex)

        if pattern:
            df = df.filter(like=pattern)

        if tags:
            # TO DO: handle meta dict
            pass

        df = pd.concat([self.data[self.datetime_columns()], df], axis=1)
        # TO DO: add interval parameter to filter on datetime? Similar to:
        # if interval:
        #    df = df[interval, :]

        if kwargs:
            # or just some kind of self.copy?
            out = Dataset(**kwargs)
            out.data = df
        else:
            out = df

        return out

        # TBD: the most natural behaviour is probably not to apply filter to self.data,
        # but to return a (new) Dataset or DataFrame. Which one should it be?

    def __getitem__(self, key):
        # pattern: str = "", regex: str = "", tags: dict = {}):
        """Access Dataset.data.columns via Dataset[ list[column_names] | regex | tags].

        Args:
            regex (str, optional): Expression for regex search in column names. Defaults to ''.
            tags (dict, optional): Dictionary with tags to search for. Defaults to {}.
        """

        # Dataset[...] should return a Dataset object (?) with only the requested items (columns).
        # but should not mutate the original object, ie "self",
        # so that if x is a Dataset and x[a] a columnwise subset of x
        # x[a] *= 100 should update x[a] "inside" the original x, without "setting" x to z[a]
        # that later references to x should return the entire x, not only x[a].
        # Is this possible, or do we need to return a copy?
        # (Then the original x is not affected by updates to x[a])?
        # Or, is there a trick using dataframe views?
        # --->
        if isinstance(key, str):
            return self.filter(
                pattern=key,
                # name=self.name,
                # data_type=self.data_type,
                # as_of_tz=self.as_of_utc,
            )
        elif isinstance(key, dict):
            return self.filter(
                tags=key,
                # name=self.name,
                # data_type=self.data_type,
                # as_of_tz=self.as_of_utc,
            )
        # regex=regex, tags=tags)

    def plot(self, *args, **kwargs):
        return self.data.plot(
            "valid_at",
            legend=len(self.data.columns) < 9,
            title=self.name,
            figsize=(12, 4),
            *args,
            **kwargs,
        )

    def vectors(self, filter: str = ""):
        """_summary_

        Args:
            filter (str, optional): _description_. Defaults to "".

        be warned: messing with variables by way of stack inspection is a dirty trick
        this runs the risk of reassigning objects, functions, or variables within the scope of the calling function
        """

        import inspect

        stack = inspect.stack()
        locals_ = stack[1][0].f_locals

        for col in self.data.columns:
            if col.__contains__(filter):
                cmd = f"{col} = self.data['{col}']"
                ts_logger.debug(cmd)
                # the original idea was running (in caller scope)
                # exec(cmd)
                locals_[col] = self.data[col]

    def groupby(self, freq: str, func: str = "auto", *args, **kwargs):
        datetime_columns = list(
            set(self.data.columns) & {"valid_at", "valid_to", "valid_from"}
        )
        datetime_columns = "valid_at"
        ts_logger.debug(f"DATASET {self.name}: datetime columns: {datetime_columns}.")

        period_index = pd.PeriodIndex(self.data[datetime_columns], freq=freq)
        ts_logger.debug(f"DATASET {self.name}: period index\n{period_index}.")

        match func:
            case "mean":
                out = self.data.groupby(period_index).mean(
                    numeric_only=True, *args, **kwargs
                )
            case "sum":
                out = self.data.groupby(period_index).sum(
                    numeric_only=True, *args, **kwargs
                )
            case "auto":
                # TO DO: QA on exact logic / use "real" metadata
                # in particular, how to check meta data and blend d1 and df2 values as appropriate
                # (this implementation is just to show how it can be done)
                # QUESTION: do we need a default for "other" series / what should it be?
                df1 = self.data.groupby(period_index).mean(
                    numeric_only=True, *args, **kwargs
                )
                ts_logger.debug(f"groupby\n{df1}.")

                df2 = (
                    self.data.groupby(period_index)
                    .sum(numeric_only=True, *args, **kwargs)
                    .filter(regex="mendgde|volum|vekt")
                )
                ts_logger.warning(f"groupby\n{df2}.")

                df1[df2.columns] = df2[df2.columns]

                out = df1
                ts_logger.warning(f"groupby\n{out}.")
                ts_logger.warning(f"DATASET {self.name}: groupby\n{out}.")

        return out

    def resample(self, freq: str, func: str = "auto", *args, **kwargs):
        # TO DO
        # monthly_max = x.resample("M").max()
        pass

    def _numeric_columns(self):
        return self.data.select_dtypes(include=np.number).columns

    def datetime_columns(self, *comparisons):
        """
        Arguments:
            *comparisons (optional) Objects to compare with.

        Returns: The (common) datetime column names of self (and comparisons) as a list of strings.
        """
        intersect = set(self.data.columns) & {"valid_at", "valid_from", "valid_to"}
        for c in comparisons:
            if isinstance(c, Dataset):
                intersect = set(c.data.columns) & intersect
            elif isinstance(c, pd.DataFrame):
                intersect = set(c.columns) & intersect

        return list(intersect)

    def math(self, other, func):
        """
        Generic helper making math functions work on numeric, non date columns of dataframe to dataframe, matrix to matrix, matrix to vector and matrix to scalar.

        Although the purpose was to limit "boilerplate" for core linear algebra functions, it also extend to other operations that follow the same differentiation pattern.

        Args:
            other (dataframe | series | matrix | vector | scalar ): One (or more?) pandas (polars to come) datframe or series, numpy matrix or vector or a scalar value.
            func (_type_): The function to be applied as `self.func(**other)` or (in some cases) with infix notation `self f other`. Note that one or more date columns of the self / lefthand side argument are preserved, ie data shifting operations are not supported.

        Raises:
            ValueError: "Unsupported operand type"
            ValueError: "Incompatible shapes."

        Returns:
            _type_: Pandas (or polars?) dataframe.
        """
        if isinstance(other, Dataset):
            ts_logger.debug(
                f"DATASET {self.name}: .math({self.name}.{func.__name__}(Dataset({other.name}))."
            )
            ts_logger.debug(
                f"DATASET {self.name}: .math({self.name},{other.name}) redirect to operating on {other.name}.data."
            )
            result_data = self.math(other.data, func)

            # return result_data
        elif isinstance(other, pd.DataFrame):
            # element-wise matrix operation
            ts_logger.debug(
                f"DATASET {self.name}: .math({self.name}.{func.__name__}(pd.dataframe)."
            )

            datetime_columns = self.datetime_columns(other)
            if not datetime_columns:
                raise ValueError("No common datetime column found.")

            # Exclude datetime columns from both DataFrames
            df1_values = self.data.drop(columns=datetime_columns)
            df2_values = other.drop(columns=datetime_columns)

            result_values = func(df1_values, df2_values)

            result_data = pd.concat(
                [self.data[list(datetime_columns)], result_values], axis=1
            )

        elif isinstance(other, (int, float)):
            numeric_columns = self._numeric_columns()
            result_data = self.data.copy()

            for col in numeric_columns:
                result_data[col] = func(result_data[col], other)

            # datetime_columns = self.datetime_columns(other)
            # self.data[:, ~list(datetime_columns)] = func(
            #     self.data[:, ~list(datetime_columns)], other
            # )

            # return result_data
        elif isinstance(other, np.ndarray):
            # Compare shape of the ndarray against the numeric_columns of self.data. There are up to 3 accepted cases (depending on the operation):
            #  * matrix;         shape = (data.numeric.rows, data.numeric.columns)
            #  * column vector;  shape = (data.numeric.rows, 1)
            #  * row vector;     shape = (1, data.numeric.columns)
            #
            if other.ndim == 1 and (
                other.shape[0] == len(self.data)
                or other.shape[0] == len(self.data.columns)
            ):
                result_data = self.data.copy()

                for col in self._numeric_columns():
                    result_data[col] = func(result_data[col], other)

                # return result_data
            else:
                raise ValueError(
                    f"Incompatible shapes for element-wise {func.__name__}"
                )
        else:
            raise ValueError("Unsupported operand type")

        # TO DO: CONSIDER returning a (new) Dataset object instead?
        return result_data
        # out = Dataset(name=self.name, data_type=self.data_type, as_of_tz=self.as_of_utc)
        # out.data = result_data
        # return out

    def __add__(self, other):
        return self.math(other, np.add)

    def __sub__(self, other):
        return self.math(other, np.subtract)

    def __mul__(self, other):
        return self.math(other, np.multiply)

    def __truediv__(self, other):
        return self.math(other, np.divide)

    def __eq__(self, other):
        return self.math(other, np.equal)

    def __gt__(self, other):
        # return self._perform_comparison(other, np.greater)
        return self.math(other, np.greater)

    def __lt__(self, other):
        # return self._perform_comparison(other, np.less)
        return self.math(other, np.less)

    def identical(self, other) -> bool:
        # check_data = self.__eq__(other)

        # return all(check_defs) and check_data.all()
        return self.__dict__ == other.__dict__

    def __repr__(self) -> str:
        return f'Dataset(name="{self.name}", data_type={repr(self.data_type)}, as_of_tz="{self.as_of_utc.isoformat()}")'

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
