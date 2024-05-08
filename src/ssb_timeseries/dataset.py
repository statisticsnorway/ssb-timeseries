# mypy: disable-error-code="assignment,attr-defined"
# ruff: noqa: RUF013
from copy import deepcopy
from datetime import datetime
from typing import Any
from typing import no_type_check

import numpy as np
import pandas as pd
from typing_extensions import Self

from ssb_timeseries import io
from ssb_timeseries import properties
from ssb_timeseries.dates import date_utc  # type: ignore[attr-defined]
from ssb_timeseries.dates import utc_iso  # type: ignore[attr-defined]
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.meta import Taxonomy
from ssb_timeseries.types import F
from ssb_timeseries.types import PathStr


class Dataset:
    """Datasets are the core unit of analysis for workflow and data storage.

    A dataset is a logical collection of data and metadata stemming from the same process origin. Series in a dataset must be
    """

    def __init__(
        self,
        name: str,
        data_type: properties.SeriesType = None,
        as_of_tz: datetime = None,
        load_data: bool = True,
        **kwargs: Any,
    ) -> None:
        """Load existing dataset or create a new one of specified type.

        The type defines
         * versioning (NONE, AS_OF, NAMED)
         * temporality (Valid AT point in time, or FROM and TO for duration)
         * value (for now only scalars)

        If data_type versioning is specified as AS_OF, a datetime *with timezone* should be provided.
        If it is not, but data is passed, AS_OF defaults to current time. Providing an AS_OF date has no effect if versioning is NONE.

        When loading existing sets, load_data = false can be set in order to suppress reading large amounts of data.
        For data_types with AS_OF versioning, not providing the AS_OF date will have the same effect.

        Metadata will always be read.
        Data is represented as Pandas dataframes; but Polars lazyframes or Pyarrow tables is likely to be better.
        Initial implementation assumes stores data in parquet files, but feather files and various database options are considered for later.

        Support for addittional "type" features/flags behaviours like sparse data may be added later (if needed).

        Data is kept in memory and not stored before explicit call to .save.
        """
        self.name: str = name
        if data_type:  # self.exists():
            self.data_type = data_type
        else:
            # TODO: if the datatype is not provided, search by name,
            # throw error if a) no set is found or b) multiple sets are found
            # ... till then, just continue
            look_for_it = search(name)
            if isinstance(look_for_it, Dataset):
                self.data_type = look_for_it.data_type
            else:
                raise ValueError(
                    f"Dataset {name} not found. Specify data_type to initialise a new set."
                )
                # TODO: for versioned series, return latest if no as_of_tz is provided
        self.as_of_utc = date_utc(as_of_tz)

        self.io = io.FileSystem(
            set_name=self.name, set_type=self.data_type, as_of_utc=self.as_of_utc
        )

        # metadata: defaults overwritten by stored overwritten by kwargs
        default_tags = {
            "name": name,
            "versioning": str(self.data_type.versioning),
            "temporality": str(self.data_type.temporality),
            "series": {},
        }
        stored_tags = self.io.read_metadata()
        kwarg_tags = kwargs.get("tags", {})

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

        if load_data and self.data_type.versioning == properties.Versioning.NONE:
            self.data = self.io.read_data()
        elif load_data and self.data_type.versioning == properties.Versioning.AS_OF:
            self.data = self.io.read_data(self.as_of_utc)
        else:
            self.data = pd.DataFrame()

        kwarg_data: pd.DataFrame = kwargs.get("data", pd.DataFrame())
        if not kwarg_data.empty:
            # TODO: if kwarg_data overlap data from file, overwrite with kwarg_data
            # ... for all versioning types? Think it through!
            self.data = kwarg_data
            # self.data.set_index(self.datetime_columns())
            # will not work for valid_from_to
            # self.data.set_index(self.datetime_columns()).to_period()
            ts_logger.debug(
                f"DATASET {self.name}: Merged {kwarg_data.size} datapoints:\n{self.data}"
            )
            # TODO: update series tags
            # tags may come in through `stored_tags['series]` with additional ones in parameter `series_tags`
            # this will add new tags or overwrite existing ones
            # (to delete or append values to an existing tag will need explicit actions)

            set_only_tags = ["series", "name"]
            # inherit_from_set_tags = {key, d[key] for key in self.tags.items() if key not in set_only_tags}
            inherit_from_set_tags = {"dataset": self.name, **self.tags}
            [inherit_from_set_tags.pop(key) for key in set_only_tags]

            ts_logger.debug(
                f"DATASET {self.name}: .tags:\n\t{self.tags}\n\tinherited: {inherit_from_set_tags} "
            )
            # TODO: apply tags provided in parameter series_tags
            # ... or just be content with the autotag?
            # kwarg_series_tags = kwargs.get("series_tags", {})

            self.tags["series"] = {
                n: {"name": n, "dataset": self.name, **inherit_from_set_tags}
                for n in self.numeric_columns()
            }

            name_pattern = kwargs.get("name_pattern", "")
            if name_pattern:
                for s in self.tags["series"]:
                    name_parts = s.split("_")
                    # [self.tags['series'][s][attribute] = value for attribute,  value in zip(name_pattern, name_parts)]
                    for attribute, value in zip(name_pattern, name_parts, strict=False):
                        self.tags["series"][s][attribute] = value

                    # [self.tags.series[k] = for k, a in zip(self.numeric_columns(), name_pattern]

        self.product: str = kwargs.get("product", "")
        self.process_stage: str = kwargs.get("process_stage", "")
        self.sharing: dict[str, str] = kwargs.get("sharing", {})

    def copy(self, new_name: str, **kwargs: Any) -> Self:
        """Create a copy of the Dataset.

        The copy need to get a new name, but unless other information is spcecified, it will be create wiht the same data_type, as_of_tz, data, and tags.
        """
        out = deepcopy(self)
        for k, v in kwargs.items():
            setattr(out, k, v)
            ts_logger.debug(f"DATASET.copy() attribute: {k}:\n {v}.")

        out.rename(new_name)
        return out

    def rename(self, new_name: str) -> None:
        """Rename the Dataset.

        For use by .copy, and on very rare other occasions. Does not move or rename any previously stored data.

        TODO: Fix that?
        """
        self.name = new_name

        self.tags["dataset"] = new_name
        for _, v in self.tags["series"].items():
            v["dataset"] = new_name

    def save(self, as_of_tz: datetime = None) -> None:
        """Persist the Dataset.

        Args:
            as_of_tz (datetime): Provide a timezone sensitive as_of date in order to create another version. The default is None, which will save with Dataset.as_of._utc (utc dates under the hood).
        """
        if as_of_tz is not None:
            self.as_of_utc = date_utc(as_of_tz)

        self.io = io.FileSystem(self.name, self.data_type, self.as_of_utc)
        ts_logger.debug(f"DATASET {self.name}: SAVE. Tags:\n\t{self.tags}.")
        if not self.tags:
            ts_logger.warning(
                f"DATASET {self.name}: attempt to save empty tags = {self.tags}."
            )
        self.io.save(meta=self.tags, data=self.data)

    def snapshot(self, as_of_tz: datetime = None) -> None:
        """Copy data snapshot to immutable processing stage bucket and shared buckets.

        Args:
            as_of_tz (datetime): Optional. Provide a timezone sensitive as_of date in order to create another version. The default is None, which will save with Dataset.as_of_utc (utc dates under the hood).
        """
        # def snapshot_name(self) -> str:
        #     # <kort-beskrivelse>_p<periode-fra-og-med>_p<perode-til-og- med>_v<versjon>.<filtype>
        #     date_from = np.min(self.data[self.datetime_columns()])
        #     date_to = np.max(self.data[self.datetime_columns()])
        #     version = self.io.last_version + 1
        #     out = f"{self.name}_p{date_from}_p{date_to}_v{version}.parquet"
        #     return out

        date_from = self.data[self.datetime_columns()].min().min()
        date_to = self.data[self.datetime_columns()].max().max()
        ts_logger.debug(
            f"DATASET {self.name}: Data {utc_iso(date_from)} - {utc_iso(date_to)}:\n{self.data.head()}\n...\n{self.data.tail()}"
        )

        self.save(as_of_tz=self.as_of_utc)

        self.io.snapshot(
            product=self.product,
            process_stage=self.process_stage,
            sharing=self.sharing,
            period_from=date_from,
            period_to=date_to,
        )

    def series(self, what: str = "names") -> Any:
        """Get series names (default) or tags."""
        if what.lower() == "names":
            return self.data.columns
        elif what.lower() == "tags":
            return self.tags["series"]
        else:
            raise ValueError(f"Unrecognised return type {what}")

    def series_tags(self, series_name: str | list[str] = "") -> Any:  # remove this?
        """Get series tags."""
        if series_name:
            return self.tags["series"][series_name]
        else:
            return self.tags["series"]

    def tag_set(self, tags: dict[str, str] = None, **kwargs: str | list[str]) -> None:
        """Tag the set.

        Tags may be provided as dictionary of tags, or as kwargs.

        In both cases they take the form of attribute-value pairs.

        Attribute (str): Attribute identifier.
        Ideally attributes relies on KLASS, ie a KLASS taxonomy defines the possible attribute values.

        Value (str): Element identifier, unique within the taxonomy. Ideally KLASS code.
        """
        if tags:
            raise NotImplementedError("Not (yet) implemented. Planned for later.")
        elif kwargs:
            raise NotImplementedError("Not (yet) implemented. Planned for later.")
            # if value not in self[attribute]:
            # self[attribute].append(value)
        else:
            raise ValueError("Must provide either tags or kwargs.")

    def tag_series(
        self, identifiers: str | list[str], tags: dict[str, str] = None, **kwargs: str
    ) -> None:
        """Tag the series.

        Tags may be provided as dictionary of tags, or as kwargs.

        In both cases they take the form of attribute-value pairs.

        Attribute (str): Attribute identifier.
        Ideally attributes relies on KLASS, ie a KLASS taxonomy defines the possible attribute values.

        Value (str): Element identifier, unique within the taxonomy. Ideally KLASS code.
        """
        if tags:
            raise NotImplementedError("Not (yet) implemented. Planned for later.")
        elif kwargs:
            raise NotImplementedError("Not (yet) implemented. Planned for later.")
            # if value not in self[attribute]:
            # self[attribute].append(value)
        else:
            raise ValueError("Must provide either tags or kwargs.")
        # should handle different datatypes for "item" :
        # name, int index, List of name or index
        # if value not in self.series[item][attribute]:
        #    self.series[attribute].append(value)

    @no_type_check
    def filter(
        self,
        pattern: str = "",
        tags: dict[Any, Any] = None,
        regex: str = "",
        output: str = "dataset",
        new_name: str = "",
        **kwargs: str | list[str],
    ) -> pd.DataFrame | Self:
        """Filter dataset.data by textual pattern, regex or metadata tag dictionary.

        Or a combination.

        Args:
            pattern (str): Text pattern for search 'like' in column names. Defaults to ''.
            regex (str): Expression for regex search in column names. Defaults to ''.
            tags (dict): Dictionary with tags to search for. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
            output (str): Output type - dataset or dataframe.(df). Defaults to 'dataset'. Short forms 'df' or 'ds' are accepted.
            new_name (str): Name of new Dataset. If not provided, a new name is generated.
            **kwargs: if provided, goes into the init of the new set.

        Returns:
            Dataset | Dataframe:
            By default a new Dataset (a deep copy of self). If output="dataframe" or "df", a dataframe.
            TODO: Explore shallow copy / nocopy options.
        """
        if regex:
            df = self.data.filter(regex=regex).copy(deep=True)
            matching_series = df.columns

        if pattern:
            df = self.data.filter(like=pattern).copy(deep=True)
            matching_series = df.columns

        if tags:
            series_tags = self.series_tags()
            # ts_logger.debug(f"DATASET.filter()\ntags to find:\n\t{tags}\ntags in series:\n\t{series_tags}")
            matching_series = [
                name
                for name, s_tags in series_tags.items()
                if all(s_tags[k] in v for k, v in tags.items())
            ]
            ts_logger.debug(f"DATASET.filter(tags) matched series:\n{matching_series} ")
            df = self.data[matching_series].copy(deep=True)

        df = pd.concat([self.data[self.datetime_columns()], df], axis=1)

        # TODO: add interval parameter to filter on datetime? Similar to:
        # if interval:
        #    df = df[interval, :]
        # ... or is it better to do this in another function?

        match output:
            case "dataframe" | "df":
                out = df
            case "dataset" | "ds" | _:
                if not new_name:
                    new_name = f"COPY of({self.name} FILTERED by pattern: {pattern}, regex: {regex} tags: {tags})"
                out = self.copy(new_name=new_name, data=df, **kwargs)
                matching_series_tags = {
                    k: v for k, v in out.tags["series"].items() if k in matching_series
                }
                out.tags["series"] = matching_series_tags
        return out

    @no_type_check
    def __getitem__(
        self, criteria: str | dict[str, str] = "", **kwargs: Any
    ) -> Self | None:
        """Access Dataset.data.columns via Dataset[ list[column_names] | pattern | tags].

        Arguments:
            criteria:  Either a string pattern or a dict of tags.
            kwargs: If criteria is empty, this is passed to filter().

        Returns:
            Self | None

        Raises:
            TypeError: If filter() returns another type than Dataset.
        """
        # pattern: str = "", regex: str = "", tags: dict = {}):
        # Dataset[...] should return a Dataset object (?) with only the requested items (columns).
        # but should not mutate the original object, ie "self",
        # so that if x is a Dataset and x[a] a columnwise subset of x
        # x[a] *= 100 should update x[a] "inside" the original x, without "setting" x to z[a]
        # that later references to x should return the entire x, not only x[a].
        # Is this possible, or do we need to return a copy?
        # (Then the original x is not affected by updates to x[a])?
        # Or, is there a trick using dataframe views?
        # --->
        if criteria and isinstance(criteria, str):
            result = self.filter(pattern=criteria)
        elif criteria and isinstance(criteria, dict):
            result = self.filter(tags=criteria)
        elif kwargs:
            ts_logger.debug(f"DATASET.__getitem__(:\n\t{kwargs} ")
            result = self.filter(**kwargs)
        else:
            return None
        if isinstance(result, Dataset):
            return result
        else:
            raise TypeError("Dataset.filter() did not return a Dataset type.")

    def plot(self, *args: Any, **kwargs: Any) -> Any:
        """Plot dataset data.

        Convenience wrapper around Dataframe.plot() with sensible defaults.
        """
        xlabels = self.datetime_columns()
        ts_logger.debug(f"Dataset.plot({args!r}, {kwargs!r}) x-labels {xlabels}")
        return self.data.plot(  # type: ignore[call-overload]
            xlabels,
            *args,
            legend=len(self.data.columns) < 9,
            title=self.name,
            figsize=(12, 4),
            **kwargs,
        )

    def vectors(self, pattern: str = "") -> None:
        """Get vectors with names equal to column names from Dataset.data.

        Args:
            pattern (str): Optional pattern for simple filtering of column names containing pattern. Defaults to "".

        Be warned: This is a hack. It (re)assigns variables in the scope of the calling function by way of stack inspection.
        This runs the risk of reassigning objects, functions, or variables.
        """
        import inspect

        stack = inspect.stack()
        locals_ = stack[1][0].f_locals

        for col in self.data.columns:
            if col.__contains__(pattern):
                cmd = f"{col} = self.data['{col}']"
                ts_logger.debug(cmd)
                # the original idea was running (in caller scope)
                # exec(cmd)
                locals_[col] = self.data[col]

    def groupby(
        self,
        freq: str,
        func: str = "auto",
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        """Group dataset data by specified frequency and function.

        Returns a new Dataset.
        """
        datetime_columns = list(
            set(self.data.columns) & {"valid_at", "valid_to", "valid_from"}
        )
        ts_logger.warning(f"DATASET {self.name}: datetime columns: {datetime_columns}.")

        # works for datetime_columns = "valid_at", untested for others
        # TODO: add support for ["valid_from", "valid_to"]
        period_index = pd.PeriodIndex(self.data[datetime_columns[0]], freq=freq)
        ts_logger.debug(f"DATASET {self.name}: period index\n{period_index}.")

        # Fix for case when **kwargs contains numeric_only
        if "numeric_only" in kwargs:
            kwargs.pop("numeric_only")
        numeric_only_value = True

        match func:
            case "mean":
                out = self.data.groupby(period_index).mean(  # type: ignore[misc]
                    *args, numeric_only=numeric_only_value, **kwargs
                )
            case "sum":
                out = self.data.groupby(period_index).sum(  # type: ignore[misc]
                    *args, numeric_only=numeric_only_value, **kwargs
                )
            case "auto":
                # TODO: QA on exact logic / use "real" metadata
                # in particular, how to check meta data and blend d1 and df2 values as appropriate
                # (this implementation is just to show how it can be done)
                # QUESTION: do we need a default for "other" series / what should it be?
                df1 = self.data.groupby(period_index).mean(  # type: ignore[misc]
                    *args, numeric_only=numeric_only_value, **kwargs
                )
                ts_logger.debug(f"groupby\n{df1}.")

                df2 = (
                    self.data.groupby(period_index)
                    .sum(*args, numeric_only=numeric_only_value, **kwargs)  # type: ignore[misc]
                    .filter(regex="mendgde|volum|vekt")
                )
                ts_logger.warning(f"groupby\n{df2}.")

                df1[df2.columns] = df2[df2.columns]

                out = df1
                ts_logger.warning(f"groupby\n{out}.")
                ts_logger.warning(f"DATASET {self.name}: groupby\n{out}.")

        new_name = f"({self.name}.groupby({freq},{func})"

        return self.__class__(
            name=new_name,
            data_type=self.data_type,
            as_of_tz=self.as_of_utc,
            data=out,
        )

    def resample(
        self,
        freq: str,
        func: F | str,
        *args: Any,  # ---noqa: ANN002
        **kwargs: Any,  # --noqa: ANN003
    ) -> Self:
        """Alter frequency of dataset data."""
        # TODO: have a closer look at dates returned for last period when upsampling
        # df = self.data.set_index(self.datetime_columns())
        df = self.data.set_index(self.datetime_columns()).copy()
        match func:
            case "min":
                out = df.resample(freq).min()
            case "max":
                out = df.resample(freq).max()
            case "sum":
                out = df.resample(freq).sum()
            case "mean":
                out = df.resample(freq).mean()
            case "ffill":
                out = df.resample(freq).ffill()
            case "bfill":
                out = df.resample(freq).bfill()
            case _:
                out = df.resample(freq, *args, **kwargs).apply(func)

        new_name = f"new set:[{self.name}.resampled({freq}, {func}]"
        return self.__class__(
            name=new_name,
            data_type=self.data_type,
            as_of_tz=self.as_of_utc,
            data=out,
        )

    # TODO: Add these? (needed to make all() and any() work?)
    # def __iter__(self):
    #     self.n = 0
    #     return self
    #
    # def __next__(self):
    #     if self.n <= self.data.columns:
    #         x = self.n
    #         self.n += 1
    #         return x
    #     else:
    #         raise StopIteration
    #     return x

    # TODO: rethink identity: is / is not behaviour
    # def identical(self, other:Self) -> bool:
    #     # check_data = self.__eq__(other:Self)
    #     # return all(check_defs) and check_data.all()
    #     return self.__dict__ == other.__dict__

    def all(self) -> bool:
        """Check if all values in series columns evaluate to true."""
        ts_logger.warning(all(self.data))
        return all(self.data[self.numeric_columns()])

    def any(self) -> bool:
        """Check if any values in series columns evaluate to true."""
        return any(self.data[self.numeric_columns()])

    def numeric_columns(self) -> list[str]:
        """Get names of all numeric series columns (ie columns that are not datetime)."""
        return list(set(self.data.columns).difference(self.datetime_columns()))

    def datetime_columns(self, *comparisons: Self | pd.DataFrame) -> list[str]:
        """Get names of datetime columns (valid_at, valid_from, valid_to).

        Args:
            *comparisons (Self | pd.DataFrame): Objects to compare with. If provided, returns the intersection of self and all comparisons.

        Returns:
            list[str]: The (common) datetime column names of self (and comparisons).
        """
        intersect = set(self.data.columns) & {"valid_at", "valid_from", "valid_to"}
        for c in comparisons:
            if isinstance(c, Dataset):
                intersect = set(c.data.columns) & intersect
            elif isinstance(c, pd.DataFrame):
                intersect = set(c.columns) & intersect
            else:
                pass

        return list(intersect)

    @no_type_check
    def math(
        self,
        other: Self | pd.DataFrame | pd.Series | int | float,
        func,  # noqa: ANN001
    ) -> Any:
        """Generic helper making math functions work on numeric, non date columns of dataframe to dataframe, matrix to matrix, matrix to vector and matrix to scalar.

        Although the purpose was to limit "boilerplate" for core linear algebra functions, it also extend to other operations that follow the same differentiation pattern.

        Args:
            other (dataframe | series | matrix | vector | scalar ): One (or more?) pandas (polars to come) datframe or series, numpy matrix or vector or a scalar value.
            func (_type_): The function to be applied as `self.func(**other:Self)` or (in some cases) with infix notation `self f other`. Note that one or more date columns of the self / lefthand side argument are preserved, ie data shifting operations are not supported.

        Raises:
            ValueError: "Unsupported operand type"
            ValueError: "Incompatible shapes."

        Returns:
            Any:   Depending on the inputs: A new dataset / vector / scalar with the result. For datasets, the name of the new set is derived from inputs and the functions applied.
        """
        if isinstance(other, Dataset):
            ts_logger.debug(
                f"DATASET {self.name}: .math({self.name}.{func.__name__}(Dataset({other.name}))."
            )
            ts_logger.debug(
                f"DATASET {self.name}: .math({self.name},{other.name}) redirect to operating on {other.name}.data."
            )
            out_data = self.data.copy()
            out_data[self.numeric_columns()] = func(
                self.data[self.numeric_columns()], other.data[other.numeric_columns()]
            )
            other_name = other.name
            other_as_of = other.as_of_utc

        elif isinstance(other, pd.DataFrame):
            # element-wise matrix operation
            ts_logger.debug(
                f"DATASET {self.name}: .math({self.name}.{func.__name__}(pd.dataframe)."
            )

            out_data = self.data.copy()
            num_cols = self.numeric_columns()
            out_data[num_cols] = func(out_data[num_cols], other[num_cols])

            other_name = "df"
            other_as_of = None

        elif isinstance(other, int | float):
            out_data = self.data.copy()

            num_cols = self.numeric_columns()
            out_data[num_cols] = func(out_data[num_cols], other)

            other_name = str(other)
            other_as_of = None

        elif isinstance(other, np.ndarray):
            # TODO: this needs more thorugh testing!
            # Compare shape of the ndarray against the numeric_columns of self.data. There are up to 3 accepted cases (depending on the operation):
            #  * matrix;         shape = (data.numeric.rows, data.numeric.columns)
            #  * column vector;  shape = (data.numeric.rows, 1)
            #  * row vector;     shape = (1, data.numeric.columns)
            #
            if other.ndim == 1 and (
                other.shape[0] == len(self.data)
                or other.shape[0] == len(self.data.columns)
            ):
                out_data = self.data.copy()

                # for col in self._numeric_columns():
                #    out_data[col] = func(out_data[col], other:Self)
                out_data[self.numeric_columns()] = func(
                    out_data[self.numeric_columns()], other
                )

                # return out_data
            else:
                raise ValueError(
                    f"Incompatible shapes for element-wise {func.__name__}"
                )
            other_name = "ndarray"
            other_as_of = None
        else:
            raise ValueError("Unsupported operand type")

        # TODO: return (new) Dataset object instead!
        # return out_data
        if other_as_of:
            out_as_of = max(self.as_of_utc, other_as_of)
        else:
            out_as_of = self.as_of_utc

        out = self.__class__(
            name=f"({self.name}.{func.__name__}.{other_name})",
            data_type=self.data_type,
            as_of_tz=out_as_of,
            data=out_data,
        )
        # TODO: Should also update series names and set/series tags.
        ts_logger.debug(
            f"DATASET.math({func.__name__}, {self.name}, {other_name}) --> {out.name}\n\t{out.data}."
        )
        return out

    # TODO: check how performance of pure pyarrow or polars compares to numpy

    def __add__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Add two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.add)

    def __radd__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Right add two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.add)

    def __sub__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Subtract two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.subtract)

    def __rsub__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Right subtract two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.subtract)

    def __mul__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Multiply two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.multiply)

    def __rmul__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Right multiply two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.multiply)

    def __truediv__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Divide two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.divide)

    def __rtruediv__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Right divide two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.divide)

    def __floordiv__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Floor divide two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.floor_divide)

    def __rfloordiv__(
        self, other: Self | pd.DataFrame | pd.Series | int | float
    ) -> Any:
        """Right floor divide two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.floor_divide)

    def __pow__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Power of two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.power)

    def __rpow__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Right power of two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.power)

    def __mod__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Modulo of two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.mod)

    def __rmod__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Right modulo of two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.mod)

    @no_type_check
    def __eq__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Check equality of two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.equal)

    def __gt__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Check greater than for two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.greater)

    def __lt__(self, other: Self | pd.DataFrame | pd.Series | int | float) -> Any:
        """Check less than for two datasets or a dataset and a dataframe, numpy array or scalar."""
        return self.math(other, np.less)

    def __repr__(self) -> str:
        """Returns a machine readable string representation of Dataset, ideally sufficient to recreate object."""
        return f'Dataset(name="{self.name}", data_type={self.data_type!r}, as_of_tz="{self.as_of_utc.isoformat()}")'

    def __str__(self) -> str:
        """Returns a human readable string representation of the Dataset."""
        return str(
            {
                "name": self.name,
                "data_type": str(self.data_type),
                "as_of_utc": self.as_of_utc,
                "series": str(self.series),
                "data": self.data.size,
            }
        )

    def aggregate(
        self,
        attribute: str,
        taxonomy: Taxonomy | int | PathStr,
        aggregate_type: str | list[str] = "sum",
    ) -> Self:
        """Aggregate dataset by taxonomy.

        Args:
            attribute: The attribute to aggregate by.
            taxonomy (Taxonomy | int | PathStr): The values for `attribute`. A taxonomy object as returned by Taxonomy(klass_id_or_path), or the id or path to retrieve one.
            aggregate_type (str | list[str]): Optional function name (or list) of the function names to apply (mean | count | sum | ...). Defaults to `sum`.

        Returns:
            Self: A dataset object with the aggregated data.
            If the taxonomy object has hierarchical structure, aggregate series are calculated for parent nodes at all levels.
            If the taxonomy is a flat list, only a single 'total' aggregate series is calculated.

        Raises:
            NotImplementedError: If the aggregation method is not implemented yet. --> TODO!
        """
        if isinstance(taxonomy, Taxonomy):
            pass
        else:
            taxonomy = Taxonomy(taxonomy)

        # TODO: alter to handle list of functions, eg ["mean", "10 percentile", "25 percentile", "median", "75 percentile", "90 percentile"]
        if isinstance(aggregate_type, str):
            match aggregate_type.lower():
                case "mean" | "average":
                    raise NotImplementedError(
                        "Aggregation method 'mean' is not implemented yet."
                    )
                case "percentile":
                    raise NotImplementedError(
                        "Aggregation method 'percentile' is not implemented yet."
                    )
                case "count":
                    raise NotImplementedError(
                        "Aggregation method 'count' is not implemented yet."
                    )
                case "sum" | _:
                    df = self.data.copy().drop(columns=self.numeric_columns())
                    for node in taxonomy.parent_nodes():
                        leaf_node_subset = self.filter(
                            tags={attribute: taxonomy.leaf_nodes()}, output="df"
                        ).drop(columns=self.datetime_columns())
                        df[node.name] = leaf_node_subset.sum(axis=1)
                        ts_logger.debug(
                            f"DATASET.aggregate(): For node '{node.name}', column {aggregate_type} for input df:\n{leaf_node_subset}\nreturned:\n{df}"
                        )
                        new_col_name = node.name
                        df = df.rename(columns={node: new_col_name})
        else:
            raise NotImplementedError(
                "Multiple aggregation methods is planned, but not yet implemented."
            )
        return self.copy(f"{self.name}.{aggregate_type}", data=df)

    # unfinished business
    # mypy: disable-error-code="no-untyped-def"
    @no_type_check
    def reindex(
        self,
        index_type: str = "dt",
        freq: str = "",
        *args,  # noqa: ANN002
    ) -> None:
        """Reindex dataset by datetime or period."""
        match index_type:
            case "dt" | "datetime":
                self.data = self.data.set_index(self.datetime_columns(), *args)
            case "p" | "period":
                p = pd.PeriodIndex(self.data[self.datetime_columns], freq=freq)
                self.data.reindex(p)
            case _:
                self.data = self.data.set_index(self.datetime_columns(), *args)


def search(
    pattern: str = "*", as_of_tz: datetime = None
) -> list[io.SearchResult] | Dataset | list[None]:
    """Search for datasets by name matching pattern."""
    found = io.find_datasets(pattern=pattern)
    ts_logger.debug(f"DATASET.search returned:\n{found} ")

    if len(found) == 1:
        # raise NotImplementedError("TODO: extract name and type from result.")
        return Dataset(
            name=found[0].name,
            data_type=properties.seriestype_from_str(found[0].type_directory),
            as_of_tz=as_of_tz,
        )
    else:
        # elif len(found) > 1:
        return found
