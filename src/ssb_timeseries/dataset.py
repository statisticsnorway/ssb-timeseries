"""The :py:mod:`ssb_timeseries.dataset` module and its :py:class:`Dataset` class is the very core of the :py:mod:`ssb_timeseries` package, defining most of the key functionality.

The dataset is the unit of analysis for both :doc:`information model <../info-model>` and :doc:`workflow integration <../workflow>`,and performance will benefit from linear algebra with sets as matrices consisting of series column vectors.

As described in the :doc:`../info-model` time series datasets may consist of any number of series of the same :py:class:`~ssb_timeseries.properties.SeriesType`.
The series types are defined by dimensionality characteristics:

* :py:class:`~ssb_timeseries.properties.Versioning` (NONE, AS_OF, NAMED)
* :py:class:`~ssb_timeseries.properties.Temporality` (Valid AT point in time, or FROM and TO for duration)
* The type of the value. For now only scalar values are supported.

Additional type determinants (sparsity, irregular frequencies, non-numeric or non-scalar values, ...) are conceivable and may be introduced later.
The types are crucial because they are reflected in the physical storage structure.
That in turn has practical implications for how the series can be interacted with, and for methods working on the data.

.. admonition:: See also
    :class: more

    The :py:mod:`ssb_timeseries.catalog` module for tools for searching for datasets or series by names or metadata.
"""

from __future__ import annotations

import re
import warnings
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Protocol
from typing import cast
from typing import no_type_check

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self  # noqa: UP035 #backport to 3.10

import matplotlib.pyplot as plt  # noqa: F401
import narwhals as nw
import narwhals.selectors as ncs
import numpy as np
from narwhals.typing import Frame
from narwhals.typing import IntoDType
from narwhals.typing import IntoFrame
from narwhals.typing import IntoFrameT
from narwhals.typing import IntoSeries
from numpy.typing import NDArray

import ssb_timeseries as ts
from ssb_timeseries import io
from ssb_timeseries import meta
from ssb_timeseries.dataframes import empty_frame
from ssb_timeseries.dataframes import is_df_like
from ssb_timeseries.dataframes import is_empty
from ssb_timeseries.dates import date_local
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import period_index
from ssb_timeseries.dates import utc_iso
from ssb_timeseries.types import F
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment,attr-defined,union-attr,arg-type,call-overload,no-untyped-call,dict-item"
# ruff: noqa: RUF013


def select_repository(name: str = "") -> Any:
    """Select a named or default repository from the configuration.

    If there is only one repo, the choice is easy and criteria does not matter.
    Otherwise, if a ``name`` is provided, only that is checked.
    If no name is provided, the first item marked with `'default': True` is picked.
    If no item is identified by name or marking as default, the last item is returned.
    (This behaviour is questionable - it may be turned into an error.)
    """
    repos = ts.active_config().repositories
    for k, v in repos.items():
        if len(repos) == 1:
            return v
        if k == name:
            return v
        elif not name and v.get("default", False):
            return v
    else:
        warnings.warn(
            f"Repository with name '{name}' could not be picked among {len(repos)}. The last one ({v['name']}) is used.",
            stacklevel=2,
        )
        return v


class IO(Protocol):
    """Interface for IO operations."""

    def save(self) -> None:
        """Save the dataset."""
        ...

    def snapshot(self) -> None:
        """Save a snapshot of the dataset."""
        ...


class Dataset:
    """Datasets are containers for series of the same :py:class:`~ssb_timeseries.properties.SeriesType` with origin from the same process.

    That generally implies some common denominator in terms of descriptive metadata,
    but more important, it allows the Dataset to become a core unit of analysis for workflow.
    It becomes a natural chunk of data for reads and writes, and calculation.

    For all the series in a dataset to be of the same :py:class:`~ssb_timeseries.properties.SeriesType` means they share dimensionality characteristics :py:class:`~ssb_timeseries.properties.Versioning` and :py:class:`~ssb_timeseries.properties.Temporality` and any other schema information that have tecnical implications for how the data is handled.
    See the :doc:`../info-model` documentation for more about that.

    The descriptive commonality is not enforced, but some aspects have technical implications.
    In particular, it is strongly encouraged to make sure that the resolutions of the series in datasets are the same, and to minimize the number of gaps in the series.
    Sparse data is a strong indication that a dataset is not well defined and that series in the set have different origins.
    'Gaps' in this context is any representation of undefined values: None, null, NAN or "not a number" values, as opposed to the number zero.
    The number zero is a gray area - it can be perfectly valid, but can also be an indication that not all the series should be part of the same set.

    :var str name: The name of the set.
    :var SeriesType data_type: The type of the contents of the set.
    :var datetime as_of_tz: The version datetime, if applicable to the ``data_type``.
    :var Dataframe data: A dataframe or table structure with one or more datetime columns defined by ``datatype`` and a column per series in the set.
    :var dict tags: A dictionary with metadata describing both the dataset itself and the series in the set.

    .. admonition:: Maintaining tags
        :class: more dropdown

        |tagging|

    """

    def __init__(
        self,
        name: str,
        data_type: ts.properties.SeriesType = None,
        as_of_tz: datetime = None,
        repository: str = "",
        load_data: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialising a dataset object either retrieves an existing set or prepares a new one.

        When preparing a new set, data_type must be specified.
        If data_type versioning is specified as AS_OF, a datetime with timezone should be provided.
        Providing an AS_OF date has no effect if versioning is NONE.
        If not, but data is passed, :py:meth:`as_of_tz` defaults to current time.
        For all dates, if no timezone is provided, CET is assumed.

        The `data` parameter accepts a dataframe with one or more date columns and one column per series.
        Initially only Pandas was supported, but this dependency is about to be relaxed to include other implementations of the samedata structure.
        Beyond Polars and Pyarrow, notable options under consideration include Ibis, DuckDB and Narwhals.

        Data is kept in memory and not stored before explicit call to :py:meth:`save`.
        The data is stored in parquet files, with JSON formatted metadata in the header.

        Metadata will always be read if the set exists.
        When loading existing sets, load_data = False will suppress reading large amounts of data.
        For data_types with AS_OF versioning, not providing the AS_OF date will have the same effect.

        If series names can be mapped to metadata, the keyword arguments ``attributes``, ``separator`` and ``regex`` will, if provided, be passed through to :py:class:`~Dataset.series_names_to_tags`.
        If series names are not easily translated to tags, :py:class:`~Dataset.tag_dataset` and :py:class:`~Dataset.tag_series` and their siblings :py:meth:`retag <Dataset.retag_series>` and :py:meth:`detag <Dataset.detag_series>` can be used for manual meta data maintenance.

        :keyword list[str] attributes: Attribute names for use with :py:class:`~Dataset.series_names_to_tags` in combination with either ``separator`` or ``regex``.
        :keyword str separator: Character(s) separating ``attributes`` for use with :py:class:`~Dataset.series_names_to_tags`.
        :keyword str regex: Regular expression with capture groups corresponding to ``attributes``. Used instead of the separator to match more complicated name patterns in :py:class:`~Dataset.series_names_to_tags`.

        .. admonition:: Maintaining tags
           :class: more dropdown

           |tagging|


        .. code::

            import ssb_timeseries as ts
            df = ts.sample_data.xyz_at()
            print(df)

            x = ts.dataset.Dataset(
                name='mydataset',
                data_type=ts.properties.SeriesType.simple(),
                data=df
            )

        .. testoutput::
            :hide:

            34

        """
        self.repository = select_repository(repository)
        self.name: str = name
        if data_type:
            self.data_type = data_type
        else:
            look_for_it = search(
                repository=self.repository["directory"],
                pattern=name,
                require_unique=True,
            )
            if isinstance(look_for_it, Dataset):
                self.data_type = look_for_it.data_type

        identify_latest: bool = (
            self.data_type.versioning == ts.properties.Versioning.AS_OF
            and not as_of_tz
            and load_data
        )
        if identify_latest:
            ts.logger.debug(
                "Init %s '%s' without 'as_of_tz' --> identifying latest.",
                self.data_type,
                self.name,
            )
            self.io = io.FileSystem(
                repository=self.repository,
                set_name=self.name,
                set_type=self.data_type,
                as_of_utc=None,
            )
            lookup_as_of = self.versions()[-1]
            if isinstance(lookup_as_of, datetime):
                as_of_tz = lookup_as_of
            self.as_of_utc = date_utc(as_of_tz)
        else:
            self.as_of_utc = date_utc(as_of_tz)

        self.io: IO = io.FileSystem(  # type: ignore[no-redef]
            repository=self.repository,
            set_name=self.name,
            set_type=self.data_type,
            as_of_utc=self.as_of_utc,
        )

        kwarg_data = kwargs.get("data", None)
        if is_df_like(kwarg_data) and not is_empty(kwarg_data):
            self.data = kwarg_data
        elif load_data:  # and self.data_type.versioning == properties.Versioning.AS_OF:
            self.data = self.io.read_data(self.as_of_utc)  # .to_native()
        else:
            self.data = empty_frame()

        self.tags = self.default_tags()
        self.tags.update(self.io.read_metadata())
        self.tag_dataset(tags=kwargs.get("dataset_tags", {}))

        # all of the following should be turned into set level tags - or find another way into the parquet files?

        # autotag:
        attributes = kwargs.get("attributes", [])
        separator = kwargs.get("separator", "_")
        regex = kwargs.get("regex", "")
        ready_to_auto_tag = _has_auto_tag_information(self, locals())
        apply_to_all = kwargs.get("series_tags", {})
        self.auto_tag_config = {
            "attributes": attributes,
            "separator": separator,
            "regex": regex,
        }
        if not is_empty(self.data):
            self.tag_series(tags=apply_to_all)
            if ready_to_auto_tag:
                self.series_names_to_tags()

        # "owner" / sharing / access
        self.product: str = kwargs.get("product", "")
        self.process_stage: str = kwargs.get("process_stage", "")
        self.sharing: dict[str, str] = kwargs.get("sharing", {})

    def copy(
        self,
        new_name: str = "",
        select: str | list[str] = "",
        **kwargs: Any,
    ) -> Self:
        """Create a copy of the Dataset.

        The copy will be created with the same data_type, as_of_tz, data, and tags.
        The name will be altered as a safeguard against accidentally overwriting existing data.
        Tags are updated accordingly, using 'Dataset.rename()'.

        Args:
            new_name: An optional new name for the copied dataset.
            select: An optional list of columns to include in the copy.
            **kwargs: Additional keyword arguments passed to underlying functions.

        Keyword Args:
            allow providing parameters for initializing the copied set.
        """
        if not new_name:
            new_name = f"COPY of {self.name}"

        data = kwargs.pop("data", self.data)
        match select.lower():
            case "dates_only":
                raise NotImplementedError("TODO!")
            case "select":
                raise NotImplementedError("TODO!")
            case "all" | _:
                ...  # data = data

        # Observed here: OSError: [Errno 36] File name too long
        # New names from .select() or column aggregations easily become too long.
        # A workaround was applied in fs.exists()
        # out = deepcopy(self)
        # out.rename(new_name)
        autotag_attr = kwargs.pop("attributes", self.auto_tag_config["attributes"])
        out = self.__class__(
            name=new_name,
            data_type=kwargs.pop("data_type", self.data_type),
            data=data,
            load_data=False,  # kwargs.pop('load_data', False),
            attributes=autotag_attr,
            **kwargs,
        )
        # not necessary?
        # for k, v in self.__dict__.items():
        #    #print(f"copy attr from self: {k=}, ")
        #    setattr(out, k, v)

        # lineage coalesce: kwarg, self, new_name
        lineage = getattr(self, "lineage", new_name)
        out.lineage = kwargs.pop("lineage", lineage)

        copied_series_tags = out.tags["series"]
        tags_to_eliminate = set(copied_series_tags.keys()) - set(out.series)
        for series_key in tags_to_eliminate:
            copied_series_tags.pop(series_key)
        out.tags["series"] = copied_series_tags
        return out

    def rename(self, new_name: str) -> None:
        """Rename the Dataset.

        For use by .copy, and on very rare other occasions. Does not move or rename any previously stored data.
        """
        self.name = new_name

        self.tags["name"] = new_name
        for _, v in self.tags["series"].items():
            v["dataset"] = new_name  # type: ignore

    def save(self, as_of_tz: datetime = None) -> None:
        """Persist the Dataset.

        Args:
            as_of_tz (datetime): Provide a timezone sensitive as_of date in order to create another version. The default is None, which will save with Dataset.as_of._utc (utc dates under the hood).
        """
        if as_of_tz is not None:
            self.as_of_utc = date_utc(as_of_tz)

        self.io = io.FileSystem(
            self.repository, self.name, self.data_type, self.as_of_utc
        )
        # ts.logger.debug("DATASET %s: SAVE. Tags:\n\t%s.", self.name, self.tags)
        if not self.tags:
            self.tags = self.default_tags()
            self.tags.update(self.io.read_metadata())

            # autotag:
            if not is_empty(self.data):
                self.series_names_to_tags()
                ts.logger.debug(
                    "DATASET %s: attempt to save empty tags = %s.", self.name, self.tags
                )

        self.io.save(meta=self.tags, data=self.data)

    def snapshot(self, as_of_tz: datetime = None) -> None:
        """Copy data snapshot to immutable processing stage bucket and shared buckets.

        Args:
            as_of_tz (datetime): Optional. Provide a timezone sensitive as_of date in order to create another version. The default is None, which will save with Dataset.as_of_utc (utc dates under the hood).
        """
        date_from = self.data[self.datetime_columns()].min().min()
        date_to = self.data[self.datetime_columns()].max().max()
        ts.logger.debug(
            "DATASET %s: Data %s - %s:\n%s\n...\n%s",
            self.name,
            utc_iso(date_from),
            utc_iso(date_to),
            self.data.head(),
            self.data.tail(),
        )

        self.save(as_of_tz=self.as_of_utc)

        self.io.snapshot(
            product=self.product,
            process_stage=self.process_stage,
            sharing=self.sharing,
            period_from=date_from,
            period_to=date_to,
        )

    def versions(self, **kwargs: Any) -> list[datetime | str]:
        """Get list of all series version markers (`as_of` dates or version names).

        By default `as_of` dates will be returned in local timezone. Provide `return_type = 'utc'` to return in UTC, 'raw' to return as-is.
        """
        versions = self.io.list_versions(
            file_pattern="*.parquet",
            pattern=self.data_type.versioning,
        )
        if not versions:
            return []
        else:
            ts.logger.debug("DATASET %s: versions: %s.", self.name, versions)

        if self.data_type.versioning == ts.properties.Versioning.AS_OF:
            return_type = kwargs.get("return_type", "local")
        else:
            return_type = "raw"

        match return_type:
            case "local":
                return [date_local(v) for v in versions]
            case "utc":
                return [date_utc(v) for v in versions]
            case "utc_iso":
                return [utc_iso(v) for v in versions]
            case _:
                return versions

    @property
    def series(self) -> list[str]:
        """Get (sorted) series names.

        This is the same as the numeric columns of the data field.
        """
        num_cols = self.numeric_columns()
        # dt_expr = ~ncs.by_dtype(nw.Datetime,nw.Date)
        # non_datetime_cols = self.nw().select(dt_expr).columns
        ## does not necessarily exist:
        ## tag_keys = self.series_tags.keys()
        # if num_cols != non_datetime_cols:
        #    raise ValueError("WFT - something fishy with series names or data types.")
        return sorted(num_cols)

        # first implementation was more complicated
        # if (
        #    self.__getattribute__("data") is None
        #    and self.__getattribute__("tags") is None
        # ):
        #    return sorted(self.series_tags.keys())
        # else:
        #    return sorted(self.numeric_columns())

    @property
    def series_tags(self) -> meta.SeriesTagDict:
        """Get series tags."""
        return self.tags["series"]  # type: ignore

    def default_tags(self) -> meta.DatasetTagDict:
        """Return default tags for set and series."""
        return {
            "name": self.name,
            "versioning": str(self.data_type.versioning),
            "temporality": str(self.data_type.temporality),
            "series": {s: {"dataset": self.name, "name": s} for s in self.series},
        }

    def tag_dataset(
        self,
        tags: meta.TagDict = None,
        **kwargs: str | list[str] | set[str],
    ) -> None:
        """Tag the set.

        Tags may be provided as dictionary of tags, or as kwargs.

        In both cases they take the form of attribute-value pairs.

        Attribute (str): Attribute identifier.
        Ideally attributes relies on KLASS, ie a KLASS taxonomy defines the possible attribute values.

        Value (str): Element identifier, unique within the taxonomy. Ideally KLASS code.

        Note that while no such restrictions are enforced, it is strongly recommended that both attribute names (``keys``) and ``values`` are standardised.
        The best way to ensure that is to use taxonomies (for SSB: KLASS code lists).
        However, custom controlled vocabularies can also be maintained in files.

        .. admonition:: Maintaining tags
            :class: more dropdown

            |tagging|


        Examples:
            >>> from ssb_timeseries.dataset import Dataset
            >>> from ssb_timeseries.properties import SeriesType
            >>> from ssb_timeseries.sample_data import create_df
            >>>
            >>> x = Dataset(name='sample_dataset',
            >>>         data_type=SeriesType.simple(),
            >>>         data=create_df(['x','y','z'],
            >>>             start_date='2024-01-01',
            >>>             end_date='2024-12-31',
            >>>             freq='MS',)
            >>> )
            >>>
            >>> x.tag_dataset(tags={'country': 'Norway', 'about': 'something_important'})
            >>> x.tag_dataset(another_attribute='another_value')


        """
        if not self.__getattribute__("tags"):
            # should not be possible, hence
            raise ValueError(f"Tags not defined for dataset: {self.name}.")

        if not tags and not kwargs:
            return
        elif not tags:
            tags = {}
        if kwargs:
            tags.update(**kwargs)

        propagate = tags.pop("propagate", True)
        if tags:
            self.tags = meta.add_tag_values(self.tags, tags, recursive=propagate)

    def tag_series(
        self,
        names: str | list[str] = "*",
        tags: meta.TagDict = None,
        **kwargs: str | list[str],
    ) -> None:
        """Tag the series identified by ``names`` with provided tags.

        Tags may be provided as dictionary of tags, or as kwargs.

        In both cases they take the form of attribute-value pairs.

        Attribute (str): Attribute identifier.
        Ideally attributes relies on KLASS, ie a KLASS taxonomy defines the possible attribute values.

        Value (str): Element identifier, unique within the taxonomy. Ideally KLASS code.

        If series names follow the same pattern of attribute values in the same order separated by the same character sequence, tags can be propagated accordingly by specifying ``attributes`` and ``separator`` parameters. The separator will default to underscore if not provided. Note that propagation by pattern will affect *all* series in the set, not only the ones identified by ``names``.

        .. admonition:: Maintaining tags
            :class: more dropdown

            |tagging|

        Examples:
            Dependencies

            >>> from ssb_timeseries.dataset import Dataset
            >>> from ssb_timeseries.properties import SeriesType
            >>> from ssb_timeseries.sample_data import create_df
            >>>
            >>> some_data = create_df(['x', 'y', 'z'], start_date='2024-01-01', end_date='2024-12-31', freq='MS')

            Tag by kwargs

            >>> x = Dataset(name='sample_set',data_type=SeriesType.simple(),data=some_data)
            >>> x.tag_series(example_1='string_1', example_2=['a', 'b', 'c'])

            Tag by dict

            >>> x = Dataset(name='sample_set',data_type=SeriesType.simple(),data=some_data)
            >>> x.tag_series(tags={'example_1': 'string_1', 'example_2': ['a', 'b', 'c']})

        """
        if not tags and not kwargs:
            return

        if not tags:
            tags = {}
        tags.update(kwargs)

        if names == "*":
            names = self.series
        elif isinstance(names, str):
            names = [names]

        inherit_from_set_tags = meta.inherit_set_tags(self.tags)

        for n in names:
            if not self.tags["series"][n]:
                self.tags["series"][n] = {"name": n}
            self.tags["series"][n].update({**inherit_from_set_tags, **tags})

    def detag_dataset(
        self,
        *args: str,
        **kwargs: Any,
    ) -> None:
        """Detag selected attributes of the set.

        Tags to be removed may be provided as list of attribute names or as kwargs with attribute-value pairs.

        .. admonition:: Maintaining tags
            :class: more dropdown

            |tagging|
        """
        self.tags = meta.delete_dataset_tags(
            self.tags,
            *args,  # [a for a in args if isinstance(a, str)]
            propagate=True,
            **kwargs,
        )

    @no_type_check  # "operator"
    def detag_series(
        self,
        *args: str,
        **kwargs: Any,
    ) -> None:
        """Detag selected attributes of series in the set.

        Tags to be removed may be specified by args or kwargs.
        Attributes listed in `args` will be removed from all series.

        For kwargs, attributes will be removed from the series if the value matches exactly. If the value is a list, the matching value is removed. If kwargs contain all=True, all attributes except defaults are removed.

        .. admonition:: Maintaining tags
            :class: more dropdown

            |tagging|
        """
        self.tags["series"] = meta.delete_series_tags(
            self.tags["series"],
            *args,
            **kwargs,
        )

    @no_type_check
    def series_names_to_tags(
        self,
        attributes: list[str] | None = None,  # /NOSONAR
        separator: str = "",  # /NOSONAR
        regex: str = "",  # /NOSONAR
    ) -> None:
        """Tag all series in the dataset based on a series 'attributes', ie a list of attributes matching positions in the series names when split on 'separator'.

        Alternatively, a regular expression with groups that match the attributes may be provided.
        Ideally attributes relies on KLASS, ie a KLASS taxonomy defines the possible attribute values.

        Value (str): Element identifier, unique within the taxonomy. Ideally KLASS code.

        .. admonition:: Maintaining tags
            :class: more dropdown

            |tagging|

        Examples:
            >>> from ssb_timeseries.dataset import Dataset
            >>> from ssb_timeseries.properties import SeriesType
            >>> from ssb_timeseries.sample_data import create_df

            Tag using attributes and dcefault separator:

            Let us create some data where the series names are formed by the values ['x', 'y', 'z']
            separated from ['a', 'b', 'c'] by an underscore:

            >>> some_data = create_df(
            >>>     ["x_a", "y_b", "z_c"],
            >>>     start_date="2024-01-01",
            >>>     end_date="2024-12-31",
            >>>     freq="MS",
            >>> )

            Then put it into a dataset and tag:

            >>> p = Dataset(
            >>>     name="sample_set",
            >>>     data_type=SeriesType.simple(),
            >>>     data=some_data,
            >>> )
            >>> p.series_names_to_tags(attributes=['XYZ', 'ABC'])

            >>> p.tags

            The above approach may be used at any time to add tags for an existing dataset, but the same arguments can also be provided when initialising the set:

            >>> z = Dataset(
            >>>     name="copy_of_sample_set",
            >>>     data_type=SeriesType.simple(),
            >>>     data=some_data,
            >>>     attributes=['XYZ', 'ABC'],
            >>> )

            Best practice is to do this only in the process that writes data to the set.
            For a finite number of series, it does not need to be repeated.

            If, on the other hand, the number of series can change over time, doing so at the time of writing ensures all series are tagged.
            Tag using attributes and regex:

            If series names are less well formed, a regular expression with groups matching the attribute list can be provided instead of the separator parameter.

            >>> more_data = create_df(
            >>>     ["x_1,,a", "y...b..", "z..1.1-23..c"],
            >>>     start_date="2024-01-01",
            >>>     end_date="2024-12-31",
            >>>     freq="MS",
            >>> )
            >>> x = Dataset(
            >>>     name="bigger_sample_set",
            >>>     data_type=SeriesType.simple(),
            >>>     data=more_data,
            >>> )
            >>> x.series_names_to_tags(attributes=['XYZ', 'ABC'], regex=r'([a-z])*([a-z])')

        """
        if attributes:
            self.auto_tag_config["attributes"] = attributes
        else:
            attributes = self.auto_tag_config["attributes"]

        if separator:
            self.auto_tag_config["separator"] = separator
        else:
            separator = self.auto_tag_config["separator"]

        if regex:
            self.auto_tag_config["regex"] = regex
        else:
            regex = self.auto_tag_config["regex"]

        apply_all = self.auto_tag_config.get("apply_to_all", {})
        inherited = meta.inherit_set_tags(self.tags)
        self.tag_series({**inherited, **apply_all})

        # always apply tags inherited from dataset and tags specificly applied to all series first
        for series_key in self.series:
            self.tags["series"].get(series_key, {}).update(inherited)
            if apply_all:
                self.tags["series"].get(series_key, {}).update(apply_all)

        if attributes:
            for series_key in self.tags["series"].keys():
                if regex:
                    name_parts = re.search(regex, series_key).groups()
                elif separator:
                    name_parts = series_key.split(separator)
                else:
                    raise AttributeError(
                        "DATASET.series_names_to_tags() requires either a regex or a separator."
                    )

                for attribute, value in zip(attributes, name_parts, strict=False):
                    # not necessary? self.tags["series"][series_key][attribute] = deepcopy(value)
                    self.tags["series"][series_key][attribute] = value
        else:
            ts.logger.warning(
                "DATASET.series_names_to_tags() requires attributes to be defined for the Dataset object or to be passed as an argument."
            )
            # raise AttributeError( "Attributes must be defined in Dataset.auto_tag_config or passed as an argument.")

    def replace_tags(
        self,
        *args: tuple[meta.TagDict, meta.TagDict],
    ) -> None:
        """Retag selected attributes of series in the set.

        The tags to be replaced and their replacements should be specified in tuple(s) of :py:type:`tag dictionaries <ssb_timeseries.meta.TagDict>`;
        each argument in ``*args`` should be on the form ``({<old_tags>},{<new_tags>})``.

        Both old and new :py:type:`TagDict` can contain multiple tags.
         - Each tuple is evaluated independently for each series in the set.
         - If the tag dict to be replaced contains multiple tags, all must match for tags to be replaced.
         - If the new tag dict contains multiple tags, all are added where there is a match.

        .. admonition:: Maintaining tags
            :class: more dropdown

            |tagging|
        """
        for a in args:
            old = a[0]
            new = a[1]

            self.tags = meta.replace_dataset_tags(self.tags, old, new, recursive=True)

    @no_type_check
    def select(
        self,
        *names: str | list[str],
        pattern: str = "",
        regex: str = "",
        tags: (meta.TagDict | list[meta.TagDict]) = None,
        output: str = "dataset",
        new_name: str = "",
        **kwargs: Any,
    ) -> IntoFrameT | Self:
        """Select series (columns) of Dataset.data by textual pattern, regex or metadata tag dictionary.

        Or a combination.

        Args:
            names (str|list[str]): Series names

        Keyword Args:
            pattern: Text pattern for search 'like' in column names. Defaults to ''.
            regex: Expression for regex search in column names. Defaults to ''.
            tags: Dictionary with tags to search for. Defaults to None. All tags in dict must be satisfied for the same series (tags are combined by AND). If a list of values is provided for a tag, the criteria is satisfied for either of them (OR).
                | list(dict) Support for list(dict) is planned, not yet implemented, to satisfy alternative sets of criteria (the dicts will be combined by OR).
            output: Output type - dataset or dataframe.(df). Defaults to 'dataset'. Short forms 'df' or 'ds' are accepted.
            new_name: Name of new Dataset. If not provided, a new name is generated.
            **kwargs: if provided, goes into the init of the new set.

        Returns:
            Dataset | Dataframe:
            By default a new Dataset (a deep copy of self).
            If output="dataframe" or "df", a dataframe.
            TODO: Explore shallow copy / nocopy options.
        """
        if not any([names, pattern, regex, tags]):
            error_message = f"DATASET.select() was called without valid criteria:\n{names=}, {pattern=}, {regex=}, {tags=}"
            raise ValueError(error_message)

        expressions = [nw.col(self.datetime_columns())]
        if names:
            expressions.append(nw.col(*names))

        if regex:
            expressions.append(ncs.matches(regex))

        if pattern:
            expressions.append(ncs.matches(f".*{pattern}.*"))

        if tags:
            if isinstance(tags, list):
                matching_series = meta.search_by_tags(self.tags["series"], *tags)
            else:
                matching_series = meta.search_by_tags(self.tags["series"], tags)
            ts.logger.debug("DATASET.select(tags) found:\n%s ", matching_series)
            expressions.append(nw.col(matching_series))

        df = nw.from_native(self.data).select(expressions).to_native()
        interval = kwargs.get("interval")
        if interval:
            ts.logger.warning(
                f"DATASET.select: Attempted call with argument 'interval' = {interval} is not supported. --> TO DO!"
            )

        match output:
            case "dataframe" | "df":
                out = df
            case "dataset" | "ds":
                if not new_name:
                    new_name = f"COPY of({self.name} SELECTED by names {names}, pattern: {pattern}, regex: {regex} tags: {tags})"
                out = self.copy(new_name, data=df, **kwargs)
                matching_series_tags = {
                    k: v
                    for k, v in out.tags["series"].items()
                    if k in out.numeric_columns()
                }
                out.tags["series"] = matching_series_tags
            case _:
                out = df
        return out

    @no_type_check
    def __getitem__(
        self,
        names_or_tags: str | meta.TagDict | Iterable[str | meta.TagDict] = "",
    ) -> Self | None:
        """Access Dataset.data.columns via Dataset.select on list[column_names | tags ] | pattern | regex.

        Arguments:
            names_or_tags:  Either a string pattern, name, list of names or a dict of tags.

        Returns:
            Self | None

        Raises:
            TypeError: If select() returns another type than Dataset.
        """
        if names_or_tags and isinstance(names_or_tags, str) and "*" in names_or_tags:
            result = self.select(regex=names_or_tags)
            # TODO: s is uncharted territory. Explore behaviour / add test cases!
        elif names_or_tags and isinstance(names_or_tags, str):
            result = self.select(names_or_tags)
        elif names_or_tags and isinstance(names_or_tags, dict):
            t = [names_or_tags]
            result = self.select(tags=t)
        elif names_or_tags and isinstance(names_or_tags, Iterable):
            n = [s for s in names_or_tags if isinstance(s, str)]
            t = [d for d in names_or_tags if isinstance(d, dict)]
            result = self.select(n, tags=t)
            # TODO: ensure test cases cover all permutations (a,b) that can be drawn from [{a},{},'a',''] and [{b},{},'b',''] (expect errors raised for combinations with only empty items).
        elif names_or_tags:
            raise TypeError(
                f"DATASET.__getitem__ received a positional argument of type {type(names_or_tags)}. Required: > str | list[str|dict] with a single name, a list of names, a list of tag dicts. Or even a list with combinations of names and tag dicts."
            )
        else:
            return None

        if isinstance(result, Dataset):
            return result
        else:
            raise TypeError(
                "DATASET.__getitem__ call to .select() did not return a Dataset type."
            )

    def __setitem__(
        self, columns: str | Iterable[str], data: IntoFrame | np.ndarray
    ) -> None:
        """Access Dataset.data.columns via Dataset[ list[column_names]."""
        if columns and isinstance(columns, str):
            columns = [columns]
        elif columns and isinstance(columns, Iterable):
            columns = list(columns)
        else:
            raise TypeError(
                "DATASET.__setitem__ columns should be specified as string or iterable, not {type(columns).__name__}."
            )
        nw_self = nw.from_native(self.nw().to_arrow())

        # if False:
        if isinstance(data, np.ndarray):
            # The array shape must be aligned with the numeric subset self[columns].
            if data.ndim == 1:
                data = data.reshape(-1, 1)

            if len(columns) != data.shape[1]:
                raise ValueError(
                    f"Key has {len(columns)} cols, but NumPy array has {data.shape[1]} cols."
                )
            if len(self) != data.shape[0]:
                raise ValueError(
                    f"Dataset has {len(self)} rows, but NumPy array has {data.shape[0]} rows."
                )

            temp_col_names = [f"__temp_{i}" for i in range(data.shape[1])]
            data_df = nw.from_dict(
                {col_name: data[:, i] for i, col_name in enumerate(temp_col_names)},
                backend=nw_self.implementation,
            )
            expressions = [
                data_df.get_column(temp_name).alias(final_name)
                for temp_name, final_name in zip(temp_col_names, columns, strict=False)
            ]
        else:
            data_to_write = nw.from_native(data, backend=nw_self.implementation).select(
                ncs.numeric()
            )

            # TODO: do more to validate/normalize dtypes here?
            # ... and validate/name/order columns?
            columns_dtypes = {
                k: _nw_normalize_dtype(v)
                for k, v in nw_self.schema.items()
                if k in columns
            }
            expressions = [
                data_to_write.get_column(original_name).alias(new_name).cast(dtype)
                for new_name, original_name, dtype in zip(
                    columns, data_to_write.columns, columns_dtypes, strict=False
                )
            ]

        self.data = nw_self.with_columns(expressions).to_native()

    def __len__(self) -> int:
        """Returns the length of the dataset along the time axis, ie. the number of rows."""
        (length, _) = self.data.shape
        return int(length)

    def plot(self, *args: Any, **kwargs: Any) -> Any:
        """Plot dataset data.

        Convenience wrapper around Dataframe.plot() with sensible defaults.
        """
        import pandas

        df = nw.from_native(self.data).to_pandas()

        if self.data_type.temporality == ts.properties.Temporality.FROM_TO:
            interval_handling = kwargs.pop("interval_handling", "interval").lower()
            match interval_handling:
                case "interval":
                    from_data = df
                    to_data = df
                    from_data["valid_to"] = from_data["valid_from"]
                    df = pandas.concat(
                        [from_data, to_data],
                        axis=0,
                        ignore_index=True,
                    ).sort_values(by=["valid_from", "valid_to"])
                    df.drop(columns=["valid_to"], inplace=True)
                    xlabels = "valid_from"
                case "midpoint":
                    xlabels = "midpoint"
                    df["midpoint"] = df[self.datetime_columns()].median(axis=1)
                    df.drop(columns=["valid_from", "valid_to"], inplace=True)

                case _:
                    raise ValueError(
                        "Invalid option for interval_handling. Must be 'from', 'to', 'interval' or 'midpoint'."
                    )
        else:
            xlabels = self.datetime_columns()[0]

        ts.logger.debug("DATASET.plot(): x labels = %s", xlabels)
        ts.logger.debug(f"Dataset.plot({args!r}, {kwargs!r}) x-labels {xlabels}")

        return df.plot(
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
            pattern (str): Optional pattern for simple selecting of column names containing pattern. Defaults to ''.

        .. warning:: Caution!
            This (re)assigns variables in the scope of the calling function by way of stack inspection and hence risks of reassigning objects, functions, or variables if they happen to have the same name.

        """
        import inspect

        stack = inspect.stack()
        locals_ = stack[1][0].f_locals

        for col in self.data.columns:
            if col.__contains__(pattern):
                cmd = f"{col} = self.data['{col}']"
                ts.logger.debug(cmd)
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

        # works for datetime_columns = "valid_at", untested for others
        # TODO: add support for ["valid_from", "valid_to"]
        period_idx = period_index(self.data[datetime_columns[0]], freq=freq)
        ts.logger.debug("DATASET %s: period index\n%s.", self.name, period_idx)

        # Fix for case when **kwargs contains numeric_only
        if "numeric_only" in kwargs:
            kwargs.pop("numeric_only")
        numeric_only_value = True

        match func:
            case "mean":
                out = self.data.groupby(period_idx).mean(
                    *args, numeric_only=numeric_only_value, **kwargs
                )
            case "sum":
                out = self.data.groupby(period_idx).sum(
                    *args, numeric_only=numeric_only_value, **kwargs
                )
            case "auto":
                # TODO: QA on exact logic / use "real" metadata
                # in particular, how to check meta data and blend d1 and df2 values as appropriate
                # (this implementation is just to show how it can be done)
                # QUESTION: do we need a default for "other" series / what should it be?
                df1 = self.data.groupby(period_idx).mean(
                    *args, numeric_only=numeric_only_value, **kwargs
                )
                ts.logger.debug(f"groupby\n{df1}.")

                df2 = (
                    self.data.groupby(period_idx)
                    .sum(*args, numeric_only=numeric_only_value, **kwargs)
                    .select(regex="mendgde|volum|vekt")
                )
                ts.logger.debug(f"groupby\n{df2}.")

                df1[df2.columns] = df2[df2.columns]

                out = df1
                ts.logger.debug(f"groupby\n{out}.")
                ts.logger.debug(f"DATASET {self.name}: groupby\n{out}.")

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
        *args: Any,
        **kwargs: Any,
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

    # TODO: Add these?
    # def __iter__(self):
    #     self.n = 0 # start at latest version
    #     return self
    #
    # def __next__(self):
    #     if self.n <= self.data.columns:
    #         x = self.n
    #         self.n += 1 # return previous version
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
        return bool(np.all(self.numeric_array()))
        # wrong: all(self.data) -- why?

    def any(self) -> bool:
        """Check if any values in series columns evaluate to true."""
        return bool(np.any(self.numeric_array()))

    def boolean_columns(self) -> list[str]:
        """Get names of all numeric series columns (ie columns that are not datetime)."""
        return list(nw.from_native(self.data).select(ncs.boolean()).columns)
        # replaces: return [c for c in self.data.columns if c not in self.datetime_columns()]

    # def datetime_columns(self, *comparisons: Self | pd.DataFrame) -> list[str]:
    def datetime_columns(self) -> list[str]:
        """Get names of applicable datetime columns (as_of, valid_at, valid_from, valid_to)."""
        return list(nw.from_native(self.data).select(ncs.datetime()).columns)
        # no need to check against column names (for current data type)?
        # intersect = set(nw.from_native(self.data).columns) & {"valid_at", "valid_from", "valid_to"}
        # return sorted(list(intersect))

    def numeric_columns(self) -> list[str]:
        """Get names of all numeric series columns (ie columns that are not datetime)."""
        return sorted(nw.from_native(self.data).select(ncs.numeric()).columns)

    def numeric_array(self, series: str | list[str] = "") -> NDArray:
        """Get the data of numeric series columns in matrix format as a Numpy NDArray.

        This will omit datetime columns, hence is convenient for linear algebra operations.
        Optionally, series names can be provided to get a subset of columns.
        """
        expr = [ncs.numeric() | ncs.boolean()]
        if series:
            expr.append(nw.col(series))
        np_array = nw.from_native(self.data).select(expr).to_numpy()
        return cast("NDArray", np_array)

    def nw(self) -> Frame:
        """Returns data as a (new) Narwhals frame.

        This allows making use not only of the functionality of Narwhals https://narwhals-dev.github.io/narwhals/api-reference/,
        but also that of other supported dataframe libraries through .to_pandas(), to_polars(),to_numpy(),.to_arrow().
        For numpy, see also 'Dataset.numeric_array()' that returns a numeric matrix omitting the date columns.
        """
        return cast(Frame, nw.from_native(self.data))

    @no_type_check
    def math(
        self,
        other: Self | IntoFrame | IntoSeries | int | float | None,
        func,  # noqa: ANN001
    ) -> Any:
        """Generic helper making math functions work on numeric, non date columns of dataframe to dataframe, matrix to matrix, matrix to vector and matrix to scalar.

        Although the purpose was to limit "boilerplate" for core linear algebra functions, it also extend to other operations that follow the same differentiation pattern.

        Args:
            other (dataframe | series | matrix | vector | scalar ): One (or more?) pandas (polars to come) dataframe or series, numpy matrix or vector or a scalar value.
            func (_type_): The function to be applied as `self.func(**other:Self)` or (in some cases) with infix notation `self f other`. Note that one or more date columns of the self / lefthand side argument are preserved, ie data shifting operations are not supported.

        Raises:
            ValueError: "Unsupported operand type"
            ValueError: "Incompatible shapes."

        Returns:
            Any:   Depending on the inputs: A new dataset / vector / scalar with the result.
            For datasets, the name of the new set is derived from inputs and the functions applied.
            If 'other' is not recognized, the 'NotImplemented' Singleton is returned so that Python can invoke  '__r<method>__' of other class.
        """
        num_cols = self.numeric_columns()
        out = self.copy()

        if isinstance(other, Dataset):
            ts.logger.debug(
                f"DATASET {self.name}: .math({self.name}.{func.__name__}(Dataset({other.name}))."
            )
            other_name = other.name
            # ValueError: operands could not be broadcast together with shapes (0,0) (4,3)
            # observed here for 'test_algebra_expression_with_multiple_dataset' -->
            # print(f"DATASET.math({func.__name__})\n=======\n{str(self)=} \n---\n{str(other)=}\n=======")
            result = func(self.numeric_array(), other.numeric_array())
            out[num_cols] = result
            out.as_of_utc = max(self.as_of_utc, other.as_of_utc)

        elif other is None:
            ts.logger.debug(f"DATASET {self.name}: .math({self.name}.{func.__name__}).")
            result = func(self.numeric_array())
            out[num_cols] = result
            other_name = ""

        elif is_df_like(other):
            other = nw.from_native(other).select(num_cols).to_numpy()
            out[num_cols] = func(self.numeric_array(), other)
            other_name = "df"

        # TODO: Add coverage elif isinstance(other, pd.Series):
        elif isinstance(other, nw.Series):
            raise NotImplementedError(
                "Adding support for Dataset data (matrix) operations with datasframe Series (vectors) is on the todo-list! Notify devs to increase priority."
            )

        elif isinstance(other, int | float):
            result = func(self.numeric_array(), other)
            out[num_cols] = result
            other_name = str(other)

        elif isinstance(other, np.ndarray):
            # TODO: this needs more thorough testing!
            # Compare shape of the ndarray against the numeric_columns of self.data. There are up to 3 accepted cases (depending on the operation):
            #  * matrix;         shape = (data.numeric.rows, data.numeric.columns)
            #  * column vector;  shape = (data.numeric.rows, 1)
            #  * row vector;     shape = (1, data.numeric.columns)
            #
            if other.ndim == 1 and (
                other.shape[0] == len(self.data)
                or other.shape[0] == len(self.data.columns)
            ):
                out[num_cols] = func(self.numeric_array(), other)
            else:
                raise ValueError(
                    f"Incompatible shapes for element-wise {func.__name__}"
                )
            other_name = "ndarray"
        else:
            # So that __r<op>__ of other object may be called, instead of:
            # raise ValueError(f"Unsupported operand type: {type(other)}.") -->
            return NotImplemented

        new_name = (f"({self.name}.{func.__name__}.{other_name})",)
        out.rename(new_name)
        out.lineage = new_name

        ts.logger.debug(f"DATASET.math: {new_name}\n\t{out.data.shape}")
        # print( f"DATASET.math: {new_name}\n\t{out.data.shape=}")
        # TODO: Should also update series names and set/series tags
        return out

    def __add__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Add two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.add.
        """
        return self.math(other, np.add)

    def __radd__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Right add handles cases where a Dataset is added to a non-dataset object.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.add.
        """
        return self.__add__(other)

    def __sub__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Subtract two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.sub.
        """
        return self.math(other, np.subtract)

    def __rsub__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Right subtract handles cases where a Dataset is subtrd from a non-Dataset.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.sub.
        """
        negative_self = self.__mul__(-1)
        return negative_self.__add__(other)

    def __mul__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Multiply two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.muliply.
        """
        return self.math(other, np.multiply)

    def __rmul__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Right multiply two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.muliply.
        """
        return self.__mul__(other)

    def __matmul__(self, other: Self | IntoFrame | IntoSeries) -> Any:
        """Matrix multiply two datasets or a dataset and a dataframe, numpy array.

        The matmul function implements the semantics of the @ operator defined in PEP 465.
        The operation inherits behavioural characteristicsa from Numpy. See np.matmul for details.

        Raises:
            ValueError
                If the last dimension of x1 is not the same size as the second-to-last dimension of x2.
                If a scalar value is passed in.
        """
        return self.math(other, np.matmul)

    def __rmatmul__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Right matrix multiply two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.matmul.
        """
        return self.__mul__(other)

    def __truediv__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Divide two datasets, or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.divide.
        """
        return self.math(other, np.divide)

    def __rtruediv__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Right true divide handles cases when non-dataset objects are divided by datasets.

        Division is performed elementwise using Numpy broadcast rules. See np.divide.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.divide.
        """
        inverse = self.__truediv__(other)
        # TODO: verify that the above always returns floats.
        # If not, we will get weird results when inverting with np.repiprocal.
        # (Also, check behaviour with decimal datatype.)
        return inverse.math(None, np.reciprocal)

    def __floordiv__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Floor divide two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.floordivide.
        """
        return self.math(other, np.floor_divide)

    def __rfloordiv__(self, other: IntoFrame | IntoSeries | int | float) -> Any:
        """Right floor divide handles cases when a dataframe, numpy array or scalar is divded by a dataset.

        Done element wise w. broadcast.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.floordivide.
        """
        inverse = self.__floordiv__(other)
        return inverse.math(None, np.reciprocal)

    def __pow__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Power of two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.power.
        """
        return self.math(other, np.power)

    def __rpow__(self, other: IntoFrame | IntoSeries | int | float) -> Any:
        """Right power of two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.power.
        """
        # raise NotImplementedError("Coming soon!")
        # self = x, other = a
        # y = a ** x (elementwise for x)
        # y = (e ** x) ** ln(a) ==>
        exp_self = self.math(None, np.exp)  # .math with None needs more testing
        ln_a = np.log(other)  # may handling of input type variations?
        return exp_self.__pow__(ln_a)

    def __mod__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Modulo of two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.mod.
        """
        return self.math(other, np.mod)

    def __rmod__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Right modulo of two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.mod.
        """
        return NotImplemented  # self.__mod__(other)

    def __neg__(self) -> Any:
        """Negative values for dataset.

        The operation returns a new dataset object. See np.negative.
        """
        return self.math(None, np.negative)

    def __pos__(self) -> Any:
        """Positive values for dataset.

        The operation returns a new dataset object. See np.positive.
        """
        return self.math(None, np.positive)

    def __abs__(self) -> Any:
        """Absolute values for dataset.

        The operation returns a new dataset object. See np.absolute.
        """
        return self.math(None, np.absolute)

    def __invert__(self) -> Any:
        """Invert the sign of values in the dataset.

        The operation is performed elementwise. See np.negative.
        """
        return self.math(-1, np.multiply)

    @no_type_check
    def __eq__(
        self, other: object
    ) -> Any:  # Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Check equality of two datasets or one dataset and a dataframe, numpy array or scalar.

        Note that this is a pure value comparison, and thus different from comparing dataset identity as reflected in the metadata of two dataset instances.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.equal.
        """
        return self.math(other, np.equal)

    def __ne__(
        self, other: Any
    ) -> Any:  # Self | IntoFrame | IntoSeries | int | float) -> Any:
        # def __ne__(self, other:Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Check inequality of two datasets or one dataset and a dataframe, numpy array or scalar.

        Note that this is a pure value comparison, and thus different from comparing dataset identity as reflected in the metadata of two dataset instances.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.not_equal.
        """
        return self.math(other, np.not_equal)

    def __gt__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Check greater than for two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.greater.
        """
        return self.math(other, np.greater)

    def __lt__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Check less than for two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.less.
        """
        return self.math(other, np.less)

    def __ge__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Check greater than or equal for two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.greater_equal.
        """
        return self.math(other, np.greater_equal)

    def __le__(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Check less than or equal for two datasets or a dataset and a dataframe, numpy array or scalar.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.less_equal.
        """
        return self.math(other, np.less_equal)

    def isclose(self, other: Self | IntoFrame | IntoSeries | int | float) -> Any:
        """Check if two datasets or a dataset and a dataframe, numpy array or scalar are nearlly equal.

        The operation is performed elementwise using Numpy broadcast rules if dimensions does not match. See np.isclose.
        """
        return self.math(other, np.isclose)

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
                "data": self.data.shape,
            }
        )

    def aggregate(
        self,
        attributes: list[str],
        taxonomies: list[int | meta.Taxonomy | dict[str, str] | PathStr],
        functions: list[str | F] | set[str | F],
        sep: str = "_",
    ) -> Self:
        """Aggregate dataset by taxonomy hierarchies.

        Args:
            attributes (list[str]): The attributes to aggregate by.
            taxonomies (list[int | meta.Taxonomy | dict[str, str] | PathStr]):
                Value definitions for `attributes` can be either ::py:class:`meta.Taxonomy` objects,
                or klass_ids, data dictionaries or paths that can be used to retrieve or construct them.
            functions (list[str|F] | set[str|F]): Optional function name (or list) of the function names to apply (mean | count | sum | ...). Defaults to `sum`.
            sep (str): Optional separator used when joining multiple attributes into names of aggregated series. Defaults to '_'.

        Returns:
            Self: A dataset object with the aggregated data.
            If the taxonomy object has hierarchical structure, aggregate series are calculated for parent nodes at all levels.
            If the taxonomy is a flat list, only a single `total` aggregate series is calculated.

        Raises:
            TypeError: If any of the taxonomy identifiere are of unexpected types.

        Examples:
            To calculate 10 and 90 percentiles and median for the dataset `x` where codes from KLASS 157 (energy_balance) distinguishes between series in the set.

            >>> from ssb_timeseries.dataset import Dataset
            >>> from ssb_timeseries.properties import SeriesType
            >>> from ssb_timeseries.sample_data import create_df
            >>> from ssb_timeseries.meta import Taxonomy
            >>>
            >>> klass157 = Taxonomy(klass_id=157)
            >>> klass157_leaves = [n.name for n in klass157.structure.root.leaves]
            >>> tag_permutation_space = {"A": klass157_leaves, "B": ["q"], "C": ["z"]}
            >>> series_names: list[list[str]] = [value for value in tag_permutation_space.values()]
            >>> sample_df = create_df(*series_names, start_date="2024-01-01", end_date="2024-12-31", freq="MS",)
            >>> sample_set = Dataset(name="sample_set",
            >>>     data_type=SeriesType.simple(),
            >>>     data=sample_df,
            >>>     attributes=["A", "B", "C"],
            >>> )
            >>>
            >>> def perc10(x):
            >>>     return x.quantile(.1, axis=1, numeric_only=True, interpolation="linear")
            >>>
            >>> def perc90(x):
            >>>     return x.quantile(.9, axis=1, numeric_only=True, interpolation="linear")
            >>>
            >>> percentiles = sample_set.aggregate(["energy_balance"], [157], [perc10, 'median', perc90])
        """
        taxonomy_dict = {}
        for name, t in zip(attributes, taxonomies, strict=False):
            if isinstance(t, meta.Taxonomy):
                obj = t
            elif isinstance(t, int):
                obj = meta.Taxonomy(klass_id=t)
            elif isinstance(t, dict):
                obj = meta.Taxonomy(data=t)
            elif isinstance(t, (str | Path)):
                obj = meta.Taxonomy(path=t)
            else:
                raise TypeError(
                    f"Taxonomy object or valid identifier expected, got {type(t)}"
                )
            taxonomy_dict[name] = obj

        df = nw.from_native(self.data).drop(self.numeric_columns()).to_pandas()
        new_series_tags = {}
        permutations = meta.permutations(taxonomy_dict, "parents")
        for p in permutations:
            criteria = {}
            for attr, value in p.items():
                criteria[attr] = taxonomy_dict[attr].leaf_nodes(value)
            output_series_name = sep.join(p.values())  # move into func loop?
            leaf_node_subset = self.select(tags=criteria, output="df")
            for func in functions:
                if isinstance(func, str):
                    new_col_name = f"{func}({output_series_name})"
                    func_name = func
                elif isinstance(func, list):
                    new_col_name = f"{func[0]}{func[1]}({output_series_name})"
                    func_name = f"{func[0]}{func[1]}"
                else:
                    new_col_name = f"{func.__name__}({output_series_name})"
                    func_name = func.__name__
                df[new_col_name] = column_aggregate(leaf_node_subset, func)
                lineage_info = {
                    "criteria": criteria,
                    "input": leaf_node_subset.columns,
                    "output": new_col_name,
                    "function": func_name,
                }
                new_series_tags[new_col_name] = lineage_info

        out = self.copy(f"{self.name}.{functions}", data=df)
        # TODO: the content of 'calculations' must be properly placed,
        # but for now it goes here.
        out.tags["calculations"] = new_series_tags
        return out

    def moving_average(
        self,
        start: int = 0,
        stop: int = 0,
        nan_rows: str = "return",
    ) -> Self:
        """Returns a new Dataset with moving averages for all series.

        The average is calculated over a time window defined by `from` and `to` period offsets.
        Negative values denotes periods before current, positive after.
        Both default to 0, ie the current period; so at least one of them should be used.

        >>> x.moving_average(start= -3, stop= -1) # xdoctest: +SKIP
        signifies the average over the three periods before (not including the current).

        Offset parameters will overflow the date range at the beginning and/or end,
        Moving averages can not be calculated.

        Set the parameter `nans` to control the behaviour in such cases:
        'return' to return rows with all NaN values (default).
        'remove' to remove these rows from both ends.

        TO DO: Add parameter to choose returned time window?
        TO DO: Add more NaN handling options?
        TO DO: Add parameter to ensure/alter sampling frequency before calculating.
        """
        n = stop - start + 1
        numbers = self.numeric_array()
        rows, columns = numbers.shape

        r = np.array(range(rows))
        r_from = r + start
        r_to = r + stop
        r_intersect = np.intersect1d(r_from, r_to)

        nans = np.ones((n, columns)) * np.nan
        numbers_ext = np.append(numbers, nans, 0)

        cumsums = np.cumsum(numbers_ext, axis=0)
        zeros = np.zeros((1, columns))
        diffs = cumsums[r_to, :] - np.append(zeros, cumsums, 0)[r_from, :]

        # ts_logger.debug("\n%s",numbers)
        # ts_logger.debug("\n%s",cumsums)
        # ts_logger.debug("\n%s",diffs)

        averages = diffs / n
        out = self.copy(f"{self.name}.mov_avg({start},{stop})")
        out[self.numeric_columns()] = averages[0:rows, :]

        # how to handle nans? choice between multiple strategies;
        # ... + choice whether to distinguish between beginning/end/middle?
        # - just return them (keep all rows) --> default: 'return'
        # - remove --> 'remove' 'remove_beginning' 'remove_end'
        # - calculate something
        match nan_rows:
            case "remove":
                result_rows = r_intersect
            case "remove_beginning":
                result_rows = np.array(range(min(r_intersect), rows))
            case "remove_end":
                result_rows = np.array(range(0, max(r_intersect)))
            case "return":
                result_rows = r
            case _:
                raise (
                    ValueError(
                        f"Received {nan_rows=}; allowed values include return | remove | ... (See the docs for more.) "
                    )
                )
            # - return some value, use method:
            #   - ignore nans when calculating avg
            #   - impute, repeat/prepend first/interpolate
            #   - estimation? henderson or other
            #   - retrieve more values?

        out.data = out.data.take(result_rows)
        # TODO: update the metadata
        return out


def column_aggregate(df: IntoFrameT, method: str | F) -> Any:
    """Helper function to calculate aggregate over dataframe columns."""
    # ts.logger.debug("DATASET.column_aggregate '%s' over columns:\n%s", method, df.columns)
    nw_df = nw.from_native(df)
    # nw_df.implementation

    # the following is not pretty, but is left as is to simplify the transition away from pandas
    # a better approach: return nw.Expr for methods  --> TODO!
    pd_df = nw_df.to_pandas()

    if isinstance(method, F):  # type: ignore
        out = method(pd_df)  # type: ignore[operator]
    else:
        match method:
            case "mean" | "average":
                out = pd_df.mean(axis=1, numeric_only=True)
            case "min" | "minimum":
                out = pd_df.min(axis=1, numeric_only=True)
            case "max" | "maximum":
                out = pd_df.max(axis=1, numeric_only=True)
            case "count":
                out = pd_df.count(axis=1, numeric_only=True)
            case "sum":
                out = pd_df.sum(axis=1, numeric_only=True)
            case "median":
                out = pd_df.quantile(0.5, axis=1, numeric_only=True)
            case ["quantile", *params] | ["percentile", *params]:
                if len(params) > 1:
                    interpolation: str = params[1]
                else:
                    interpolation = "linear"
                quantile = float(params[0])
                if quantile > 1:
                    quantile /= 100
                out = pd_df.quantile(
                    quantile,
                    axis=1,
                    numeric_only=True,
                    interpolation=interpolation,
                )
            case _:
                raise NotImplementedError(
                    f"Aggregation method '{method}' is not implemented (yet)."
                )
    return nw.new_series(
        name=f"{method}({pd_df.columns})",
        values=out,
        backend=nw_df.implementation,
    )


def search(
    pattern: str = "*",
    as_of_tz: datetime = None,
    repository: str = "",
    require_unique: bool = False,
) -> list[io.SearchResult] | Dataset | None:
    """Search for datasets by name matching pattern.

    Returns:
         list[io.SearchResult] | Dataset | list[None]: The dataset for a single match, a list for no or multiple matches.

    Raises:
        ValueError: If `require_unique = True` and a unique result is not found.
    """
    found = io.find_datasets(
        pattern=pattern,
        repository=repository,
    )
    ts.logger.debug(
        "DATASET.search for '%s'\nin repositories\n%s\nreturned:\n%s",
        pattern,
        repository,
        found,
    )
    number_of_results = len(found)

    if number_of_results == 1:
        return Dataset(
            name=found[0].name,
            data_type=ts.properties.seriestype_from_str(found[0].type_directory),
            as_of_tz=as_of_tz,
        )
    elif number_of_results == 0:
        return None
    elif require_unique:
        raise ValueError(
            f"Search for '{pattern}' returned:\n{number_of_results} results when exactly one was expected:\n{found}",
        )
    else:
        return found


def _has_auto_tag_information(ds: Dataset, caller_workspace: dict) -> bool:
    """Checks if a dataset has the information required to run .series_names_to_tags()."""
    autotag_config_exists = hasattr(ds, "auto_tag_config")
    attr_var_exists = "attributes" in caller_workspace
    if attr_var_exists and caller_workspace["attributes"]:
        return True
    elif (
        autotag_config_exists
        and hasattr(ds["auto_tag_config"], "attributes")
        and ds["auto_tag_config"]["attributes"]
    ):
        return not is_empty(ds.data)
        # (no need to check separator or regex)
    else:
        return False


def _nw_normalize_dtype(t: IntoDType) -> IntoDType:
    """Normalize datatypes."""
    match t:
        case nw.Int8 | nw.Int16 | nw.Int32:
            out = nw.Int64
        case nw.Float32:
            out = nw.Float64
        case nw.Date:
            out = nw.Datetime
        case _:
            out = t
    return out


# ==============================================================================
if __name__ == "__main__":
    import doctest

    doctest.testmod()
