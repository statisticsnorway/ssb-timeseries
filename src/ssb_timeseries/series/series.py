"""The individual Series of a Dataset.

Tags and technical properties are controlled through the Dataset.
Series should generally only be initialized through the Dataset. The typical use case is `Dataset.__iter__`.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import narwhals as nw

from ..dataframes import eager
from ..dataframes.date_cols import temporal_columns
from ..types import SeriesType

if TYPE_CHECKING:
    import pandas
    import polars
    import pyarrow
    from narwhals.typing import Frame


class Series:
    """An individual `Series` from a Dataset."""

    def __init__(
        self,
        name: str,
        data_type: SeriesType,
        tags: dict,
        data: Frame,
        as_of_utc: datetime | None = None,
    ) -> None:
        """Create a new Series object."""
        self.name = name
        self.data_type = data_type
        self.as_of_utc = as_of_utc
        self.tags = tags
        self.data = nw.from_native(data)

    @property
    def dataset(self) -> str:
        """Return the name of the Dataset the Series belongs to."""
        return self.tags["dataset"]

    @property
    def long_name(self) -> str:
        """Return the concatenation of Dataset name and Series name."""
        return f"{self.tags['dataset']}::{self.name}"

    @property
    def nw(self) -> Frame:
        """Returns Series as a (new) Narwhals Frame."""
        return nw.from_native(self.data)

    @property
    def pa(self) -> pyarrow.Table:
        """Returns Series as a (new) Arrow table."""
        return eager(self.data).to_arrow()

    @property
    def pd(self) -> pandas.DataFrame:
        """Returns Series as a (new) Pandas dataframe."""
        return eager(self.data).to_pandas()

    @property
    def pl(self) -> polars.DataFrame:
        """Returns Series as a (new) Polars dataframe."""
        return eager(self.data).to_polars()

    def nixtla(self) -> pandas.DataFrame:
        """Returns Series in a Pandas DataFrame with Nixtla long format."""
        df = eager(self.data)
        date_cols = temporal_columns(df)
        return (
            df.select([date_cols[0], self.name])
            .rename(
                {
                    date_cols[0]: "ds",
                    self.name: "y",
                }
            )
            .with_columns(nw.lit(self.long_name).alias("unique_id"))
            .to_pandas()
        )
