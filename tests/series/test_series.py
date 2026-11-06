"""Tests the Series class."""

import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import AS_OF_AT
from ssb_timeseries.types import NONE_AT
from ssb_timeseries.types import NONE_FROM_TO
from ssb_timeseries.types import SeriesType
from ssb_timeseries.types import Versioning


def _new_dataset(
    data_type: SeriesType, name: str, implementation: str = "pandas"
) -> Dataset:
    """Return an in-memory dataset of the given type with the series x and y."""
    return Dataset(
        name=name,
        data_type=data_type,
        as_of_tz=date_utc("2022-01-01")
        if data_type.versioning == Versioning.AS_OF
        else None,
        data=create_df(
            ["x", "y"],
            start_date="2022-01-01",
            end_date="2022-04-03",
            freq="MS",
            temporality=data_type.temporality,
            implementation=implementation,
        ),
    )


def test_iterating_a_dataset_yields_one_series_per_series_name() -> None:
    """Each Series carries only its own data column, plus the time column."""
    dataset_name = "test-series-iteration"
    dataset = Dataset(
        name=dataset_name,
        data_type=NONE_AT,
        data=create_df(
            ["x", "y"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )

    names = []
    for series in dataset:
        names.append(series.name)
        assert series.dataset == dataset_name
        assert series.long_name == f"{dataset_name}::{series.name}"
        assert series.name in series.pa.column_names
        assert len(series.pa.column_names) == 2
        assert series.pa.num_rows == len(dataset.data)

    assert names == ["x", "y"]


@pytest.mark.parametrize("implementation", ["pandas", "polars", "pyarrow"])
@pytest.mark.parametrize(
    ("data_type", "temporal_columns"),
    [
        (NONE_AT, ["valid_at"]),
        (NONE_FROM_TO, ["valid_from", "valid_to"]),
    ],
)
def test_series_carries_the_temporal_columns_and_its_own_values(
    data_type: SeriesType, temporal_columns: list[str], implementation: str
) -> None:
    """The projection holds the time columns and the series' own values only."""
    dataset = _new_dataset(
        data_type,
        f"test-series-projection-{data_type.temporality.name}-{implementation}",
        implementation,
    )

    series = next(iter(dataset))

    assert series.pa.column_names == [*temporal_columns, series.name]


@pytest.mark.parametrize(
    ("data_type", "versioned"),
    [(NONE_AT, False), (AS_OF_AT, True)],
)
def test_series_carries_the_dataset_type_and_the_as_of_timestamp(
    data_type: SeriesType, versioned: bool
) -> None:
    """The type is the dataset's, and the as-of is carried only when versioned."""
    dataset = _new_dataset(data_type, f"test-series-type-{data_type.versioning.name}")

    series = next(iter(dataset))

    assert series.data_type is dataset.data_type
    if versioned:
        assert series.as_of_utc is not None
        assert series.as_of_utc == dataset.as_of_utc
    else:
        assert series.as_of_utc is None


def test_mutating_a_series_does_not_change_the_dataset() -> None:
    """The Series' frame is a copy, not a view of the dataset's data."""
    dataset = _new_dataset(NONE_AT, "test-series-detached-from-dataset")
    series = next(iter(dataset))
    before = dataset.pd[series.name].tolist()

    series.data.to_native().loc[0, series.name] = -1

    assert dataset.pd[series.name].tolist() == before


def test_mutating_the_dataset_does_not_change_a_produced_series() -> None:
    """A Series keeps the values it was produced with."""
    dataset = _new_dataset(NONE_AT, "test-series-detached-from-later-changes")
    series = next(iter(dataset))
    before = series.pd[series.name].tolist()

    dataset.data.loc[0, series.name] = -1

    assert series.pd[series.name].tolist() == before


def test_series_tags_are_a_detached_copy_of_the_dataset_tags() -> None:
    """Mutating a Series' tags leaves the dataset unchanged, nested values included."""
    dataset = _new_dataset(NONE_AT, "test-series-detached-tags")
    dataset.tags["series"]["x"]["aliases"] = ["x1", "x2"]

    series = next(iter(dataset))
    series.tags["aliases"].append("x3")
    series.tags["status"] = "changed"

    assert series.tags is not dataset.series_tags["x"]
    assert dataset.series_tags["x"]["aliases"] == ["x1", "x2"]
    assert "status" not in dataset.series_tags["x"]
