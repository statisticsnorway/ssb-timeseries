"""Reproducing https://github.com/statisticsnorway/ssb-timeseries/issues/267.

The tag type contract is that :py:data:`SeriesTags` maps series names to
:py:data:`Tags` tag dictionaries, and that ``DatasetTags['series']`` is a
:py:data:`SeriesTags` mapping.
A regression had ``SeriesTagDict = dict[str, TagValue]``, i.e. series names
mapped directly to tag *values* rather than to tag dictionaries, which broke
``filter_tags``/``search_by_tags`` and ``Dataset.series_tags`` at runtime.
"""

from __future__ import annotations

import logging
import uuid
from typing import get_args
from typing import get_type_hints

from ssb_timeseries import meta
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import DatasetTags
from ssb_timeseries.types import SeriesName
from ssb_timeseries.types import SeriesTags
from ssb_timeseries.types import SeriesType
from ssb_timeseries.types import TagKey
from ssb_timeseries.types import Tags
from ssb_timeseries.types import TagValue

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment,index"

test_logger = logging.getLogger(__name__)


def test_tags_maps_tag_keys_to_tag_values() -> None:
    assert get_args(Tags) == (TagKey, TagValue)


def test_series_tags_maps_series_names_to_tag_dicts() -> None:
    assert get_args(SeriesTags) == (SeriesName, Tags)


def test_dataset_tags_series_field_is_series_tags_mapping() -> None:
    series_field = get_type_hints(DatasetTags)["series"]
    assert series_field == SeriesTags
    assert get_args(series_field) == (SeriesName, Tags)


def test_filter_tags_accepts_and_returns_series_tag_dicts() -> None:
    tags: SeriesTags = {
        "series_a": {"unit": "kWh", "country": ["NO", "SE"]},
        "series_b": {"unit": "MWh"},
    }
    result = meta.filter_tags(tags, {"unit": "kWh"})
    assert result == {"series_a": {"unit": "kWh", "country": ["NO", "SE"]}}
    assert all(isinstance(value, dict) for value in result.values())


def test_search_by_tags_returns_series_names() -> None:
    tags: SeriesTags = {
        "series_a": {"unit": "kWh"},
        "series_b": {"unit": "MWh"},
    }
    assert meta.search_by_tags(tags, {"unit": "kWh"}) == ["series_a"]


def test_dataset_series_tags_values_are_tag_dicts() -> None:
    name = f"test-issue-267-{uuid.uuid4().hex}"
    ds = Dataset(
        name=name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags={"unit": "kWh"},
        data=create_df(
            ["a", "b"],
            ["p", "q"],
            start_date="2022-01-01",
            end_date="2022-03-01",
            freq="MS",
        ),
    )

    series_tags = ds.series_tags
    assert series_tags == ds.tags["series"]
    assert series_tags
    for series_name, tags in series_tags.items():
        assert isinstance(series_name, str)
        assert isinstance(tags, dict)
        assert tags["unit"] == "kWh"
