import logging
import uuid

import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors


@log_start_stop
def test_init_dataset_returns_expected_set_level_tags(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = f"test-setlevel-tags-{uuid.uuid4().hex}"
    set_tags = {
        "About": "ImportantThings",
        "SeriesDifferentiatingAttributes": ["A", "B", "C"],
        "Country": "Norway",
    }
    series_tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        series_tags=series_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )

    # tags exist
    assert x.tags
    assert x.series_tags

    """ We expect DATASET tags on the form:
    {
        'name': 'test-datetimecols-eec23c979e7b4976b77d48f005ffd7b2',
        'versioning': 'AS_OF',
        'temporality': 'AT',
        'About': 'ImportantThings',
        'SeriesDifferentiatingAttributes': ['A', 'B', 'C']
    }
    with *at least* the above provided attributes (possibly / quite likely many more)
    """
    assert x.tags["name"] == set_name
    assert x.tags["versioning"] == str(x.data_type.versioning)
    assert x.tags["temporality"] == str(x.data_type.temporality)
    assert x.tags["About"] == "ImportantThings"
    assert x.tags["SeriesDifferentiatingAttributes"] == ["A", "B", "C"]


@log_start_stop
def test_init_dataset_returns_mandatory_series_tags_plus_tags_inherited_from_dataset(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = f"test-mandatory-plus-inherited-{uuid.uuid4().hex}"
    set_tags = {
        "About": "ImportantThings",
        "SeriesDifferentiatingAttributes": [
            "A",
            "B",
            "C",
        ],  # it might be a good idea to include something of the sort?
        "Country": "Norway",
    }
    series_tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        series_tags=series_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        name_pattern=["A", "B", "C"],
    )

    """ we expect SERIES tags to include most of the dataset tags:
    {<series_name>: {
        'dataset': 'test-datetimecols-eec23c979e7b4976b77d48f005ffd7b2',
        'name': <series name>,
        'versioning': 'AS_OF',
        'temporality': 'AT',
        'About': 'ImportantThings',
        'SeriesDifferentiatingAttributes': ['A', 'B', 'C'],
        **{ any series specific tags passed to init via the `series_tags` parameter}
    }

    """
    assert x.series_tags() == x.tags["series"]

    # each numeric column should be a key in x.tags["series"]
    d = x.tags["series"]
    assert [key for key in d.keys()].sort() == x.numeric_columns().sort()

    for key in d.keys():
        ts_logger.debug(f" ... {d[key]}")
        assert d[key]["dataset"] == set_name
        assert d[key]["name"] == key
        assert d[key]["versioning"] == str(x.data_type.versioning)
        assert d[key]["temporality"] == str(x.data_type.temporality)
        assert d[key]["About"] == "ImportantThings"
        assert d[key]["SeriesDifferentiatingAttributes"] == ["A", "B", "C"]


@log_start_stop
def test_find_data_using_metadata_attributes(caplog: pytest.LogCaptureFixture) -> None:
    """Filter series in set by series tags."""
    caplog.set_level(logging.DEBUG)

    set_name = f"test-datetimecols-{uuid.uuid4().hex}"
    set_tags = {
        "About": "ImportantThings",
        "Country": "Norway",
    }
    series_tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        series_tags=series_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        name_pattern=["A", "B", "C"],
    )

    x_attr_A_equals_a = x.filter(tags={"A": "a"})
    expected_matches = ["a_p_z", "a_q_z", "a_r_z"]

    ts_logger.debug(
        f"x_attr_A_equals_a: \n\t{x_attr_A_equals_a.series()}\n vs expected:\n\t{expected_matches}"
    )
    assert isinstance(x_attr_A_equals_a, Dataset)
    assert sorted(x_attr_A_equals_a.numeric_columns()) == sorted(expected_matches)

    returned_series_tags = x_attr_A_equals_a.tags["series"]
    for key in returned_series_tags.keys():
        assert returned_series_tags[key]["dataset"] != set_name  # TODO: update metadata
        assert returned_series_tags[key]["name"] == key
        assert returned_series_tags[key]["A"] == "a"

    # raise AssertionError("In order to see DEBUG logs while testing.")


@pytest.mark.skipif(True, reason="Not ready yet.")
@log_start_stop
def test_update_metadata_attributes() -> None:
    # TO DO:
    # Updating metadata by changing an attribute value should
    # ... update metadata.json
    # ... keep previous version

    ts_logger.debug("don't worrry, be happy ...")
    raise AssertionError()


@pytest.mark.skipif(True, reason="Not ready yet.")
def test_updated_tags_propagates_to_column_names_accordingly() -> None:
    # TO DO:
    # my_dataset.update_metadata('column_name', 'metadata_tag')
    # ... --> versioning

    ts_logger.debug("don't worrry, be happy ...")
    raise AssertionError()
