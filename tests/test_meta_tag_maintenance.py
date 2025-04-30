# mypy: ignore-errors = True

import logging
import uuid

import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# ---mypy: disable-error-code="attr-defined,no-untyped-def,union-attr,index,call-overload"

test_logger = logging.getLogger(__name__)


def test_init_dataset_returns_expected_set_level_tags(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = f"test-setlevel-tags-{uuid.uuid4().hex}"
    set_tags = {
        "About": "ImportantThings",
        "SeriesDifferentiatingAttributes": ["A", "B", "C"],
        "Country": "Norway",
    }
    series_tags_permutation_space = {
        "A": ["a", "b", "c"],
        "B": ["p", "q", "r"],
        "C": ["z"],
    }
    tag_values: list[list[str]] = [
        value for value in series_tags_permutation_space.values()
    ]
    extra_tags_for_all_series = {"unit": "kWh"}
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        dataset_tags=set_tags,
        series_tags=extra_tags_for_all_series,
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
    test_logger.debug(f"tags: {x.tags}")
    assert x.tags["name"] == set_name
    assert x.tags["versioning"] == str(x.data_type.versioning)
    assert x.tags["temporality"] == str(x.data_type.temporality)
    assert x.tags["About"] == "ImportantThings"
    assert x.tags["SeriesDifferentiatingAttributes"] == ["A", "B", "C"]


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
    series_tags_permutation_space = {
        "A": ["a", "b", "c"],
        "B": ["p", "q", "r"],
        "C": ["z"],
    }
    tag_values: list[list[str]] = [
        value for value in series_tags_permutation_space.values()
    ]
    extra_tags_for_all_series = {"unit": "kWh"}

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        dataset_tags=set_tags,
        series_tags=extra_tags_for_all_series,
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
    assert x.series_tags == x.tags["series"]

    # each numeric column should be a key in x.tags["series"]
    d = x.tags["series"]
    assert [key for key in d.keys()].sort() == x.numeric_columns().sort()

    for key in d.keys():
        test_logger.debug(f" ... {d[key]}")
        assert d[key]["dataset"] == set_name
        assert d[key]["name"] == key
        assert d[key]["versioning"] == str(x.data_type.versioning)
        assert d[key]["temporality"] == str(x.data_type.temporality)
        assert d[key]["About"] == "ImportantThings"
        assert d[key]["SeriesDifferentiatingAttributes"] == ["A", "B", "C"]
        for x_key, x_value in extra_tags_for_all_series.items():
            assert d[key][x_key] == x_value


def test_tag_set_with_kwargs(
    new_dataset_as_of_at: Dataset, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    new_dataset_as_of_at.tag_dataset(
        example_1="string_1",
        example_2=["a", "b", "c"],
    )
    test_logger.debug(f"tags: {new_dataset_as_of_at.tags}")

    # check that the tags are applied to the dataset
    assert new_dataset_as_of_at.tags["example_1"] == "string_1"
    assert new_dataset_as_of_at.tags["example_2"] == ["a", "b", "c"]
    # check that the tags propagate to series in set
    for series_tags in new_dataset_as_of_at.tags["series"].values():
        assert series_tags["example_1"] == "string_1"
        assert series_tags["example_2"] == ["a", "b", "c"]


def test_tag_set_with_dict(
    new_dataset_as_of_at: Dataset, caplog: pytest.LogCaptureFixture
) -> None:
    new_dataset_as_of_at.tag_dataset(
        tags={"example_1": "string_1", "example_2": ["a", "b", "c"]}
    )
    caplog.set_level(logging.DEBUG)
    # check that the tags are applied to the dataset
    assert new_dataset_as_of_at.tags["example_1"] == "string_1"
    assert new_dataset_as_of_at.tags["example_2"] == ["a", "b", "c"]

    # check that the tags propagate to series in set
    for series_tags in new_dataset_as_of_at.tags["series"].values():
        assert series_tags["example_1"] == "string_1"
        assert series_tags["example_2"] == ["a", "b", "c"]


def test_tag_set_with_both_dict_and_kwargs(new_dataset_as_of_at: Dataset) -> None:
    new_dataset_as_of_at.tag_dataset(
        tags={"example_1": "string_1"}, example_2=["a", "b", "c"]
    )
    # check that the tags are applied to the dataset
    assert new_dataset_as_of_at.tags["example_1"] == "string_1"
    assert new_dataset_as_of_at.tags["example_2"] == ["a", "b", "c"]

    # check that the tags propagate to series in set
    for series_tags in new_dataset_as_of_at.tags["series"].values():
        assert series_tags["example_1"] == "string_1"
        assert series_tags["example_2"] == ["a", "b", "c"]


def test_tagging_set_second_time_appends(
    new_dataset_as_of_at: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    new_dataset_as_of_at.tag_dataset(
        tags={"example_1": "string_1"}, example_2=["a", "b", "c"]
    )
    new_dataset_as_of_at.tag_dataset(tags={"example_1": "string_2"})
    # check that the new tag is applied correctly to the dataset
    assert new_dataset_as_of_at.tags["example_1"] == ["string_1", "string_2"]
    # while the second tag stays the same
    assert new_dataset_as_of_at.tags["example_2"] == ["a", "b", "c"]

    # ... and that this is true also for the series in set
    for series_tags in new_dataset_as_of_at.tags["series"].values():
        assert series_tags["example_1"] == ["string_1", "string_2"]
        assert series_tags["example_2"] == ["a", "b", "c"]


def test_tagging_with_empty_dict_does_nothing(
    new_dataset_as_of_at: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    new_dataset_as_of_at.tag_dataset(example_1="string_1", example_2=["a", "b", "c"])
    new_dataset_as_of_at.tag_dataset(tags={})
    new_dataset_as_of_at.tag_dataset()

    # check that set tags stay the same
    assert new_dataset_as_of_at.tags["name"] == "test-new-dataset-as-of-at"
    assert new_dataset_as_of_at.tags["example_1"] == "string_1"
    assert new_dataset_as_of_at.tags["example_2"] == ["a", "b", "c"]

    # ... and that this is true also for the series in set
    for series_tags in new_dataset_as_of_at.tags["series"].values():
        assert series_tags["dataset"] == "test-new-dataset-as-of-at"
        assert series_tags["example_1"] == "string_1"
        assert series_tags["example_2"] == ["a", "b", "c"]


def test_detag_dataset_arg_removes_single_value_tags(
    existing_small_set: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    # check that TAGS EXIST BEFORE
    assert existing_small_set.tags["E"] == "e"
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags["E"] == "e"

    existing_small_set.detag_dataset("E")
    test_logger.debug(f"Tags:\n\tE: {existing_small_set.tags.get('E')}")

    # ... BUT NOT AFTER
    assert existing_small_set.tags.get("E") is None
    for series_tags in existing_small_set.tags["series"].values():
        test_logger.debug(series_tags)
        assert series_tags.get("E") is None


def test_detag_dataset_arg_removes_all_list_values_from_tags(
    existing_small_set: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    # check that TAGS EXIST BEFORE
    assert existing_small_set.tags["F"] == ["f1", "f2"]
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags["F"] == ["f1", "f2"]

    # existing_small_set.detag_dataset("F")
    existing_small_set.detag_dataset("F")
    test_logger.debug(f"Tags:\n\tF: {existing_small_set.tags.get('F')}")

    # ... BUT NOT AFTER
    assert existing_small_set.tags.get("F") is None
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags.get("F") is None


def test_detag_dataset_kwarg_removes_single_value_tags(
    existing_small_set: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    # check that TAGS EXIST BEFORE
    assert existing_small_set.tags["E"] == "e"
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags["E"] == "e"

    existing_small_set.detag_dataset(E="e")
    test_logger.debug(f"Tags:\n\tE: {existing_small_set.tags.get('E')}")

    # ... BUT NOT AFTER (value is removed --> attribure is removed)
    assert existing_small_set.tags.get("E") is None
    for series_tags in existing_small_set.tags["series"].values():
        test_logger.debug(series_tags)
        assert series_tags.get("E") is None


def test_detag_dataset_kwarg_removes_all_list_values_from_tags(
    existing_small_set: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    # check that expected TAGS EXIST BEFORE
    assert existing_small_set.tags["F"] == ["f1", "f2"]
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags["F"] == ["f1", "f2"]

    # existing_small_set.detag_dataset("F")
    existing_small_set.detag_dataset(F=["f1", "f2"])
    test_logger.debug(f"Tags:\n\tF: {existing_small_set.tags.get('F')}")

    # ... BUT NOT AFTER (entire list is removed --> attribute is removed)
    assert existing_small_set.tags.get("F") is None
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags.get("F") is None


def test_detag_dataset_kwarg_removes_only_specified_value_from_tag_with_multiple_values(
    existing_small_set: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    # check that expected TAGS EXIST BEFORE
    assert existing_small_set.tags["F"] == ["f1", "f2"]
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags["F"] == ["f1", "f2"]

    existing_small_set.detag_dataset(F="f1")
    test_logger.debug(f"Tags:\n\tF: {existing_small_set.tags.get('F')}")

    # ... BUT NOT AFTER (only 'f1' has been removed,
    # the attribute remains, now left with a *string* value 'f2')
    assert existing_small_set.tags.get("F") == "f2"
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags.get("F") == "f2"


def test_detag_series_removes_tags_from_series_but_not_from_set(
    existing_small_set: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    # tag the set!
    existing_small_set.tag_dataset(example_1="string_1", example_2=["a", "b", "c"])

    # check that the tags are applied to the set
    # ... and the series
    assert existing_small_set.tags["example_1"] == "string_1"
    assert existing_small_set.tags["example_2"] == ["a", "b", "c"]
    for series_tags in existing_small_set.tags["series"].values():
        assert series_tags["example_1"] == "string_1"
        assert series_tags["example_2"] == ["a", "b", "c"]

    existing_small_set.save()
    # detag the series!
    y = Dataset(existing_small_set.name)
    y.detag_series("example_1", example_2="b")

    # check that the tags are removed from the series
    # ..but not from the set
    test_logger.debug(f"existing_small_set.tags: {existing_small_set.tags}")
    for series_tags in y.tags["series"].values():
        assert series_tags.get("example_1") is None
        assert series_tags["example_2"] == ["a", "c"]
    assert y.tags["example_1"] == "string_1"
    assert y.tags["example_2"] == ["a", "b", "c"]


def test_retag_dataset(
    existing_small_set: Dataset,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    x = existing_small_set.filter(tags={"A": "a1"})
    test_logger.debug(f"Series:\t{x.tags}")
    assert x.series == ["a1_b_c"]

    existing_small_set.replace_tags(({"A": "a1"}, {"A1": "a11", "A2": "a21"}))
    test_logger.debug(f"Series:\t{existing_small_set['a1_b_c'].tags}")

    assert existing_small_set.filter(tags={"A": "a1"}).series == []
    assert existing_small_set.filter(tags={"A1": "a11"}).series == ["a1_b_c"]
    assert existing_small_set.filter(tags={"A2": "a21"}).series == ["a1_b_c"]
