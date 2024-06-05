import logging
import uuid

import pytest

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.meta import Taxonomy
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: disable-error-code="attr-defined,no-untyped-def"


@log_start_stop
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
        tags=set_tags,
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
        tags=set_tags,
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
        ts_logger.debug(f" ... {d[key]}")
        assert d[key]["dataset"] == set_name
        assert d[key]["name"] == key
        assert d[key]["versioning"] == str(x.data_type.versioning)
        assert d[key]["temporality"] == str(x.data_type.temporality)
        assert d[key]["About"] == "ImportantThings"
        assert d[key]["SeriesDifferentiatingAttributes"] == ["A", "B", "C"]
        for x_key, x_value in extra_tags_for_all_series.items():
            assert d[key][x_key] == x_value


@log_start_stop
def test_find_data_using_single_metadata_attribute(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Filter series in set by series tag: {'A': 'a'}."""
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
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        name_pattern=["A", "B", "C"],
    )

    x_filtered_on_attribute_a = x.filter(tags={"A": "a"})
    expected_matches = ["a_p_z", "a_q_z", "a_r_z"]

    ts_logger.debug(
        f"x_filtered_on_attribute_a: \n\t{x_filtered_on_attribute_a.series}\n vs expected:\n\t{expected_matches}"
    )
    assert isinstance(x_filtered_on_attribute_a, Dataset)
    assert sorted(x_filtered_on_attribute_a.numeric_columns()) == sorted(
        expected_matches
    )

    returned_series_tags = x_filtered_on_attribute_a.tags["series"]
    for key in returned_series_tags.keys():
        assert returned_series_tags[key]["dataset"] != set_name  # TODO: update metadata
        assert returned_series_tags[key]["name"] == key
        assert returned_series_tags[key]["A"] == "a"


@log_start_stop
def test_find_data_using_multiple_metadata_attributes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Filter series in set by series tags: {'A': 'a', 'B': 'q'}.

    Returned series should satisfy {'A': 'a'} AND {'B': 'q'}
    """
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
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        name_pattern=["A", "B", "C"],
    )

    x_filtered_on_attribute_a_and_b = x.filter(tags={"A": "a", "B": "q"})
    expected_matches = ["a_q_z"]

    ts_logger.debug(
        f"x_filtered_on_attribute_a: \n\t{x_filtered_on_attribute_a_and_b.series}\n vs expected:\n\t{expected_matches}"
    )
    assert isinstance(x_filtered_on_attribute_a_and_b, Dataset)
    assert sorted(x_filtered_on_attribute_a_and_b.numeric_columns()) == sorted(
        expected_matches
    )

    returned_series_tags = x_filtered_on_attribute_a_and_b.tags["series"]
    for key in returned_series_tags.keys():
        assert returned_series_tags[key]["dataset"] != set_name  # TODO: update metadata
        assert returned_series_tags[key]["name"] == key
        assert returned_series_tags[key]["A"] == "a"
        assert returned_series_tags[key]["B"] == "q"


@log_start_stop
def test_find_data_using_metadata_criteria_with_single_attribute_and_multiple_values(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Filter series in set by series tags: {'A': ['a', 'b']}.

    Returned series should satisfy {'A': 'a'} OR {'A': 'b'}
    """
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
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        name_pattern=["A", "B", "C"],
    )

    x_filtered_on_attribute_a = x.filter(tags={"A": ["a", "b"]})
    expected_matches = ["a_p_z", "a_q_z", "a_r_z", "b_p_z", "b_q_z", "b_r_z"]

    ts_logger.debug(
        f"x_filtered_on_attribute_a: \n\t{x_filtered_on_attribute_a.series}\n vs expected:\n\t{expected_matches}"
    )
    assert isinstance(x_filtered_on_attribute_a, Dataset)
    assert sorted(x_filtered_on_attribute_a.numeric_columns()) == sorted(
        expected_matches
    )

    returned_series_tags = x_filtered_on_attribute_a.tags["series"]
    for key in returned_series_tags.keys():
        assert returned_series_tags[key]["dataset"] != set_name  # TODO: update metadata
        assert returned_series_tags[key]["name"] == key
        assert (
            returned_series_tags[key]["A"] == "a"
            or returned_series_tags[key]["A"] == "b"
        )


@pytest.mark.skip(reason="Not ready yet.")
@log_start_stop
def test_update_metadata_attributes() -> None:
    # TO DO:
    # Updating metadata by changing an attribute value should
    # ... update metadata.json
    # ... keep previous version

    ts_logger.debug("don't worrry, be happy ...")
    raise AssertionError()


@pytest.mark.skip(reason="Not ready yet.")
def test_updated_tags_propagates_to_column_names_accordingly() -> None:
    # TO DO:
    # my_dataset.update_metadata('column_name', 'metadata_tag')
    # ... --> versioning

    ts_logger.debug("don't worrry, be happy ...")
    raise AssertionError()


@pytest.mark.skip(reason="Not ready yet.")
def test_aggregate_sum_for_flat_list_taxonomy(
    caplog,
) -> None: ...


@log_start_stop
def test_aggregate_sums_for_hierarchical_taxonomy(
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass157_leaves, "B": ["q"], "C": ["z"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        name_pattern=["A", "B", "C"],
    )

    assert len(x.numeric_columns()) == len(klass157_leaves)

    y = x.aggregate("A", klass157, "sum")
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns()) == len(klass157.parent_nodes())
    assert sorted(y.numeric_columns()) == sorted(
        [f"sum({n.name})" for n in klass157.parent_nodes()]
    )
    y_data = y.data[y.numeric_columns()]
    ts_logger.debug(f"{set_name} --> \n{y_data}")
    assert all(y_data.notna())
    assert all(y_data.notnull())


@log_start_stop
def test_aggregate_mean_for_hierarchical_taxonomy(
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass157_leaves, "B": ["pq"], "C": ["xyz"]}
    tag_values: list[list[str]] = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=create_df(
            *tag_values, start_date="2020-01-01", end_date="2024-01-03", freq="YS"
        ),
        tags=set_tags,
        name_pattern=["A", "B", "C"],
    )

    assert len(x.numeric_columns()) == len(klass157_leaves)

    y = x.aggregate("A", klass157, "mean")
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns()) == len(klass157.parent_nodes())
    assert sorted(y.numeric_columns()) == sorted(
        [f"mean({n.name})" for n in klass157.parent_nodes()]
    )
    y_data = y.data[y.numeric_columns()]
    ts_logger.debug(f"{set_name} --> \n{y_data}")
    assert all(y_data.notna())
    assert all(y_data.notnull())


@log_start_stop
def test_aggregate_multiple_methods_for_hierarchical_taxonomy(
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]

    set_name = conftest.function_name()
    set_tags = {
        "Country": "Norway",
    }
    series_tags = {"A": klass157_leaves, "B": ["pq"], "C": ["xyz"]}
    tag_values = [value for value in series_tags.values()]

    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        tags=set_tags,
        data=create_df(
            *tag_values, start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
        name_pattern=["A", "B", "C"],
    )
    # ts_logger.debug(f"Dataset: {x.name}\nSeries:\n{x.tags['series']}\nx.tags")

    assert len(x.numeric_columns()) == len(klass157_leaves)
    multiple_functions = ["count", "sum", "mean"]
    y = x.aggregate(
        attribute="A",
        taxonomy=klass157,
        aggregate_function=multiple_functions,
    )
    assert isinstance(y, Dataset)
    assert len(y.numeric_columns()) == len(
        klass157.parent_nodes() * len(multiple_functions)
    )
    # TODO: double check this:
    # assert sorted(y.numeric_columns()) == sorted(
    #     [n.name for n in klass157.parent_nodes()]
    # )
    y_data = y.data[y.numeric_columns()]
    ts_logger.debug(f"{set_name} --> \n{y_data}")
    assert all(y_data.notna())
    assert all(y_data.notnull())
    assert all(y_data["mean(12.3)"] == y_data["sum(12.3)"] / y_data["count(12.3)"])
