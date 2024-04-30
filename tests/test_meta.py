import logging
import os
import uuid

import pytest
from bigtree import get_tree_diff
from bigtree import print_tree

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc  # , now_utc, date_round
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.meta import Taxonomy
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors


@log_start_stop
def test_init_dataset_returns_expected_set_level_tags(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = f"test-datetimecols-{uuid.uuid4().hex}"
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
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = f"test-datetimecols-{uuid.uuid4().hex}"
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
def test_read_flat_codes_from_klass() -> None:
    activity = Taxonomy(697)
    ts_logger.debug(f"captured ...\n{activity.entities}")

    assert activity.entities.shape == (16, 9)
    assert activity.structure.max_depth == 2
    assert activity.structure.root.name == "0"


@log_start_stop
def test_read_hierarchical_codes_from_klass() -> None:
    energy_balance = Taxonomy(157)
    ts_logger.debug(f"captured ...\n{energy_balance.print_tree()}")

    assert energy_balance.structure.root.name == "0"
    assert energy_balance.structure.max_depth == 4
    assert energy_balance.structure.max_depth == 4
    assert energy_balance.structure.max_depth == 4


@log_start_stop
def test_replace_chars_in_flat_codes(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    k697 = Taxonomy(
        id_or_path=697,
        substitute={
            "_": ".",
            "aa": "å",
            "lagerf": "lagerføring",
            "lagere": "lagerendring",
        },  # multiple replacements! --> generate substitution dict from json file if required
    )
    k697_names = [n.name for n in k697.structure.root.children]
    ts_logger.debug(f"klass 697 codes:\n{k697_names}")
    ts_logger.debug(
        f"tree ...\n{print_tree(k697.structure.root, attr_list=['fullname'])}"
    )

    assert sorted(k697_names) == sorted(
        [
            "bruk.omvandl",
            "bruk.råstoff",  # changed!
            "bruk.red",
            "bruk.stasj",
            "bruk.trans",
            "eksport",
            "import",
            "lagerendring",  # changed!
            "lagerføring",  # changed!
            "prod.pri",
            "prod.sek",
            "svinn.annet",
            "svinn.distr",
            "svinn.fakl",
            "svinn.lager",
        ]
    )


@log_start_stop
def test_replace_chars_in_hierarchical_codes(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    k157 = Taxonomy(
        id_or_path=157,
        substitute={
            ".": "/",
        },
    )
    # compare for leaf nodes of sub tree
    k157_names = [n.name for n in k157.structure.root["1"].leaves]
    ts_logger.debug(f"klass 157 codes:\n{k157_names}")
    ts_logger.debug(
        f"tree ...\n{print_tree(k157.structure.root['1'], attr_list=['fullname'])}"
    )

    assert sorted(k157_names) == sorted(
        [
            "1/1/1",
            "1/1/2",
            "1/1/3",
            "1/2",
        ]
    )


@log_start_stop
def test_hierarchical_codes_retrieved_from_klass_and_reloaded_from_json_file_are_identical(
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)

    temp_file = f"temp-{uuid.uuid4()}.json"
    try:
        klass157.save(temp_file)
        file157 = Taxonomy(temp_file)
    finally:
        os.remove(temp_file)

    # compare all leaf nodes of sub tree
    k157_names = [n.name for n in klass157.structure.root.leaves]
    f157_names = [n.name for n in file157.structure.root.leaves]
    assert k157_names == f157_names

    ts_logger.debug(f"klass157 ...\n{print_tree(klass157.structure)}")
    ts_logger.debug(f"file157 ...\n{print_tree(file157.structure)}")

    diff = get_tree_diff(klass157.structure, file157.structure)
    if diff:
        ts_logger.debug(f"diff:\n{print_tree(diff)}")
        # --> assert should fail
    else:
        ts_logger.debug(f"diff: {diff}")
        # --> assert should pass

    assert klass157 == file157


@pytest.mark.skipif(True, reason="Not ready yet.")
@log_start_stop
def test_find_data_using_metadata_attributes() -> None:
    # metadata - test extendeded attribute set
    # find data via metadata
    # metadata = my_dataset.metadata

    ts_logger.debug("don't worrry, be happy ...")
    assert True


@pytest.mark.skipif(True, reason="Not ready yet.")
@log_start_stop
def test_update_metadata_attributes() -> None:
    # TO DO:
    # Updating metadata by changing an attribute value should
    # ... update metadata.json
    # ... keep previous version

    ts_logger.debug("don't worrry, be happy ...")
    assert True


@pytest.mark.skipif(True, reason="Not ready yet.")
def test_updated_tags_propagates_to_column_names_accordingly() -> None:
    # TO DO:
    # my_dataset.update_metadata('column_name', 'metadata_tag')
    # ... --> versioning

    ts_logger.debug("don't worrry, be happy ...")
    assert True
