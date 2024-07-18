import logging
import uuid

import bigtree
import pytest
from bigtree import get_tree_diff
from bigtree import print_tree

from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.meta import Taxonomy
from ssb_timeseries.meta import filter_tags
from ssb_timeseries.meta import search_by_tags
from ssb_timeseries.meta import to_tag_value
from ssb_timeseries.meta import unique_tag_values

# mypy: disable-error-code="no-untyped-def,attr-defined,func-returns-value,operator"


@log_start_stop
def test_search_by_tags(new_dataset_none_at, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    series_tags = new_dataset_none_at.tags["series"]
    ts_logger.debug(f"test filter_by_tags ... series tags: {series_tags}")
    out = search_by_tags(series_tags, {"A": ["a", "b"], "B": "p", "C": "z1"})
    assert isinstance(out, list)
    assert sorted(out) == sorted(["a_p_z1", "b_p_z1"])


@log_start_stop
def test_filter_tags(new_dataset_none_at, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    series_tags = new_dataset_none_at.tags["series"]
    ts_logger.debug(f"test filter_by_tags ... series tags: {series_tags}")
    out = filter_tags(series_tags, {"A": ["a", "b"], "B": "q", "C": "z1"})
    ts_logger.debug(f"test filter_by_tags ... {out}")
    assert isinstance(out, dict)
    assert sorted(out) == sorted(
        {
            "b_q_z1": {
                "name": "b_q_z1",
                "dataset": "test-existing-dataset-none-at",
                "versioning": "NONE",
                "temporality": "AT",
                "A": "b",
                "B": "q",
                "C": "z1",
                "D": "d",
            },
            "a_q_z1": {
                "name": "a_q_z1",
                "dataset": "test-existing-dataset-none-at",
                "versioning": "NONE",
                "temporality": "AT",
                "A": "a",
                "B": "q",
                "C": "z1",
                "D": "d",
            },
        }
    )


@log_start_stop
def test_read_flat_code_list_from_klass_returns_two_level_tree() -> None:
    activity = Taxonomy(697)
    ts_logger.debug(f"captured ...\n{activity.entities}")

    assert activity.entities.shape == (16, 9)
    assert activity.structure.max_depth == 2
    assert activity.structure.root.name == "0"
    assert activity.structure.diameter == 2


@log_start_stop
def test_read_hierarchical_code_set_from_klass_returns_multi_level_tree() -> None:
    energy_balance = Taxonomy(157)
    ts_logger.debug(f"captured ...\n{energy_balance.print_tree()}")

    assert energy_balance.structure.root.name == "0"
    assert energy_balance.structure.diameter == 6
    assert energy_balance.structure.max_depth == 4
    assert energy_balance.structure.max_depth == 4


@log_start_stop
def test_taxonomy_subtree(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)
    klass157_subtree = klass157.subtree("1.1")
    ts_logger.debug(f"tree ...\n{klass157_subtree}")
    assert isinstance(klass157_subtree, bigtree.Node)
    assert klass157_subtree.root.name == "1.1"
    assert klass157_subtree.diameter == 2
    assert klass157_subtree.max_depth == 2

    assert [n.name for n in klass157_subtree.leaves] == ["1.1.1", "1.1.2", "1.1.3"]


@log_start_stop
def test_get_leaf_nodes_from_hierarchical_klass_taxonomy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    tree = Taxonomy(157).structure
    leaves = [n.name for n in tree.root["1"].leaves]
    # ts_logger.debug(f"{print_tree(tree)} ...\n{leaves}")
    ts_logger.debug(f"{tree} ...\n{leaves}")

    assert leaves == [
        "1.1.1",
        "1.1.2",
        "1.1.3",
        "1.2",
    ]


@log_start_stop
def test_get_leaf_nodes_from_middle_of_hierarchical_klass_taxonomy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)
    subtree = klass157.subtree("1.1")
    leaves = [n.name for n in subtree.root.leaves]
    # ts_logger.debug(f"{print_tree(subtree)} ...\n{leaves}")

    assert leaves == [
        "1.1.1",
        "1.1.2",
        "1.1.3",
    ]


@log_start_stop
def test_get_item_from_hierarchical_klass_taxonomy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)
    tree = klass157.structure
    node_1_1 = bigtree.find_name(tree.root, "1.1")

    assert [n.name for n in node_1_1.children] == ["1.1.1", "1.1.2", "1.1.3"]

    assert klass157["1.1"] == node_1_1


@log_start_stop
def test_get_parent_nodes_from_hierarchical_klass_taxonomy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)
    tree = klass157.structure

    leaf_nodes = [n.name for n in tree.root.leaves]
    all_nodes = [n.name for n in tree.root.descendants] + [tree.root.name]
    parent_nodes = [n for n in all_nodes if n not in leaf_nodes]
    parent_nodes2 = [n.name for n in klass157.parent_nodes()]

    ts_logger.debug(f"All nodes:\n\t{all_nodes}")

    assert len(all_nodes) == len(leaf_nodes) + len(parent_nodes)
    assert sorted(parent_nodes) == sorted(parent_nodes2)


@pytest.mark.xfail(reason="Tree diff error?")
@log_start_stop
def test_taxonomy_minus_subtree(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)
    klass157_subtree = klass157.subtree("1.1")
    ts_logger.warning(f"tree ...\n{klass157_subtree}")
    # rest = bigtree.get_tree_diff(klass157.structure.root, klass157_subtree)
    # rest = klass157.structure.root - klass157_subtree
    rest = klass157 - klass157_subtree
    assert isinstance(rest, bigtree.Node)


@log_start_stop
def test_replace_chars_in_flat_codes(caplog: pytest.LogCaptureFixture) -> None:
    """The substitute parameter of Taxonomy init allows making changes to codes when reading a taxonomy list.

    While best practice calls for direct match to KLASS, this allows mappings with (minor) deviations.
    """
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
            "bruk.råstoff",  # changed by substitution above!
            "bruk.red",
            "bruk.stasj",
            "bruk.trans",
            "eksport",
            "import",
            "lagerendring",  # changed by substitution above!
            "lagerføring",  # changed by substitution above!
            "prod.pri",
            "prod.sek",
            "svinn.annet",
            "svinn.distr",
            "svinn.fakl",
            "svinn.lager",
        ]
    )


@log_start_stop
def test_replace_chars_in_hierarchical_codes(caplog: pytest.LogCaptureFixture) -> None:
    """The substitute parameter of Taxonomy init allows making changes to codes when reading a taxonomy list.

    While best practice calls for direct match to KLASS, this allows mappings with (minor) deviations.
    """
    caplog.set_level(logging.DEBUG)
    k157 = Taxonomy(
        id_or_path=157,
        substitute={
            ".": "/",
        },
    )
    # compare for leaf nodes of sub tree
    # k157_names = [n.name for n in k157.structure.root["1"].leaves]
    k157_names = [n.name for n in k157["1"].leaves]
    ts_logger.debug(f"klass 157 codes:\n{k157_names}")
    ts_logger.debug(
        f"tree ...\n{print_tree(k157.structure['1'], attr_list=['fullname'])}"
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
    caplog: pytest.LogCaptureFixture, tmp_path_factory: pytest.TempPathFactory
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(157)

    temp_file = tmp_path_factory.mktemp("temp") / f"temp-{uuid.uuid4()}.json"

    klass157.save(temp_file)
    file157 = Taxonomy(temp_file)
    # finally:
    #    fs.rm(temp_file)

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


def test_to_tag_value__for_list_with_multiple_values_returns_list() -> None:
    assert to_tag_value(["a", "b", "c"]) == ["a", "b", "c"]


def test_to_tag_value_for_set_with_multiple_values_returns_list() -> None:
    assert to_tag_value(set(["a", "b", "c"])) == ["a", "b", "c"]


def test_to_tag_value_for_set_with_one_value_returns_string() -> None:
    assert to_tag_value(set(["abc"])) == "abc"


def test_to_tag_value_for_list_with_one_value_returns_string() -> None:
    assert to_tag_value(["abc"]) == "abc"


def test_to_tag_value_for_string_returns_string() -> None:
    assert to_tag_value("abc") == "abc"


def test_unique_list_for_set_returns_list() -> None:
    assert unique_tag_values(set(["a", "b", "c"])) == ["a", "b", "c"]


def test_unique_list_for_list_returns_list() -> None:
    assert unique_tag_values(["a", "b", "c"]) == ["a", "b", "c"]


def test_unique_list_for_list_returns_only_unique_items() -> None:
    assert unique_tag_values(["a", "b", "b", "c"]) == ["a", "b", "c"]
