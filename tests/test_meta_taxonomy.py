# type: ignore
# ruff: noqa
import logging
import uuid

# from numpy import log
import pytest
from bigtree import get_tree_diff
from bigtree import print_tree
from bigtree import levelordergroup_iter
from bigtree import findall

from ssb_timeseries import fs
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.meta import Taxonomy


@pytest.mark.skipif(False, reason="")
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
def test_get_leaf_nodes_from_hierarchical_klass_taxonomy(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    tree = Taxonomy(157).structure
    leaves = [n.name for n in tree.root["1"].leaves]
    ts_logger.debug(f"{print_tree(tree)} ...\n{leaves}")

    assert leaves == [
        "1.1.1",
        "1.1.2",
        "1.1.3",
        "1.2",
    ]


@log_start_stop
def test_get_parent_nodes_from_hierarchical_klass_taxonomy(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    energy_balance = Taxonomy(157)
    tree = energy_balance.structure

    leaf_nodes = [n.name for n in tree.root.leaves]
    ts_logger.debug(f"{print_tree(tree)} ...\n{leaf_nodes}")

    all_nodes = [n.name for n in tree.root.descendants]
    parent_nodes = [a for a in all_nodes if a not in leaf_nodes]
    parent_nodes2 = [n.name for n in tree.root.descendants if n not in tree.root.leaves]

    # levelordergroup_iter(
    #     energy_balance.structure,
    #     filter_condition=None,
    #     stop_condition=None,
    #     max_depth=0,
    # )

    ts_logger.debug(f"All nodes:\n\t{all_nodes}")

    assert len(all_nodes) == len(leaf_nodes) + len(parent_nodes)
    assert parent_nodes == parent_nodes2


@log_start_stop
def test_replace_chars_in_flat_codes(caplog) -> None:
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
        fs.rm(temp_file)

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
