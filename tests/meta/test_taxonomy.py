import logging
import uuid

# import bigtree
import pytest

# from bigtree import get_tree_diff
# from bigtree import print_tree
import ssb_timeseries as ts
from ssb_timeseries.meta.taxonomy import Taxonomy

# mypy: disable-error-code="no-untyped-def,attr-defined,func-returns-value,operator"


def test_read_flat_code_list_from_klass_returns_two_level_tree() -> None:
    activity = Taxonomy(klass_id=697)
    ts.logger.debug(f"captured ...\n{activity.entities}")

    assert activity.entities.shape == (16, 9)
    assert len(activity.leaf_nodes) == 15
    assert len(activity.parent_nodes) == 1
    # assert activity.structure.max_depth == 2
    assert activity.root == "0"
    # assert activity.structure.diameter == 2


def test_read_flat_code_list_date() -> None:
    activity = Taxonomy(klass_id=697, from_date="1997-11-01")
    ts.logger.debug(f"captured ...\n{activity.entities}")

    assert activity.entities.shape == (16, 9)
    assert len(activity.leaf_nodes) == 15
    assert len(activity.parent_nodes) == 1
    # assert activity.structure.max_depth == 2
    assert activity.root == "0"
    # assert activity.structure.diameter == 2


def test_different_dates_nace() -> None:
    nace_today = Taxonomy(klass_id=6)
    nace_2020 = Taxonomy(klass_id=6, from_date="2020-01-01")
    assert nace_today.entities.shape != nace_2020.entities.shape


def test_read_hierarchical_code_set_from_klass_returns_multi_level_tree() -> None:
    energy_balance = Taxonomy(klass_id=157)
    ts.logger.debug(f"captured ...\n{energy_balance.print_tree()}")

    assert energy_balance.root == "0"
    # assert energy_balance.structure.diameter == 6
    # assert energy_balance.structure.max_depth == 4
    # assert energy_balance.structure.max_depth == 4


def test_taxonomy_subtree(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)
    klass157_subtree = klass157.subtree("1.1")
    ts.logger.debug(f"tree ...\n{klass157_subtree}")
    assert isinstance(klass157_subtree, Taxonomy)
    assert klass157_subtree.root == "1.1"
    # assert klass157_subtree.diameter == 2
    # assert klass157_subtree.max_depth == 2

    assert klass157_subtree.leaf_nodes == ["1.1.1", "1.1.2", "1.1.3"]


def test_get_leaf_nodes_from_hierarchical_klass_taxonomy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    tree = Taxonomy(klass_id=157)  # .structure
    # leaves = [n.name for n in tree.root["1"].leaves]
    leaves = tree.agg_dict["1"]
    # ts.logger.debug(f"{print_tree(tree)} ...\n{leaves}")
    ts.logger.debug(f"{tree} ...\n{leaves}")

    assert leaves == [
        "1.1.1",
        "1.1.2",
        "1.1.3",
        "1.2",
    ]


def test_get_leaf_nodes_from_middle_of_hierarchical_klass_taxonomy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)
    # leaves = [n.name for n in subtree.root.leaves]
    leaves = klass157.agg_dict["1.1"]
    # ts.logger.debug(f"{print_tree(subtree)} ...\n{leaves}")

    assert leaves == [
        "1.1.1",
        "1.1.2",
        "1.1.3",
    ]


def test_get_item_from_hierarchical_klass_taxonomy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)
    tree = klass157.structure
    # node_1_1 = bigtree.find_name(tree.root, "1.1")

    assert list(tree.predecessors("1.1")) == ["1.1.1", "1.1.2", "1.1.3"]

    # assert klass157["1.1"] == node_1_1


def test_get_parent_nodes_from_hierarchical_klass_taxonomy(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)

    # leaf_nodes = [n.name for n in tree.root.leaves]
    # all_nodes = [n.name for n in tree.root.descendants] + [tree.root.name]
    parent_nodes = [n for n in klass157.all_nodes if n not in klass157.leaf_nodes]
    parent_nodes2 = klass157.parent_nodes

    ts.logger.debug(f"All nodes:\n\t{klass157.all_nodes}")

    assert len(klass157.all_nodes) == len(klass157.leaf_nodes) + len(
        klass157.parent_nodes
    )
    assert sorted(parent_nodes) == sorted(parent_nodes2)


# TODO: Replace with a functioning test
# @pytest.mark.xfail(reason="Tree diff error?")
# def test_taxonomy_minus_subtree(
#     caplog: pytest.LogCaptureFixture,
# ) -> None:
#     caplog.set_level(logging.DEBUG)
#     klass157 = Taxonomy(klass_id=157)
#     klass157_subtree = klass157.subtree("1.1")
#     ts.logger.warning(f"tree ...\n{klass157_subtree}")
#     # rest = bigtree.get_tree_diff(klass157.structure.root, klass157_subtree)
#     # rest = klass157.structure.root - klass157_subtree

#     rest = klass157 - klass157_subtree
#     # ts.logger.warning(f"...\n{rest}")

# assert isinstance(rest, bigtree.Node)


def test_replace_chars_in_flat_codes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The substitute parameter of Taxonomy init allows making changes to codes when reading a taxonomy list.

    While best practice calls for direct match to KLASS, this allows mappings with (minor) deviations.
    """
    caplog.set_level(logging.DEBUG)
    k697 = Taxonomy(
        klass_id=697,
        substitutions={
            "_": ".",
            "aa": "å",
            "lagerf": "lagerføring",
            "lagere": "lagerendring",
        },  # multiple replacements! --> generate substitution dict from json file if required
    )
    k697_names = k697.leaf_nodes  # [n.name for n in k697.structure.root.children]
    ts.logger.debug(f"klass 697 codes:\n{k697_names}")
    # ts.logger.debug(
    #     f"tree ...\n{print_tree(k697.structure.root, attr_list=['fullname'])}"
    # )

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


def test_replace_chars_in_hierarchical_codes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The substitute parameter of Taxonomy init allows making changes to codes when reading a taxonomy list.

    While best practice calls for direct match to KLASS, this allows mappings with (minor) deviations.
    """
    caplog.set_level(logging.DEBUG)
    k157 = Taxonomy(
        klass_id=157,
        substitutions={
            ".": "/",
        },
    )
    # compare for leaf nodes of sub tree
    k157_names = k157.leaf_nodes[:4]  # [n.name for n in k157["1"].leaves]
    ts.logger.debug(f"klass 157 codes:\n{k157_names}")
    # ts.logger.warning(
    #     f"tree ...\n{print_tree(k157.structure.root['1'], attr_list=['fullname'])}"
    # )

    assert sorted(k157_names) == sorted(
        [
            "1/1/1",
            "1/1/2",
            "1/1/3",
            "1/2",
        ]
    )


def test_hierarchical_codes_retrieved_from_klass_and_reloaded_from_json_file_are_identical(
    caplog: pytest.LogCaptureFixture,
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    caplog.set_level(logging.DEBUG)
    klass157 = Taxonomy(klass_id=157)

    temp_file = tmp_path_factory.mktemp("temp") / f"temp-{uuid.uuid4()}.json"

    klass157.save(temp_file)
    file157 = Taxonomy(path=temp_file)
    # finally:
    #    fs.rm(temp_file)

    # compare all leaf nodes of sub tree
    k157_names = klass157.leaf_nodes  # [n.name for n in klass157.structure.root.leaves]
    f157_names = file157.leaf_nodes  # [n.name for n in file157.structure.root.leaves]
    assert k157_names == f157_names

    # ts.logger.debug(f"klass157 ...\n{print_tree(klass157.structure)}")
    # ts.logger.debug(f"file157 ...\n{print_tree(file157.structure)}")

    # diff = get_tree_diff(klass157.structure, file157.structure)
    # if diff:
    #     ts.logger.debug(f"diff:\n{print_tree(diff)}")
    #     # --> assert should fail
    # else:
    #     ts.logger.debug(f"diff: {diff}")
    #     # --> assert should pass

    assert isinstance(klass157, Taxonomy) and isinstance(file157, Taxonomy)
    assert klass157 == file157


import pandas as pd

simple_nx_data_no_root = pd.DataFrame(
    {
        "code": ["100", "200", "200", "300", "400", "A", "B", "C"],
        "parentCode": ["A", "A", "B", "C", "C", "F1", "F1", "F2"],
    }
)
simple_nx_data_one_root = pd.DataFrame(
    {
        "code": [
            "100",
            "200",
            "200",
            "300",
            "400",
            "A",
            "B",
            "C",
            "F1",
            "F2",
        ],
        "parentCode": [
            "A",
            "A",
            "B",
            "C",
            "C",
            "F1",
            "F1",
            "F2",
            "F",
            "F",
        ],
    }
)
simple_nx_data_multiple_roots = pd.DataFrame(
    {
        "code": [
            "100",
            "200",
            "200",
            "300",
            "400",
            "A",
            "B",
            "C",
            "F1",
            "F2",
            "100",
            "300",
        ],
        "parentCode": [
            "A",
            "A",
            "B",
            "C",
            "C",
            "F1",
            "F1",
            "F2",
            "F",
            "F",
            "AAA",
            "AAA",
        ],
    }
)


def test_nx_from_df() -> None:
    nx_taxonomy = Taxonomy(data=simple_nx_data_no_root)
    assert nx_taxonomy.root == "0"
    assert nx_taxonomy.agg_dict[nx_taxonomy.root] == nx_taxonomy.leaf_nodes


def test_nx_root_from_df() -> None:
    nx_taxonomy = Taxonomy(data=simple_nx_data_one_root)
    assert nx_taxonomy.root == "F"
    assert nx_taxonomy.agg_dict[nx_taxonomy.root] == nx_taxonomy.leaf_nodes


def test_nx_multiple_from_df() -> None:
    nx_taxonomy = Taxonomy(data=simple_nx_data_multiple_roots)
    assert nx_taxonomy.root == "F"
    assert nx_taxonomy.agg_dict[nx_taxonomy.root] == nx_taxonomy.leaf_nodes


def test_simple_subtree() -> None:
    nx_taxonomy = Taxonomy(data=simple_nx_data_no_root)
    assert nx_taxonomy.subtree("F2").root == "F2"
    assert nx_taxonomy.subtree("F2").leaf_nodes == ["300", "400"]
