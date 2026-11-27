"""Tests for pure metadata assessment helpers."""

import pytest

from ssb_timeseries.meta import candidate_key_groups
from ssb_timeseries.meta import comparison_keys
from ssb_timeseries.meta import find_dimensions
from ssb_timeseries.meta import match_series_by_tags
from ssb_timeseries.meta import reorder_series_tags
from ssb_timeseries.meta import series_overlap_ratio
from ssb_timeseries.meta import tag_diff
from ssb_timeseries.meta import tag_overlap_ratio
from ssb_timeseries.meta import tags_equal


def test_comparison_keys_uses_default_exclusions_and_modifiers() -> None:
    tags = {
        "a": {
            "name": "a",
            "dataset": "dataset-a",
            "repository": "repository-a",
            "versioning": "NONE",
            "temporality": "AT",
            "geo": "NO",
            "product": "bread",
            "calculations": {"input": ["a"]},
        }
    }

    assert comparison_keys(tags) == ["geo", "product"]
    assert comparison_keys(tags, include={"versioning", "temporality"}) == [
        "geo",
        "product",
        "temporality",
        "versioning",
    ]
    assert comparison_keys(tags, include={"repository"}) == [
        "geo",
        "product",
        "repository",
    ]
    assert comparison_keys(tags, exclude={"geo"}) == ["product"]


def test_comparison_keys_rejects_conflicting_or_unknown_keys() -> None:
    tags = {"a": {"geo": "NO", "product": "bread"}}

    with pytest.raises(ValueError, match="both included and excluded"):
        comparison_keys(tags, include={"geo"}, exclude={"geo"})
    with pytest.raises(ValueError, match="Unknown tag keys"):
        comparison_keys(tags, include={"typo"})


def test_find_dimensions_returns_single_and_combined_dimensions() -> None:
    single = {
        "a": {"geo": "NO", "product": "bread"},
        "b": {"geo": "NO", "product": "milk"},
    }
    combined = {
        "a": {"geo": "NO", "kind": "x"},
        "b": {"geo": "NO", "kind": "y"},
        "c": {"geo": "SE", "kind": "x"},
        "d": {"geo": "SE", "kind": "y"},
    }

    assert find_dimensions(single) == [("product",)]
    assert find_dimensions(combined) == [("geo", "kind")]


def test_find_dimensions_rejects_invalid_combination_size() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        find_dimensions({"a": {"geo": "NO"}}, max_combination_size=0)


def test_candidate_key_groups_returns_all_signature_groups() -> None:
    tags = {
        "a": {"geo": "NO"},
        "b": {"geo": "NO"},
        "c": {"geo": "SE"},
    }

    assert candidate_key_groups(tags) == {
        (("NO",),): ["a", "b"],
        (("SE",),): ["c"],
    }
    assert candidate_key_groups(tags, keys=["geo"]) == {
        (("NO",),): ["a", "b"],
        (("SE",),): ["c"],
    }


def test_tag_diff_and_tags_equal_normalize_list_values() -> None:
    a = {"geo": "NO", "items": ["b", "a"]}
    b = {"geo": "NO", "items": ["a", "b"]}

    assert tags_equal(a, b)

    diff = tag_diff({"geo": "NO", "unit": "kWh"}, {"geo": "SE"})

    assert diff.equal is False
    assert diff.different == {"geo": ("NO", "SE")}
    assert diff.missing_in_a == []
    assert diff.missing_in_b == ["unit"]


def test_tag_overlap_ratio_uses_assessed_keys() -> None:
    a = {"geo": "NO", "unit": "kWh"}
    b = {"geo": "SE", "unit": "kWh"}

    assert tag_overlap_ratio(a, b) == 0.5
    assert tag_overlap_ratio(a, b, exclude={"geo"}) == 1.0
    assert tag_overlap_ratio({}, {}) == 0.0


def test_match_series_by_tags_ignores_identity_location_and_type_tags() -> None:
    source = {
        "s_iff": {
            "name": "s_iff",
            "dataset": "left",
            "repository": "repo-a",
            "versioning": "NONE",
            "temporality": "AT",
            "geo": "NO",
            "kind": "IFF",
        },
        "s_hus": {
            "name": "s_hus",
            "dataset": "left",
            "repository": "repo-a",
            "versioning": "NONE",
            "temporality": "AT",
            "geo": "NO",
            "kind": "HUS",
        },
    }
    target = {
        "t_iff": {
            "name": "t_iff",
            "dataset": "right",
            "repository": "repo-b",
            "versioning": "AS_OF",
            "temporality": "FROM_TO",
            "geo": "NO",
            "kind": "IFF",
        },
        "t_hus": {
            "name": "t_hus",
            "dataset": "right",
            "repository": "repo-b",
            "versioning": "AS_OF",
            "temporality": "FROM_TO",
            "geo": "NO",
            "kind": "HUS",
        },
    }

    assert match_series_by_tags(source, target) == {
        "t_iff": "s_iff",
        "t_hus": "s_hus",
    }

    with pytest.raises(ValueError, match="exactly one source series"):
        match_series_by_tags(source, target, include={"repository"})


def test_reorder_series_tags_is_ordered_and_detached() -> None:
    tags = {"z": {"geo": "NO"}, "a": {"geo": "SE"}}

    reordered = reorder_series_tags(tags, ["a", "z"])

    assert list(reordered) == ["a", "z"]
    reordered["a"]["geo"] = "X"
    assert tags["a"]["geo"] == "SE"

    with pytest.raises(ValueError, match="every series"):
        reorder_series_tags(tags, ["a"])
    assert list(reorder_series_tags(tags, ["a", "missing"], strict=False)) == [
        "a",
        "z",
    ]


def test_series_overlap_ratio_counts_unique_matches() -> None:
    source = {
        "s1": {"geo": "NO", "kind": "x"},
        "s2": {"geo": "SE", "kind": "y"},
    }
    target = {
        "t1": {"geo": "NO", "kind": "x"},
        "t2": {"geo": "NO", "kind": "z"},
    }

    assert series_overlap_ratio(source, target) == 0.5
    assert series_overlap_ratio(source, source) == 1.0
