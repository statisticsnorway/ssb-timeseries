"""Tests for series alignment and alignment warnings."""

import warnings

import narwhals as nw
import pandas as pd
import pytest

from ssb_timeseries.dataset import AlignmentWarning
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dataset import SeriesAlignmentWarning
from ssb_timeseries.types import NONE_AT


def _dataset(
    name: str,
    values: dict[str, float],
    tags: dict[str, dict[str, str]],
    order: list[str] | None = None,
) -> Dataset:
    """Build a Dataset with explicit physical series order and tags."""
    data: dict[str, object] = {"valid_at": pd.to_datetime(["2025-01-31"], utc=True)}
    data.update(values)
    ds = Dataset(
        name=name,
        data_type=NONE_AT,
        data=pd.DataFrame(data),
        find_existing=False,
    )
    ds.tags["series"] = {
        series: {"name": series, **tags.get(series, {})} for series in values
    }
    if order:
        ds.data = nw.from_native(ds.data).select(["valid_at", *order]).to_native()
    return ds


def _values(ds: Dataset) -> dict[str, float]:
    """Return the first row of a Dataset as a name/value mapping."""
    return ds.pd.set_index("valid_at").iloc[0].to_dict()


def test_align_series_by_names_reorders_to_target_physical_order() -> None:
    left = _dataset("left", {"z": 20.0, "a": 6.0}, {}, ["z", "a"])
    right = _dataset("right", {"z": 2.0, "a": 3.0}, {}, ["a", "z"])

    aligned = left.align_series(right, names=True)

    assert nw.from_native(aligned.data).columns == ["valid_at", "a", "z"]
    assert list(aligned.tags["series"]) == ["a", "z"]
    assert _values(aligned + right) == {"a": 9.0, "z": 22.0}
    assert nw.from_native(left.data).columns == ["valid_at", "z", "a"]
    assert nw.from_native(right.data).columns == ["valid_at", "a", "z"]


def test_align_series_by_tags_renames_and_reorders_to_target() -> None:
    source = _dataset(
        "source",
        {"s_iff": 20.0, "s_hus": 6.0},
        {
            "s_iff": {"geo": "NO", "kind": "IFF"},
            "s_hus": {"geo": "NO", "kind": "HUS"},
        },
        ["s_iff", "s_hus"],
    )
    target = _dataset(
        "target",
        {"t_iff": 2.0, "t_hus": 3.0},
        {
            "t_iff": {"geo": "NO", "kind": "IFF"},
            "t_hus": {"geo": "NO", "kind": "HUS"},
        },
        ["t_iff", "t_hus"],
    )

    aligned = source.align_series(target, tags=True)

    assert nw.from_native(aligned.data).columns == [
        "valid_at",
        "t_iff",
        "t_hus",
    ]
    assert aligned.tags["series"]["t_iff"]["kind"] == "IFF"
    assert aligned.tags["series"]["t_hus"]["kind"] == "HUS"
    assert _values(aligned + target) == {"t_iff": 22.0, "t_hus": 9.0}
    assert list(source.tags["series"]) == ["s_iff", "s_hus"]


def test_align_series_with_explicit_tag_keys() -> None:
    source = _dataset(
        "source",
        {"s1": 20.0, "s2": 6.0},
        {
            "s1": {"geo": "NO", "kind": "x"},
            "s2": {"geo": "NO", "kind": "y"},
        },
    )
    target = _dataset(
        "target",
        {"t1": 2.0, "t2": 3.0},
        {
            "t1": {"geo": "SE", "kind": "x"},
            "t2": {"geo": "SE", "kind": "y"},
        },
    )

    with pytest.raises(ValueError, match="exactly one source series"):
        source.align_series(target, tags=True)

    aligned = source.align_series(target, tags=["kind"])

    assert _values(aligned + target) == {"t1": 22.0, "t2": 9.0}


def test_align_series_rejects_invalid_criteria() -> None:
    left = _dataset("left", {"z": 20.0, "a": 6.0}, {}, ["z", "a"])
    right = _dataset("right", {"z": 2.0, "a": 3.0}, {}, ["a", "z"])

    with pytest.raises(ValueError, match="exactly one"):
        left.align_series(right)
    with pytest.raises(ValueError, match="exactly one"):
        left.align_series(right, names=True, tags=True)
    with pytest.raises(ValueError, match="target dataset"):
        left.align_series(right, names=["missing"])


def test_math_warns_when_tag_matches_are_in_different_order() -> None:
    left = _dataset(
        "left",
        {"s_iff": 20.0, "s_hus": 6.0},
        {
            "s_iff": {"geo": "NO", "kind": "IFF"},
            "s_hus": {"geo": "NO", "kind": "HUS"},
        },
        ["s_iff", "s_hus"],
    )
    right = _dataset(
        "right",
        {"t_hus": 3.0, "t_iff": 2.0},
        {
            "t_hus": {"geo": "NO", "kind": "HUS"},
            "t_iff": {"geo": "NO", "kind": "IFF"},
        },
        ["t_hus", "t_iff"],
    )

    with pytest.warns(SeriesAlignmentWarning, match="different order"):
        result = left + right

    assert _values(result) == {"s_iff": 23.0, "s_hus": 8.0}


def test_math_does_not_warn_when_names_match_but_order_differs() -> None:
    left = _dataset("left", {"z": 20.0, "a": 6.0}, {}, ["z", "a"])
    right = _dataset("right", {"z": 2.0, "a": 3.0}, {}, ["a", "z"])

    with warnings.catch_warnings():
        warnings.simplefilter("error", AlignmentWarning)
        result = left + right

    assert _values(result) == {"z": 22.0, "a": 9.0}


def test_math_does_not_warn_when_tag_matches_are_in_same_order() -> None:
    left = _dataset(
        "left",
        {"s_iff": 20.0, "s_hus": 6.0},
        {
            "s_iff": {"geo": "NO", "kind": "IFF"},
            "s_hus": {"geo": "NO", "kind": "HUS"},
        },
        ["s_iff", "s_hus"],
    )
    right = _dataset(
        "right",
        {"t_iff": 2.0, "t_hus": 3.0},
        {
            "t_iff": {"geo": "NO", "kind": "IFF"},
            "t_hus": {"geo": "NO", "kind": "HUS"},
        },
        ["t_iff", "t_hus"],
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", AlignmentWarning)
        result = left + right

    assert _values(result) == {"s_iff": 22.0, "s_hus": 9.0}
