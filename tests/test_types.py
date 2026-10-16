"""Tests for the SeriesType dataclass and the Versioning/Temporality enums."""

from dataclasses import FrozenInstanceError

import pytest

from ssb_timeseries.types import AS_OF_AT
from ssb_timeseries.types import AS_OF_FROM_TO
from ssb_timeseries.types import NONE_AT
from ssb_timeseries.types import NONE_FROM_TO
from ssb_timeseries.types import SeriesType
from ssb_timeseries.types import Temporality
from ssb_timeseries.types import Versioning


@pytest.mark.parametrize(
    "constant, expected_versioning, expected_temporality",
    [
        (NONE_AT, Versioning.NONE, Temporality.AT),
        (NONE_FROM_TO, Versioning.NONE, Temporality.FROM_TO),
        (AS_OF_AT, Versioning.AS_OF, Temporality.AT),
        (AS_OF_FROM_TO, Versioning.AS_OF, Temporality.FROM_TO),
    ],
)
def test_seriestype_constants_have_correct_versioning_and_temporality(
    constant, expected_versioning, expected_temporality
):
    """Test the module-level SeriesType constants carry the expected versioning and temporality."""
    assert constant.versioning == expected_versioning
    assert constant.temporality == expected_temporality


def test_seriestype_constants_equal_the_matching_constructed_value():
    """Test each constant equals the explicit SeriesType it stands for."""
    assert NONE_AT == SeriesType(versioning=Versioning.NONE, temporality=Temporality.AT)
    assert NONE_FROM_TO == SeriesType(
        versioning=Versioning.NONE, temporality=Temporality.FROM_TO
    )
    assert AS_OF_AT == SeriesType(
        versioning=Versioning.AS_OF, temporality=Temporality.AT
    )
    assert AS_OF_FROM_TO == SeriesType(
        versioning=Versioning.AS_OF, temporality=Temporality.FROM_TO
    )


@pytest.mark.parametrize(
    "constant, expected_str",
    [
        (NONE_AT, "NONE_AT"),
        (NONE_FROM_TO, "NONE_FROM_TO"),
        (AS_OF_AT, "AS_OF_AT"),
        (AS_OF_FROM_TO, "AS_OF_FROM_TO"),
    ],
)
def test_seriestype_constants_have_stable_string_spelling(constant, expected_str):
    """Test the constants keep the persisted directory-name spelling."""
    assert str(constant) == expected_str


@pytest.mark.parametrize(
    "name, expected_type",
    [
        ("NONE_AT", NONE_AT),
        ("none_at", NONE_AT),
        ("None_At", NONE_AT),
        ("NONE_FROM_TO", NONE_FROM_TO),
        ("as_of_at", AS_OF_AT),
        ("AS_OF_FROM_TO", AS_OF_FROM_TO),
        ("  as_of_from_to  ", AS_OF_FROM_TO),
    ],
)
def test_seriestype_from_name_is_case_insensitive(name, expected_type):
    """Test SeriesType.from_name accepts the canonical names in any case."""
    assert SeriesType.from_name(name) == expected_type


@pytest.mark.parametrize("name", ["simple", "estimate", "asof", "NONE", "AT", ""])
def test_seriestype_from_name_rejects_non_canonical_names(name):
    """Test SeriesType.from_name rejects legacy aliases and the bare axis names."""
    with pytest.raises(ValueError, match="Invalid SeriesType name"):
        SeriesType.from_name(name)


def test_seriestype_from_tag_values_accepts_strings_and_enums():
    """Test SeriesType.from_tag_values handles both stored strings and enum members."""
    assert SeriesType.from_tag_values("NONE", "AT") == NONE_AT
    assert SeriesType.from_tag_values("AS_OF", "FROM_TO") == AS_OF_FROM_TO
    assert SeriesType.from_tag_values(Versioning.AS_OF, Temporality.AT) == AS_OF_AT


def test_versioning_date_columns_returns_correct_columns():
    """Test the date_columns property of the Versioning enum returns correct columns."""
    assert Versioning.AS_OF.date_columns == ["as_of"]
    assert Versioning.NONE.date_columns == []
    assert Versioning.NAMES.date_columns == []


def test_temporality_date_columns_returns_correct_columns():
    """Test the date_columns property of the Temporality enum returns correct columns."""
    assert Temporality.AT.date_columns == ["valid_at"]
    assert Temporality.FROM_TO.date_columns == ["valid_from", "valid_to"]
    assert Temporality.NONE.date_columns == []


def test_seriestype_date_columns_returns_combined_sorted_columns():
    """Test the date_columns property of SeriesType returns combined and sorted columns."""
    assert sorted(AS_OF_FROM_TO.date_columns) == sorted(
        ["as_of", "valid_from", "valid_to"]
    )
    assert sorted(AS_OF_AT.date_columns) == sorted(["as_of", "valid_at"])
    assert sorted(NONE_AT.date_columns) == sorted(["valid_at"])
    assert sorted(NONE_FROM_TO.date_columns) == sorted(["valid_from", "valid_to"])


def test_seriestype_str_representation_is_correct():
    """Test the __str__ method of SeriesType returns the correct string representation."""
    assert str(AS_OF_AT) == "AS_OF_AT"


def test_seriestype_equality_comparison_is_correct():
    """Test the equality comparison of SeriesType is value based."""
    assert AS_OF_AT == SeriesType(
        versioning=Versioning.AS_OF, temporality=Temporality.AT
    )
    assert AS_OF_AT != AS_OF_FROM_TO
    assert AS_OF_AT is not None


def test_seriestype_is_hashable():
    """Test equal SeriesType values collapse to a single set member."""
    assert len({NONE_AT, SeriesType(Versioning.NONE, Temporality.AT)}) == 1


def test_seriestype_is_immutable():
    """Test SeriesType instances cannot be mutated."""
    with pytest.raises(FrozenInstanceError):
        NONE_AT.temporality = Temporality.FROM_TO  # type: ignore[misc]


def test_constants_are_re_exported_at_package_root():
    """Test the four constants are importable from the top-level package."""
    import ssb_timeseries

    assert ssb_timeseries.NONE_AT is NONE_AT
    assert ssb_timeseries.NONE_FROM_TO is NONE_FROM_TO
    assert ssb_timeseries.AS_OF_AT is AS_OF_AT
    assert ssb_timeseries.AS_OF_FROM_TO is AS_OF_FROM_TO
