"""Test the types module."""

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


def test_seriestype_constants_equal_the_matching_factory_methods():
    """Test each constant equals the factory method(s) that produce the same SeriesType."""
    assert NONE_AT == SeriesType.none_at() == SeriesType.simple()
    assert NONE_FROM_TO == SeriesType.from_to()
    assert AS_OF_AT == SeriesType.as_of_at() == SeriesType.estimate()
    assert AS_OF_FROM_TO == SeriesType.as_of_from_to()


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


def test_seriestype_init_with_string_keywords_sets_correct_versioning_and_temporality():
    """Test SeriesType initialization with string type_def keywords sets correct versioning and temporality."""
    st_asof = SeriesType("asof")
    assert st_asof.versioning == Versioning.AS_OF
    assert st_asof.temporality == Temporality.FROM_TO

    st_estimate = SeriesType("estimate")
    assert st_estimate.versioning == Versioning.AS_OF
    assert st_estimate.temporality == Temporality.AT

    st_names = SeriesType("names")
    assert st_names.versioning == Versioning.NAMES
    assert st_names.temporality == Temporality.FROM_TO  # Default


def test_seriestype_init_with_simple_keyword_sets_none_at():
    """Test SeriesType initialization with 'simple' keyword sets versioning to NONE and temporality to AT."""
    st_simple = SeriesType("simple")
    assert st_simple.versioning == Versioning.NONE
    assert st_simple.temporality == Temporality.AT


def test_seriestype_init_with_non_string_keyword_sets_correct_versioning_and_temporality():
    """Test SeriesType initialization with a non-string keyword (hotfix test) sets correct versioning and temporality."""
    st = SeriesType(123, "as_of")
    assert st.versioning == Versioning.AS_OF
    assert st.temporality == Temporality.FROM_TO


def test_seriestype_init_with_explicit_args_sets_correct_versioning_and_temporality():
    """Test SeriesType initialization with explicit versioning and temporality args sets correct values."""
    st = SeriesType(versioning=Versioning.NAMES, temporality=Temporality.NONE)
    assert st.versioning == Versioning.NAMES
    assert st.temporality == Temporality.NONE


def test_seriestype_init_explicit_args_override_keywords():
    """Test that explicit arguments override keywords in type_def during SeriesType initialization."""
    st = SeriesType(
        "asof", "at", versioning=Versioning.NONE, temporality=Temporality.FROM_TO
    )
    assert st.versioning == Versioning.NONE
    assert st.temporality == Temporality.FROM_TO


def test_seriestype_init_defaults_to_none_from_to():
    """Test default SeriesType initialization results in NONE versioning and FROM_TO temporality."""
    st = SeriesType()
    assert st.versioning == Versioning.NONE
    assert st.temporality == Temporality.FROM_TO


@pytest.mark.parametrize(
    "method_name, expected_versioning, expected_temporality",
    [
        ("none_at", Versioning.NONE, Temporality.AT),
        ("simple", Versioning.NONE, Temporality.AT),
        ("from_to", Versioning.NONE, Temporality.FROM_TO),
        ("as_of_at", Versioning.AS_OF, Temporality.AT),
        ("as_of_from_to", Versioning.AS_OF, Temporality.FROM_TO),
        ("estimate", Versioning.AS_OF, Temporality.AT),
    ],
)
def test_seriestype_classmethod_constructors_return_correct_types(
    method_name, expected_versioning, expected_temporality
):
    """Test the convenience classmethods of SeriesType return correctly configured types."""
    method = getattr(SeriesType, method_name)
    st = method()
    assert st.versioning == expected_versioning
    assert st.temporality == expected_temporality
    assert st == SeriesType(
        versioning=expected_versioning, temporality=expected_temporality
    )


def test_seriestype_date_columns_returns_combined_sorted_columns():
    """Test the date_columns property of SeriesType returns combined and sorted columns."""
    assert sorted(SeriesType.as_of_from_to().date_columns) == sorted(
        ["as_of", "valid_from", "valid_to"]
    )
    assert sorted(SeriesType.as_of_at().date_columns) == sorted(["as_of", "valid_at"])
    assert sorted(SeriesType.simple().date_columns) == sorted(["valid_at"])
    assert sorted(SeriesType.from_to().date_columns) == sorted(
        ["valid_from", "valid_to"]
    )


def test_seriestype_str_representation_is_correct():
    """Test the __str__ method of SeriesType returns the correct string representation."""
    st = SeriesType.as_of_at()
    assert str(st) == "AS_OF_AT"


def test_seriestype_repr_representation_is_correct():
    """Test the __repr__ method of SeriesType returns the correct machine-readable representation."""
    st = SeriesType.as_of_at()
    assert repr(st) == "SeriesType(Versioning.AS_OF,Temporality.AT)"


def test_seriestype_equality_comparison_is_correct():
    """Test the __eq__ method of SeriesType for correct equality comparisons."""
    assert SeriesType.as_of_at() == SeriesType.estimate()
    assert SeriesType.as_of_at() != SeriesType.as_of_from_to()
    assert SeriesType.as_of_at() is not None
