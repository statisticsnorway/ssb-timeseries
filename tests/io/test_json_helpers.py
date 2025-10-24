"""Unit tests for the `json_helpers` I/O module."""

import json

from ssb_timeseries.io.json_helpers import sanitize_for_json
from ssb_timeseries.io.json_helpers import tags_from_json
from ssb_timeseries.io.json_helpers import tags_to_json
from ssb_timeseries.properties import SeriesType


def test_sanitize_for_json():
    """Test that the sanitize_for_json function correctly converts ssb-timeseries types to JSON-serializable strings."""
    tags = {
        "name": "test-dataset",
        "data_type": SeriesType.simple(),
    }
    sanitized_tags = sanitize_for_json(tags)
    assert sanitized_tags["data_type"] == "NONE_AT"


def test_tags_to_json():
    """Test that the tags_to_json function correctly serializes a tag dictionary."""
    tags = {
        "name": "test-dataset",
        "data_type": "NONE_AT",
    }
    json_tags = tags_to_json(tags)
    assert isinstance(json_tags["json"], bytes)
    assert json.loads(json_tags["json"].decode()) == tags


def test_tags_from_json():
    """Test that the tags_from_json function correctly deserializes a tag dictionary."""
    tags = {
        "name": "test-dataset",
        "data_type": "NONE_AT",
    }
    json_tags = {b"json": json.dumps(tags).encode("utf8")}
    deserialized_tags = tags_from_json(json_tags)
    assert deserialized_tags == tags
