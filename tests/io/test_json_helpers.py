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


def test_sanitize_for_json_with_nested_structures():
    """Test that sanitize_for_json correctly handles nested dictionaries and lists."""
    nested_tags = {
        "name": "nested-dataset",
        "config": {
            "type": SeriesType.as_of_from_to(),
            "versions": [
                {"tag": "v1", "spec": SeriesType.simple()},
                {"tag": "v2", "spec": SeriesType.from_to()},
            ],
        },
    }
    sanitized = sanitize_for_json(nested_tags)
    assert sanitized["config"]["type"] == "AS_OF_FROM_TO"
    assert sanitized["config"]["versions"][0]["spec"] == "NONE_AT"
    assert sanitized["config"]["versions"][1]["spec"] == "NONE_FROM_TO"


def test_tags_from_json():
    """Test that the tags_from_json function correctly deserializes a tag dictionary."""
    tags = {
        "name": "test-dataset",
        "data_type": "NONE_AT",
    }
    json_tags = {b"json": json.dumps(tags).encode("utf8")}
    deserialized_tags = tags_from_json(json_tags)
    assert deserialized_tags == tags
