"""Provides helper functions for working with JSON data."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from typing import cast

from ..meta import TagDict
from ..types import SeriesType
from ..types import Temporality
from ..types import Versioning

# mypy: disable-error-code="assignment, union-attr"


def sanitize_for_json(d: dict) -> dict[str, Any]:
    """Recursively convert custom ssb-timeseries types to JSON-serializable strings."""
    if not isinstance(d, dict):
        return d
    sanitized_dict = {}
    for k, v in d.items():
        if isinstance(v, SeriesType | Versioning | Temporality):
            sanitized_dict[k] = str(v)
        elif isinstance(v, datetime):
            sanitized_dict[k] = v.isoformat()
        elif isinstance(v, dict):
            sanitized_dict[k] = sanitize_for_json(v)
        elif isinstance(v, list):
            sanitized_dict[k] = [
                sanitize_for_json(item) if isinstance(item, dict) else item
                for item in v
            ]
        else:
            sanitized_dict[k] = v
    return sanitized_dict


def tags_to_json(x: TagDict) -> dict[str, bytes]:
    """Serialize a tag dictionary into a format suitable for Parquet metadata.

    See Also:
        https://arrow.apache.org/docs/python/generated/pyarrow.schema.html
    """
    sanitized_tags = sanitize_for_json(x)
    j = {"json": json.dumps(sanitized_tags).encode("utf8")}
    return j


def tags_from_json(
    dict_with_json_string: dict[str | bytes, str | bytes],
    byte_encoded: bool = True,
) -> TagDict:  # dict[str, Any]:
    """Deserialize a tag dictionary from the Parquet metadata format.

    This is the reverse of `tags_to_json`.
    """
    if byte_encoded:
        d = json.loads(dict_with_json_string[b"json"].decode())
    else:
        d = json.loads(dict_with_json_string["json"])
    return cast(TagDict, d)
