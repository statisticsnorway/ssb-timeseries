"""Unit tests for the `parquet_schema` I/O helper."""

import datetime as dt
import logging

import pyarrow

from ssb_timeseries.io import parquet_schema as io
from ssb_timeseries.io.json_helpers import tags_from_json
from ssb_timeseries.io.parquet_schema import series_are_all_boolean
from ssb_timeseries.types import NONE_AT


def test_parquet_schema_returns_none_when_no_series_meta():
    """Test that parquet_schema returns None when there is no series metadata."""
    tags = {"name": "test-dataset", "series": {}}
    schema = io.parquet_schema(NONE_AT, tags)
    assert schema is None


def _boolean_frame(*extra: tuple[str, list]) -> pyarrow.Table:
    columns: dict = {
        "valid_at": pyarrow.array(
            [dt.datetime(2022, 1, 1, tzinfo=dt.UTC)],
            type=pyarrow.timestamp("ns", tz="UTC"),
        ),
        "flag_a": [True],
        "flag_b": [False],
    }
    columns.update(dict(extra))
    return pyarrow.table(columns)


def test_series_are_all_boolean_when_every_series_is_boolean():
    """A set whose series are all boolean counts as an all-boolean set."""
    data = pyarrow.table({"flag_a": [True, False], "flag_b": [False, True]})
    assert series_are_all_boolean(data, ["flag_a", "flag_b"]) is True


def test_series_are_not_all_boolean_for_a_mixed_set():
    """One non-boolean series makes the whole set mixed, not all-boolean."""
    data = pyarrow.table({"flag_a": [True, False], "value_b": [1.0, 2.0]})
    assert series_are_all_boolean(data, ["flag_a", "value_b"]) is False


def _boolean_tags() -> dict:
    return {
        "name": "all-boolean",
        "versioning": "NONE",
        "temporality": "AT",
        "series": {"flag_a": {}, "flag_b": {}},
    }


def test_parquet_schema_writes_all_boolean_series_as_boolean():
    """An all-boolean series set must keep the boolean type in the schema."""
    schema = io.parquet_schema(NONE_AT, _boolean_tags(), data=_boolean_frame())
    assert schema.field("flag_a").type == pyarrow.bool_()
    assert schema.field("flag_b").type == pyarrow.bool_()


def test_parquet_schema_keeps_numeric_default_for_mixed_series():
    """A mixed set keeps the numeric default until mixed types are decided."""
    data = _boolean_frame(("flag_b", [1.0]))
    schema = io.parquet_schema(NONE_AT, _boolean_tags(), data=data)
    assert schema.field("flag_a").type == pyarrow.float64()
    assert schema.field("flag_b").type == pyarrow.float64()


def test_parquet_schema_without_data_keeps_numeric_default():
    """Callers that have no frame at hand (eg. reads) keep the numeric default."""
    schema = io.parquet_schema(NONE_AT, _boolean_tags())
    assert schema.field("flag_a").type == pyarrow.float64()


def test_io_parquet_schema_for_all_series_types(
    one_new_set_for_each_data_type,
    conftest,
    caplog,
) -> None:
    """Test that parquet_schema generates a correct schema for all series types."""
    caplog.set_level(logging.DEBUG)
    dataset = one_new_set_for_each_data_type
    schema = io.parquet_schema(dataset.data_type, dataset.tags)

    # The schema should include all series columns AND the date columns defined by the SeriesType
    # This was the source of the original bug: using dataset.datetime_columns (from the in-memory dataframe)
    # instead of dataset.data_type.date_columns (which correctly includes 'as_of' for versioned types).
    expected_columns = set(dataset.series + dataset.data_type.date_columns)
    assert set(schema.names) == expected_columns

    # Verify that the metadata for each series is correctly embedded in the schema
    for key in dataset.series:
        tags = tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v
