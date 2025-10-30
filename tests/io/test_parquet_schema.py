"""Unit tests for the `parquet_schema` I/O helper."""

import logging

from ssb_timeseries.io import parquet_schema as io
from ssb_timeseries.io.json_helpers import tags_from_json


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
