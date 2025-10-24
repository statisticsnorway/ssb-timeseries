"""Unit tests for the `parquet_schema` I/O helper."""

import logging

from ssb_timeseries.io import parquet_schema as io
from ssb_timeseries.io.json_helpers import tags_from_json


def test_io_parquet_schema_as_of_at(
    new_dataset_none_at,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_none_at
    schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns)
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


def test_io_parquet_schema_none_from_to(
    new_dataset_none_from_to,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_none_from_to
    schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns)
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


def test_io_parquet_schema_none_at(
    new_dataset_as_of_at,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_as_of_at
    schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns)
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v


def test_io_parquet_schema_as_of_from_to(
    new_dataset_as_of_from_to,
    conftest,
    caplog,
) -> None:
    caplog.set_level(logging.DEBUG)
    dataset = new_dataset_as_of_from_to
    schema = io.parquet_schema(dataset.data_type, dataset.tags)
    assert set(schema.names) == set(dataset.series + dataset.datetime_columns)
    for key in dataset.series:
        # tags = json.loads(schema.field(key).metadata[b'json'].decode())
        tags = tags_from_json(schema.field(key).metadata)
        logging.debug(f"{key=}:\n{tags=}")
        assert tags["name"] == key
        for k, v in tags.items():
            assert dataset.tags["series"][key][k] == v
