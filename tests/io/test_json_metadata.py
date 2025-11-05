"""Unit tests for the `json_metadata` I/O handler."""

import logging

from pytest import LogCaptureFixture

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.io import fs
from ssb_timeseries.io import json_metadata
from ssb_timeseries.types import SeriesType

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

test_logger = logging.getLogger(__name__)


# =============================== TESTS ===================================


def test_read_existing_metadata_works_for_all_series_types(
    one_existing_set_for_each_data_type: Dataset,
    conftest,
    caplog: LogCaptureFixture,
) -> None:
    """Test that reading metadata from a pre-saved dataset works for all series types."""
    caplog.set_level(logging.DEBUG)
    existing_dataset = one_existing_set_for_each_data_type
    json_handler = json_metadata.JsonMetaIO(repository=conftest.repo)

    # 1. Verify that the metadata file exists
    assert fs.exists(json_handler.fullpath(existing_dataset.name)), (
        "Metadata file does not exist."
    )

    # 2. Read the metadata back and verify it matches the source
    read_tags = json_handler.read(set_name=existing_dataset.name)
    assert read_tags == existing_dataset.tags, (
        f"Tag mismatch for type {existing_dataset.data_type}."
    )


def test_search_for_dataset_by_exact_name_in_single_repo_returns_the_set(
    conftest,
    xyz_at,
    caplog: LogCaptureFixture,
):
    """Test that searching for a dataset by exact name returns the correct dataset."""
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    x = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        load_data=False,
        data=xyz_at,
    )
    json_handler = json_metadata.JsonMetaIO(repository=conftest.repo)
    json_handler.write(set_name=x.name, tags=x.tags)
    search_pattern = set_name
    datasets_found = json_handler.search(equals=search_pattern)
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 1
    assert datasets_found[0]["object_name"] == set_name
    assert datasets_found[0]["object_tags"] == x.tags


def test_search_for_dataset_by_part_of_name_with_one_match_returns_the_set(
    conftest,
    xyz_at,
    caplog: LogCaptureFixture,
):
    """Test that searching for a dataset by part of its name returns the correct dataset."""
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    x = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        load_data=False,
        data=xyz_at,
    )
    json_handler = json_metadata.JsonMetaIO(repository=conftest.repo)
    json_handler.write(set_name=x.name, tags=x.tags)
    search_pattern = set_name[-17:-1]
    datasets_found = json_handler.search(
        contains=search_pattern, datasets=True, series=False
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")
    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 1
    assert datasets_found[0]["object_name"] == set_name
    assert datasets_found[0]["object_tags"] == x.tags


def test_search_for_dataset_by_part_of_name_with_multiple_matches_returns_list(
    conftest,
    xyz_at,
    caplog: LogCaptureFixture,
):
    """Test that searching for a dataset by part of its name with multiple matches returns a list of datasets."""
    caplog.set_level(logging.DEBUG)
    base_name = conftest.function_name_hex()
    json_handler = json_metadata.JsonMetaIO(repository=conftest.repo)

    x = Dataset(
        name=f"{base_name}_1",
        data_type=SeriesType.simple(),
        data=xyz_at,
    )
    json_handler.write(set_name=x.name, tags=x.tags)
    y = Dataset(
        name=f"{base_name}_2",
        data_type=SeriesType.simple(),
        data=xyz_at,
    )
    json_handler.write(set_name=y.name, tags=y.tags)

    search_pattern = base_name
    datasets_found = json_handler.search(
        contains=search_pattern, datasets=True, series=False
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert datasets_found
    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 2


def test_search_for_nonexisting_dataset_returns_none(
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    json_handler = json_metadata.JsonMetaIO(repository=conftest.repo)
    datasets_found = json_handler.search(pattern=set_name)

    assert not datasets_found
