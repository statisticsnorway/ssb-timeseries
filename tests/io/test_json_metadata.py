import logging

import pytest
from pytest import LogCaptureFixture

from ssb_timeseries import fs
from ssb_timeseries.dataset import Dataset

# from ssb_timeseries.dataset import search
from ssb_timeseries.io import MetaIO
from ssb_timeseries.properties import SeriesType

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

test_logger = logging.getLogger(__name__)
# test_logger = logging.getLogger()
# test_logger = ts.logger

# metadata tests copied from test_dataset_core


@pytest.fixture()
def meta_io(conftest):
    """Get a MetaIO instance for the configured repository without specifying dataset."""
    yield MetaIO(repository=conftest.repo["name"])


# =============================== TESTS ===================================


def test_read_existing_metadata_works_for_all_series_types(
    one_existing_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that reading metadata from a pre-saved dataset works for all series types."""
    caplog.set_level(logging.DEBUG)
    existing_dataset = one_existing_set_for_each_data_type

    # 1. Verify that the metadata file exists
    meta_handler = MetaIO(existing_dataset).dh
    assert fs.exists(meta_handler.fullpath(existing_dataset.name)), (
        "Metadata file does not exist."
    )

    # 2. Read the metadata back and verify it matches the source
    read_tags = meta_handler.read(set_name=existing_dataset.name)
    assert read_tags == existing_dataset.tags, (
        f"Tag mismatch for type {existing_dataset.data_type}."
    )


def test_search_for_dataset_by_exact_name_in_single_repo_returns_the_set(
    conftest,
    meta_io,
    xyz_at,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    x = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        load_data=False,
        data=xyz_at,
    )
    x.save()
    search_pattern = set_name
    datasets_found = meta_io.search(equals=search_pattern)
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 1
    assert datasets_found[0]["object_name"] == set_name
    assert datasets_found[0]["object_tags"] == x.tags


def test_search_for_dataset_by_part_of_name_with_one_match_returns_the_set(
    conftest,
    meta_io,
    xyz_at,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    x = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        load_data=False,
        data=xyz_at,
    )
    x.save()
    search_pattern = set_name[-17:-1]
    datasets_found = meta_io.search(
        contains=search_pattern, datasets=True, series=False
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")
    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 1
    assert datasets_found[0]["object_name"] == set_name
    assert datasets_found[0]["object_tags"] == x.tags


def test_search_for_dataset_by_part_of_name_with_multiple_matches_returns_list(
    conftest,
    meta_io,
    xyz_at,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    base_name = conftest.function_name_hex()

    x = Dataset(
        name=f"{base_name}_1",
        data_type=SeriesType.simple(),
        data=xyz_at,
    )
    x.save()
    y = Dataset(
        name=f"{base_name}_2",
        data_type=SeriesType.simple(),
        data=xyz_at,
    )
    y.save()

    search_pattern = base_name
    datasets_found = meta_io.search(
        contains=search_pattern, datasets=True, series=False
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert datasets_found
    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 2


def test_search_for_nonexisting_dataset_returns_none(
    conftest,
    meta_io,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    datasets_found = meta_io.search(pattern=set_name)

    assert not datasets_found
