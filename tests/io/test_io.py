"""Integration tests for the high-level I/O facade in `io/__init__.py`."""

import logging

# from pathlib import Path
from pytest import LogCaptureFixture

from ssb_timeseries import io
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.types import SeriesType

# from ssb_timeseries.dates import datelike_to_utc


# mypy: ignore-errors

test_logger = logging.getLogger(__name__)


def test_read_existing_data_works_for_all_series_types(
    one_existing_set_for_each_data_type: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    """Test that reading a pre-saved dataset works for all series types."""
    caplog.set_level(logging.DEBUG)
    existing_dataset = one_existing_set_for_each_data_type
    expected_shape = existing_dataset.data.shape

    # 1. Verify that the data file exists, as it's created by the fixture
    assert io.DataIO(existing_dataset).dh.exists, "Data file does not exist."

    # 2. Create a new dataset instance to read the data back
    read_dataset = Dataset(
        name=existing_dataset.name,
        data_type=existing_dataset.data_type,
        as_of_tz=existing_dataset.as_of_utc,
    )

    # 3. Verify the data is read correctly and has the expected shape
    assert read_dataset.data.shape == expected_shape, (
        f"Data shape mismatch for type {existing_dataset.data_type}."
    )


def test_search_for_dataset_by_part_of_name_with_multiple_matches_returns_list(
    conftest,
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
    io.save(x)
    y = Dataset(
        name=f"{base_name}_2",
        data_type=SeriesType.simple(),
        data=xyz_at,
    )
    io.save(y)

    search_pattern = base_name[-22:-1]
    datasets_found = io.search(
        contains=search_pattern,
        # repository=conftest.repo["name"],
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert datasets_found
    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 2


def test_search_for_dataset_by_setname_contains_with_one_match_returns_list_with_one_item(
    conftest,
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
    io.save(x)
    search_pattern = set_name[-17:-1]
    datasets_found = io.search(
        # specify repo to ensure only one match; necessary because same repo is used twice
        repository=conftest.repo["name"],
        contains=search_pattern,
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")
    assert isinstance(datasets_found, list) and len(datasets_found) == 1
    assert datasets_found[0]["object_name"] == set_name
    assert datasets_found[0]["object_tags"] == x.tags


def test_search_for_nonexisting_dataset_returns_none(
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    datasets_found = io.search(pattern=set_name)

    assert not datasets_found


def test_search_for_dataset_by_setname_equals_in_single_repo_returns_list_with_one_item(
    conftest,
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
    io.save(x)
    search_pattern = set_name
    # specify repo to ensure only one match; necessary because same repo is used twice for tests
    datasets_found = io.search(
        repository=conftest.repo["name"],
        equals=search_pattern,
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert isinstance(datasets_found, list) and len(datasets_found) == 1
    assert datasets_found[0]["object_name"] == set_name
    assert datasets_found[0]["object_tags"] == x.tags


def test_search_for_dataset_by_setname_pattern_in_single_repo_returns_list_with_one_item(
    conftest,
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
    io.save(x)
    search_pattern = set_name.replace("set", "*").replace("y", "?")
    # specify repo to ensure only one match; necessary because same repo is used twice for tests
    datasets_found = io.search(
        repository=conftest.repo["name"],
        pattern=search_pattern,
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert isinstance(datasets_found, list) and len(datasets_found) == 1
    assert datasets_found[0]["object_name"] == set_name
    assert datasets_found[0]["object_tags"] == x.tags


def test_search_for_dataset_in_multiple_repos_returns_list_with_multiple_items(
    conftest,
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
        repository="test_1",
    )
    io.save(x)
    y = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        load_data=False,
        data=xyz_at,
        repository="test_2",
    )
    io.save(y)
    datasets_found = io.search(
        equals=set_name,
    )
    test_logger.debug(f"search for {set_name} returned: {datasets_found!s}")

    assert isinstance(datasets_found, list) and len(datasets_found) == 2
