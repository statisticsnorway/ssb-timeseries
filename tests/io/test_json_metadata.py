"""Unit tests for the `json_metadata` I/O handler."""

import logging

import pytest
from pytest import LogCaptureFixture

from ssb_timeseries.config import Config
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.io import fs
from ssb_timeseries.io import json_metadata
from ssb_timeseries.types import SeriesType

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

test_logger = logging.getLogger(__name__)


# =============================== HELPERS ===============================


def metadata_handler(repository: str | dict = ""):
    """Build the metadata handler the way the dispatcher would."""
    repo = (
        repository
        if isinstance(repository, dict)
        else Config.active().repositories[repository]
    )
    return json_metadata.JsonMetaIO(
        repository=repository,
        path=str(repo["catalog"]["options"]["path"]),
    )


# =============================== TESTS ===================================


def test_read_existing_metadata_works_for_all_series_types(
    one_existing_set_for_each_data_type: Dataset,
    conftest,
    caplog: LogCaptureFixture,
) -> None:
    """Test that reading metadata from a pre-saved dataset works for all series types."""
    caplog.set_level(logging.DEBUG)
    existing_dataset = one_existing_set_for_each_data_type
    json_handler = metadata_handler(conftest.repo)

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
    json_handler = metadata_handler(conftest.repo)
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
    json_handler = metadata_handler(conftest.repo)
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
    json_handler = metadata_handler(conftest.repo)

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


@pytest.mark.parametrize("repository", [None, ""], ids=["missing", "empty"])
def test_write_rejects_tags_without_repository(
    conftest,
    repository,
) -> None:
    """Metadata must name the repository it is written to."""
    set_name = conftest.function_name_hex()
    json_handler = metadata_handler(conftest.repo)
    tags = {"name": set_name}
    if repository is not None:
        tags["repository"] = repository

    with pytest.raises(ValueError, match="must contain a non-empty 'repository' tag"):
        json_handler.write(set_name=set_name, tags=tags)

    assert not fs.exists(json_handler.fullpath(set_name))


def test_write_propagates_filesystem_errors(
    conftest,
    monkeypatch,
    caplog: LogCaptureFixture,
) -> None:
    """Filesystem failures must be raised instead of reported as successful writes."""
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    json_handler = metadata_handler(conftest.repo)

    def fail_write(*_args, **_kwargs) -> None:
        raise OSError("forced metadata write failure")

    monkeypatch.setattr(json_metadata.fs, "write_json", fail_write)

    with pytest.raises(OSError, match="forced metadata write failure"):
        json_handler.write(
            set_name=set_name,
            tags={"name": set_name, "repository": conftest.repo["name"]},
        )

    assert "JsonMetaIO.write.error" in caplog.text
    assert "JsonMetaIO.write.success" not in caplog.text


def test_search_for_nonexisting_dataset_returns_none(
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    json_handler = metadata_handler(conftest.repo)
    datasets_found = json_handler.search(pattern=set_name)

    assert not datasets_found


def test_deprecated_search_result_warns_and_is_still_constructible() -> None:
    """Assert the `SearchResult` deprecation shim keeps old callers working.

    `SearchResult` was superseded by `CatalogItem`. The PEP 562 shim must stay
    importable so callers get a warning rather than an ImportError, and
    `pyproject.toml` sets `filterwarnings = ["error"]`, so an access that failed
    to warn would be a hard failure here.
    """
    with pytest.warns(DeprecationWarning, match="CatalogItem"):
        search_result = json_metadata.SearchResult

    item = search_result(name="a-set", type_directory="some/directory")
    assert item.name == "a-set"
    assert item.type_directory == "some/directory"
    assert isinstance(item, tuple)


def test_json_metadata_still_rejects_unknown_attributes() -> None:
    """Assert the deprecation shim does not mask typos as deprecated names.

    A permissive `__getattr__` would silently return something for any
    misspelling, turning a clear AttributeError into a baffling failure later.
    """
    misspelled = "SearchResul"
    with pytest.raises(AttributeError, match="has no attribute"):
        getattr(json_metadata, misspelled)
