import logging
import uuid

import pytest
from pytest import LogCaptureFixture

from ssb_timeseries.dataset import Dataset

# from ssb_timeseries.dataset import search
from ssb_timeseries.dates import now_utc
from ssb_timeseries.io import MetaIO
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.properties import Versioning

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


def test_metadata_exists_only_after_set_create_and_save(
    caplog: LogCaptureFixture,
    xyz_at,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = f"test-metafile-exists-{uuid.uuid4().hex}"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=now_utc(rounding="Min"),
    )
    assert not MetaIO(x).dh.exists
    x.data = xyz_at
    x.save()
    assert MetaIO(x).dh.read(set_name=x.name) == x.tags


def test_read_metadata_for_existing_simple_set_returns_expected_values(
    existing_simple_set: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = existing_simple_set.name
    x = Dataset(name=set_name, data_type=SeriesType.simple())
    assert MetaIO(x).dh.read(set_name=x.name) == existing_simple_set.tags
    assert x.tags["name"] == set_name and x.tags["versioning"] == str(Versioning.NONE)


def test_read_metadata_for_existing_estimate_set_returns_expected_values(
    existing_estimate_set: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = existing_estimate_set.name
    as_of = existing_estimate_set.as_of_utc
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=as_of,
    )

    test_logger.debug(MetaIO(x).dh.fullpath())
    assert MetaIO(x).dh.read(set_name=x.name) == existing_estimate_set.tags
    assert x.tags["name"] == set_name
    assert x.tags["versioning"] == str(Versioning.AS_OF)
    for _, v in x.series_tags.items():
        assert v["A"] in ["a", "b", "c"]


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
