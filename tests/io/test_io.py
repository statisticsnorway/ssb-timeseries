import logging
import uuid

# from pathlib import Path
from pytest import LogCaptureFixture

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dataset import search
from ssb_timeseries.io import DataIO

# from ssb_timeseries.dates import datelike_to_utc
from ssb_timeseries.properties import SeriesType

# mypy: ignore-errors

test_logger = logging.getLogger(__name__)


def test_datafile_exists_after_create_dataset_and_save(
    conftest,
    xyz_at,
    caplog,
) -> None:
    set_name = f"{conftest.function_name()}_{uuid.uuid4().hex}"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        data=xyz_at,
    )

    x.save()
    check = DataIO(x).dh.exists
    assert check


def test_read_existing_simple_data(
    existing_simple_set: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = existing_simple_set.name
    x = Dataset(name=set_name, data_type=SeriesType.simple())
    test_logger.debug(f"DATASET {x.name}: \n{x.data}")
    if DataIO(x).dh.exists:
        test_logger.debug(DataIO(x).dh.fullpath)
        test_logger.debug(f"{x.data=}")
        test_logger.debug(f"{x.data['valid_at'].unique()=}")
        assert x.data.shape == (12, 28)
    else:
        test_logger.debug(
            f"DATASET {x.name}: Data not found at {DataIO(x).dh.fullpath}. Writing."
        )
        raise AssertionError


def test_read_existing_estimate_data(
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

    assert DataIO(x).dh.exists
    test_logger.debug(x)
    assert x.data.shape == (12, 28)


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
    x.save()
    y = Dataset(
        name=f"{base_name}_2",
        data_type=SeriesType.simple(),
        data=xyz_at,
    )
    y.save()

    search_pattern = base_name
    datasets_found = search(
        pattern=search_pattern,
        repository=conftest.repo["directory"]["path"],
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert datasets_found
    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 2


def test_search_for_dataset_by_part_of_name_with_one_match_returns_the_set(
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
    x.save()
    search_pattern = set_name[-17:-1]
    datasets_found = search(
        # specify repo to ensure only one match; necessary because same repo is used twice
        repository=conftest.repo["directory"]["path"],
        pattern=search_pattern,
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")
    assert isinstance(datasets_found, Dataset)
    assert datasets_found.name == set_name
    assert datasets_found.data_type == SeriesType.simple()


def test_search_for_nonexisting_dataset_returns_none(
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    datasets_found = search(pattern=set_name)

    assert not datasets_found


def test_search_for_dataset_by_exact_name_in_single_repo_returns_the_set(
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
    x.save()
    search_pattern = set_name
    datasets_found = search(
        # specify repo to ensure only one match; necessary because same repo is used twice
        repository=conftest.repo["directory"]["path"],
        pattern=search_pattern,
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert isinstance(datasets_found, Dataset)
    assert datasets_found.name == set_name
    assert datasets_found.data_type == SeriesType.simple()
