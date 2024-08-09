# mypy: ignore-errors = True

import logging
from collections import namedtuple

import pytest

# from ssb_timeseries.catalog import CatalogItem
from ssb_timeseries.catalog import Catalog
from ssb_timeseries.catalog import ObjectType
from ssb_timeseries.catalog import Repository
from ssb_timeseries.logging import ts_logger

# from ssb_timeseries.meta import Taxonomy
# mypy: ignore-errors
# ruff: noqa


@pytest.fixture()
def test_init_repo_1(
    buildup_and_teardown: "Config",  # type: ignore
    caplog: pytest.LogCaptureFixture,
) -> None:  # type: ignore
    """Init repo 'test_1'."""
    caplog.set_level(logging.DEBUG)
    config = buildup_and_teardown
    ts_logger.debug(f"{config}")

    repo = Repository(name="test_1", directory=config.catalog)
    assert isinstance(repo, Repository)
    yield repo


@pytest.fixture()
def test_init_repo_2(
    buildup_and_teardown: "Config",  # type: ignore
    caplog: pytest.LogCaptureFixture,
) -> None:  # type: ignore
    """Init repo 'test_2'."""
    caplog.set_level(logging.DEBUG)
    config = buildup_and_teardown
    ts_logger.debug(f"{config}")

    repo = Repository(name="test_2", directory=config.catalog)
    assert isinstance(repo, Repository)
    yield repo


def test_datasets_with_no_params_lists_all_datasets_in_a_single_repo(
    existing_sets,
    test_init_repo_1,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)
    # for ds in existing_sets:
    #     ts_logger.debug(f"{ds.name}\t{ds.io.metadata_fullpath}")
    expected = {ds.name for ds in existing_sets}

    all_sets_in_test_repo_1 = {ds.object_name for ds in test_init_repo_1.datasets()}
    ts_logger.debug(f"{all_sets_in_test_repo_1=}")
    assert test_init_repo_1.count(object_type="dataset") >= 3
    assert all_sets_in_test_repo_1 >= expected


def test_init_catalog_with_one_repo(
    test_init_repo_1,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)

    catalog = Catalog(config=[test_init_repo_1])
    assert isinstance(catalog, Catalog)
    assert catalog.count(object_type="datasets") > 0


def test_init_catalog_with_two_repos(
    test_init_repo_1,
    test_init_repo_2,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)

    catalog = Catalog(config=[test_init_repo_1, test_init_repo_2])
    assert isinstance(catalog, Catalog)
    assert catalog.count(object_type="datasets") > 0


def test_init_catalog_with_repo_like_objects(
    test_init_repo_1,
    test_init_repo_2,
    buildup_and_teardown,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)

    # any object with .name and .directory should work(?)
    # we will try with dict and namedtuple:
    ConfigTuple = namedtuple("ConfigTuple", ["name", "directory"])
    config_from_named_tuple = ConfigTuple(
        "test_named_tuple", buildup_and_teardown.catalog
    )
    catalog = Catalog(
        config=[
            test_init_repo_1,
            test_init_repo_2,
            config_from_named_tuple,
        ]
    )
    assert isinstance(catalog, Catalog)
    assert catalog.count(object_type="dataset") > 0
    assert catalog.count(object_type="dataset") == test_init_repo_1.count(
        object_type="dataset"
    ) * len(catalog.repository)


def test_catalog_datasets_or_series_called_with_no_params_lists_all_datasets_for_catalog_w_one_repo(
    existing_sets,
    test_init_repo_1,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)

    catalog = Catalog(config=[test_init_repo_1])
    ts_logger.debug(f"Data catalog with same repo twice:\n{catalog}")

    all_sets_in_catalog = {
        ds.repository_name + ":" + ds.object_name for ds in catalog.datasets()
    }
    assert catalog.count(object_type="dataset") >= 3
    expected = {
        r.name + ":" + ds.name for ds in existing_sets for r in [test_init_repo_1]
    }
    assert all_sets_in_catalog >= expected


def test_catalog_datasets_or_series_called_with_no_params_lists_all_datasets_for_catalog_w_two_repos(
    existing_sets,
    test_init_repo_1,
    test_init_repo_2,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)
    expected = {ds.name for ds in existing_sets}

    catalog = Catalog(config=[test_init_repo_1, test_init_repo_2])

    ts_logger.debug(f"Data catalog with same repo twice:\n{catalog}")

    all_sets_in_catalog = {
        ds.repository_name + ":" + ds.object_name for ds in catalog.datasets()
    }
    ts_logger.warning(f"{all_sets_in_catalog=}")
    assert catalog.count(object_type="dataset") >= 2 * len(existing_sets)
    expected = {
        r.name + ":" + ds.name
        for ds in existing_sets
        for r in [test_init_repo_1, test_init_repo_2]
    }
    assert all_sets_in_catalog >= expected

    # assert catalog.count(object_type="series") >= 2 * 57
    # expected = {
    #     r.name + ":" + s.dataset + ":" + s.name
    #     for s in existing_sets["series"]
    #     for r in [test_init_repo_1, test_init_repo_2]
    # }
    # assert all_sets_in_catalog >= expected


def test_find_datasets_using_single_set_attribute(
    caplog,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)

    ...
