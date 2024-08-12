import logging
from collections import namedtuple

import pytest

# from ssb_timeseries.catalog import CatalogItem
from ssb_timeseries.catalog import Catalog
from ssb_timeseries.catalog import Repository
from ssb_timeseries.logging import ts_logger

# from ssb_timeseries.meta import Taxonomy

# mypy: ignore-errors
# ruff: noqa


@pytest.fixture()
def repo_1(
    buildup_and_teardown,
):
    """Init repo 'test_1'."""
    config = buildup_and_teardown
    repo = Repository(name="test_1", directory=config.catalog)
    yield repo


@pytest.fixture()
def repo_2(
    buildup_and_teardown,
):
    """Init repo 'test_2'."""
    config = buildup_and_teardown
    repo = Repository(name="test_2", directory=config.catalog)
    yield repo


@pytest.fixture()
def catalog_with_one_repo(
    repo_1,
):
    """Init catalog with repo_1."""
    catalog = Catalog(config=[repo_1])
    yield catalog


@pytest.fixture()
def catalog_with_two_repos(
    repo_1,
    repo_2,
):
    """Init catalog with repo_1 and repo_2."""
    catalog = Catalog(config=[repo_1, repo_2])
    yield catalog


def test_init_repo_1_and_repo_2(
    repo_1: Repository,
    repo_2: Repository,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Init repo 'test_1'."""
    caplog.set_level(logging.DEBUG)

    assert isinstance(repo_1, Repository)
    assert isinstance(repo_2, Repository)


def test_repository_datasets_called_with_no_params_lists_all_datasets_in_a_single_repo(
    existing_sets,
    repo_1,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)
    expected = {ds.name for ds in existing_sets}

    all_sets_in_test_repo_1 = {ds.object_name for ds in repo_1.datasets()}
    ts_logger.debug(f"{all_sets_in_test_repo_1=}")
    assert repo_1.count(object_type="dataset") >= 3
    assert all_sets_in_test_repo_1 >= expected


def test_repository_series_called_with_no_params_lists_all_series_in_a_single_repo(
    existing_sets,
    repo_1,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)

    # Because series names are unique only within the dataset,
    # we concat set and series names before comparing:

    expected_series = []
    for ds in existing_sets:
        [expected_series.append(f"{ds.name}:{s}") for s in ds.series]

    series_of_repo_1 = repo_1.series()
    names_of_all_series_in_repo_1 = {
        f"{s.parent}:{s.object_name}" for s in series_of_repo_1
    }

    assert repo_1.count(object_type="series") >= 57
    assert set(names_of_all_series_in_repo_1) >= set(expected_series)


def test_init_catalog_with_one_repo(
    catalog_with_one_repo: Catalog,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
    assert isinstance(catalog, Catalog)
    assert catalog.count(object_type="datasets") > 0


def test_init_catalog_with_two_repos(
    catalog_with_two_repos: Catalog,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_two_repos
    assert isinstance(catalog, Catalog)
    assert catalog.count(object_type="datasets") > 0


def test_init_catalog_with_repo_like_objects(
    repo_1: Repository,
    repo_2: Repository,
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
            repo_1,
            repo_2,
            config_from_named_tuple,
        ]
    )
    assert isinstance(catalog, Catalog)
    assert catalog.count(object_type="dataset") > 0
    assert catalog.count(object_type="dataset") == repo_1.count(
        object_type="dataset"
    ) * len(catalog.repository)


def test_catalog_datasets_called_with_no_params_lists_all_datasets_for_catalog_w_one_repo(
    existing_sets,
    catalog_with_one_repo: Catalog,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No criteria returns all datasets."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
    repos = [r.name for r in catalog.repository]

    all_sets_in_catalog = {
        ds.repository_name + ":" + ds.object_name for ds in catalog.datasets()
    }
    assert catalog.count(object_type="dataset") >= 3
    expected_sets = {r + ":" + ds.name for ds in existing_sets for r in repos}
    assert all_sets_in_catalog >= expected_sets


def test_catalog_datasets_called_with_no_params_lists_all_datasets_for_catalog_w_two_repos(
    existing_sets,
    catalog_with_two_repos,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No criteria returns all datasets."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_two_repos
    repos = [r.name for r in catalog.repository]

    all_sets_in_catalog = {
        ds.repository_name + ":" + ds.object_name for ds in catalog.datasets()
    }
    assert catalog.count(object_type="dataset") == len(all_sets_in_catalog)
    assert catalog.count(object_type="dataset") >= len(repos) * len(existing_sets)

    expected_sets = {r + ":" + ds.name for ds in existing_sets for r in repos}
    assert all_sets_in_catalog >= expected_sets


def test_catalog_series_called_with_no_params_lists_all_datasets_for_catalog_w_two_repos(
    existing_sets,
    catalog_with_two_repos,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No criteria returns all seriess."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_two_repos
    repos = [r.name for r in catalog.repository]

    all_series_in_catalog = {
        f"{ds.repository_name}:{ds.parent}:{ds.object_name}" for ds in catalog.series()
    }

    assert catalog.count(object_type="series") == len(all_series_in_catalog)
    assert catalog.count(object_type="series") >= len(repos) * 57

    expected_series = set()
    for each_set in existing_sets:
        [
            expected_series.add(f"{r}:{each_set.name}:{s}")
            for s in each_set.series
            for r in repos
        ]
    assert all_series_in_catalog >= expected_series


# paramterize to test for permutations over:
# unit_of_test: repository, catalog_w_repo,  catalog_w_two_repos, catalog_w_repo_like_obj
# objects: 'datasets' 'series' 'both'
# criteria: none, equals, contains, regex, single_tag, multiple_tags, tags_with_multiple_values,
# return_types:  list[CatalogItem] for valid criteria; None for invalid criteria


def test_find_datasets_using_single_set_attribute(
    caplog,
) -> None:
    """Find datasets where ."""
    caplog.set_level(logging.DEBUG)

    ...
