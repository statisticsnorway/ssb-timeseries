import logging
from collections import namedtuple
from math import log

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
    catalog_with_one_repo,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No criteria returns all datasets."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
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
    catalog_with_one_repo,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No criteria returns all seriess."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
    repos = [r.name for r in catalog.repository]

    all_series_in_catalog = {
        f"{ds.repository_name}:{ds.parent}:{ds.object_name}" for ds in catalog.series()
    }

    assert catalog.count(object_type="series") == len(all_series_in_catalog)
    assert catalog.count(object_type="series") >= len(repos) * 57

    expected_series = set()
    for member_set in existing_sets:
        [
            expected_series.add(f"{r}:{member_set.name}:{s}")
            for s in member_set.series
            for r in repos
        ]
    assert all_series_in_catalog >= expected_series


def test_repository_search_by_tag_dict_none(
    repo_1: Repository,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Filtering by tags = None returns everything"""
    caplog.set_level(logging.DEBUG)
    # tags = None is the default
    assert set(repo_1.datasets(tags=None)) == set(repo_1.datasets())
    assert len(repo_1.series(tags=None)) == len(repo_1.series())
    assert len(repo_1.items(tags=None)) == len(repo_1.items())

    # tags = None returns all items
    assert len(repo_1.datasets(tags=None)) >= 3
    assert len(repo_1.series(tags=None)) >= 57
    assert len(repo_1.items(tags=None)) >= 60


def test_repository_search_by_tag_dict(
    repo_1: Repository,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Filtering by tags: {<attritbute>: <value>}."""
    caplog.set_level(logging.DEBUG)
    # tags = None returns all items
    assert len(repo_1.datasets(tags=None)) >= 3
    assert len(repo_1.series(tags=None)) >= 57
    assert len(repo_1.items(tags=None)) >= 60

    # tags = dict() returns all matching items
    # TODO
    criteria = {"A": "a1"}
    assert len(repo_1.datasets(tags=criteria)) >= 0  # none expected
    assert len(repo_1.series(tags=criteria)) >= 1
    assert len(repo_1.items(tags=criteria)) >= 1


def test_catalog_search_by_tag_dict(
    catalog_with_one_repo,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Simple test case for filtering by tags."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
    num_repos = len(catalog.repository)

    def log_items(cc):
        for c in catalog.items(tags=cc):
            ts_logger.debug(
                f"\t{c.repository_name} {c.object_type} {c.parent}.{c.object_name} {c.has_tags(criteria)}"
            )

    criteria = {"A": "a"}
    log_items(criteria)
    assert len(catalog.datasets(tags=criteria)) == 0 * num_repos
    assert len(catalog.series(tags=criteria)) == 18 * num_repos
    assert len(catalog.items(tags=criteria)) == 18 * num_repos

    criteria = {"E": "e"}
    log_items(criteria)
    assert len(catalog.datasets(tags=criteria)) == 1 * num_repos
    assert len(catalog.series(tags=criteria)) == 3 * num_repos
    assert len(catalog.items(tags=criteria)) == 4 * num_repos

    criteria = {"A": "a", "E": "e"}
    log_items(criteria)
    assert len(catalog.datasets(tags=criteria)) == 0 * num_repos
    assert len(catalog.series(tags=criteria)) == 0 * num_repos
    assert len(catalog.items(tags=criteria)) == 0 * num_repos


def test_catalog_search_by_tag_dict_multiple_criteria(
    catalog_with_one_repo,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Simple test case for filtering by tags."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
    num_repos = len(catalog.repository)
    # The tag: D=d gives a match only in dataset 'test-exising-small-dataset'
    criteria = {"B": "b", "D": "d"}
    for result in catalog.items(tags=criteria):
        ts_logger.debug(
            f"\t{result.repository_name}\t{result.object_type}\t{result.parent}.{result.object_name}\t{result.has_tags(criteria)}"
        )
    assert len(catalog.datasets(tags=criteria)) == 0 * num_repos
    assert len(catalog.series(tags=criteria)) == 3 * num_repos
    assert len(catalog.items(tags=criteria)) == 3 * num_repos


def test_catalog_search_by_list_of_tag_dicts(
    catalog_with_one_repo,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo

    criteria_1 = {"dataset": "test-existing-estimate-dataset", "A": "a"}
    criteria_2 = {"dataset": "test-existing-small-dataset", "A": "a2"}

    for result in catalog.items(tags=[criteria_1, criteria_2]):
        ts_logger.debug(
            f"\t{result.repository_name}\t{result.object_type}\t{result.parent}.{result.object_name}"
        )

    assert len(catalog.items(tags=[criteria_1, criteria_2])) == len(
        catalog.items(tags=criteria_1)
    ) + len(catalog.items(tags=criteria_2))


# --------------------------------------------
# Work in progress: parameterize tests
# --------------------------------------------

# The test cases above cover key features with the most obvious parameters.
# However, the test cases are not exhaustive. Errors could occur for specific combinations of parameters.
#
# Hence, it makes sense to test permutations over:
# unit_of_test: repository, catalog_w_repo,  catalog_w_two_repos, catalog_w_repo_like_obj
# objects: 'datasets' 'series' 'items'
# criteria:
#  (valid, innvalid) x (none, equals, contains, single_criteria_tag_dict, multiple_criteria_tag_dict, tags_dict_criteria_with_multiple_values, ...)
# result_mappings: (valid --> list[CatalogItem], invalid --> None)


@pytest.fixture(params=["datasets", "series"])
def return_type(request):
    """Iterate over result object types."""
    return request.param


@pytest.fixture(params=[repo_1, catalog_with_one_repo])
def target_config(request):
    """Iterate over target configurations: Repo, Catalog with one or more repos, or repo like objects."""
    return request.param()


@pytest.fixture(
    params=[
        (None, None, (3, 3 * 57)),
        ("equals", "test-existing-small-dataset", (1, 57)),
        ("equals", "test-existing-ssssmall-dataset", (0, 0)),
        ("contains", "estimate", (1, 57)),
        ("contains", "esttimate", (0, 0)),
    ],
    ids=[
        "none",
        "valid_equals",
        "invalid_equals",
        "valid_contains",
        "invalid_contains",
    ],
)
def test_case(request):
    """Iterate over result object types."""
    return request.param


# def test_criteria(return_type, test_case):
#     """Iterate over test cases, result object types, and target configurations."""
#     catalog_or_repo = target_config
#     parameter_name = test_case[0]
#     parameter_value = test_case[1]
#     if return_type == "datasets":
#         expected = test_case[2][0]
#     elif return_type == "series":
#         expected = test_case[2][1]
#     criteria = {parameter_name: parameter_value}
#     assert catalog_or_repo.count(object_type=return_type, **criteria) == expected
