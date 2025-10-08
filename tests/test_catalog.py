import logging
from collections import namedtuple
from math import log

import pytest

import ssb_timeseries as ts
from ssb_timeseries.catalog import Catalog
from ssb_timeseries.catalog import Repository


# mypy: ignore-errors
# ruff: noqa

test_logger = logging.getLogger()


@pytest.fixture()
def repo_1(
    buildup_and_teardown,
):
    """Init repo 'test_1'."""
    config = buildup_and_teardown
    r = config.repositories
    # cheat an use the same repo twice:
    repo = Repository(name="test_1", catalog=r["test_1"]["catalog"]["path"])
    yield repo


@pytest.fixture()
def repo_2(
    buildup_and_teardown,
):
    """Init repo 'test_2'."""
    config = buildup_and_teardown
    r = config.repositories
    # cheat an use the same repo twice:
    repo = Repository(name="test_2", catalog=r["test_2"]["catalog"]["path"])
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


@pytest.fixture(scope="function")
def find_this_existing_set(buildup_and_teardown):
    """Create and save an estimate (as_of_at) dataset with tags that lend themselves to unique matches.

    {"A": "a1-hil" | "a2-lkjsd" | "a3-qwj"} uniquely identifies series in the set
    {"D": "d-ukji"} identifies all series in this set, but not the set itself
    {"E": "e-askli", ...} identifies this dataset + all the series in the set
    """

    tags = {"A": ["a1-hil", "a2-lkjsd", "a3-qwj"], "B": ["b"], "C": ["c"]}
    tag_values = [value for value in tags.values()]
    x = ts.dataset.Dataset(
        name="find_this_existing_set",
        data_type=ts.properties.SeriesType.estimate(),
        as_of_tz="2022-01-01",
        data=ts.sample_data.create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2024-01-03",
            freq="YS",
        ),
        attributes=["A", "B", "C"],
        series_tags={"D": "d-ukji"},
        dataset_tags={"E": "e-askli", "F": ["f1", "f2"]},
    )
    x.save()
    yield x


@pytest.fixture(scope="function")
def existing_sets(
    existing_estimate_set,
    existing_simple_set,
    existing_small_set,
    find_this_existing_set,
):
    """Return (some) existing (previously saved) example dataset, among them special set above."""
    yield [
        existing_estimate_set,
        existing_simple_set,
        find_this_existing_set,
        existing_small_set,
    ]


# ================================ tests =================================


def test_ts_catalog_returns_a_catalog_object() -> None:
    catalog = ts.get_catalog()
    assert isinstance(catalog, Catalog)


def test_ts_catalog_w_existing_sets_returns_a_catalog_over_sets_and_series(
    existing_sets,
) -> None:
    catalog = ts.get_catalog()
    number_of_sets = len(catalog.datasets())
    number_of_series = len(catalog.series())
    number_of_items = len(catalog.items())
    assert number_of_sets >= 3
    assert number_of_series >= 57
    assert number_of_sets + number_of_series == number_of_items


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
    test_logger.debug(f"{all_sets_in_test_repo_1=}")
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

    # any object with .name and .catalog should work(?)
    # we will try namedtuple:
    ConfigTuple = namedtuple("ConfigTuple", ["name", "catalog"])
    repo_1_catalog = buildup_and_teardown.repositories["test_1"]["catalog"]["path"]
    tuple_repo = ConfigTuple("test_named_tuple", repo_1_catalog)
    catalog = Catalog(
        config=[
            repo_2,
            repo_1,
            tuple_repo,
        ]
    )
    test_logger.warning(f"{catalog.repositories=}")
    assert isinstance(catalog, Catalog)
    assert len(catalog.repositories) == 3
    assert catalog.count(object_type="dataset") > 0
    n_0 = catalog.repositories[0].count(object_type="dataset")
    n_1 = catalog.repositories[1].count(object_type="dataset")
    n_2 = catalog.repositories[2].count(object_type="dataset")

    assert n_0 == 0
    assert n_1 == n_2 == catalog.count(object_type="dataset") / 2


def test_catalog_datasets_called_with_no_params_lists_all_datasets_for_catalog_w_one_repo(
    existing_sets,
    catalog_with_one_repo: Catalog,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No criteria returns all datasets."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
    repos = [r.name for r in catalog.repositories]

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
    repos = [r.name for r in catalog.repositories]

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
    repos = [r.name for r in catalog.repositories]

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
    find_this_existing_set,
) -> None:
    """Filtering by tags: {<attritbute>: <value>}."""
    caplog.set_level(logging.DEBUG)
    # tags = None returns all items
    assert len(repo_1.datasets(tags=None)) >= 3
    assert len(repo_1.series(tags=None)) >= 57
    assert len(repo_1.items(tags=None)) >= 60

    # tags = dict() returns all matching items
    # TODO
    criteria = {"A": "a1-hil"}
    assert len(repo_1.datasets(tags=criteria)) >= 0  # none expected
    assert len(repo_1.series(tags=criteria)) >= 1
    assert len(repo_1.items(tags=criteria)) >= 1


def test_catalog_search_by_tag_dict(
    catalog_with_one_repo,
    caplog: pytest.LogCaptureFixture,
    find_this_existing_set,
) -> None:
    """Filtering by tags chosen so that hits are expected for 'find_this_existing_set' only."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
    num_repos = len(catalog.repositories)

    def log_items(cc):
        for c in catalog.items(tags=cc):
            test_logger.debug(
                f"\t{c.repository_name} {c.object_type} {c.parent}.{c.object_name} {c.has_tags(criteria)}"
            )

    criteria = {"A": "a1-hil"}
    log_items(criteria)
    assert len(catalog.datasets(tags=criteria)) == 0 * num_repos
    assert len(catalog.series(tags=criteria)) == 1 * num_repos
    assert len(catalog.items(tags=criteria)) == 1 * num_repos

    criteria = {"E": "e-askli"}
    log_items(criteria)
    assert len(catalog.datasets(tags=criteria)) == 1 * num_repos
    assert len(catalog.series(tags=criteria)) == 3 * num_repos
    assert len(catalog.items(tags=criteria)) == 4 * num_repos

    criteria = {"A": "a1-nothing-to-be-found", "E": "e-askli"}
    log_items(criteria)
    assert len(catalog.datasets(tags=criteria)) == 0 * num_repos
    assert len(catalog.series(tags=criteria)) == 0 * num_repos
    assert len(catalog.items(tags=criteria)) == 0 * num_repos


def test_catalog_search_by_tag_dict_multiple_criteria(
    catalog_with_one_repo,
    caplog: pytest.LogCaptureFixture,
    existing_sets,
) -> None:
    """Simple test case for filtering by tags."""
    caplog.set_level(logging.DEBUG)
    catalog = catalog_with_one_repo
    num_repos = len(catalog.repositories)
    # The tag: D=d-ukji gives a match only in dataset 'find_this_existing_set'
    criteria = {"B": "b", "D": "d-ukji"}
    for result in catalog.items(tags=criteria):
        test_logger.debug(
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
        test_logger.debug(
            f"\t{result.repository_name}\t{result.object_type}\t{result.parent}.{result.object_name}"
        )

    assert len(catalog.items(tags=[criteria_1, criteria_2])) == len(
        catalog.items(tags=criteria_1)
    ) + len(catalog.items(tags=criteria_2))


# --------------------------------------------
# Work in progress: parametrize tests
# --------------------------------------------

# The test cases above cover key features with the most obvious parameters.
# However, they are not exhaustive. Errors could occur for specific combinations of parameters.
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
