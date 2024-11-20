import inspect
import os
from pathlib import Path

import pytest

from ssb_timeseries import config
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors

TEST_DIR = ""
ENV_VAR_NAME = "TIMESERIES_CONFIG"


class Helpers:
    configuration: config.Config = config.CONFIG

    @staticmethod
    def test_dir() -> str:
        return TEST_DIR

    @staticmethod
    def function_name() -> str:
        return str(inspect.stack()[1][3])


@pytest.fixture(scope="function", autouse=False)
def conftest() -> Helpers:
    h = Helpers()
    return h


@pytest.fixture(
    scope="session",
    autouse=True,
)
def buildup_and_teardown(
    tmp_path_factory,
):
    """To make sure that tests do not change the configuration file."""
    before_tests = config.CONFIG
    env_var_value = os.environ.pop(ENV_VAR_NAME, None)

    if before_tests.configuration_file:
        print(
            f"Before running tests:\nTIMESERIES_CONFIG: {before_tests.configuration_file}:\n{before_tests.to_json()}"
        )
        cfg_file = Path(before_tests.configuration_file).name
        config_file_for_testing = str(tmp_path_factory.mktemp("config") / cfg_file)
        config.CONFIG.configuration_file = config_file_for_testing
        config.CONFIG.timeseries_root = str(tmp_path_factory.mktemp("series_data"))
        config.CONFIG.catalog = str(tmp_path_factory.mktemp("metadata"))
        config.CONFIG.bucket = str(tmp_path_factory.mktemp("production-bucket"))
        config.CONFIG.save(config_file_for_testing)
        # config.CONFIG.save(cfg_file)
        TEST_DIR = config_file_for_testing
        Helpers.configuration = config.CONFIG

    else:
        print(
            f"No configuration file found before tests:\nTIMESERIES_CONFIG: {before_tests.configuration_file}\n..raise error?"
        )

    print(f"Current configurations:\n{config.CONFIG}")

    # run tests
    yield config.CONFIG

    # teardown: reset config
    if config.CONFIG != before_tests:
        print(
            f"Configurations was changed by tests:\n{config.CONFIG}\nReverting to original:\n{before_tests}"
        )
        before_tests.save(before_tests.configuration_file)
    else:
        print(
            f"Final configurations after tests are identical to orginal:\n{config.CONFIG}\nReverting to original:\n{before_tests}"
        )
    os.environ[ENV_VAR_NAME] = env_var_value


@pytest.fixture(scope="session", autouse=False)
def tag_values():
    """Define series names for which to generate test data."""
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    yield tag_values


@pytest.fixture(scope="session", autouse=False)
def abc_at(tag_values):
    df = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-12-03",
        freq="MS",
        temporality="AT",
    )
    yield df


@pytest.fixture(scope="session", autouse=False)
def abc_from_to(tag_values):
    df = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-12-03",
        freq="MS",
        temporality="FROM_TO",
    )
    yield df


@pytest.fixture(scope="function", autouse=False)
def xyz_at():
    df = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
        temporality="AT",
    )
    yield df


@pytest.fixture(scope="function", autouse=False)
def xyz_from_to():
    df = create_df(
        ["x", "y", "z"],
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
        temporality="FROM_TO",
    )
    yield df


@pytest.fixture(scope="function", autouse=False)
def new_dataset_none_at(abc_at):
    """A fixture to create simple dataset before running the test."""
    # buildup: create dataset
    x = Dataset(
        name="test-new-dataset-none-at",
        data_type=SeriesType.simple(),
        series_tags={"D": "d"},
        data=abc_at,
        name_pattern=["A", "B", "C"],
        dataset_tags={"E": "Eee"},
    )

    # run tests
    yield x
    # file was not saved, so no teardown is necessary


@pytest.fixture(scope="function", autouse=False)
def new_dataset_as_of_at(abc_at):
    """A fixture to create simple dataset before running the test."""
    # buildup: create dataset
    x = Dataset(
        name="test-new-dataset-as-of-at",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags={"D": "d"},
        data=abc_at,
        name_pattern=["A", "B", "C"],
        dataset_tags={"E": "Eee"},
    )
    # run tests
    yield x
    # file was not saved, so no teardown is necessary


@pytest.fixture(scope="function", autouse=False)
def new_dataset_none_from_to(abc_from_to):
    """A fixture to create simple dataset before running the test."""
    # buildup: create dataset
    x = Dataset(
        name="test-new-dataset-none-from-to",
        data_type=SeriesType.from_to(),
        series_tags={"D": "d"},
        data=abc_from_to,
        name_pattern=["A", "B", "C"],
        dataset_tags={"E": "Eee"},
    )
    # run tests
    yield x
    # file was not saved, so no teardown is necessary


@pytest.fixture(scope="function", autouse=False)
def new_dataset_as_of_from_to(abc_from_to):
    """A fixture to create simple dataset before running the test."""
    # buildup: create dataset
    x = Dataset(
        name="test-new-as-of-from-to",
        data_type=SeriesType.as_of_from_to(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags={"D": "d"},
        data=abc_from_to,
        name_pattern=["A", "B", "C"],
        dataset_tags={"E": "Eee"},
    )

    # run tests
    yield x
    # file was not saved, so no teardown is necessary


@pytest.fixture(scope="function", autouse=False)
def one_new_set_for_each_data_type(
    new_dataset_none_at,
    new_dataset_none_from_to,
    new_dataset_as_of_at,
    new_dataset_as_of_from_to,
):
    """A fixture returning one example dataset for each data type in a list."""
    yield [
        new_dataset_none_at,
        new_dataset_none_from_to,
        new_dataset_as_of_at,
        new_dataset_as_of_from_to,
    ]
    # file was not saved, so no teardown is necessary


@pytest.fixture(scope="module", autouse=False)
def existing_simple_set(abc_at):
    """Create a simple dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    # buildup: create dataset and save
    x = Dataset(
        name="test-existing-simple-dataset",
        data_type=SeriesType.simple(),
        data=abc_at,
        name_pattern=["A", "B", "C"],
    )
    x.save()
    # run tests
    yield x

    # TEARDOWN
    # Puzzling observation:
    # Data is written for every invocation in test_dataset_core.py, as expected for function scope.
    # However, the writes create duplicate data!
    # This is not reproduced within an explicit test in test_dataset_core.py:
    # test_same_simple_data_written_multiple_times_does_not_create_duplicates(...)
    # --> Workarounds:
    #  change scope of this fixture to module or session
    # ... or delete data for each invocation:
    # fs.rmtree(x.io.data_dir)
    # fs.rm(x.io.metadata_fullpath)


@pytest.fixture(scope="function", autouse=False)
def existing_estimate_set(abc_at):
    """Create an estimeat (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    # buildup: create dataset and save
    x = Dataset(
        name="test-existing-estimate-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_at,
        name_pattern=["A", "B", "C"],
    )
    x.save()

    # tests run here
    yield x

    # teardown: cleaning up files is handled by session scoped fixture
    # fs.rmtree(x.io.data_dir)
    # fs.rm(x.io.metadata_fullpath)


@pytest.fixture(scope="function", autouse=False)
def existing_from_to_set(abc_from_to):
    """Create an estimeat (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    # buildup: create dataset and save
    x = Dataset(
        name="test-existing-small-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=abc_from_to,
        name_pattern=["A", "B", "C"],
        series_tags={"D": "d"},
        dataset_tags={"E": "e", "F": ["f1", "f2"]},
    )
    x.save()

    # tests run here
    yield x

    # teardown
    # fs.rmtree(x.io.data_dir)
    # fs.rm(x.io.metadata_fullpath)


@pytest.fixture(scope="function", autouse=False)
def existing_small_set():
    """Create an estimeat (as_of_at) dataset (and save so that files are existing) before running the test. Delete files afterwards."""
    # buildup: create dataset and save
    tags = {"A": ["a1", "a2", "a3"], "B": ["b"], "C": ["c"]}
    tag_values = [value for value in tags.values()]
    x = Dataset(
        name="test-existing-small-dataset",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2024-01-03",
            freq="YS",
        ),
        name_pattern=["A", "B", "C"],
        series_tags={"D": "d"},
        dataset_tags={"E": "e", "F": ["f1", "f2"]},
    )
    x.save()

    # tests run here
    yield x

    # teardown
    # fs.rmtree(x.io.data_dir)
    # fs.rm(x.io.metadata_fullpath)


@pytest.fixture(scope="function", autouse=False)
def existing_sets(existing_estimate_set, existing_simple_set, existing_small_set):
    """A fixture returning one existing (previously saved) example dataset for each data type."""
    yield [
        existing_estimate_set,
        existing_simple_set,
        existing_small_set,
    ]
