import logging
import uuid
from datetime import timedelta

import pytest
from pytest import LogCaptureFixture

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dataset import search
from ssb_timeseries.dataset import select_repository
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import now_utc
from ssb_timeseries.fs import file_count
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.properties import Versioning
from ssb_timeseries.sample_data import create_df

# mypy: ignore-errors
# disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr,comparison-overlap"

# test_logger = logging.getLogger(__name__)
test_logger = logging.getLogger()
# test_logger = ts.logger


def test_select_repository():
    default = select_repository()
    test_1 = select_repository(name="test_1")
    test_2 = select_repository(name="test_2")
    assert default == test_1
    assert test_1 != test_2


def test_dataset_instance_created(
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    example = Dataset(name="test-no-dir-created", data_type=SeriesType.simple())
    assert isinstance(example, Dataset)


@pytest.mark.skip(reason="TODO: revisit dataset.__repr__.")
def test_dataset_instance_created_equals_repr(
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name="test-no-dir-created",
        data_type=SeriesType.simple(),
        as_of_tz="2022-01-01",
        data=create_df(
            ["p", "q", "r"],
            start_date="2022-01-01",
            end_date="2022-10-03",
            freq="MS",
        ),
    )
    test_logger.debug(f"Dataset a: {a!r}")
    b = eval(repr(a))
    test_logger.debug(f"Dataset b: {b!r}")

    # TODO: fix __repr__ OR identical so that this works
    assert a.identical(b)


@pytest.mark.skip(reason="TODO: revisit dataset.identical.")
def test_dataset_instance_identity(
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name="test-no-dir-need-to-be-created",
        data_type=SeriesType.simple(),
        as_of_tz="2022-01-01",
    )
    b = Dataset(
        name="test-no-dir-need-to-be-created",
        data_type=SeriesType.simple(),
        as_of_tz="2022-01-01",
    )
    c = Dataset(
        name="test-no-dir-created-different",
        data_type=SeriesType.simple(),
        as_of_tz="2022-12-01",
    )

    # TBD: when should two instances of a dataset be considered the same?
    # ... name and type + how many more attributes?
    assert a.identical(a)
    assert a.identical(b)
    assert not a.identical(c)


def test_dataset_copy_creates_new_instance(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    original = Dataset(name="test-copying-original-set", data_type=SeriesType.simple())
    new_name = "test-copying-copied-set"
    copy = original.copy(new_name)

    assert isinstance(copy, Dataset)
    assert id(original) != id(copy)
    assert copy.name == new_name
    assert copy.data_type == original.data_type


def test_dataset_copy_creates_set_with_new_name_and__otherwise_identical_attributes(
    xyz_at,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    original = Dataset(
        name="test-copying-original-set", data_type=SeriesType.simple(), data=xyz_at
    )
    assert isinstance(original, Dataset)
    copy = original.copy(new_name="test-copying-copied-set")
    assert isinstance(copy, Dataset)

    assert id(copy) != id(original)
    assert copy.name != original.name
    assert copy.tags != original.tags
    # popping dataset name from set and series tags would remove all differences
    # ... but renaming either object should have same effect:
    copy.rename(original.name)
    # this should still be true
    assert id(copy) != id(original)
    # .. but because of the rename these should now work
    assert copy.name == original.name
    assert copy.tags == original.tags
    assert (copy.numeric_array() == original.numeric_array()).all()

    # and this should also hold
    check_equal = copy == original
    assert isinstance(check_equal, Dataset)
    assert check_equal.data.shape == original.data.shape
    assert check_equal.all()


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
    check = x.io.datafile_exists()
    assert check


def test_metafile_exists_after_create_dataset_and_save(
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
    x.data = xyz_at
    x.save()
    test_logger.debug(x.io.metadata_fullpath)
    assert x.io.metadatafile_exists()


def test_same_simple_data_written_multiple_times_does_not_create_duplicates(
    caplog: LogCaptureFixture,
    conftest,
    xyz_at,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = f"{conftest.function_name()}-{uuid.uuid4().hex}"
    expected_data_size = xyz_at.shape
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=now_utc(rounding="Min"),
        data=xyz_at,
    )
    x.save()
    x.save()
    x.save()
    x.save()
    y = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=now_utc(rounding="Min"),
    )
    test_logger.debug(f"{y.data=}\n==\n{xyz_at=}")
    test_logger.debug(f"{y.data['valid_at'].unique()=}")
    assert y.data.shape == expected_data_size


def test_read_existing_simple_metadata(
    existing_simple_set: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = existing_simple_set.name
    x = Dataset(name=set_name, data_type=SeriesType.simple())
    if x.io.metadatafile_exists():
        test_logger.debug(x.io.metadata_fullpath)
        test_logger.debug(x.tags)
        test_logger.debug(x.tags["name"])
        assert x.tags["name"] == set_name and x.tags["versioning"] == str(
            Versioning.NONE
        )
    else:
        test_logger.debug(
            f"DATASET {x.name}: Metadata not found at {x.io.metadata_fullpath}. Writing."
        )
        raise AssertionError


def test_read_existing_simple_data(
    existing_simple_set: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = existing_simple_set.name
    x = Dataset(name=set_name, data_type=SeriesType.simple())
    test_logger.debug(f"DATASET {x.name}: \n{x.data}")
    if x.io.datafile_exists():
        test_logger.debug(x.io.data_fullpath)
        test_logger.debug(f"{x.data=}")
        test_logger.debug(f"{x.data['valid_at'].unique()=}")
        assert x.data.shape == (12, 28)
    else:
        test_logger.debug(
            f"DATASET {x.name}: Data not found at {x.io.data_fullpath}. Writing."
        )
        raise AssertionError


def test_read_existing_estimate_metadata(
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

    assert x.io.metadatafile_exists()
    test_logger.debug(x.io.metadata_fullpath)
    test_logger.debug(x.tags)
    assert x.tags["name"] == set_name
    assert x.tags["versioning"] == str(Versioning.AS_OF)
    for _, v in x.series_tags.items():
        assert v["A"] in ["a", "b", "c"]


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

    assert x.io.datafile_exists()
    test_logger.debug(x)
    assert x.data.shape == (12, 28)


def test_load_existing_set_without_loading_data(
    conftest,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = conftest.function_name()
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        load_data=False,
        as_of_tz=date_utc("2022-01-01"),
    )
    assert x.data.empty
    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-10-03", freq="MS"
    )
    x.save()
    assert not x.data.empty


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
        repository=conftest.repo["directory"],
        pattern=search_pattern,
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert isinstance(datasets_found, Dataset)
    assert datasets_found.name == set_name
    assert datasets_found.data_type == SeriesType.simple()


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
        repository=conftest.repo["directory"],
        pattern=search_pattern,
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")
    assert isinstance(datasets_found, Dataset)
    assert datasets_found.name == set_name
    assert datasets_found.data_type == SeriesType.simple()


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
        repository=conftest.repo["directory"],
    )
    test_logger.debug(f"search  for {search_pattern} returned: {datasets_found!s}")

    assert datasets_found
    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 2


def test_search_for_nonexisting_dataset_returns_none(
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    set_name = conftest.function_name_hex()
    datasets_found = search(pattern=set_name)

    assert not datasets_found


def test_list_versions_after_n_writes_returns_n_versions_with_different_data(
    caplog: LogCaptureFixture, existing_estimate_set: Dataset, conftest
):
    caplog.set_level(logging.DEBUG)

    new_set_name = conftest.function_name_hex()
    new_set = existing_estimate_set.copy(new_set_name)
    versions_before_saving = new_set.versions()
    assert len(versions_before_saving) == 0

    as_of_dates = [
        "2024-01-01",
        "2024-02-01",
        "2024-03-01",
        "2024-04-01",
        "2024-05-01",
        "2024-06-01",
    ]
    for d in as_of_dates:
        growing_data = create_df(
            ["a", "b"], start_date="2023-01-01", end_date=d, freq="MS"
        )
        ds = Dataset(
            new_set_name, new_set.data_type, date_utc(d), data=growing_data
        ).save()
        test_logger.debug("Compare first and last as_of date:\n%s\n", str(ds))

    versions_after_saving = new_set.versions()
    assert len(versions_after_saving) == len(as_of_dates)

    first_vs = Dataset(new_set_name, as_of_tz=date_utc(as_of_dates[0]))
    last_vs = Dataset(new_set_name, as_of_tz=date_utc(as_of_dates[-1]))
    assert len(first_vs) != len(last_vs)


@pytest.mark.skip("TO DO: test the right thing")
def test_read_versioned_data_partitions_by_as_of(
    caplog: LogCaptureFixture,
    existing_estimate_set: Dataset,
    conftest,
):
    caplog.set_level(logging.DEBUG)

    new_set_name = conftest.function_name_hex()
    new_set = existing_estimate_set.copy(new_set_name)
    versions_before_saving = new_set.versions()
    assert isinstance(new_set, Dataset)
    assert len(versions_before_saving) == 0

    as_of_dates = [
        "2024-01-01",
        "2024-02-01",
        "2024-03-01",
        "2024-04-01",
        "2024-05-01",
        "2024-06-01",
    ]
    for d in as_of_dates:
        new_set.as_of_utc = date_utc(d)
        new_set.data = create_df(
            new_set.series, start_date="2023-01-01", end_date=d, freq="MS"
        )
        new_set.save()

    # TO DO: this works, but we should check stored returned columns / Hive partitioning, not just the versions!
    versions_after_saving = new_set.versions()
    assert len(versions_after_saving) > len(versions_before_saving)


def test_list_versions_return_empty_list_for_set_not_saved(
    caplog: LogCaptureFixture, new_dataset_none_from_to: Dataset
):
    caplog.set_level(logging.DEBUG)

    assert new_dataset_none_from_to.versions() == []


def test_list_versions_return_latest_for_not_versioned_set(
    caplog: LogCaptureFixture, existing_simple_set: Dataset
):
    caplog.set_level(logging.DEBUG)

    assert existing_simple_set.versions() == ["latest"]


def test_dataset_getitem_by_string(
    existing_simple_set: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    x = existing_simple_set
    y = x["b_q_z1"]
    test_logger.debug(f"y = x['b']\n{y}")
    test_logger.debug(f"{__name__}look at y:\n\t{y}")
    test_logger.debug(f"{__name__}look at x:\n\t{x.data}")
    assert isinstance(y, Dataset)

    assert id(x) != id(y)
    assert list(y.data.columns) == ["valid_at", "b_q_z1"]


def test_dataset_getitem_by_tags(
    new_dataset_none_at: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    x = new_dataset_none_at
    y = x[[{"A": "a", "B": "q", "C": "z1"}]]
    assert isinstance(y, Dataset)

    test_logger.debug(f"y = x['b']\n{y}")
    test_logger.debug(f"{__name__}look at y:\n\t{y}")
    test_logger.debug(f"{__name__}look at x:\n\t{x.data}")
    assert id(x) != id(y)
    assert list(y.data.columns) == ["valid_at", "a_q_z1"]


def test_filter_dataset_by_regex_return_dataframe(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    y = x.filter(regex="^x", output="dataframe")
    test_logger.debug(f"y = x.filter(regex='^x')\n{y}")

    assert list(y.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


def test_filter_dataset_by_regex_return_dataset(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    y = x.filter(regex="^x")
    test_logger.debug(f"y = x.filter(regex='^x')\n{y}")

    assert isinstance(y, Dataset)
    assert list(y.data.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


def test_correct_datetime_columns_valid_at(
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name=f"test-datetimecols-{uuid.uuid4().hex}",
        data_type=SeriesType.simple(),
        data=create_df(
            ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
        ),
    )
    test_logger.debug(f"test_datetime_columns: {a.datetime_columns()}")
    assert a.datetime_columns() == ["valid_at"]
    assert a.numeric_columns() == ["x", "y", "z"]


def test_correct_datetime_columns_valid_from_to(
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name=f"test-datetimecols-{uuid.uuid4().hex}",
        data_type=SeriesType.as_of_from_to(),
        as_of_tz=now_utc(),
        data=create_df(
            ["x", "y", "z"],
            start_date="2022-01-01",
            end_date="2022-04-03",
            freq="MS",
            temporality="FROM_TO",
        ),
    )
    test_logger.debug(f"test_datetime_columns: {a.datetime_columns()}")
    assert a.datetime_columns() == ["valid_from", "valid_to"]
    assert a.numeric_columns() == ["x", "y", "z"]


def test_versioning_as_of_creates_new_file(
    existing_estimate_set: Dataset, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    x = existing_estimate_set
    y = x * 1.1
    files_before = file_count(x.io.data_dir)
    x.as_of_utc = now_utc()
    x.data = y.data
    x.save()
    files_after = file_count(x.io.data_dir)
    assert files_after == files_before + 1


def test_versioning_as_of_init_without_version_selects_latest(
    existing_estimate_set: Dataset, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    x = Dataset(existing_estimate_set.name)
    test_logger.debug(
        f"Init with only name of existing versioned set: {x.name}\n\t... identified {x.as_of_utc} as latest version."
    )
    assert isinstance(x, Dataset)
    assert x.as_of_utc == existing_estimate_set.versions()[-1]

    as_of = [now_utc() - timedelta(hours=n) for n in range(4)]
    for d in as_of:
        x.as_of_utc = date_utc(d)
        test_logger.debug(f"As of date:{d=}\nseries: {x.series}")
        x.data = create_df(
            x.series, start_date="2024-01-01", end_date="2024-12-03", freq="MS"
        )
        x.save()
    y = Dataset(existing_estimate_set.name)
    assert isinstance(y, Dataset)
    assert y.as_of_utc == max(as_of)


# @pytest.mark.filterwarnings("ignore")
# warning occuring here is not expected --> indicates an issue with .save()?
# unrelated to intent of test, so ignore her
# TODO: investigate
def test_versioning_none_appends_to_existing_file(
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    dataset_name = f"test-merge-{uuid.uuid4().hex}"
    dataset_type = SeriesType.simple()
    a = Dataset(
        name=dataset_name,
        data_type=dataset_type,
        data=create_df(
            ["x", "y", "z"], start_date="2022-01-01", end_date="2023-12-03", freq="MS"
        ),
    )
    a.save()

    b = Dataset(
        name=dataset_name,
        data_type=dataset_type,
        data=create_df(
            ["x", "y", "z"], start_date="2023-01-01", end_date="2024-12-03", freq="MS"
        ),
    )
    b.save()

    c = Dataset(name=dataset_name, data_type=dataset_type)
    test_logger.debug(
        f"DATASET: {a.name}: First write {len(a.data)} rows, second write {len(b.data)} rows (50% new) --> combined {len(c.data)} rows."
    )
    test_logger.debug(f"{a.data}\n{b.data}\n{c.data}\n{len(c.data)}")
    assert len(c.data) > len(a.data)
    assert len(c.data) > len(b.data)
    assert len(c.data) < len(a.data) + len(b.data)


def test_get_dataset_series_and_series_tags(
    new_dataset_none_at: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    x = new_dataset_none_at
    series_names = x.series
    series_tags = x.series_tags
    series_tags_keys = [k for k in series_tags.keys()]
    assert isinstance(series_names, list)
    assert isinstance(series_tags, dict)
    assert len(series_names) == len(series_tags_keys)
    assert sorted(series_names) == sorted(series_tags_keys)
