import logging
import uuid
from datetime import timedelta

import pytest
from pytest import LogCaptureFixture

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dataset import search
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import now_utc
from ssb_timeseries.fs import file_count
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.properties import SeriesType
from ssb_timeseries.properties import Versioning
from ssb_timeseries.sample_data import create_df

# mypy: disable-error-code="arg-type,attr-defined,no-untyped-def,union-attr"


# BUCKET = CONFIG.bucket


@log_start_stop
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
    ts_logger.warning(f"Dataset a: {a!r}")
    b = eval(repr(a))
    ts_logger.warning(f"Dataset b: {b!r}")

    # TODO: fix __repr__ OR identical so that this works
    assert a.identical(b)


@pytest.mark.skip(reason="TODO: revisit dataset.identical.")
@log_start_stop
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
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    original = Dataset(name="test-copying-original-set", data_type=SeriesType.simple())
    new_name = "test-copying-copied-set"
    copy = original.copy(new_name)

    assert id(copy) != id(original)
    assert copy.name != original.name
    assert copy.tags != original.tags
    # popping dataset name from set and series tags before comparing should remove all differences:
    # original.tags.pop("name")
    # copy.tags.pop("name")
    # for series in copy.tags["series"].values():
    #     series.pop("dataset")
    # for series in original.tags["series"].values():
    #     series.pop("dataset")
    # ... but renaming either object should have same effect:
    copy.rename(original.name)
    # this should still be true
    assert id(copy) != id(original)
    # ..buyt because
    assert copy.name == original.name
    assert copy.tags == original.tags
    assert all(copy.data == original.data)
    # but this should now hold!
    assert copy == original


@log_start_stop
def test_create_dataset_with_correct_data_size() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x", "y", "z"]}
    x = Dataset(
        name="test-no-dir-created",
        data_type=SeriesType.simple(),
        series_tags=tags,
        dataset_tags={},
    )

    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    assert x.data.size == 280


@log_start_stop
def test_datafile_exists_after_create_dataset_and_save(
    conftest,
    bup_and_tdwn,
    caplog,
) -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    tag_values = [value for value in tags.values()]
    set_name = f"{conftest.function_name()}_{uuid.uuid4().hex}"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        # series_tags=tags,
        # dataset_tags={},
        data=create_df(
            *tag_values,
            start_date="2022-01-01",
            end_date="2022-10-03",
            freq="MS",
        ),
    )

    x.save()
    check = x.io.datafile_exists()
    assert check


@log_start_stop
def test_metafile_exists_after_create_dataset_and_save(
    bup_and_tdwn,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    set_name = f"test-metafile-exists-{uuid.uuid4().hex}"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=now_utc(rounding="Min"),
        # series_tags=tags,
        dataset_tags={},
    )

    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )

    x.save()
    ts_logger.debug(x.io.metadata_fullpath)
    assert x.io.metadatafile_exists()


@log_start_stop
def test_read_existing_simple_metadata(
    existing_simple_set: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = existing_simple_set.name
    x = Dataset(name=set_name, data_type=SeriesType.simple())
    if x.io.metadatafile_exists():
        ts_logger.debug(x.io.metadata_fullpath)
        ts_logger.debug(x.tags)
        ts_logger.debug(x.tags["name"])
        assert x.tags["name"] == set_name and x.tags["versioning"] == str(
            Versioning.NONE
        )
    else:
        ts_logger.debug(
            f"DATASET {x.name}: Metadata not found at {x.io.metadata_fullpath}. Writing."
        )
        raise AssertionError


@log_start_stop
def test_read_existing_simple_data(
    existing_simple_set: Dataset,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = existing_simple_set.name
    x = Dataset(name=set_name, data_type=SeriesType.simple())
    ts_logger.debug(f"DATASET {x.name}: \n{x.data}")
    if x.io.datafile_exists():
        ts_logger.debug(x.io.data_fullpath)
        ts_logger.debug(x.data)
        assert x.data.size == 280
    else:
        ts_logger.debug(
            f"DATASET {x.name}: Data not found at {x.io.data_fullpath}. Writing."
        )
        raise AssertionError


@log_start_stop
def test_read_existing_estimate_metadata(
    bup_and_tdwn,
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
    ts_logger.debug(x.io.metadata_fullpath)
    ts_logger.debug(x.tags)
    assert x.tags["name"] == set_name
    assert x.tags["versioning"] == str(Versioning.AS_OF)


@log_start_stop
def test_read_existing_estimate_data(
    bup_and_tdwn,
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
    ts_logger.debug(x)
    assert x.data.size == 364


@log_start_stop
def test_load_existing_set_without_loading_data(
    bup_and_tdwn,
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
        series_tags=tags,
    )
    assert x.data.empty
    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-10-03", freq="MS"
    )
    x.save()
    assert not x.data.empty


@log_start_stop
def test_search_for_dataset_by_exact_name(
    bup_and_tdwn,
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    unique_new = uuid.uuid4().hex
    set_name = f"{conftest.function_name()}_{unique_new}"
    x = Dataset(name=set_name, data_type=SeriesType.simple(), load_data=False)
    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    x.save()
    datasets_found = search(pattern=set_name)
    ts_logger.debug(f"datasets: {datasets_found!s}")

    assert isinstance(datasets_found, Dataset)
    assert datasets_found.name == set_name
    assert datasets_found.data_type == SeriesType.simple()


@log_start_stop
def test_search_for_dataset_by_part_of_name_one_match(
    bup_and_tdwn,
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    unique_new = uuid.uuid4().hex
    set_name = f"{conftest.function_name()}_{unique_new}"
    x = Dataset(name=set_name, data_type=SeriesType.simple(), load_data=False)
    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    x.save()
    datasets_found = search(pattern=unique_new)
    ts_logger.debug(f"datasets: {datasets_found!s}")
    assert isinstance(datasets_found, Dataset)
    assert datasets_found.name == set_name
    assert datasets_found.data_type == SeriesType.simple()


def test_search_for_dataset_by_part_of_name_two_matches(
    bup_and_tdwn,
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    unique_new = str(uuid.uuid4())
    set_name_1 = f"{conftest.function_name()}_{unique_new}-1"
    set_name_2 = f"{conftest.function_name()}_{unique_new}-2"
    tag_values = [["p", "q", "r"]]
    df = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    x = Dataset(name=set_name_1, data_type=SeriesType.simple(), data=df)
    x.save()
    y = Dataset(name=set_name_2, data_type=SeriesType.simple(), data=df)
    y.save()
    datasets_found = search(pattern=unique_new)
    ts_logger.debug(f"datasets: {datasets_found!s}")
    assert datasets_found
    assert isinstance(datasets_found, list)
    assert len(datasets_found) == 2


@log_start_stop
def test_search_for_nonexisting_dataset_returns_none(
    conftest,
    caplog: LogCaptureFixture,
):
    caplog.set_level(logging.DEBUG)
    unique_new = uuid.uuid4().hex
    set_name = f"{conftest.function_name()}_{unique_new}"
    x = Dataset(name=set_name, data_type=SeriesType.simple(), load_data=False)
    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    datasets_found = search(pattern=unique_new)

    ts_logger.debug(f"datasets: {datasets_found!s}")
    assert not datasets_found


@log_start_stop
def test_list_versions(caplog: LogCaptureFixture, existing_estimate_set: Dataset):
    caplog.set_level(logging.DEBUG)

    versions_before_saving = existing_estimate_set.versions()
    assert len(versions_before_saving) == 1

    for d in ["2024-01-01", "2024-02-01", "2024-03-01"]:
        existing_estimate_set.as_of_utc = date_utc(d)
        existing_estimate_set.data = create_df(
            existing_estimate_set.series, start_date="2023-01-01", end_date=d, freq="MS"
        )
        ts_logger.debug(f"Do we have data?\n{existing_estimate_set.data}")
        existing_estimate_set.save()

    versions_after_saving = existing_estimate_set.versions()
    assert len(versions_after_saving) == 4


@log_start_stop
def test_list_versions_return_empty_list_for_set_not_saved(
    caplog: LogCaptureFixture, new_dataset_none_from_to: Dataset
):
    caplog.set_level(logging.DEBUG)

    assert new_dataset_none_from_to.versions() == []


@log_start_stop
def test_list_versions_return_latest_for_not_versioned_set(
    caplog: LogCaptureFixture, existing_simple_set: Dataset
):
    caplog.set_level(logging.DEBUG)

    assert existing_simple_set.versions() == ["latest"]


@log_start_stop
def test_dataset_getitem_by_string(
    existing_simple_set: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    x = existing_simple_set
    y = x["b_q_z1"]
    assert isinstance(y, Dataset)

    ts_logger.debug(f"y = x['b']\n{y}")
    ts_logger.debug(f"{__name__}look at y:\n\t{y}")
    ts_logger.debug(f"{__name__}look at x:\n\t{x.data}")
    assert id(x) != id(y)
    assert list(y.data.columns) == ["valid_at", "b_q_z1"]


@log_start_stop
def test_dataset_getitem_by_tags(
    new_dataset_none_at: Dataset, caplog: LogCaptureFixture
):
    caplog.set_level(logging.DEBUG)

    x = new_dataset_none_at
    y = x[{"A": "a", "B": "q", "C": "z1"}]
    assert isinstance(y, Dataset)

    ts_logger.debug(f"y = x['b']\n{y}")
    ts_logger.debug(f"{__name__}look at y:\n\t{y}")
    ts_logger.debug(f"{__name__}look at x:\n\t{x.data}")
    assert id(x) != id(y)
    assert list(y.data.columns) == ["valid_at", "a_q_z1"]


@log_start_stop
def test_filter_dataset_by_regex_return_dataframe(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    y = x.filter(regex="^x", output="dataframe")
    ts_logger.debug(f"y = x.filter(regex='^x')\n{y}")

    assert list(y.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


@log_start_stop
def test_filter_dataset_by_regex_return_dataset(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    y = x.filter(regex="^x")
    ts_logger.debug(f"y = x.filter(regex='^x')\n{y}")

    assert isinstance(y, Dataset)
    assert list(y.data.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


@log_start_stop
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
    ts_logger.debug(f"test_datetime_columns: {a.datetime_columns()}")
    assert a.datetime_columns() == ["valid_at"]


@log_start_stop
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
    ts_logger.debug(f"test_datetime_columns: {a.datetime_columns()}")
    assert a.datetime_columns().sort() == ["valid_from", "valid_to"].sort()


@log_start_stop
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


@log_start_stop
def test_versioning_as_of_init_without_version_selects_latest(
    existing_estimate_set: Dataset, caplog: LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    x = Dataset(existing_estimate_set.name)
    ts_logger.warning(
        f"Init with only name of existing versioned set: {x.name}\n\t... identified {x.as_of_utc} as latest version."
    )
    assert isinstance(x, Dataset)
    assert x.as_of_utc == existing_estimate_set.versions()[-1]

    as_of = [now_utc() - timedelta(hours=n) for n in range(4)]
    for d in as_of:
        x.as_of_utc = date_utc(d)
        ts_logger.warning(f"As of date:{d=}\nseries: {x.series}")
        x.data = create_df(
            x.series, start_date="2024-01-01", end_date="2024-12-03", freq="MS"
        )
        x.save()
    y = Dataset(existing_estimate_set.name)
    assert isinstance(y, Dataset)
    assert y.as_of_utc == max(as_of)


@log_start_stop
def test_versioning_none_appends_to_existing_file(
    bup_and_tdwn,
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name=f"test-merge-{uuid.uuid4().hex}",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
    )
    a.data = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-12-03", freq="MS"
    )
    a.save()

    b = Dataset(name=a.name, data_type=a.data_type, load_data=False)
    b.data = create_df(
        ["x", "y", "z"], start_date="2022-07-01", end_date="2023-06-03", freq="MS"
    )
    b.save()

    c = Dataset(name=a.name, data_type=a.data_type, load_data=True)
    ts_logger.debug(
        f"DATASET: {a.name}: First write {a.data.size} values, writing {b.data.size} values (50% new) --> combined {c.data.size} values."
    )

    assert a.data.size < c.data.size
    assert b.data.size < c.data.size


@log_start_stop
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
