import logging
import uuid

import pytest
from pytest import LogCaptureFixture

from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import date_utc
from ssb_timeseries.dates import now_utc
from ssb_timeseries.io import CONFIG
from ssb_timeseries.logging import log_start_stop
from ssb_timeseries.logging import ts_logger
from ssb_timeseries.properties import SeriesType  # , Temporality
from ssb_timeseries.properties import Versioning  # , Temporality
from ssb_timeseries.sample_data import create_df

# from ssb_timeseries import fs

# mypy: ignore-errors

BUCKET = CONFIG.bucket


@log_start_stop
def test_dataset_instance_created(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    example = Dataset(name="test-no-dir-created", data_type=SeriesType.simple())
    assert isinstance(example, Dataset)


@pytest.mark.skipif(True, reason="revisit dataset.__repr__.")
def test_dataset_instance_created_equals_repr(caplog: LogCaptureFixture) -> None:
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
    assert a is a
    # TO DO: CHECK THIS
    # assert a == a
    # TEMPORARY DISABLED skip_<name>
    # TO DO: fix __repr__ OR identical so that this works
    assert a.identical(b)


@pytest.mark.skipif(True, reason="revisit dataset.identical.")
@log_start_stop
def test_dataset_instance_identity(caplog: LogCaptureFixture) -> None:
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

    # TEMPORARY DISABLED skip_<name>
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
    assert copy.name == new_name
    assert original != copy


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
def test_datafile_exists_after_create_dataset_and_save() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    set_name = f"test-{uuid.uuid4().hex}"
    x = Dataset(
        name=set_name,
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
    x.save()
    check = x.io.datafile_exists()
    assert check


@log_start_stop
def test_metafile_exists_after_create_dataset_and_save(
    caplog: LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    set_name = f"test-metafile-exists-{uuid.uuid4().hex}"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=now_utc(rounding="Min"),
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

    x.save()
    ts_logger.warning(x.io.metadata_fullpath)
    assert x.io.metadatafile_exists()


@log_start_stop
def test_read_existing_simple_metadata(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    set_name = "persisted-example-simple"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        series_tags=tags,
    )
    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
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
        x.save()
        raise AssertionError


@log_start_stop
def test_read_existing_simple_data(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    set_name = "persisted-example-simple"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.simple(),
        series_tags=tags,
    )
    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    if x.io.datafile_exists():
        ts_logger.debug(x.io.data_fullpath)
        ts_logger.debug(x.data)
        assert x.data.size == 280
    else:
        ts_logger.debug(
            f"DATASET {x.name}: Data not found at {x.io.data_fullpath}. Writing."
        )
        x.save()
        raise AssertionError


@log_start_stop
def test_read_existing_estimate_metadata(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    set_name = "persisted-example-estimate"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags=tags,
    )
    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    if x.io.metadatafile_exists():
        ts_logger.debug(x.io.metadata_fullpath)
        ts_logger.debug(x.tags)
        assert x.tags["name"] == set_name
        assert x.tags["versioning"] == str(Versioning.AS_OF)
    else:
        ts_logger.warning(
            f"DATASET {x.name}: Metadata not found at {x.io.metadata_fullpath}. Writing."
        )
        x.save()
        raise AssertionError


@log_start_stop
def test_read_existing_estimate_data(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    set_name = "persisted-example-estimate"
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        series_tags=tags,
    )
    tag_values: list[list[str]] = [value for value in tags.values()]
    ts_logger.debug(x.as_of_utc)
    ts_logger.debug(x.data)
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2023-01-03",
        freq="MS",
    )
    ts_logger.debug(x.data)
    if x.io.datafile_exists():
        ts_logger.debug(x)
        assert x.data.size == 364
    else:
        ts_logger.warning(
            f"DATASET {x.name}: Data not found at {x.io.data_fullpath}. Writing."
        )
        x.save()
        raise AssertionError


@log_start_stop
def test_load_existing_set_without_loading_data(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = "persisted-example-estimate"
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
def test_search_for_dataset_by_part_of_name(caplog: LogCaptureFixture):
    caplog.set_level(logging.DEBUG)
    unique_new = uuid.uuid4().hex
    x = Dataset(
        name=f"test-find-{unique_new}", data_type=SeriesType.simple(), load_data=False
    )
    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    x.save()
    datasets = x.search(unique_new)
    ts_logger.debug(f"datasets: {datasets!s}")
    assert datasets == [f"test-find-{unique_new}"]


@log_start_stop
def test_dataset_getitem_by_string(caplog: LogCaptureFixture):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a", "b", "c"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-06-01", freq="MS"
    )
    y = x["b"]
    ts_logger.debug(f"y = x['b']\n{y}")

    ts_logger.debug(f"{__name__}look at y: {y}")
    ts_logger.debug(f"{__name__}look at x: {x.data}")
    assert list(y.data.columns) == ["valid_at", "b"]
    assert list(x.data.columns) == ["valid_at", "a", "b", "c"]
    # confirm that x and y are not the same object


@log_start_stop
def test_filter_dataset_by_regex_return_dataframe(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    y = x.filter(regex="^x", output="dataframe")
    ts_logger.debug(f"y = x.filter(regex='^x')\n{y.data}")

    # assert isinstance(y, Dataset)
    assert list(y.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


@log_start_stop
def test_filter_dataset_by_regex(caplog: LogCaptureFixture):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    y = x.filter(regex="^x")
    ts_logger.debug(f"y = x.filter(regex='^x')\n{y.data}")

    assert isinstance(y, Dataset)
    assert list(y.data.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


@log_start_stop
def test_filter_dataset_by_regex_new_dataset(caplog: LogCaptureFixture):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    y = x.filter(
        regex="^x", name="test-filter", data_type=SeriesType.simple(), load_data=False
    )
    ts_logger.debug(f"datasets: {y}")
    ts_logger.debug(f"datasets: {x}")

    assert list(y.data.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


@log_start_stop
def test_correct_datetime_columns_valid_at(caplog: LogCaptureFixture) -> None:
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
def test_correct_datetime_columns_valid_from_to(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name=f"test-datetimecols-{uuid.uuid4().hex}",
        data_type=SeriesType.as_of_from_to(),
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
def test_versioning_as_of_creates_new_file(caplog: LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    # raise AssertionError
    pass
    # TO DO:
    # Verify behaviours of data types / saving correctly:
    # AS_OF --> new files, keep/compare multiple versions


@log_start_stop
def test_versioning_none_appends_to_existing_file(caplog: LogCaptureFixture) -> None:
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