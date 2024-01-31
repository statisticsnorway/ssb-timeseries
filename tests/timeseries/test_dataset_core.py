import os
import uuid

# import numpy as np
# import pandas as pd
# import functools
import logging

from timeseries.dates import now_utc, date_utc
from timeseries.logging import ts_logger, log_start_stop
from timeseries.dataset import Dataset
from timeseries.properties import SeriesType, Versioning  # , Temporality
from timeseries.sample_data import create_df

from timeseries.io import PRODUCT_BUCKET as BUCKET
from timeseries.io import STATISTICS_PRODUCT as PRODUCT


@log_start_stop
def test_dataset_instance_created(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    example = Dataset(name="test-no-dir-created", data_type=SeriesType.simple())
    assert isinstance(example, Dataset)


def SKIP_test_dataset_instance_created_equals_repr(caplog) -> None:
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
    ts_logger.warning(f"Dataset a: {repr(a)}")
    b = eval(repr(a))
    ts_logger.warning(f"Dataset b: {repr(b)}")
    assert a is a
    # TO DO: CHECK THIS
    # assert a == a
    # TEMPORARY DISABLED skip_<name>
    # TO DO: fix __repr__ OR identical so that this works
    assert a.identical(b)


@log_start_stop
def SKIP_test_dataset_instance_identity(caplog) -> None:
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
def test_metafile_exists_after_create_dataset_and_save(caplog) -> None:
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
    ts_logger.debug(x.io.metadata_fullpath)
    assert x.io.metadatafile_exists()


@log_start_stop
def test_read_existing_simple_metadata(caplog) -> None:
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
        assert False


@log_start_stop
def test_read_existing_simple_data(caplog) -> None:
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
        assert False


@log_start_stop
def test_read_existing_estimate_metadata(caplog) -> None:
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
        assert False


@log_start_stop
def test_read_existing_estimate_data(caplog) -> None:
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
        assert False


@log_start_stop
def test_load_existing_set_without_loading_data(caplog) -> None:
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
    x.save
    assert not x.data.empty


@log_start_stop
def test_publish_simple_set_has_higher_snapshot_file_count_after(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-snapshot-simple",
        data_type=SeriesType.simple(),
        load_data=False,
        data=create_df(
            ["p", "q", "r"], start_date="2022-01-01", end_date="2022-12-31", freq="YS"
        ),
    )

    x.stage = "statistikk"
    stage_path = x.io.snapshot_directory(product=PRODUCT, process_stage=x.stage)
    path_123 = x.io.dir(BUCKET, PRODUCT, "shared", "s123")
    path_234 = x.io.dir(BUCKET, PRODUCT, "shared", "s234")
    x.sharing = [
        {
            "team": "s123",
            "path": path_123,
        },
        {
            "team": "s234",
            "path": path_234,
        },
    ]
    x.save()

    path_123 = x.io.dir(path_123, x.name)
    path_234 = x.io.dir(path_234, x.name)

    count_before_snapshot = len(os.listdir(stage_path))
    count_before_123 = len(os.listdir(path_123))
    count_before_234 = len(os.listdir(path_234))

    x.snapshot()

    count_after_snapshot = len(os.listdir(stage_path))
    count_after_123 = len(os.listdir(path_123))
    count_after_234 = len(os.listdir(path_234))

    def log(path, before, after):
        ts_logger.debug(
            f"SNAPSHOT to {path}\n\tfile count before:{before}, after: {after}"
        )

    log(stage_path, count_before_snapshot, count_after_snapshot)
    log(path_123, count_before_123, count_after_123)
    log(path_234, count_before_234, count_after_234)

    assert count_before_snapshot < count_after_snapshot
    assert count_before_123 < count_after_123
    assert count_before_234 < count_after_234
    # assert False


@log_start_stop
def test_snapshot_estimate_has_higher_file_count_after(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-snapshot-estimate",
        data_type=SeriesType.estimate(),
        as_of_tz=date_utc("2022-01-01"),
        load_data=False,
        data=create_df(
            ["p", "q", "r"], start_date="2022-01-01", end_date="2022-12-31", freq="YS"
        ),
    )

    x.stage = "statistikk"
    stage_path = x.io.snapshot_directory(product=PRODUCT, process_stage=x.stage)
    path_123 = x.io.dir(BUCKET, PRODUCT, "shared", "s123")
    path_234 = x.io.dir(BUCKET, PRODUCT, "shared", "s234")
    x.sharing = [
        {
            "team": "s123",
            "path": path_123,
        },
        {
            "team": "s234",
            "path": path_234,
        },
    ]

    path_123 = x.io.dir(path_123, x.name)
    path_234 = x.io.dir(path_234, x.name)

    x.save()

    count_before_snapshot = len(os.listdir(stage_path))
    count_before_123 = len(os.listdir(path_123))
    count_before_234 = len(os.listdir(path_234))

    x.snapshot()

    count_after_snapshot = len(os.listdir(stage_path))
    count_after_123 = len(os.listdir(path_123))
    count_after_234 = len(os.listdir(path_234))

    def log(path, before, after):
        ts_logger.debug(
            f"SNAPSHOT to {path}\n\tfile count before:{before}, after: {after}"
        )

    log(stage_path, count_before_snapshot, count_after_snapshot)
    log(path_123, count_before_123, count_after_123)
    log(path_234, count_before_234, count_after_234)

    assert count_before_snapshot < count_after_snapshot
    assert count_before_123 < count_after_123
    assert count_before_234 < count_after_234
    # assert False


@log_start_stop
def test_search_for_dataset_by_part_of_name(caplog):
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
    ts_logger.warning(f"datasets: {str(datasets)}")
    assert datasets == [f"test-find-{unique_new}"]


@log_start_stop
def test_dataset_getitem_by_string(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a", "b", "c"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-06-01", freq="MS"
    )
    y = x["b"]
    ts_logger.debug(f"y = x['b']\n{y}")

    # TO DO / DECISION: we now get a dataframe for y, should probably get a (new) dataset?
    # ... but then need to update name / metadata?
    # assert list(y.data.columns) == ["valid_at", "b"]
    ts_logger.debug(f"{__name__}look at y: {y}")
    ts_logger.debug(f"{__name__}look at x: {x.data}")
    assert list(y.columns) == ["valid_at", "b"]
    assert list(x.data.columns) == ["valid_at", "a", "b", "c"]

    # confirm that x and y are not the same object
    # y.iloc[:, 1:] = y.iloc[:, 1:] * 2
    # ts_logger.debug(f"look at x again: {x.data}")
    # try to the same with subscripting , but it does not work
    # x["b"] = x["b"] * 2
    # ts_logger.debug(f"look at x again: {x.data}")


@log_start_stop
def test_filter_dataset_by_regex(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    df = x.filter(regex="^x")
    ts_logger.warning(f"y = x.filter(regex='^x')\n{df}")

    assert list(df.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


@log_start_stop
def test_filter_dataset_by_regex_new_dataset(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-filter", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["a_x", "b_x", "c", "xd", "xe"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    y = x.filter(
        regex="^x", name="test-filter", data_type=SeriesType.simple(), load_data=False
    )
    ts_logger.warning(f"datasets: {y}")
    ts_logger.warning(f"datasets: {x}")

    assert list(y.data.columns) == ["valid_at", "xd", "xe"]
    assert list(x.data.columns) == ["valid_at", "a_x", "b_x", "c", "xd", "xe"]


@log_start_stop
def test_correct_datetime_columns_valid_at(caplog) -> None:
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
def test_correct_datetime_columns_valid_from_to(caplog) -> None:
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
def test_versioning_as_of_creates_new_file(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    # assert False
    pass
    # TO DO:
    # Verify behaviours of data types / saving correctly:
    # AS_OF --> new files, keep/compare multiple versions


@log_start_stop
def test_versioning_none_appends_to_existing_file(caplog) -> None:
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
    ts_logger.warning(
        f"DATASET: {a.name}: First write {a.data.size} values, writing {b.data.size} values (50% new) --> combined {c.data.size} values."
    )

    assert a.data.size < c.data.size
    assert b.data.size < c.data.size


@log_start_stop
def test_find_data_using_metadata_attributes() -> None:
    # metadata - test extendeded attribute set
    # find data via metadata
    # metadata = my_dataset.metadata
    pass


@log_start_stop
def test_update_metadata_attributes() -> None:
    # TO DO:
    # Updating metadata by changing an attribute value should
    # ... update metadata.json
    # ... keep previous version
    pass


@log_start_stop
def test_updated_tags_propagates_to_column_names_accordingly() -> None:
    # TO DO:
    # my_dataset.update_metadata('column_name', 'metadata_tag')
    # ... --> versioning
    pass
