import os
import uuid
import numpy as np
import pandas as pd
import functools
import logging

from timeseries.dates import now_utc, date_utc
from timeseries.logging import log_start_stop, ts_logger
from timeseries.dataset import Dataset
from timeseries.properties import SeriesType, Versioning
from timeseries.sample_data import create_df


def insert_cap_log(func, caplog):
    """Alter log level when decorated functions fail."""

    @functools.wraps(func)
    def wrapper(caplog, *args, **kwargs):
        caplog.set_level(logging.DEBUG)
        out = func(caplog, *args, **kwargs)
        return out

    return wrapper


@log_start_stop
def test_dataset_instance_created() -> None:
    example = Dataset(name="test-no-dir-created", data_type=SeriesType.simple())
    assert isinstance(example, Dataset)


@log_start_stop
def test_dataset_instance_created_equals_repr(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    example = Dataset(
        name="test-no-dir-created", data_type=SeriesType.simple(), as_of_tz=None
    )
    assert example is example
    # TO DO: fix so that this works?
    #  assert eval(repr(example)) == example


@log_start_stop
def test_create_dataset_with_correct_data_size() -> None:
    # tags = {"A": ["a", "b", "c"], "B": ["pp", "qq", "rr"], "C": ["x1", "y1", "z1"]}
    tags = {"A": ["a", "b", "c"], "B": ["pp", "qq", "rr"], "C": ["x1", "y1", "z1"]}
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
def test_dataset_datadir_created() -> None:
    example = Dataset(
        name="test-dataset-datadir-created", data_type=SeriesType.simple()
    )
    example.io.purge()
    example.save()
    assert os.path.isdir(example.io.data_dir)


@log_start_stop
def test_dataset_metadir_created() -> None:
    example = Dataset(
        name="test-dataset-metadir-created", data_type=SeriesType.simple()
    )
    example.io.purge()
    example.save()
    assert os.path.isdir(example.io.metadata_dir)


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
    if os.path.isfile(x.io.metadata_fullpath):
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
    if os.path.isfile(x.io.data_fullpath):
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
        as_of_utc=date_utc("2022-01-01"),
        series_tags=tags,
    )
    tag_values = [value for value in tags.values()]
    x.data = create_df(
        *tag_values,
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    if os.path.isfile(x.io.metadata_fullpath):
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
    if os.path.isfile(x.io.data_fullpath):
        ts_logger.debug(x)
        assert x.data.size == 364
    else:
        ts_logger.warning(
            f"DATASET {x.name}: Data not found at {x.io.data_fullpath}. Writing."
        )
        x.save()
        assert False


# TO DO: -----------------------------


@log_start_stop
def test_load_existing_set_without_loading_data(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    set_name = "persisted-example-estimate"
    tags = {"A": ["a", "b", "c"], "B": ["p", "q", "r"], "C": ["x1", "y1", "z1"]}
    x = Dataset(
        name=set_name,
        data_type=SeriesType.estimate(),
        load_data=False,
        as_of_utc=date_utc("2022-01-01"),
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
def test_dataset_add(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    data1 = {
        "valid_at": pd.date_range(start="2022-01-01", periods=5),
        "x1": [1, 2, 3],
        "x2": [10, 20, 30],
    }
    data2 = {
        "valid_at": pd.date_range(start="2022-01-01", periods=5),
        "x1": [3, 2, 1],
        "x2": [30, 20, 10],
    }

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_utc=None,
        data=pd.DataFrame([data1]),  # , index="valid_at"
    )
    b = Dataset(
        name="test-temp-b",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_utc=None,
        data=pd.DataFrame([data2]),  # , index="valid_at"
    )
    # ts_logger.warning(f"{','.join(a.series_names)}")

    c = a + b
    ts_logger.warning(f"\nc.iloc[:, 1:]")

    validation = (c == np.array([[6, 6, 6], [60, 60, 60]])).all()

    ts_logger.warning(c)

    assert validation


@log_start_stop
def test_versioning_as_of_creates_new_file(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    # assert False
    pass
    # TO DO:
    # Verify behaviours of data types / saving correctly:
    # AS_OF --> new files, keep/compare multiple versions


@log_start_stop
def test_versioning_none_appends_to_existing_file() -> None:
    pass
    # TO DO:
    # NONE --> append data, overwrite with new
    # (for now, no testing of retrievability via bucket versioning)
    # read estimate data, compare with previous -

    # naming conventions / storage of parquet files


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


@log_start_stop
def other() -> None:
    # check performance - create benchmarks for big datasets
    # test performance - pandas versus polars
    pass
