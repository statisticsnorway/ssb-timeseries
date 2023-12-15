import os
import uuid
import numpy as np
import pandas as pd
import functools
import logging

from timeseries.dates import now_utc, date_utc
from timeseries.logging import log_start_stop, ts_logger
from timeseries.dataset import Dataset
from timeseries.properties import SeriesType, Versioning, Temporality
from timeseries.sample_data import create_df


# def insert_cap_log(func, caplog):
#     """Alter log level when decorated functions fail."""

#     @functools.wraps(func)
#     def wrapper(caplog, *args, **kwargs):
#         caplog.set_level(logging.DEBUG)
#         out = func(caplog, *args, **kwargs)
#         return out

#     return wrapper


@log_start_stop
def test_dataset_instance_created(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    example = Dataset(name="test-no-dir-created", data_type=SeriesType.simple())
    assert isinstance(example, Dataset)


def skip_test_dataset_instance_created_equals_repr(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    a = Dataset(
        name="test-no-dir-created", data_type=SeriesType.simple(), as_of_tz="2022-01-01"
    )
    ts_logger.warning(f"Dataset a: {repr(a)}")
    b = eval(repr(a))
    ts_logger.warning(f"Dataset b: {repr(b)}")
    assert a is a
    assert a.identical(a)
    # TEMPORARY DISABLED skip_<name>
    # TO DO: fix __repr__ OR identical so that this works
    assert a.identical(b)


@log_start_stop
def skip_test_dataset_instance_identity(caplog) -> None:
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


def test_dataset_math(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    a_data = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=a_data,  # , index="valid_at"
    )

    scalar = 1000
    col_vector = np.ones((1, 3)) * scalar
    row_vector = np.ones((4, 1)) * scalar
    matrix = np.ones((4, 3)) * scalar

    ts_logger.debug(f"matrix:\n{a + scalar}")
    ts_logger.debug(f"matrix:\n{a - scalar}")
    ts_logger.debug(f"matrix:\n{a * scalar}")
    ts_logger.debug(f"matrix:\n{a / scalar}")

    ts_logger.debug(f"matrix:\n{a + a_data}")
    ts_logger.debug(f"matrix:\n{a - a_data}")
    ts_logger.debug(f"matrix:\n{a * a_data}")
    ts_logger.debug(f"matrix:\n{a / a_data}")

    ts_logger.debug(f"matrix:\n{a + a}")
    ts_logger.debug(f"matrix:\n{a - a}")
    ts_logger.debug(f"matrix:\n{a * a}")
    ts_logger.debug(f"matrix:\n{a / a}")

    assert all((a + a) == (a + a_data))
    assert all((a - a) == (a - a_data))
    assert all((a * a) == (a * a_data))
    assert all((a / a) == (a / a_data))


@log_start_stop
def test_dataset_groupby_sum(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-groupby-sum", data_type=SeriesType.simple(), load_data=False)

    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2023-02-28", freq="D"
    )
    assert x.data.shape == (424, 4)
    ts_logger.warning(f'groupby:\n{x.groupby("M", "sum")}')
    assert x.groupby("M", "sum").shape == (14, 3)


@log_start_stop
def test_dataset_groupby_mean(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-groupby-mean", data_type=SeriesType.simple(), load_data=False
    )

    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2023-02-28", freq="D"
    )
    assert x.data.shape == (424, 4)
    df1 = x.groupby("M", "mean")
    ts_logger.warning(f"groupby:\n{df1}")
    assert df1.shape == (14, 3)


@log_start_stop
def test_dataset_resample(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(name="test-resample", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    assert x.data.shape == (1, 4)
    # df = x.data.resample("M").mean()
    df = x.resample("M", "mean")
    ts_logger.warning(f"resample:\n{df}")
    # assert df.shape == (12, 3)


@log_start_stop
def skip_test_find(caplog):
    caplog.set_level(logging.DEBUG)
    new = uuid.uuid4().hex
    x = Dataset(name=f"test-find-{new}", data_type=SeriesType.simple(), load_data=False)
    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-12-31", freq="YS"
    )
    x.save()
    datasets = x.find(new)
    ts_logger.warning(f"datasets: {str(datasets)}")
    # assert df.shape == (12, 3)

    assert False


@log_start_stop
def test_dataset_vectors(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-vectors",
        data_type=SeriesType.estimate(),
        load_data=False,
        as_of_tz=date_utc("2022-01-01"),
        series_tags={},
    )
    assert x.data.empty
    tag_values = [["p", "q", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-10-03", freq="MS"
    )
    x.vectors()
    ts_logger.debug(f"matrix:\n{eval('p') == x.data['p']}")

    # variables should be defined for all columns
    assert "valid_at" in locals()
    assert "p" in locals()
    assert "q" in locals()
    assert "r" in locals()

    # and should evaluate to x.data[column_name]
    assert all(eval("valid_at") == x.data["valid_at"])
    assert all(eval("p") == x.data["p"])
    assert all(eval("q") == x.data["q"])
    assert all(eval("r") == x.data["r"])


@log_start_stop
def test_dataset_vectors_with_filter(caplog):
    caplog.set_level(logging.DEBUG)

    x = Dataset(
        name="test-vectors",
        data_type=SeriesType.estimate(),
        load_data=False,
        as_of_tz=date_utc("2022-01-01"),
        series_tags={},
    )
    assert x.data.empty
    tag_values = [["px", "qx", "r"]]
    x.data = create_df(
        *tag_values, start_date="2022-01-01", end_date="2022-10-03", freq="MS"
    )
    x.vectors("x")

    # variables should be defined only for some columns
    assert "valid_at" not in locals()
    assert "px" in locals()
    assert "qx" in locals()
    assert "r" not in locals()

    # and should evaluate to x.data[column_name] for the defined ones
    assert all(eval("px") == x.data["px"])
    assert all(eval("qx") == x.data["qx"])


@log_start_stop
def skip_test_dataset_add_to_dataframe(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    data1 = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=data1,  # , index="valid_at"
    )

    b = a.data + 2
    c = a + 2
    # ts_logger.warning(f"{','.join(a.series_names)}")

    ts_logger.warning(c)
    assert all(b == c)


@log_start_stop
def skip_test_dataset_subtract_two_dataframes(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    data1 = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )
    data2 = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=data1,  # , index="valid_at"
    )
    b = Dataset(
        name="test-temp-b",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=data2,  # , index="valid_at"
    )
    # ts_logger.warning(f"{','.join(a.series_names)}")

    c = a - b
    ts_logger.warning(c)

    assert all(c == data1 - data2)


@log_start_stop
def skip_test_dataset_subtract_from_dataframe(caplog) -> None:
    caplog.set_level(logging.DEBUG)

    data1 = create_df(
        ["x", "y", "z"], start_date="2022-01-01", end_date="2022-04-03", freq="MS"
    )

    a = Dataset(
        name="test-temp-a",
        data_type=SeriesType.simple(),
        load_data=False,
        as_of_tz=None,
        data=data1,  # , index="valid_at"
    )
    b = a.data - 2
    c = a - 2
    b == c
    # assert all(b == c)


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
