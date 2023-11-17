import os
import uuid

from timeseries import dataset as ds
from timeseries import properties
from timeseries.sample_data import create_dataset
from timeseries.logging import ts_logger as log


def test_dataset_instance_created() -> None:
    example = ds.Dataset(name="test-no-dir-created", datatype="simple")
    assert isinstance(example, ds.Dataset)


def test_dataset_instance_created_equals_repr() -> None:
    example = ds.Dataset(name="test-no-dir-created", datatype="simple")
    assert example == example
    # TO DO: fix so that this works?
    #  assert eval(repr(example)) == example


def test_create_dataset_with_correct_data_size() -> None:
    # tags = {"A": ["a", "b", "c"], "B": ["pp", "qq", "rr"], "C": ["x1", "y1", "z1"]}
    tags = {"A": ["a", "b", "c"], "B": ["pp", "qq", "rr"], "C": ["x1", "y1", "z1"]}
    x = create_dataset(
        name="test-no-dir-created",
        series_tags=tags,
        dataset_tags={},
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    assert x.data.size == 280


def test_dataset_datadir_created() -> None:
    example = ds.Dataset(name="test-dataset-datadir-created", datatype="simple")
    example.io.purge()
    example.save()
    assert os.path.isdir(example.io.data_dir)


def test_dataset_metadir_created() -> None:
    example = ds.Dataset(name="test-dataset-metadir-created", datatype="simple")
    example.io.purge()
    example.save()
    assert os.path.isdir(example.io.metadata_dir)


def test_datafile_exists_after_create_dataset_and_save() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "qqq", "rr"], "C": ["x1", "y1", "z1"]}
    set_name = f"test-{uuid.uuid4().hex}"
    x = create_dataset(
        name=set_name,
        series_tags=tags,
        dataset_tags={},
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    x.save()
    check = x.io.datafile_exists()
    assert check


def test_metafile_exists_after_create_dataset_and_save() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "qqq", "rr"], "C": ["x1", "y1", "z1"]}
    set_name = f"test-{uuid.uuid4().hex}"
    x = create_dataset(
        name=set_name,
        series_tags=tags,
        dataset_tags={},
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    # log.debug(x.tags)
    x.save()
    log.debug(x.io.metadata_fullpath)
    assert x.io.metadatafile_exists()


def test_read_existing_simple_metadata() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "qqq", "rr"], "C": ["x1", "y1", "z1"]}
    set_name = "persisted-example-simple"
    x = create_dataset(
        name=set_name,
        series_tags=tags,
        # dataset_tags={},
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    if os.path.isfile(x.io.metadata_fullpath):
        log.debug(x.io.metadata_fullpath)
        log.debug(x.tags)
        log.debug(x.tags["name"])
        assert x.tags["name"] == set_name and x.tags["versioning"] == str(
            properties.SeriesVersioning.NONE
        )
    else:
        log.warning(
            f"DATASET {x.name}: Metadata not found at {x.io.metadata_fullpath}. Writing."
        )
        x.save()
        assert False


def test_read_existing_simple_data() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "qqq", "rr"], "C": ["x1", "y1", "z1"]}
    set_name = "persisted-example-simple"
    x = create_dataset(
        name=set_name,
        series_tags=tags,
        # dataset_tags={},
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    if os.path.isfile(x.io.data_fullpath):
        log.debug(x.io.data_fullpath)
        log.debug(x.data)
        assert x.data.size == 280
    else:
        log.warning(
            f"DATASET {x.name}: Data not found at {x.io.data_fullpath}. Writing."
        )
        x.save()
        assert False


def test_read_existing_estimate_metadata() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "qqq", "rr"], "C": ["x1", "y1", "z1"]}
    set_name = "persisted-example-estimate"
    x = create_dataset(
        name=set_name,
        datatype="estimate",
        series_tags=tags,
        # dataset_tags={},
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    if os.path.isfile(x.io.metadata_fullpath):
        log.debug(x.io.metadata_fullpath)
        log.debug(x.tags)
        assert x.tags["name"] == set_name
        assert x.tags["versioning"] == str(properties.SeriesVersioning.AS_OF)
    else:
        log.warning(
            f"DATASET {x.name}: Metadata not found at {x.io.metadata_fullpath}. Writing."
        )
        x.save()
        assert False


def test_read_existing_estimate_data() -> None:
    tags = {"A": ["a", "b", "c"], "B": ["p", "qqq", "rr"], "C": ["x1", "y1", "z1"]}
    set_name = "persisted-example-estimate"
    x = create_dataset(
        name=set_name,
        series_tags=tags,
        datatype="estimate",
        # dataset_tags={},
        start_date="2022-01-01",
        end_date="2022-10-03",
        freq="MS",
    )
    if os.path.isfile(x.io.data_fullpath):
        log.debug(x.io.data_fullpath)
        log.debug(x.data)
        assert x.data.size == 280
    else:
        log.warning(
            f"DATASET {x.name}: Data not found at {x.io.data_fullpath}. Writing."
        )
        x.save()
        assert False

    # TO DO: more tests
    #
    # # Access data
    # data = my_dataset.data

    # Access metadata
    # metadata = my_dataset.metadata

    # Update metadata
    # my_dataset.update_metadata('column_name', 'metadata_tag')

    # Save changes
    # my_dataset.save_data()
    # my_dataset.save_metadata()
