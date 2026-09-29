"""Tests that the I/O handler layer resolves and instantiates through configuration.

The handler classes are also exercised directly by `test_pyarrow_simple.py` and
`test_pyarrow_hive.py`, so a wrong class name in `BUILTIN_IO_HANDLERS` went unnoticed
for as long as the registry and the modules agreed by accident.
"""

import uuid
from copy import deepcopy

import pytest

from ssb_timeseries import io
from ssb_timeseries.config import Config
from ssb_timeseries.config.constants import BUILTIN_IO_HANDLERS
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.dates import now_utc
from ssb_timeseries.io.protocols import ArchiveWrite
from ssb_timeseries.io.protocols import DataReadWrite
from ssb_timeseries.io.protocols import MetadataReadWrite
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.types import SeriesType

# mypy: disable-error-code="no-untyped-def,no-untyped-call,arg-type,attr-defined,assignment"


@pytest.mark.parametrize("handler_name", sorted(BUILTIN_IO_HANDLERS))
def test_every_builtin_handler_name_matches_the_class_it_names(
    handler_name: str,
) -> None:
    """A built-in must name a class that exists under exactly that name."""
    configured = BUILTIN_IO_HANDLERS[handler_name]["handler"]
    module_path, _, class_name = configured.rpartition(".")
    resolved = io._handler_class(handler_name)

    assert resolved.__qualname__ == class_name
    assert resolved.__module__ == module_path


@pytest.mark.parametrize("handler_name", sorted(BUILTIN_IO_HANDLERS))
def test_every_builtin_handler_declares_its_options_block(
    handler_name: str,
) -> None:
    """`options` is required by `FileRepoConfig`, so every built-in must carry it."""
    assert "options" in BUILTIN_IO_HANDLERS[handler_name]


@pytest.mark.parametrize("handler_name", ["simple-parquet", "hive-partitioned-parquet"])
def test_every_data_handler_instantiates_through_the_configuration_layer(
    conftest, handler_name: str
) -> None:
    """Data handlers must be constructible from a repository configuration alone."""
    repository = deepcopy(conftest.configuration.repositories["test_1"])
    repository["directory"]["handler"] = handler_name

    handler = io._io_handler(
        handler_type="data",
        repository=repository,
    )

    configured = BUILTIN_IO_HANDLERS[handler_name]["handler"]
    assert type(handler).__qualname__ == configured.rpartition(".")[2]


def test_the_metadata_handler_instantiates_through_the_configuration_layer(
    conftest,
) -> None:
    """The metadata handler must be constructible from a repository configuration alone."""
    repository = deepcopy(conftest.configuration.repositories["test_1"])

    handler = io._io_handler(
        handler_type="metadata",
        repository=repository,
    )

    configured = BUILTIN_IO_HANDLERS["json"]["handler"]
    assert type(handler).__qualname__ == configured.rpartition(".")[2]


def test_instantiating_a_data_handler_does_not_modify_the_active_configuration(
    conftest,
) -> None:
    """The repository configuration must come back out of the handler layer unchanged."""
    repository = Config.active().repositories["test_1"]
    untouched = deepcopy(repository)

    io._io_handler(
        handler_type="data",
        repository=repository,
    )

    assert Config.active().repositories["test_1"] == untouched


def test_the_active_configuration_stays_serializable_after_using_a_data_handler(
    conftest, tmp_path
) -> None:
    """Regression: a handler used to inject `as_of_utc` into the active configuration."""
    io._io_handler(
        handler_type="data",
        repository=Config.active().repositories["test_1"],
    )

    Config.active().save(tmp_path / "config_after_data_handler.json")

    assert (tmp_path / "config_after_data_handler.json").exists()


def test_the_active_configuration_survives_a_full_save_cycle_through_the_facade(
    conftest,
    one_new_set_for_each_data_type,
    tmp_path,
) -> None:
    """The existing tests only build a handler directly.

    A full cycle through the facade builds handlers, reads the configuration,
    and writes data and metadata, which is where a handler was injecting
    `as_of_utc` into the active configuration. The configuration must come back
    out unchanged, and must still serialise.
    """
    untouched = deepcopy(Config.active().repositories["test_1"])
    ds = one_new_set_for_each_data_type

    io.save(ds)
    io.read_metadata(ds.repository, set_name=ds.name)
    io.read_data(ds.repository, set_name=ds.name, as_of_tz=ds.as_of_utc)
    io.versions(ds)
    io.find(ds.repository, series="x")

    assert Config.active().repositories["test_1"] == untouched
    Config.active().save(tmp_path / "config_after_full_cycle.json")
    assert (tmp_path / "config_after_full_cycle.json").exists()


def test_the_configured_path_reaches_the_data_handler_as_its_root(
    conftest, tmp_path
) -> None:
    """A handler must read its root from `options`, not by digging into the repository."""
    repository = deepcopy(Config.active().repositories["test_1"])
    configured_path = str(tmp_path / "options-supplied-root")
    repository["directory"]["options"]["path"] = configured_path

    handler = io._io_handler(
        handler_type="data",
        repository=repository,
    )

    assert handler.root == configured_path


def test_a_configured_file_pattern_reaches_the_data_handler(
    conftest,
    one_new_set_for_each_data_type,
) -> None:
    """The file pattern is a property of the storage, so it comes from config.

    The facade used to pass it on every call, which forced a filename concept
    onto handlers that have no filenames.
    """
    dataset = one_new_set_for_each_data_type
    repository = deepcopy(conftest.configuration.repositories["test_1"])

    io._io_handler(handler_type="data", repository=repository).write(
        dataset.ref, data=dataset.data, tags=dataset.tags
    )

    # Configured to match the written data file.
    repository["directory"]["options"]["file_pattern"] = "*.parquet"
    assert io._io_handler(handler_type="data", repository=repository).versions(
        dataset.ref
    )

    # Configured to match nothing, which proves the option is read rather than
    # ignored in favour of a hard-coded default.
    repository["directory"]["options"]["file_pattern"] = "*.not-parquet"
    assert (
        io._io_handler(handler_type="data", repository=repository).versions(dataset.ref)
        == []
    )

    # Absent, and defaulted by the handler to its own parquet convention.
    del repository["directory"]["options"]["file_pattern"]
    assert io._io_handler(handler_type="data", repository=repository).versions(
        dataset.ref
    )


def test_the_configured_path_reaches_the_metadata_handler_as_its_directory(
    conftest, tmp_path
) -> None:
    """The metadata handler must read its directory from `options` as well."""
    repository = deepcopy(Config.active().repositories["test_1"])
    configured_path = str(tmp_path / "options-supplied-catalog")
    repository["catalog"]["options"]["path"] = configured_path

    handler = io._io_handler(
        handler_type="metadata",
        repository=repository,
    )

    assert handler.dir == configured_path


@pytest.mark.parametrize("handler_type", ["data", "metadata"])
def test_a_handler_accepts_options_it_does_not_recognize(
    conftest, handler_type: str
) -> None:
    """Handlers must tolerate unknown options rather than rejecting them.

    The dispatcher cannot know what a given handler needs, so unknown keys are
    retained on the instance for the handler to use on its own terms.
    """
    repository = deepcopy(Config.active().repositories["test_1"])
    binding = "directory" if handler_type == "data" else "catalog"
    repository[binding]["options"]["unrecognised_option"] = "some-value"
    handler = io._io_handler(
        handler_type=handler_type,
        repository=repository,
    )

    assert handler.options["unrecognised_option"] == "some-value"


@pytest.mark.parametrize("handler_type", ["data", "metadata"])
def test_a_handler_cannot_take_its_dataset_identity_from_configuration(
    conftest,
    handler_type: str,
) -> None:
    """A configuration key must not be able to masquerade as a dataset attribute.

    A handler is configured from the repository alone, so it has no dataset
    identity of its own to override.
    Which dataset an operation concerns is named in that operation, whether by
    the `DatasetRef` a data handler takes or by the `name` a metadata handler
    takes, since the metadata layer cannot know a series type.
    """
    repository = deepcopy(conftest.configuration.repositories["test_1"])
    if handler_type == "data":
        repository["directory"]["options"]["set_name"] = "shadowing-the-dataset"
    else:
        repository["catalog"]["options"]["set_name"] = "shadowing-the-dataset"

    handler = io._io_handler(
        handler_type=handler_type,
        repository=repository,
    )

    assert not hasattr(handler, "set_name")
    assert "set_name" in handler.options


def test_the_dispatcher_rejects_an_unhandled_handler_type(
    conftest,
) -> None:
    """An unknown handler type must fail loudly rather than fall through."""
    with pytest.raises(ValueError, match="Unhandlked handler type"):
        io._io_handler(
            handler_type="not-a-handler-type",  # type: ignore[arg-type]
            repository=deepcopy(Config.active().repositories["test_1"]),
        )


@pytest.fixture
def two_datasets_of_different_types(conftest):
    """Build two datasets that differ in both series type and content.

    The fixture `one_new_set_for_each_data_type` is cached per test, so a second
    call would hand back the same object. These are built directly instead.
    """
    versioned = Dataset(
        name=f"crosstalk_versioned_{uuid.uuid4().hex}",
        data_type=SeriesType("as_of", "at"),
        as_of_tz=now_utc(),
        data=create_df(
            ["x"],
            start_date="2022-01-01",
            end_date="2022-04-01",
            freq="MS",
            temporality="AT",
        ),
    )
    unversioned = Dataset(
        name=f"crosstalk_unversioned_{uuid.uuid4().hex}",
        data_type=SeriesType("none", "at"),
        data=create_df(
            ["y"],
            start_date="2021-01-01",
            end_date="2021-03-01",
            freq="MS",
            temporality="AT",
        ),
    )
    assert versioned.name != unversioned.name
    return versioned, unversioned


def test_a_data_handler_serves_two_datasets_of_different_types(
    conftest,
    two_datasets_of_different_types,
) -> None:
    """One handler instance must serve any number of datasets, with no cross-talk.

    This is the property the handler redesign exists to provide, and nothing
    asserted it until now. The two datasets deliberately differ in series type,
    because a handler that carried the type between operations would look for
    the second dataset's file under the first dataset's layout.
    """
    versioned, unversioned = two_datasets_of_different_types
    repository = conftest.configuration.repositories["test_1"]

    handler = io._io_handler(handler_type="data", repository=repository)

    handler.write(versioned.ref, data=versioned.data, tags=versioned.tags)
    handler.write(unversioned.ref, data=unversioned.data, tags=unversioned.tags)

    assert handler.exists(versioned.ref)
    assert handler.exists(unversioned.ref)

    # Each read must return its own dataset, not the one written last.
    # The two carry different series, so the columns prove which file was read.
    versioned_read = handler.read(versioned.ref)
    unversioned_read = handler.read(unversioned.ref)

    assert versioned_read.num_rows == versioned.data.shape[0]
    assert unversioned_read.num_rows == unversioned.data.shape[0]
    assert "x" in versioned_read.column_names
    assert "y" not in versioned_read.column_names
    assert "y" in unversioned_read.column_names
    assert "x" not in unversioned_read.column_names

    # Each dataset must report only its own version marker, not the other's.
    # A NONE dataset reports the literal "latest".
    assert handler.versions(unversioned.ref) == ["latest"]
    assert handler.versions(versioned.ref) == [versioned.as_of_utc]


def test_a_metadata_handler_serves_two_datasets(
    conftest,
    two_datasets_of_different_types,
) -> None:
    """One metadata handler instance must serve any number of datasets.

    The metadata layer holds no dataset identity, so the tags written for one
    dataset must not leak into the file of another.
    """
    first, second = two_datasets_of_different_types
    repository = conftest.configuration.repositories["test_1"]

    handler = io._io_handler(handler_type="metadata", repository=repository)

    handler.write(name=first.name, tags=first.tags)
    handler.write(name=second.name, tags=second.tags)

    assert handler.read(first.name) == first.tags
    assert handler.read(second.name) == second.tags
    assert handler.exists(first.name)
    assert handler.exists(second.name)


def test_the_facade_hands_a_data_handler_the_ref_and_nothing_else(
    conftest,
    monkeypatch,
    one_new_set_for_each_data_type,
) -> None:
    """The shared contract must not carry storage specific arguments.

    The facade used to pass `file_pattern` and `pattern` on every call, which
    described a filename layout that only some handlers have. A handler backed
    by a database or an HTTP API has no filenames, so the stub below takes only
    a ref: if the facade grows any argument again, this raises TypeError.
    """
    recorded: list = []

    class RefOnlyHandler:
        def __init__(self, repository, **options) -> None:
            self.repository = repository

        def versions(self, dataset) -> list:
            recorded.append(dataset)
            return []

    monkeypatch.setattr(io, "_handler_class", lambda handler_name: RefOnlyHandler)
    ds = one_new_set_for_each_data_type

    io.versions(ds)

    assert recorded == [ds.ref]


@pytest.mark.parametrize("handler_name", ["simple-parquet", "hive-partitioned-parquet"])
def test_a_registered_data_handler_satisfies_the_data_protocol(
    handler_name: str,
) -> None:
    """A handler the dispatcher may instantiate must satisfy the protocol.

    The protocols are declared `runtime_checkable` but nothing ever checked
    them, so a handler could be registered and satisfy nothing.
    """
    assert isinstance(io._handler_class(handler_name), DataReadWrite)


def test_a_registered_metadata_handler_satisfies_the_metadata_protocol() -> None:
    """A handler the dispatcher may instantiate must satisfy the protocol."""
    assert isinstance(io._handler_class("json"), MetadataReadWrite)


def test_the_registered_archive_handler_satisfies_the_archive_protocol() -> None:
    """Archiving has its own contract, and the registered handler must meet it.

    An archive writes and keeps a version, which is neither reading a dataset
    nor writing its metadata, so a handler that satisfied only one of those
    contracts would pass the checks above and still be wrong here.
    """
    assert isinstance(io._handler_class("archive"), ArchiveWrite)
