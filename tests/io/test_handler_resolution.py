"""Tests that the I/O handler layer resolves and instantiates through configuration.

The handler classes are also exercised directly by `test_pyarrow_simple.py` and
`test_pyarrow_hive.py`, so a wrong class name in `BUILTIN_IO_HANDLERS` went unnoticed
for as long as the registry and the modules agreed by accident.
"""

from copy import deepcopy

import pytest

from ssb_timeseries import io
from ssb_timeseries.config import Config
from ssb_timeseries.config.constants import BUILTIN_IO_HANDLERS
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
        set_name="handler-resolution-probe",
        set_type=SeriesType.simple(),
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
        set_name="handler-resolution-probe",
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
        set_name="configuration-integrity-probe",
        set_type=SeriesType.simple(),
    )

    assert Config.active().repositories["test_1"] == untouched


def test_the_active_configuration_stays_serializable_after_using_a_data_handler(
    conftest, tmp_path
) -> None:
    """Regression: a handler used to inject `as_of_utc` into the active configuration."""
    io._io_handler(
        handler_type="data",
        repository=Config.active().repositories["test_1"],
        set_name="configuration-integrity-probe",
        set_type=SeriesType.simple(),
    )

    Config.active().save(tmp_path / "config_after_data_handler.json")

    assert (tmp_path / "config_after_data_handler.json").exists()


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
        set_name="options-probe",
        set_type=SeriesType.simple(),
    )

    assert handler.root == configured_path


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
        set_name="options-probe",
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
        set_name="options-probe",
        **({"set_type": SeriesType.simple()} if handler_type == "data" else {}),
    )

    assert handler.options["unrecognised_option"] == "some-value"


def test_the_dispatcher_separates_dataset_identity_from_handler_options(
    conftest,
) -> None:
    """A configuration key must not be able to masquerade as a dataset attribute.

    The handler needs to know which dataset it operates on, and the dispatcher
    needs to hand over the repository's `options`, without the two being
    interchangeable.
    A configured `set_name` must be overridden by the dataset actually
    requested, and must not end up in the handler's `options`.
    """
    repository = deepcopy(Config.active().repositories["test_1"])
    repository["directory"]["options"]["set_name"] = "shadowing-the-dataset"

    handler = io._io_handler(
        handler_type="data",
        repository=repository,
        set_name="separation-probe",
        set_type=SeriesType.simple(),
    )

    assert handler.set_name == "separation-probe"
    assert "set_name" not in handler.options


def test_the_dispatcher_rejects_an_unhandled_handler_type(
    conftest,
) -> None:
    """An unknown handler type must fail loudly rather than fall through."""
    with pytest.raises(ValueError, match="Unhandlked handler type"):
        io._io_handler(
            handler_type="not-a-handler-type",  # type: ignore[arg-type]
            repository=deepcopy(Config.active().repositories["test_1"]),
            set_name="unhandled-type-probe",
        )
