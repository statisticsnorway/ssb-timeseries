"""Tests for the configurations that ship with the notebooks.

`notebooks/minimal_configuration.json` and `notebooks/sharing_config.json` are what a
contributor's `marimo` session and `tools/export_all_guides.py` load, but the notebook
tests point `TIMESERIES_CONFIG` at a fixture built in `tests/conftest.py` instead, so
nothing exercised these two files.

Both defects that reached the guides were silent rather than loud.
`is_valid_config` accepted a handler string naming a module that had been deleted,
because a string is a well-formed value whether or not anything answers to it.
A configuration whose `snapshots` section had been renamed to `archives` archived
nothing at all, because `io.archive` reads `Config.active()["archives"] or {}`, finds
it empty and returns.

These tests read the shipped files as committed, rather than a fixture standing in for
one, so they cannot drift with the fixture in the way the issue describes.
"""

import json
from pathlib import Path

import pytest

from ssb_timeseries.config import BUILTIN_IO_HANDLERS
from ssb_timeseries.config import is_valid_config
from ssb_timeseries.config import validate_handlers

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"

SHIPPED_CONFIGURATIONS = ["minimal_configuration.json", "sharing_config.json"]


def load_shipped_configuration(filename: str) -> dict:
    """Read a configuration that ships with the notebooks, as committed."""
    return json.loads((NOTEBOOKS_DIR / filename).read_text(encoding="utf-8"))


def referenced_handler_names(configuration: dict) -> set[str]:
    """Collect every handler name the configuration points at, from every section.

    `repositories` reach handlers through `directory` and `catalog`, while `archives` and
    `sharing` reach them through `directory` alone.
    """
    names = set()

    for repository in configuration.get("repositories", {}).values():
        for section in ("directory", "catalog"):
            if section in repository:
                names.add(repository[section]["handler"])

    for section in ("archives", "sharing"):
        for destination in configuration.get(section, {}).values():
            names.add(destination["directory"]["handler"])

    return names


@pytest.mark.parametrize("filename", SHIPPED_CONFIGURATIONS)
def test_a_shipped_configuration_is_a_valid_config_dict(filename: str) -> None:
    """The file must be structurally valid, or every load of it warns and falls back."""
    configuration = load_shipped_configuration(filename)

    is_valid, reason = is_valid_config(configuration)

    assert is_valid is True, reason


@pytest.mark.parametrize("filename", SHIPPED_CONFIGURATIONS)
def test_every_handler_a_shipped_configuration_names_resolves(filename: str) -> None:
    """A handler naming a deleted module passes validation and fails at the first read.

    `minimal_configuration.json` named `ssb_timeseries.io.snapshot.FileSystem` long after
    that module was deleted, and `is_valid_config` accepted it throughout, because it
    checks the type of a handler string and never whether anything answers to it.
    """
    configuration = load_shipped_configuration(filename)

    is_resolvable, reason = validate_handlers(configuration)

    assert is_resolvable is True, reason


@pytest.mark.parametrize("filename", SHIPPED_CONFIGURATIONS)
def test_every_handler_a_shipped_configuration_uses_is_declared_in_it(
    filename: str,
) -> None:
    """A handler name must be declared, because `io_handlers` is a full override.

    `_handler_class` reads `config.io_handlers[name]` with no fallback to
    `BUILTIN_IO_HANDLERS`, so a name that is not declared cannot be reached at all.
    `sharing_config.json` once declared `my_snapshot_handler`, which exists nowhere, and
    pointed two of its sections at it.
    """
    configuration = load_shipped_configuration(filename)
    declared = set(configuration["io_handlers"])

    undeclared = referenced_handler_names(configuration) - declared

    assert not undeclared, f"{filename} uses handlers it does not declare: {undeclared}"


@pytest.mark.parametrize("filename", SHIPPED_CONFIGURATIONS)
def test_a_shipped_configuration_names_builtins_the_way_they_are_registered(
    filename: str,
) -> None:
    """A hand-copied handler path is the defect these configurations are prone to.

    The notebook configurations transcribe the handler strings rather than reading them
    from `BUILTIN_IO_HANDLERS`, which is how they came to disagree with the registry.
    Comparing the two catches that before a read does.
    """
    configuration = load_shipped_configuration(filename)
    registered = {
        name: entry["handler"]
        for name, entry in BUILTIN_IO_HANDLERS.items()
        if name in configuration["io_handlers"]
    }
    declared = {
        name: entry["handler"]
        for name, entry in configuration["io_handlers"].items()
        if name in BUILTIN_IO_HANDLERS
    }

    assert declared == registered, (
        f"{filename} transcribes a handler differently from the registry: "
        f"{ {k: (declared[k], registered[k]) for k in declared if declared[k] != registered[k]} }"
    )


def test_the_sharing_configuration_declares_where_data_is_archived_and_shared() -> None:
    """The sections must be present, or persisting a dataset silently writes nothing.

    `io.archive` reads `Config.active()["archives"] or {}` and returns when it is empty,
    and the same holds for sharing, so a renamed or dropped section yields a guide that
    exports successfully and demonstrates no archiving.
    """
    configuration = load_shipped_configuration("sharing_config.json")

    assert configuration.get("archives"), (
        "sharing_config.json declares no archive destination, so io.archive returns "
        "without writing anything"
    )
    assert configuration.get("sharing"), (
        "sharing_config.json declares no sharing destination, so shared data is "
        "silently not shared"
    )
