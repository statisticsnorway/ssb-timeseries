"""Types used by the SSB timeseries library configuration."""

from __future__ import annotations

try:
    from typing import NotRequired
    from typing import Required
    from typing import TypedDict
except ImportError:
    from typing_extensions import NotRequired  # noqa: UP035 #backport to 3.10
    from typing_extensions import Required  # noqa: UP035 #backport to 3.10
    from typing_extensions import TypedDict

from typing import Any
from typing import TypeAlias

# mypy: disable-error-code="assignment, arg-type, override,call-arg,has-type,no-untyped-def,attr-defined,import-untyped,"


class FileRepoConfig(TypedDict):
    """Links a path and a IO handler for a single file based repository."""

    handler: Required[str]
    options: Required[dict[str, Any]]


class Repository(TypedDict):
    """Defines data and metadata handling for time series repositories."""

    name: NotRequired[str]
    directory: Required[FileRepoConfig]
    catalog: NotRequired[FileRepoConfig]
    default: NotRequired[bool]


FileBasedRepository: TypeAlias = Repository


class ConfigDict(TypedDict):
    """Required attributes for configuration."""

    configuration_file: Required[str]
    io_handlers: Required[dict[str, Any]]
    repositories: Required[dict[str, Repository]]
    snapshots: NotRequired[dict[str, Repository]]
    sharing: NotRequired[dict[str, Repository]]
    log_file: NotRequired[str]
    logging: Required[dict[str, Any]]


def is_valid_config(configuration: ConfigDict) -> tuple[bool, object]:
    """Check if a dictionary is a valid configuration :py:class:`ConfigDict`."""
    # The ConfigDict.__required_keys__ includes optional fields like 'snapshots' and 'sharing'
    # which causes a ValidationError when the default configuration is loaded.
    # To fix this, we explicitly define the required keys.
    # missing_required = ConfigDict.__required_keys__ - set(configuration.keys())
    required_keys = {"configuration_file", "io_handlers", "repositories", "logging"}
    missing_required = required_keys - set(configuration.keys())
    if missing_required:
        msg = f"Configuration is missing required fields: {list(missing_required)}\n{configuration}"
        return (False, msg)

    wrong_type = []
    for (
        cfg_key,
        cfg_expected_type,
    ) in ConfigDict().items():  # type: ignore [typeddict-item]
        config_item = configuration.get(cfg_key, None)
        cfg_got_type = type(config_item)
        if cfg_got_type is type(cfg_expected_type):
            wrong_type.append(
                f"{cfg_key} - got {cfg_got_type} - expected {cfg_expected_type}"
            )

    if wrong_type:
        msg = f"Configuration fields have wrong type: {wrong_type}"
        return (False, msg)

    return (True, None)


class MissingEnvironmentVariableError(Exception):
    """The environment variable TIMESEREIS_CONFIG must be defined."""

    ...


class ValidationError(Exception):
    """Configuration validation error."""

    ...
