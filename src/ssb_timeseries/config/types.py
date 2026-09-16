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


class MissingEnvironmentVariableError(Exception):
    """The environment variable TIMESEREIS_CONFIG must be defined."""

    ...


class ValidationError(Exception):
    """Configuration validation error."""

    ...
