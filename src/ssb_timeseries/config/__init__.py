"""Configurations for the SSB timeseries library.

An environment variable :py:const:`ENV_VAR_NAME` is expected to point to a JSON file with configurations.
If these exist, they will be loaded and put into a Config object when the configuration module is loaded.

In most cases, this would happen behind the scene when :py:mod:`ssb_timeseries.dataset` or :py:mod:`ssb_timeseries.catalog` are imported.

Directly accessing the configuration module should only be required when manipulating configurations from Python code.

Example:
    >>> from ssb_timeseries.config import Config
    >>>
    >>> cfg = Config.active()

    ... modify, eg. disable library logging
    (leave the responsibility to the application using it):
    >>> cfg.logging = {}

    >>> # doctest: +SKIP
    >>> cfg.save()
    >>> # doctest: -SKIP
    >>> cfg.activate()

For switching between preset configurations, use the `timeseries-config` command from a terminal::

    poetry run timeseries-config <option>

which is equivalent to::

    python ./config.py <option>

See :py:func:`ssb_timeseries.config.main` for details on the named options.
"""

from __future__ import annotations

import copy
import json
import logging
import os
import sys
import warnings
from pathlib import Path

try:
    from typing import NotRequired
    from typing import Required
    from typing import Self
    from typing import TypedDict
except ImportError:
    from typing_extensions import NotRequired  # noqa: UP035 #backport to 3.10
    from typing_extensions import Required  # noqa: UP035 #backport to 3.10
    from typing_extensions import Self  # noqa: UP035 #backport to 3.10
    from typing_extensions import TypedDict

from typing import Any
from typing import TypeAlias

from ..types import PathStr
from .constants import BUILTIN_IO_HANDLERS
from .constants import DAPLA_TEAM_CONTEXT
from .constants import DEFAULTS
from .constants import ENV_VAR_NAME
from .constants import LOGGING_PRESETS
from .constants import PACKAGE_NAME
from .constants import PRESETS
from .types import ConfigDict
from .types import FileBasedRepository
from .types import FileRepoConfig
from .types import Repository

# mypy: disable-error-code="assignment, arg-type, override,call-arg,has-type,no-untyped-def,attr-defined,import-untyped,"

_config_logger = logging.getLogger(__name__)


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


def unset_env_var() -> str:
    """Unset the environment variable :py:const:`ENV_VAR_NAME` and return the value that was unset."""
    return os.environ.pop(ENV_VAR_NAME, "")


def active_file(path: PathStr = "") -> str:
    """If a path is provided, sets environment variable :py:const:`ENV_VAR_NAME` to specify the location of the configuration file, and returns the value of the environment variable.

    If called without the `path` parameter, it just returns the environment variable value.
    """
    if path:
        os.environ[ENV_VAR_NAME] = str(path)
        _config_logger.debug(f"Set environment variable {ENV_VAR_NAME} to {path}")

    return os.environ.get(ENV_VAR_NAME, "")


class Config:
    """Configuration for reading, modifying, saving, and activating timeseries configurations.

    A configuration can be loaded from a specified file,
    from the file identified by :py:const:`ENV_VAR_NAME`,
    or from the default configuration preset.
    Configuration values can also be provided or overridden as keyword arguments.

    A newly created configuration exists only in memory.
    Use :py:meth:`save` to persist it to a file and :py:meth:`activate` to make it the active configuration.
    The active configuration can be retrieved with :py:meth:`active` or reloaded from its file with :py:meth:`refresh`.
    """

    _active: Self
    configuration_file: PathStr
    """The path to the configuRation file."""
    repositories: dict[str, Repository]
    """Defines storage locations for time series data and metadata."""
    snapshots: dict[str, Repository]
    """Defines the storage locations for persisting (archiving) data in stable states."""
    sharing: dict[str, Repository]
    """Defines the storage locations for shared data."""
    io_handlers: dict[str, Any]
    """IO handlers for repository, snapshotts and sharing."""
    logging: dict[str, Any]
    """Logging configuration as a valid :py:mod:`logging.dictConfig`."""

    def __init__(self, **kwargs) -> None:  # noqa: D417, ANN003, RUF100
        """Initialize Config object from keyword arguments.

        Keyword Arguments:
            preset (str): Optional. Name of a preset configuration. If provided, the preset configuration is loaded, and no other parameters are considered.
            configuration_file (str): Path to the configuration file. If the parameter is not provided, the environment variable :py:const:`ENV_VAR_NAME` is used. If the environment variable is not set, the default configuration file location is used.
            repositories (list[FileBasedRepository]): New in version 0.5.0. Replaces bucket, timeseries_root and catalog.
            log_file (str): Path to the log file.
            bucket (str): Name of the GCS bucket.
            ignore_file (bool):

        Raises:
            :py:exc:`FileNotFoundError`: If the configuration file as implied by provided or not provided parameters does not exist.   # noqa: DAR402
            :py:exc:`ValidationError`: If the resulting configuration is not valid.   # noqa: DAR402
            :py:exc:`EnvVarNotDefinedeError`: If the environment variable :py:const:`ENV_VAR_NAME` is not defined.

        Examples:
            To load an existing preset configuration:

                >>> from ssb_timeseries.config import Config
                >>> config = Config(preset='defaults')

            ... or (specific to Statistics Norway and Dapla):

                >>> from ssb_timeseries.config import Config
                >>> config = Config(preset='daplalab')
        """
        preset_name = kwargs.pop("preset", "")
        ignore_file = kwargs.pop("ignore_file", False)
        param_specified_config_file = kwargs.get("configuration_file", "")

        kwargs_are_complete_config = is_valid_config(kwargs)[0]

        if preset_name := kwargs.pop("preset", ""):
            _config_logger.debug(f"Loading preset configuration {preset_name}.")
            self.apply(copy.deepcopy(PRESETS[preset_name]))
            return
        elif kwargs_are_complete_config:
            _config_logger.debug("Complete configuration in parameters.\n%s", kwargs)
            self.apply(kwargs)
            return
        elif param_specified_config_file:
            _config_logger.info(
                f"Loading configuration from {param_specified_config_file}"
            )

            if set(kwargs.keys()) == {"configuration_file"}:
                # if the config file is the only parameter,
                # it is an error when it does not exist
                no_file_is_an_error = True
            else:
                # If kwargs form a complete config,
                # the config file location is by definition specified.
                # It may be ok if the file does not exist,
                # but consider if it should be created or raise warning if not?
                no_file_is_an_error = not is_valid_config(kwargs)[0]

            if not ignore_file:
                config_from_file = load_json_file(
                    path=param_specified_config_file,
                    error_on_missing=no_file_is_an_error,
                )
            else:
                config_from_file = {}

            config_values = copy.deepcopy(PRESETS["default"])
            config_values.update(config_from_file)  # type: ignore [typeddict-item]
        elif active_file():
            _config_logger.debug(f"Loading configuration from {active_file()}")
            config_values = load_json_file(
                path=active_file(),
                error_on_missing=True,
            )
            # elif not active_file():
        #    raise MissingEnvironmentVariableError
        else:
            ...
            # _config_logger.warning(
            #    f"The environment variable {ENV_VAR_NAME} did not exist and no configuration file parameter was provided. Loading default configuration."
            # )
            config_values = copy.deepcopy(PRESETS["defaults"])

        config_values.update(kwargs)  # type: ignore [typeddict-item]
        self.apply(config_values)

    def apply(self, configuration: dict) -> None:
        """Set configuration values from a dictionary."""
        log_config = configuration.get("logging", {})
        if not log_config:
            configuration["logging"] = {}
        elif isinstance(log_config, str):
            warnings.warn(
                "string based log config! TO DO: look up named option", stacklevel=2
            )
            configuration["logging"] = {"str": log_config}

        config_ok, reason = is_valid_config(configuration=configuration)

        if not config_ok:
            _config_logger.error(f"Invalid configuration {configuration}\n{reason}.")
            raise ValidationError(f"Invalid configuration:\n{configuration}\n{reason}.")

        logfile = configuration.pop("log_file", "")
        if logfile:  # and not logging:
            # configuration["logging"] = {"logfile": logfile}
            _config_logger.warning(
                "The config option 'log_file' has been deprecated. Use dictConfig instead."
            )
        else:
            ...
            # --- if logging is valid logging.dictConfig -->
            # (add file handler first?)
            # handlers = configuration['logging'].get('handlers',{})
            # filehandler =  handlers.get('file',{})
            # if logfile and filehandler:
            #     configuration['logging']['handlers']['file']['filename'] = logfile

        for key, value in configuration.items():
            setattr(self, key, value)

    @property
    def is_valid(self) -> bool:
        """Check if the configuration has all required fields."""
        result: bool = is_valid_config(self.__dict__)[0]
        return result

    @property
    def log_file(self) -> str:
        """Get file name from logging configuration, if a file based log handler is defined."""
        logging = getattr(self, "logging", {})
        handlers = logging.get("handlers", {})
        file_handler = handlers.get("file", {})
        if file_handler:
            return str(file_handler["filename"])
        else:
            return ""

    def __getitem__(self, item: str) -> Any | None:
        """Get the value of a configuration."""
        return getattr(self, str(item), None)

    def __eq__(self, other: Self | dict) -> bool:
        """Equality test."""
        if isinstance(other, dict):
            return self.__dict__ == other
        else:
            return self.__dict__ == other.__dict__

    def __str__(self) -> str:
        """Return timeseries configurations as JSON string."""
        return json.dumps(self.__dict__, sort_keys=True, indent=2)

    def activate(self) -> Self:
        """Update the process wide active in-memory configuration, and if its configuration file exists, update the environment variable :py:const:`ENV_VAR_NAME` to point to that file.

        Note that this does not save the file.
        See `.save()`.
        """
        from ..io import fs

        type(self)._active = self

        if fs.exists(self.configuration_file):
            active_file(self.configuration_file)
        else:
            _config_logger.warning(
                f"A new configuration was cached in memory, but the specified configuration file does not exist:'{self.configuration_file}'."
            )
        return self

    @classmethod
    def active(cls) -> Config:
        """Return the (in-memory) active configuration.

        This does not read from file, use `.refresh()` to reload it.
        """
        if getattr(cls, "_active", None) is None:
            cls.refresh()
        return cls._active

    @classmethod
    def refresh(cls) -> Self:
        """Reload the configuration from the file identified by :py:const:`ENV_VAR_NAME`, activate it and return it."""
        return cls(configuration_file=active_file()).activate()

    def save(self, path: PathStr = "") -> None:
        """Saves configurations to the JSON file defined by `path` or :py:attr:`configuration_file`.

        If `path` is provided, it takes presence and :attr:`.configuration_file` will be updated accordingly.

        Note that `.save()` does not activate the configuration instance.
        Use `.activate()` to make it the active configuration,
        or `.refresh()` to reload the active configuration from its file.

        Args:
            path (PathStr): Full path of the JSON file to save to. If not specified, it will attempt to use the environment variable :py:const:`ENV_VAR_NAME` before falling back to the default location `$HOME/.config/ssb_timeseries/timeseries_config.json`.

        Raises:
            ValueError: If `path` is not provided and :attr:`configuration_file` is not set.
        """
        from ..io import fs

        if path:
            self.configuration_file = str(path)
        elif not self.configuration_file:
            raise ValueError(
                "Configuration file must have a value or path must be specified."
            )
        else:
            path = self.configuration_file

        fs.write_text(content=str(self), path=str(path), file_format="json")
        if not fs.exists(self.log_file):
            fs.touch(self.log_file)


class MissingEnvironmentVariableError(Exception):
    """The environment variable :py:const:`ENV_VAR_NAME` must be defined."""

    ...


class ValidationError(Exception):
    """Configuration validation error."""

    ...


def load_json_file(path: PathStr, error_on_missing: bool = False) -> dict:
    """Read configurations from a JSON file into a dictionary."""
    from ..io import fs

    if fs.exists(path):
        from_json = fs.read_json(path)
        if not isinstance(from_json, dict):
            from_json = json.loads(from_json)

        return from_json

    elif error_on_missing:
        raise FileNotFoundError(
            f"A configuration file at {path} file was specified, but does not exist."
        )
    else:
        return {}


class DictObject(object):  # noqa
    """Helper class to convert dict to object."""

    def __init__(self, dict_: dict) -> None:
        self.__dict__.update(dict_)

    @classmethod
    def from_dict(cls, d: dict):  # noqa: ANN206
        return json.loads(json.dumps(d), object_hook=DictObject)


def presets(named_config: str) -> dict | ConfigDict:  # noqa: RUF100
    """Retrieve a preset configuration dictionary.

    Raises:
        ValueError: If args is not 'home' | 'daplalab'.
    """
    if named_config in PRESETS:
        cfg = PRESETS[named_config]
        # cfg["logging"]["handlers"]["file"]["filename"] = cfg.pop("log_file", "")
        return cfg
    else:
        raise ValueError(
            f"Named configuration preset '{named_config}' was not recognized."
        )


def main(*args: str | PathStr) -> None:
    """Set configurations to predefined defaults when run from command line.

    Use:
        ```
        poetry run timeseries-config <option>
        ```
    or
        ```
        python ./config.py <option>`
        ```

    Args:
        *args (str): 'home' | 'default' | 'daplalab'.

    Raises:
        ValueError: If args is not 'home' | 'default' | 'daplalab'. # noqa: DAR402

    """
    if args:
        config_identifier = str(args[0])
    elif len(sys.argv) > 1:
        config_identifier = sys.argv[1]
    else:
        raise ValueError(
            "A configuration preset must be specified. "
            f"Available options are: {', '.join(PRESETS)}"
        )

    cfg = Config(preset=config_identifier)
    cfg.save()
    cfg.activate()

    _config_logger.info(
        "Activated configuration preset %r:\n... saved to file %s",
        config_identifier,
        cfg.configuration_file,
    )


def path_str(*args) -> str:
    """Concatenate paths as string: str(Path(...))."""
    return str(Path(*args))


def activate_discovered_config(fail_on_no_config: bool = False) -> None:
    """Try to find the config.

    And take appropriate action if it can not be found.
    """
    from ..io import fs

    _config_file = active_file()
    if _config_file and fs.exists(_config_file):
        _cfg = Config(configuration_file=_config_file)
        _cfg.activate()
        fs.touch(_cfg.log_file)

    elif fail_on_no_config:
        raise MissingEnvironmentVariableError(
            f"Environment variable {ENV_VAR_NAME} must be defined."
        )
    elif _config_file and not fs.exists(_config_file):
        raise FileNotFoundError(
            f"The configuration file {_config_file} was identified by {ENV_VAR_NAME}, but could not be found."
        )

    # elif DAPLALAB: ... # it may be OK that the config file does not exist
    elif DAPLA_TEAM_CONTEXT and fs.is_gcs(_config_file):
        # """On DAPLA we consider it more severe if configs are missing."""

        _config_logger.warning(
            "%s: No configuration file was found at %s.",
            DAPLA_TEAM_CONTEXT,
            _config_file,
        )


__all__ = [
    "Config",
    "constants",
]

if __name__ == "__main__":
    """Execute when called directly, ie not via import statements."""
    ...
else:
    import traceback

    traceback.print_stack()

    activate_discovered_config()
