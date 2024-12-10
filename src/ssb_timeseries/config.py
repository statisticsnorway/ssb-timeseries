"""Configurations for the SSB timeseries library.

An environment variable TIMESERIES_CONFIG is expected to point to a JSON file with configurations.
If these exist, they will be loaded and put into a Config object CONFIG when the configuration module is loaded.

In most cases, this would happen behind the scene when :py:mod:`ssb_timeseries.dataset` or :py:mod:`ssb_timeseries.catalog` are imported.

Directly accessing the configuration module should only be required when manipulating configurations from Python code.

Example:
    >>> # doctest: +SKIP
    >>> from ssb_timeseries.config import CONFIG
    >>> CONFIG.catalog = 'gs://{bucket}/timeseries/metadata/'
    >>> CONFIG.save()
    >>> # doctest: -SKIP

For switching between preset configurations, use the `timeseries-config` command::

    poetry run timeseries-config <option>

which is equivalent to::

    python ./config.py <option>

See :py:func:`ssb_timeseries.config.main` for details on the named options.

"""

import json
import logging
import os
import sys
from pathlib import Path

from jsonschema import validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from typing_extensions import Self

from ssb_timeseries import fs
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment, arg-type, override,call-arg,has-type,no-untyped-def,attr-defined,import-untyped,"

PACKAGE_NAME = "ssb_timeseries"
ENV_VAR_NAME = "TIMESERIES_CONFIG"


def unset_env_var() -> str:
    """Unsets the environment variable :py:const:`ENV_VAR_NAME` and returns the value that was unset."""
    return os.environ.pop(ENV_VAR_NAME, "")


def active_file(path: PathStr = "") -> str:
    """If a path is provided, sets environment variable :py:const:`ENV_VAR_NAME` to specify the location of the configuration file.

    Returns the value of the environment variable by way of :py:func:`get_active_file`.
    """
    if path:
        os.environ[ENV_VAR_NAME] = str(path)
        logging.debug(f"Set environment variable {ENV_VAR_NAME} to {path}")

    return os.environ.get(ENV_VAR_NAME, "")


HOME = str(Path.home())
SHARED_PROD = "gs://ssb-prod-dapla-felles-data-delt/tidsserier"
SHARED_TEST = "gs://ssb-test-dapla-felles-data-delt/tidsserier"
GCS = SHARED_PROD

JOVYAN = "/home/jovyan"
DAPLALAB_HOME = "/home/jovyan/work"
SSB_DIR_NAME = "tidsserier"
ROOT_DIR_NAME = "tidsserier"
META_DIR_NAME = "metadata"
SSB_CONF_DIR = "konfigurasjon"
LINUX_CONF_DIR = ".config"
SSB_LOGDIR = "logger"
LOGDIR = "logs"
LOGFILE = "timeseries.log"
CONFIGFILE = "timeseries_config.json"

DAPLA_TEAM_CONTEXT = os.getenv("DAPLA_TEAM_CONTEXT", "")
DAPLA_ENV = os.getenv("DAPLA_ENVIRONMENT", "")
"""Returns the Dapla environment: 'prod' | test | dev"""
DAPLA_TEAM = os.getenv("DAPLA_TEAM", "")
"""Returns the Dapla team/project name.'"""
DAPLA_BUCKET = f"gs://{DAPLA_TEAM}-{DAPLA_ENV}"
"""Returns the Dapla product bucket name for the current environment: gs://{DAPLA_TEAM}-{DAPLA_ENV}."""
PRESETS: dict[str, dict] = {
    "default": {
        "configuration_file": str(Path(HOME, LINUX_CONF_DIR, PACKAGE_NAME, CONFIGFILE)),
        "bucket": str(Path(HOME)),
        "timeseries_root": str(Path(HOME, ROOT_DIR_NAME)),
        "catalog": str(Path(HOME, ROOT_DIR_NAME, META_DIR_NAME)),
        "log_file": str(Path(HOME, LOGDIR, LOGFILE)),
    },
    "home": {
        "configuration_file": str(Path(HOME, LINUX_CONF_DIR, PACKAGE_NAME, CONFIGFILE)),
        "bucket": str(Path(HOME)),
        "timeseries_root": str(Path(HOME, ROOT_DIR_NAME)),
        "catalog": str(Path(HOME, ROOT_DIR_NAME, META_DIR_NAME)),
        "log_file": str(Path(HOME, ROOT_DIR_NAME, LOGDIR, LOGFILE)),
    },
    "gcs": {
        "configuration_file": str(Path(GCS, ROOT_DIR_NAME, CONFIGFILE)),
        "bucket": str(Path(GCS)),
        "timeseries_root": str(Path(GCS, ROOT_DIR_NAME)),
        "catalog": str(Path(HOME, ROOT_DIR_NAME, META_DIR_NAME)),
        "log_file": str(Path(HOME, ROOT_DIR_NAME, LOGFILE)),
    },
    "shared-test": {
        "configuration_file": str(Path(HOME, SSB_CONF_DIR, PACKAGE_NAME, CONFIGFILE)),
        "bucket": str(Path(SHARED_TEST)),
        "timeseries_root": str(Path(SHARED_TEST, ROOT_DIR_NAME)),
        "catalog": str(Path(SHARED_TEST, ROOT_DIR_NAME, META_DIR_NAME)),
        "log_file": str(Path(SHARED_TEST, LOGDIR, LOGFILE)),
    },
    "shared-prod": {
        "configuration_file": str(
            Path(SHARED_PROD, SSB_CONF_DIR, PACKAGE_NAME, CONFIGFILE)
        ),
        "bucket": str(Path(SHARED_PROD)),
        "timeseries_root": str(Path(SHARED_PROD, ROOT_DIR_NAME)),
        "catalog": str(Path(SHARED_PROD, ROOT_DIR_NAME, META_DIR_NAME)),
        "log_file": str(Path(SHARED_PROD, LOGDIR, LOGFILE)),
    },
    "jovyan": {
        "configuration_file": str(
            Path(JOVYAN, LINUX_CONF_DIR, PACKAGE_NAME, CONFIGFILE)
        ),
        "bucket": str(Path(JOVYAN)),
        "timeseries_root": str(Path(JOVYAN, ROOT_DIR_NAME)),
        "catalog": str(Path(HOME, ROOT_DIR_NAME, META_DIR_NAME)),
        "log_file": str(Path(JOVYAN, LOGDIR, LOGFILE)),
    },
    "dapla": {
        "configuration_file": str(
            Path(DAPLA_BUCKET, SSB_CONF_DIR, PACKAGE_NAME, CONFIGFILE)
        ),
        "bucket": str(Path(JOVYAN)),
        "timeseries_root": str(Path(DAPLA_BUCKET, ROOT_DIR_NAME)),
        "catalog": str(Path(DAPLA_BUCKET, ROOT_DIR_NAME, META_DIR_NAME)),
        "log_file": str(Path(DAPLA_BUCKET, SSB_LOGDIR, LOGFILE)),
    },
}

PRESETS["defaults"] = PRESETS["default"]
DEFAULTS = PRESETS["default"]


class Config:
    """Configuration class; for reading and writing timeseries configurations.

    If instantiated with no parameters, an existing configuration file is exepected to exist: either in a location specified by the environment variable TIMESERIES_CONFIG or in the default location in the user's home directory. If not, an error is returned.

    If the :py:attr:`configuration_file` attribute is specified, configurations will be loaded from that file. No other parameters are required. A :py:exc:`FileNotFoundError` or :py:exc:`FileDoesNotExist` error will be returned if the file is not found. In this case, no attempt is made to load configurations from locations specified by environment variable or defaults.

    If any additional parameters are provided, they will override values from the configuration file. If the result is not a valid configuration, a ValidationError is raised.

    If one or more parameters are provided, but the `configuration_file` parameter is not among them, configurations are identified by the environment variable TIMESERIES_CONFIG or the default configuration file location (in that order of priority). Provided parameters override values from the configuration file. If the result is not a valid configuration, an error is raised.

    The returned configuration will not be saved, but held in memory only till the :py:meth:`save` method is called. Then the configuration will be savedto a file and the environment variable TIMESERIES_CONFIG set to reflect the location of the file.

    """

    configuration_file: PathStr
    """The path to the configuRation file."""
    timeseries_root: PathStr
    """The root directory for data storage of a repository."""
    catalog: PathStr
    """The path to the metadata directory of a repository ."""
    log_file: PathStr
    """The path to the log file."""
    bucket: PathStr
    """The topmost level of the GCS bucket for the team."""

    def __init__(self, **kwargs) -> None:  # noqa: D417, ANN003, DAR101, DAR402, RUF100
        """Initialize Config object from keyword arguments.

        Keyword Arguments:
            preset (str): Optional. Name of a preset configuration. If provided, the preset configuration is loaded, and no other parameters are considered.
            configuration_file (str): Path to the configuration file. If the parameter is not provided, the environment variable TIMESERIES_CONFIG is used. If the environment variable is not set, the default configuration file location is used.
            timeseries_root (str): Path to the root directory for time series data. If one of these identifies a vaild json file, the configuration is loaded from that file and no other parameters are required. If provided, they will override values from the configuration file.
            catalog (str): Path to the catalog file.
            log_file (str): Path to the log file.
            bucket (str): Name of the GCS bucket.

        Raises:
            :py:exc:`FileNotFoundError`: If the configuration file as implied by provided or not provided parameters does not exist.   # noqa: DAR402
            :py:exc:`ValidationError`: If the resulting configuration is not valid.   # noqa: DAR402
            :py:exc:`EnvVarNotDefinedeError`: If the environment variable TIMESERIES_CONFIG is not defined.

        Examples:
            Load an existing config from TIMESERIES_CONFIG or default location:

                >>> from ssb_timeseries.config import Config
                >>> config = Config.active()
        """
        preset_name = kwargs.pop("preset", "")
        ignore_file = kwargs.pop("ignore_file", False)
        param_specified_config_file = kwargs.get("configuration_file", "")
        kwargs_are_complete_config = is_valid_config(kwargs)[0]

        if preset_name:
            logging.debug(f"Loading preset configuration {preset_name}.")
            self.apply(PRESETS[preset_name])
            return
        elif kwargs_are_complete_config:
            logging.debug("Complete configuration in parameters.")
            self.apply(kwargs)
            return
        elif param_specified_config_file:
            logging.warning(f"Loading configuration from {param_specified_config_file}")
            if set(kwargs.keys()) == set(["configuration_file"]):
                no_file_is_an_error = True
            else:
                no_file_is_an_error = not is_valid_config(kwargs)[0]

            if not ignore_file:
                config_from_file = load_json_file(
                    path=param_specified_config_file,
                    error_on_missing=no_file_is_an_error,
                )
            else:
                config_from_file = {}

            config_values = PRESETS["default"]
            config_values.update(config_from_file)
            logging.debug(f"{config_values=}")
        elif active_file():
            # if the path is specified by the environment variable, not finding it is an error
            logging.warning(f"Loading configuration from {active_file()}")
            config_values = load_json_file(
                path=active_file(),
                error_on_missing=True,
            )
        # elif not active_file():
        #    raise MissingEnvironmentVariableError
        else:
            logging.warning(
                f"The environment variable {ENV_VAR_NAME} did not exist and no configuration file parameter was provided. Loading default configuration."
            )
            config_values = PRESETS["defaults"]

        config_values.update(kwargs)

        self.apply(config_values)

    def apply(self, configuration: dict) -> None:
        """Set configuration values from a dictionary."""
        is_valid, reason = is_valid_config(configuration=configuration)
        if is_valid:
            for key, value in configuration.items():
                setattr(self, key, value)
        else:
            logging.error(f"Invalid configuration {configuration}\n{reason}.")
            raise ValidationError(f"Invalid configuration {configuration}.")

    @property
    def is_valid(self) -> bool:
        """Check if the configuration has all required fields."""
        result: bool = is_valid_config(self.__dict__())[0]
        return result

    def save(self, path: PathStr = "") -> None:
        """Saves configurations to the JSON file defined by :py:param:`path` or :py:class:`Config.configuration_file`.

        If :py:param:`path` is set, it will take presence and `configuration_file` will be set.

        Args:
            path (PathStr): Full path of the JSON file to save to. If not specified, it will attempt to use the environment variable TIMESERIES_CONFIG before falling back to the default location `$HOME/.config/ssb_timeseries/timeseries_config.json`.

        Raises:
            ValueError: If :py:param:`path` is not provided and `configuration_file`is not set.
        """
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

        active_file(str(path))

    def __getitem__(self, item: str) -> str:
        """Get the value of a configuration."""
        return str(getattr(self, item))

    def __eq__(self, other: Self | dict) -> bool:
        """Equality test."""
        if isinstance(other, dict):
            return self.__dict__() == other
        else:
            return self.__dict__() == other.__dict__()

    def __dict__(self) -> dict:
        """Return timeseries configurations as JSON string."""
        fields = [
            "configuration_file",
            "timeseries_root",
            "catalog",
            "log_file",
            "bucket",
        ]
        out = {}
        for field in fields:
            out[field] = self[field]
        return out

    def __str__(self) -> str:
        """Return timeseries configurations as JSON string."""
        return json.dumps(self.__dict__(), sort_keys=True, indent=2)

    @classmethod
    def active(cls) -> Self:
        """Force reload and return the configuration identified by :py:const:`ENV_VAR_NAME`."""
        return cls(configuration_file=active_file())


class MissingEnvironmentVariableError(Exception):
    """The environment variable TIMESEREIS_CONFIG must be defined."""

    ...


class ValidationError(Exception):
    """Configuration validation error."""

    ...


def load_json_file(path: PathStr, error_on_missing: bool = False) -> dict:
    """Read configurations from a JSON file into a Config object."""
    if fs.exists(path):
        from_json = fs.read_json(path)
        if not isinstance(from_json, dict):
            return json.loads(from_json)  # type:ignore
        else:
            return from_json

    elif error_on_missing:
        raise FileNotFoundError(
            f"A configuration file at {path} file was specified, but does not exist."
        )
    else:
        return {}


def migrate_to_new_config_location(file_to_copy: PathStr = "") -> str:
    """Copy existing configuration files to the new default location $HOME/.config/ssb_timeseries/.

    The first file copied will be set to active.

    Args:
        file_to_copy (PathStr): Optional. Path to a existing configuration file. If not provided, the function will look in the most common location for SSBs old JupyterLab and DaplaLab.
    """
    DEFAULTS["configuration_file"]

    if file_to_copy:
        fs.cp(file_to_copy, DEFAULTS["configuration_file"])
        return str(file_to_copy)
    else:
        copy_these = [
            {
                "replace": "_active",
                "source": active_file(),
            },
            {
                "replace": "_home",
                "source": path_str(HOME, "timeseries_config.json"),
            },
            {
                "replace": "_jovyan",
                "source": "/home/joyan/timeseries_config.json",
            },
            {
                "replace": "_daplalab",
                "source": "/home/joyan/work/timeseries_config.json",
            },
        ]
        copied = []
        not_found = []

        for c in copy_these:
            if fs.exists(c["source"]):
                # copy all to .config, but let filename signal where it was copied from
                target = DEFAULTS["configuration_file"].replace(
                    ".json", f"{c['replace']}.json"
                )
                fs.cp(c["source"], target)
                copied.append(target)
            else:
                not_found.append(c["source"])
        else:
            logging.warning(f"Configuration files were not found: {not_found}.")

    if copied:
        # copy the first file = make it the active one
        fs.cp(copied[0], DEFAULTS["configuration_file"])
        active_file(copied[0])
        logging.info(
            f"Configuration files were copied: {copied}.\nActive: {copied[0]}."
        )
        return str(copied[0])
    else:
        # no files were found --> create one from defaults?
        # new = Config(preset="default")
        # ew.save()
        return ""


def configuration_schema(version: str = "0.3.1") -> dict:
    """Return the JSON schema for the configuration file."""
    match version:
        case "0.3.1" | "0.3.2" | _:
            cfg_schema = {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "bucket": {"type": "string"},
                    "configuration_file": {"type": "string"},
                    "log_file": {"type": "string"},
                    "catalog": {"type": "string"},
                    "timeseries_root": {"type": "string"},
                },
                "required": [
                    "bucket",
                    "configuration_file",
                    "log_file",
                    "catalog",
                    "timeseries_root",
                ],
            }
    logging.debug(f"Schema {version}:{cfg_schema}")
    return cfg_schema


class DictObject(object):  # noqa
    """Helper class to convert dict to object."""

    def __init__(self, dict_: dict) -> None:  # noqa: D107
        self.__dict__.update(dict_)

    @classmethod
    def from_dict(cls, d: dict):  # noqa: ANN206, D102
        return json.loads(json.dumps(d), object_hook=DictObject)


def is_valid_config(configuration: dict) -> tuple[bool, object]:
    """Check if a dictionary is a valid configuration.

    A valid configuration has the same keys as DEFAULTS.
    """
    try:
        out = (True, None)
        validate(
            instance=configuration,
            schema=configuration_schema(),
        )
    except JsonValidationError as err:
        logging.debug(f"Invalid configuration {configuration}\nValidation error. {err}")
        out = (False, err)
    return out


def presets(named_config: str) -> dict:  # noqa: RUF100, DAR201
    """Set configurations to predefined defaults.

    Raises:
        ValueError: If args is not 'home' | 'gcs' | 'jovyan'.
    """
    if named_config in PRESETS:
        return PRESETS[named_config]
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
        *args (str): 'home' | 'gcs' | 'jovyan'.

    Raises:
        ValueError: If args is not 'home' | 'gcs' | 'jovyan'. # noqa: DAR402

    """
    if args:
        config_identifier: PathStr = args[0]
    else:
        config_identifier = sys.argv[1]

    print(f"Update configuration with named preset: '{config_identifier}'.")
    cfg = Config(preset=config_identifier)
    cfg.save(path=cfg.configuration_file)

    print(
        f"Preset configuration '{config_identifier}' was applied:\n\t{cfg.__dict__}\nSaved to file: {cfg.configuration_file}.\nEnvironment variable set: {os.getenv('TIMESERIES_CONFIG')=}"
    )


def path_str(*args) -> str:  # noqa: ANN002
    """Concatenate paths as string: str(Path(...))."""
    return str(Path(*args))


if __name__ == "__main__":
    """Execute when called directly, ie not via import statements."""
    # ??? `poetry run timeseries-config <option>` does not appear to go this route.
    # --> then it is not obvious that this is a good idea.
    print(f"Name of the script      : {sys.argv[0]=}")
    print(f"Arguments of the script : {sys.argv[1:]=}")
    main(sys.argv[1])
else:
    if active_file():
        if fs.exists(active_file()):
            CONFIGURATION_FILE = active_file()
        elif DAPLA_TEAM_CONTEXT:
            raise MissingEnvironmentVariableError(
                f"Environment variable {ENV_VAR_NAME} must be defined and point to a configuration file."
            )
        else:
            print(
                f"No configuration file was foumd at {active_file()}.\nOther locatsions will be tried. Files found will be copied to the default location and the first candidate will be set to active, ie copied once more to {DEFAULTS['configuration_file']}"
            )
            CONFIGURATION_FILE = migrate_to_new_config_location()
            if not fs.exists(CONFIGURATION_FILE):
                raise FileNotFoundError(
                    f"No configuration file was found at {active_file()}."
                )
    else:
        CONFIGURATION_FILE = ""  # PRESETS["defaults"]["configuration_file"]
        # str(DEFAULTS["configuration_file"])

    active_file(CONFIGURATION_FILE)
    CONFIG = Config(configuration_file=CONFIGURATION_FILE)
    """A Config object."""
    fs.touch(CONFIG.log_file)
    # do not save
    # CONFIG.save()
