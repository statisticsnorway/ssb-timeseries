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

from typing_extensions import Self

from ssb_timeseries import fs
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment, arg-type, override,call-arg,has-type,no-untyped-def,attr-defined"

PACKAGE_NAME = "ssb_timeseries"
ENV_VAR_NAME: str = "TIMESERIES_CONFIG"
ENV_VAR_FILE: str = os.getenv(ENV_VAR_NAME, "")

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
DAPLA_ENV = os.getenv("DAPLA_ENVIRONMENT", "")  # PROD, TEST, DEV
DAPLA_TEAM = os.getenv("DAPLA_TEAM", "")
DAPLA_BUCKET = f"gs://{DAPLA_TEAM}-{DAPLA_ENV}"

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
DEFAULTS = PRESETS["default"]


def set_env(path: str) -> None:
    """Set environment variable :py:const:`ENV_VAR_NAME` to the location of the configuration file."""
    os.environ[ENV_VAR_NAME] = path
    logging.warning(f"Set environment variable {ENV_VAR_NAME} to {path}")


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

        Examples:
            Load an existing config from TIMESERIES_CONFIG or default location:

                >>> from ssb_timeseries.config import Config
                >>> config = Config()

            Load config, change parameter and save:

                >>> config.save()
        """
        preset_name = kwargs.pop("preset", "")
        if preset_name:
            named_config = presets(preset_name)
            self.apply(named_config)
            return

        param_specified_config_file = kwargs.pop("configuration_file", "")
        if param_specified_config_file and not kwargs:
            config_values = load_json_file(
                path=param_specified_config_file,
                error_on_missing=True,
            )
            self.apply(config_values)
            return
        elif param_specified_config_file and kwargs:
            config_values = presets("default")
            config_from_file = load_json_file(
                path=param_specified_config_file,
                error_on_missing=False,
            )
            config_values.update(config_from_file)
        elif not kwargs and not ENV_VAR_FILE:
            raise EnvVarNotDefinedeError
        elif ENV_VAR_FILE:
            # if the path is specified by the environment variable, not finding it is an error
            logging.warning(f"Loading configuration from {ENV_VAR_FILE}")
            config_values = load_json_file(
                path=ENV_VAR_FILE,
                error_on_missing=True,
            )
            if kwargs:
                config_values.update(kwargs)
        else:
            config_values = presets("defaults")

        config_values.update(kwargs)

        self.apply(config_values)

    def apply(self, configs: dict) -> None:
        """Set configuration values from a dictionary."""
        if is_valid_config(configs):
            for key, value in configs.items():
                if isinstance(value, tuple):
                    setattr(self, key, path_str(*value))
                else:
                    setattr(self, key, value)
        else:
            logging.error(f"Invalid configuration {configs.keys()}.")
            raise ValidationError(f"Invalid configuration {configs}.")

    @property
    def is_valid(self) -> bool:
        """Check if the configuration has all required fields."""
        return is_valid_config(self.__dict__)

    def save(self, path: PathStr = "") -> None:
        """Saves configurations to the JSON file defined by `configuration_file`.

        Args:
            path (PathStr): Full path of the JSON file to save to. If not specified, it will attempt to use the environment variable TIMESERIES_CONFIG before falling back to the default location `$HOME/.config/ssb_timeseries/timeseries_config.json`.
        """
        if not path:
            path = self.configuration_file

        fs.write_json(content=self.__dict__, path=str(path))
        if not fs.exists(self.log_file):
            fs.touch(self.log_file)

        set_env(str(path))

    def __getitem__(self, item: str) -> str:
        """Get the value of a configuration."""
        return str(getattr(self, item))

    def __eq__(self, other: Self) -> bool:
        """Equality test."""
        return self.__dict__ == other.__dict__

    def to_json(self, original_implementation: bool = False) -> str:
        """Return timeseries configurations as JSON string."""
        return json.dumps(str(self), sort_keys=True, indent=4)


class EnvVarNotDefinedeError(ValueError):
    """The environment variable TIMESEREIS_CONFIG must be defined."""

    ...


class ValidationError(ValueError):
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
            f"A configuration {path} file was specified, but does not exist."
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
                "source": ENV_VAR_FILE,
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
        set_env(copied[0])
        logging.info(
            f"Configuration files were copied: {copied}.\nActive: {copied[0]}."
        )
        return str(copied[0])
    else:
        # no files were found --> create one from defaults?
        # new = Config(preset="default")
        # ew.save()
        return ""


def is_valid_config(config: dict) -> bool:
    """Check if a dictionary is a valid configuration.

    A valid configuration has the same keys as DEFAULTS.
    """
    return sorted(dict(config).keys()) == sorted(dict(DEFAULTS).keys())


def presets(named_config: str) -> dict:  # noqa: RUF100, DAR201
    """Set configurations to predefined defaults.

    Raises:
        ValueError: If args is not 'home' | 'gcs' | 'jovyan'.
    """
    if named_config == "defaults":
        named_config = "default"

    if named_config in PRESETS:
        cfg = PRESETS[named_config]
    elif fs.exists(named_config):
        cfg = load_json_file(named_config).__dict__
    else:
        raise ValueError(
            f"Named configuration preset '{named_config}' was not recognized."
        )

    return cfg


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
    cfg = Config(presets=config_identifier)
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
    if ENV_VAR_FILE:
        if fs.exists(ENV_VAR_FILE):
            CONFIGURATION_FILE = ENV_VAR_FILE
        else:
            print(
                f"No configuration file was foumd at {ENV_VAR_FILE}.\nOther locatsions will be tried. Files found will be copied to the default location and the first candidate will be set to active, ie copied onsce more to {DEFAULTS['configuration_file']}"
            )
            CONFIGURATION_FILE = migrate_to_new_config_location()
            if not fs.exists(CONFIGURATION_FILE):
                raise FileNotFoundError(
                    f"No configuration file was found at {ENV_VAR_FILE}."
                )
    else:
        CONFIGURATION_FILE = path_str(DEFAULTS["configuration_file"])

    CONFIG = Config(configuration_file=CONFIGURATION_FILE)
    """A Config object."""

    # do not save
    # CONFIG.save()
