"""Configurations for the SSB timeseries library.

An environment variable TIMESERIES_CONFIG is expected to point to a JSON file with configurations.
If these exist, they will be loaded and put into a Config object CONFIG when the configuration module is loaded.

In most cases, this will happen behind the scene when the core libraries are loaded.

Using the configuration module should only be necessary in order to manipulate configurations from Python code.

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
DAPLA_ENV = os.getenv("DAPLA_ENVIRONMENT", "")  # PROD, TEST, DEV
DAPLA_TEAM = os.getenv("DAPLA_TEAM", "")
DAPLA_BUCKET = f"gs://{DAPLA_TEAM}-{DAPLA_ENV}"
JOVYAN = "/home/jovyan"
DAPLALAB_HOME = "/home/jovyan/work"
ROOT_DIR_NAME = "tidsserier"
META_DIR_NAME = "metadata"
LINUX_CONF_DIR = ".config"
LOGDIR = "logs"
LOGFILE = "timeseries.log"
CONFIGFILE = "timeseries_config.json"

DEFAULTS = {
    "configuration_file": os.path.join(
        HOME,
        LINUX_CONF_DIR,
        PACKAGE_NAME,
        CONFIGFILE,
    ),
    "timeseries_root": os.path.join(HOME, ROOT_DIR_NAME),
    "catalog": os.path.join(HOME, ROOT_DIR_NAME, META_DIR_NAME),
    "log_file": os.path.join(HOME, LOGDIR, LOGFILE),
    "bucket": HOME,
}

if ENV_VAR_FILE:
    CONFIGURATION_FILE = ENV_VAR_FILE
else:
    CONFIGURATION_FILE = DEFAULTS["configuration_file"]


def set_env(path: str = CONFIGURATION_FILE) -> None:
    """Set environment variable :py:const:`ENV_VAR_NAME` to the location of the configuration file."""
    os.environ[ENV_VAR_NAME] = path


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
            config_values = DEFAULTS
            config_from_file = load_json_file(
                path=param_specified_config_file,
                error_on_missing=False,
            )
            config_values.update(config_from_file)
        elif ENV_VAR_FILE:
            # if the path is specified by the environment variable, not finding it is an error
            config_values = load_json_file(
                path=ENV_VAR_FILE,
                error_on_missing=True,
            )
        else:
            config_values = load_json_file(
                path=DEFAULTS["configuration_file"],
                error_on_missing=True,
            )

        config_values.update(kwargs)

        self.apply(config_values)

    def apply(self, configs: dict) -> None:
        """Set configuration values from a dictionary."""
        if is_valid_config(configs):
            for key, value in configs.items():
                setattr(self, key, value)
        else:
            raise ValidationError(f"Invalid configuration {configs}.")

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
        if HOME == JOVYAN:
            # For some reason `os.environ[ENV_VAR_NAME] = path` does not work:
            cmd = f"export TIMESERIES_CONFIG={CONFIGURATION_FILE}"
            os.system(cmd)
            # os.system(f"echo '{cmd}' >> ~/.bashrc")
        else:
            os.environ[ENV_VAR_NAME] = path
        os.environ[ENV_VAR_NAME] = path

    def __getitem__(self, item: str) -> str:
        """Get the value of a configuration."""
        return str(getattr(self, item))

    def __eq__(self, other: Self) -> bool:
        """Equality test."""
        return self.__dict__ == other.__dict__

    def to_json(self, original_implementation: bool = False) -> str:
        """Return timeseries configurations as JSON string."""
        return json.dumps(str(self), sort_keys=True, indent=4)


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


def migrate_to_new_config_location(
    file_to_copy: PathStr = "",
) -> None:
    """Copy an existing configuration files to the new default location $HOME/.config/ssb_timeseries/timeseries_config.json.

    Args:
        file_to_copy (PathStr): Optional. Path to a existing configuration file. If not provided, the function will look in the most common location for SSBs old JupyterLab and DaplaLab.
    """
    import logging as ts_config_logger

    if file_to_copy:
        fs.cp(file_to_copy, DEFAULTS["configuration_file"])
    else:
        copy_these = [
            {
                "replace": "_active",
                "source": CONFIGURATION_FILE,
            },
            {
                "replace": "_home",
                "source": path_str(HOME, "timeseries_config.json"),
            },
            {
                "replace": "_env",
                "source": ENV_VAR_FILE,
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
            ts_config_logger.warning(
                f"Configuration files were not found: {not_found}."
            )

        if copied:
            # copy the first file = make it the active one
            fs.cp(copied[0], DEFAULTS["configuration_file"])
            ts_config_logger.info(
                f"Configuration files were copied: {copied}.\nCopied {copied[0]} to detectable file {DEFAULTS['configuration_file']}."
            )
        else:
            # no files were found --> create one from defaults
            new = Config(preset="default")
            new.save()


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
    match named_config:
        case "default" | "defaults":
            cfg = {
                "configuration_file": DEFAULTS["configuration_file"],
                "bucket": DEFAULTS["bucket"],
                "timeseries_root": DEFAULTS["timeseries_root"],
                "catalog": DEFAULTS["catalog"],
                "log_file": DEFAULTS["log_file"],
            }
        case "home":
            cfg = {
                "configuration_file": CONFIGURATION_FILE,
                "bucket": HOME,
                "timeseries_root": path_str(HOME, ROOT_DIR_NAME),
                "catalog": path_str(HOME, ROOT_DIR_NAME, META_DIR_NAME),
                "log_file": path_str(HOME, ROOT_DIR_NAME, LOGDIR, LOGFILE),
            }
        case "gcs":
            cfg = {
                "configuration_file": path_str(GCS, ROOT_DIR_NAME, CONFIGFILE),
                "bucket": GCS,
                "timeseries_root": path_str(GCS, ROOT_DIR_NAME),
                "catalog": path_str(HOME, ROOT_DIR_NAME, META_DIR_NAME),
                "log_file": path_str(HOME, ROOT_DIR_NAME, LOGFILE),
            }
        case "jovyan":
            cfg = {
                "configuration_file": CONFIGURATION_FILE,
                "bucket": JOVYAN,
                "timeseries_root": path_str(JOVYAN, ROOT_DIR_NAME),
                "catalog": path_str(HOME, ROOT_DIR_NAME, META_DIR_NAME),
                "log_file": path_str(JOVYAN, LOGDIR, LOGFILE),
            }
        case "dapla":
            cfg = {
                "configuration_file": path_str(
                    DAPLA_BUCKET,
                    ROOT_DIR_NAME,
                    "konfigurasjon",
                    CONFIGFILE,
                ),
                "bucket": JOVYAN,
                "timeseries_root": path_str(JOVYAN, ROOT_DIR_NAME),
                "catalog": path_str(HOME, ROOT_DIR_NAME, META_DIR_NAME),
                "log_file": path_str(JOVYAN, LOGDIR, LOGFILE),
            }
        case _:
            if fs.exists(named_config):
                cfg = Config(configuration_file=named_config).__dict__
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


def path_str(*args: str) -> str:
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
    if not fs.exists(CONFIGURATION_FILE):
        print(
            f"No configuration file was foumd at {CONFIGURATION_FILE}. Other locations will be tried. Files found will be copied to the default location  and the first candidate will be set to active, ie copied onsce more to {DEFAULTS['configuration_file']}"
        )
        migrate_to_new_config_location()

    CONFIG = Config(configuration_file=CONFIGURATION_FILE)
    """A Config object."""

    # do not save
    # CONFIG.save()
