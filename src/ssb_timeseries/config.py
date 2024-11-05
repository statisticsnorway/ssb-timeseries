"""Configurations for the SSB timeseries library.

An environment variable TIMESERIES_CONFIG is expected to point to a JSON file with configurations.
If these exist, they will be loaded and put into a Config object CONFIG when the configuration module is loaded.

In most cases, this will happen behind the scene when the core libraries are loaded.

Using the configuration module should only be necessary in order to manipulate configurations from Python code.

Example:
    >>> # xdoctest: +SKIP
    >>> from ssb_timeseries.config import CONFIG
    >>> CONFIG.catalog('gs://some_bucket/timeseries_config.json')
    >>> CONFIG.save()
    >>> # xdoctest: -SKIP

For switching between preset configurations, use the `timeseries-config` command::

    poetry run timeseries-config <option>

which is equivalent to::

    python ./config.py <option>

See :py:func:`ssb_timeseries.config.main` for details on the named options.

"""

import json
import os
import sys
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

from typing_extensions import Self

from ssb_timeseries import fs
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment, arg-type, override,call-arg,has-type"


GCS = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier"
JOVYAN = "/home/jovyan"
HOME = str(Path.home())
LOGFILE = "timeseries.log"

DEFAULTS = {
    "configuration_file": os.path.join(HOME, "timeseries_config.json"),
    "timeseries_root": os.path.join(HOME, "series_data"),
    "catalog": os.path.join(HOME, "series_data", "metadata"),
    "log_file": os.path.join(HOME, "logs", LOGFILE),
    "bucket": HOME,
}
CONFIGURATION_FILE: str = os.getenv("TIMESERIES_CONFIG", DEFAULTS["configuration_file"])


@dataclass(slots=False)
class Config:
    """Configuration class."""

    configuration_file: str = CONFIGURATION_FILE
    timeseries_root: str = DEFAULTS["timeseries_root"]
    catalog: str = DEFAULTS["catalog"]
    log_file: str = DEFAULTS["log_file"]
    bucket: str = DEFAULTS["bucket"]

    def __getitem__(self, item: str) -> str:
        """Get the value of a configuration."""
        d = asdict(self)
        return str(d[item])

    def __eq__(self, other: Self) -> bool:
        """Equality test."""
        return asdict(self) == other.__dict__()

    def to_json(self, original_implementation: bool = False) -> str:
        """Return timeseries configurations as JSON string."""
        if original_implementation:
            return json.dumps(
                self, default=lambda o: o.__dict__(), sort_keys=True, indent=4
            )
        else:
            return json.dumps(asdict(self), sort_keys=True, indent=4)

    def save(self, path: PathStr = CONFIGURATION_FILE) -> None:
        """Saves configurations to JSON file and set environment variable TIMESERIES_CONFIG to the location of the file.

        Args:
            path (PathStr): Full path of the JSON file to save to. Defaults to the value of the environment variable TIMESERIES_CONFIG.
        """
        fs.write_json(content=self.to_json(), path=str(path))
        if not fs.exists(self.log_file):
            fs.touch(self.log_file)
        if HOME == JOVYAN:
            # For some reason `os.environ["TIMESERIES_CONFIG"] = path` does not work:
            cmd = f"export TIMESERIES_CONFIG={CONFIGURATION_FILE}"
            os.system(cmd)
            # os.system(f"echo '{cmd}' >> ~/.bashrc")
        else:
            os.environ["TIMESERIES_CONFIG"] = path

    @classmethod
    def load(cls, path: PathStr) -> Self:
        """Read the properties from a JSON file into a Config object."""
        if fs.exists(path):
            json_file = json.loads(fs.read_json(path))

            return cls(
                configuration_file=str(path),
                bucket=json_file.get("bucket"),
                timeseries_root=json_file.get("timeseries_root"),
                catalog=json_file.get("catalog"),
                # product=json_file.get("product"),
                log_file=json_file.get("log_file"),
            )
        else:
            raise FileNotFoundError(
                "Cfg.load() was called with an empty or invalid path."
            )

    def __dict__(self) -> dict[str, str]:
        """Return timeseries configurations as dict."""
        return asdict(self)


CONFIG = Config(configuration_file=CONFIGURATION_FILE)
"""A Config object."""

CONFIG.save()


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
        ValueError: If args is not 'home' | 'gcs' | 'jovyan'.

    """
    if args:
        config_identifier: PathStr = args[0]
    else:
        config_identifier = sys.argv[1]

    print(
        f"Update configuration file TIMESERIES_CONFIG: {CONFIGURATION_FILE}, with named presets: '{config_identifier}'."
    )
    match config_identifier:
        case "home":
            identifier_is_named_option = True
            bucket = HOME
            timeseries_root = path_str(HOME, "series_data")
            catalog = path_str(HOME, "series_data", "metadata")
            log_file = DEFAULTS["log_file"]
        case "gcs":
            identifier_is_named_option = True
            bucket = GCS
            timeseries_root = path_str(GCS, "series_data")
            catalog = path_str(HOME, "series_data", "metadata")
            log_file = path_str(HOME, "logs", LOGFILE)
        case "jovyan":
            identifier_is_named_option = True
            bucket = JOVYAN
            timeseries_root = path_str(JOVYAN, "series_data")
            catalog = path_str(HOME, "series_data", "metadata")
            log_file = path_str(JOVYAN, "logs", LOGFILE)
        case _:
            identifier_is_named_option = False
            identifier_is_existing_file = fs.exists(config_identifier)
            bucket = None

    if identifier_is_named_option:
        cfg = Config(
            configuration_file=CONFIGURATION_FILE,
            bucket=bucket,
            timeseries_root=timeseries_root,
            catalog=catalog,
            log_file=log_file,
        )
    elif identifier_is_existing_file:
        cfg = Config(configuration_file=config_identifier)
    else:
        raise ValueError(
            f"Unrecognised named configuration preset '{config_identifier}'."
        )

    cfg.save(CONFIGURATION_FILE)
    print(cfg)
    print(os.getenv("TIMESERIES_CONFIG"))


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
