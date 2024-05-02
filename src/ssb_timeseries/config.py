# from dataclasses import dataclass
import json
import os
import sys
from pathlib import Path

from ssb_timeseries import fs
from ssb_timeseries.types import PathStr

# mypy: disable-error-code="assignment, arg-type"


GCS = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier"
JOVYAN = "/home/jovyan"
HOME = str(Path.home())

DEFAULT_BUCKET = HOME
DEFAULT_TIMESERIES_LOCATION = os.path.join(HOME, "series_data")
DEFAULT_CONFIG_LOCATION = os.path.join(HOME, "timeseries_config.json")
DEFAULT_LOG_FILE_LOCATION: str = os.path.join(HOME, "logs", "timeseries.log")
TIMESERIES_CONFIG: str = os.getenv("TIMESERIES_CONFIG", DEFAULT_CONFIG_LOCATION)


class Config:
    """Timeseries configurations: bucket, product, timeseries_root, log_file."""

    def __init__(self, configuration_file: str = "", **kwargs: str) -> None:
        """Create or retrieve configurations.

        If called with no parameters, Config attempts to read from the file specified by the environment variable TIMSERIES_CONFIG. If that does not succeed, applies defaults.

        Args:
            configuration_file (str): Tries to read this before falling back to environment variable. Defaults to "".
            kwargs (str):  Configuration options:

        Kwargs:
            - bucket              - The "production bucket" location. Sharing and snapshots typically go in the sub directories hee, depending on configs.
            - product             - Optional sub directory for "production bucket".
            - timeseries_root     - Series data are stored in tree underneath. Defaults to '$HOME/series_data/'
            - log_file            - Exactly that. Defaults to '$HOME/series_data/'
        """
        if fs.exists(configuration_file):
            self.load(configuration_file)
            self.configuration_file = configuration_file
            os.environ["TIMESERIES_CONFIG"] = configuration_file
        elif configuration_file:
            if fs.exists(TIMESERIES_CONFIG):
                self.load(TIMESERIES_CONFIG)
                self.save(configuration_file)
            else:
                self.__set_default_config()

        elif fs.exists(TIMESERIES_CONFIG):
            self.load(TIMESERIES_CONFIG)
            self.configuration_file = TIMESERIES_CONFIG

        if kwargs:
            log_file = kwargs.get("log_file", "")
            if log_file:
                self.log_file = log_file
            elif not self.log_file:
                self.log_file = DEFAULT_LOG_FILE_LOCATION

            timeseries_root = kwargs.get("timeseries_root", "")
            if timeseries_root:
                self.timeseries_root = timeseries_root
            elif not self.timeseries_root:
                self.timeseries_root = DEFAULT_TIMESERIES_LOCATION

            bucket = kwargs.get("bucket", "")
            if bucket:
                self.bucket = bucket
            elif not self.bucket:
                self.bucket = DEFAULT_BUCKET

            product = kwargs.get("product", "")
            if product:
                self.product = product

        if not hasattr(self, "log_file"):
            self.__set_default_config()

        self.save()

    @property
    def file_system_type(self) -> str:
        """Returns 'gcs' if Config.timeseries_root is on Google Cloud Storage,  otherwise'local'."""
        if self.timeseries_root.startswith("gs://"):
            return "gcs"
        else:
            return "local"

    def toJSON(self) -> str:
        """Return timeseries configurations as JSON string."""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def __str__(self) -> str:
        """Human readable string representation of configuration object: JSON string."""
        return self.toJSON()

    def load(self, path: PathStr) -> None:
        """Read the properties from a JSON file into a Config object."""
        if path:
            read_from_file = json.loads(fs.read_json(path))

            self.bucket = read_from_file.get("bucket")
            self.timeseries_root = read_from_file.get("timeseries_root")
            self.product = read_from_file.get("product", "")
            self.log_file = read_from_file.get("log_file", "")
        else:
            raise ValueError("Config.load(<path>) was called with an empty path.")

    def save(self, path: PathStr = TIMESERIES_CONFIG) -> None:
        """Saves configurations to JSON file and set environment variable TIMESERIES_CONFIG to the location of the file.

        Args:
            path (PathStr): Full path of the JSON file to save to. Defaults to the value of the environment variable TIMESERIES_CONFIG.
        """
        fs.write_json(content=self.toJSON(), path=path)
        if HOME == JOVYAN:
            # For some reason `os.environ["TIMESERIES_CONFIG"] = path` does not work:
            cmd = f"export TIMESERIES_CONFIG={TIMESERIES_CONFIG}"
            os.system(cmd)
            # os.system(f"echo '{cmd}' >> ~/.bashrc")
        else:
            os.environ["TIMESERIES_CONFIG"] = path

    def __set_default_config(self) -> None:
        self.bucket = DEFAULT_BUCKET
        self.configuration_file = DEFAULT_CONFIG_LOCATION
        self.log_file = DEFAULT_LOG_FILE_LOCATION
        self.product = ""
        self.timeseries_root = DEFAULT_TIMESERIES_LOCATION
        fs.touch(self.log_file)


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
    # for a, arg in enumerate(sys.argv[1:]):
    #    print(f"{a} - {arg}")

    TIMESERIES_CONFIG = os.getenv("TIMESERIES_CONFIG", DEFAULT_CONFIG_LOCATION)
    if not TIMESERIES_CONFIG:
        print(
            "Environvent variable TIMESERIES_CONFIG is empty. Using default: {DEFAULT_CONFIG_LOCATION}."
        )
        os.environ["TIMESERIES_CONFIG"] = DEFAULT_CONFIG_LOCATION
        TIMESERIES_CONFIG = DEFAULT_CONFIG_LOCATION

    if args:
        named_config = args[0]
    else:
        named_config = sys.argv[1]

    print(
        f"Update configuration file TIMESERIES_CONFIG: {TIMESERIES_CONFIG}, with named presets: '{named_config}'."
    )
    match named_config:
        case "home":
            bucket = HOME
            timeseries_root = os.path.join(HOME, "series_data")
            log_file = DEFAULT_LOG_FILE_LOCATION
        case "gcs":
            bucket = GCS
            timeseries_root = os.path.join(GCS, "series_data")
            log_file = os.path.join(HOME, "logs", "timeseries.log")
        case "jovyan":
            bucket = JOVYAN
            timeseries_root = os.path.join(JOVYAN, "series_data")
            log_file = os.path.join(JOVYAN, "logs", "timeseries.log")
        case _:
            raise ValueError(
                f"Unrecognised named configuration preset '{named_config}'."
            )

    cfg = Config(
        configuration_file=TIMESERIES_CONFIG,
        bucket=bucket,
        timeseries_root=timeseries_root,
        log_file=log_file,
    )
    cfg.save(TIMESERIES_CONFIG)
    print(cfg)
    print(os.getenv("TIMESERIES_CONFIG"))


if __name__ == "__main__":
    # Execute when called directly, ie not via import statements.
    # ??? `poetry run timeseries-config <option>` does not appear to go this route.
    print(f"Name of the script      : {sys.argv[0]=}")
    print(f"Arguments of the script : {sys.argv[1:]=}")
    main(sys.argv[1])
