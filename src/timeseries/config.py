# from dataclasses import dataclass
import sys
import os
import json

from timeseries import fs

GCS = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier"
JOVYAN = "/home/jovyan"
HOME: str = os.getenv("HOME")

DEFAULT_BUCKET = HOME
DEFAULT_TIMESERIES_LOCATION = os.path.join(HOME, "series_data")
DEFAULT_CONFIG_LOCATION = os.path.join(HOME, "timeseries_config.json")
DEFAULT_LOG_FILE_LOCATION: str = os.path.join(HOME, "logs", "timeseries.log")
TIMESERIES_CONFIG: str = os.getenv("TIMESERIES_CONFIG", DEFAULT_CONFIG_LOCATION)


class Config:
    def __init__(self, configuration_file: str = "", **kwargs) -> None:
        """Create or retrieve configurations from within production code. If called with no parameters, it first tries to read from config file as specified by environment variable TIMSERIES_CONFIG. If that does not succeed, applies defaults.

        Args:
            configuration_file (str, optional): If provided, it tries that before falling back to the environment variable. Defaults to "".
            kwargs:
                bucket              - The "production bucket" location. Sharing and snapshots typically go in the sub directories hee, depending on configs.
                prdouct             - Optional sub directory for "production bucket".
                timeseries_root     - Series data are stored in tree underneath. Defaults to '$HOME/series_data/'
                log_file            - Exactly that. Defaults to '$HOME/series_data/'
        """

        if fs.exists(configuration_file):
            """If the configuration_file is specified and exists, load it."""
            self.load(configuration_file)
            self.configuration_file = configuration_file
            os.environ["TIMESERIES_CONFIG"] = configuration_file
        elif configuration_file:
            if fs.exists(TIMESERIES_CONFIG):
                self.load(TIMESERIES_CONFIG)
                self.save(configuration_file)
            else:
                self.bucket = DEFAULT_BUCKET
                self.timeseries_root = DEFAULT_TIMESERIES_LOCATION
                self.log_file = DEFAULT_LOG_FILE_LOCATION

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

        self.save()

    @property
    def file_system_type(self) -> str:
        """Returns 'gcs' if Config.timeseries_root is on Google Cloud Storage,  otherwise'local'."""
        if self.timeseries_root.startswith("gs://"):
            return "gcs"
        else:
            return "local"

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def __str__(self) -> str:
        return self.toJSON()

    def load(self, path):
        if path:
            from_file = json.loads(fs.read_json(path))
            print(f"{path} red as {type(from_file)} \n{from_file}")

            self.bucket = from_file.get("bucket")
            self.timeseries_root = from_file.get("timeseries_root")
            self.product = from_file.get("product", "")
            self.log_file = from_file.get("log_file", "")
        else:
            print("Config.load(path) was called with an empty path.")

    def save(self, path=TIMESERIES_CONFIG):
        """Saves configurations to JSON file and set environment variable TIMESERIES_CONFIG to the location of the file.

        Args:
            path (pathlike/string, optional): Full path of the JSON file to save to. Defaults to the value of the environment variable TIMESERIES_CONFIG.
        """
        fs.write_json(content=self.toJSON(), path=path)
        if HOME == JOVYAN:
            # For some reason `os.environ["TIMESERIES_CONFIG"] = path` does not work:
            cmd = f"export TIMESERIES_CONFIG={TIMESERIES_CONFIG}"
            os.system(cmd)
            # os.system(f"echo '{cmd}' >> ~/.bashrc")
        else:
            os.environ["TIMESERIES_CONFIG"] = path


def main(*args):
    print(f"Timeseries config executed as main! {sys.argv[0]}")
    # for a, arg in enumerate(sys.argv[1:]):
    #    print(f"{a} - {arg}")

    TIMESERIES_CONFIG = os.getenv("TIMESERIES_CONFIG", DEFAULT_CONFIG_LOCATION)
    if not TIMESERIES_CONFIG:
        os.environ["TIMESERIES_CONFIG"] = DEFAULT_CONFIG_LOCATION
        TIMESERIES_CONFIG = DEFAULT_CONFIG_LOCATION

    print(f"Configuration file: {TIMESERIES_CONFIG}")

    option = sys.argv[1]
    print(f"Resetting configurations for: '{option}'.")

    match option:
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
            bucket = DEFAULT_BUCKET
            timeseries_root = DEFAULT_TIMESERIES_LOCATION
            log_file = DEFAULT_LOG_FILE_LOCATION

    cfg = Config(
        configuration_file=TIMESERIES_CONFIG,
        bucket=bucket,
        timeseries_root=timeseries_root,
        log_file=log_file,
    )
    cfg.save(TIMESERIES_CONFIG)
    print(cfg)


if __name__ == "__main__":
    # Execute when the module is not initialized from an import statement.
    print(f"Name of the script      : {sys.argv[0]=}")
    print(f"Arguments of the script : {sys.argv[1:]=}")

    main(sys.argv)
