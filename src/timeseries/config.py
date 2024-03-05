# from dataclasses import dataclass
import sys
import os
import json

from timeseries import fs

GCS = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier"
JOVYAN = "/home/jovyan"
HOME: str = os.getenv("HOME")

DEFAULT_TIMESERIES_LOCATION = os.path.join(HOME, "series_data")
DEFAULT_CONFIG_LOCATION = os.path.join(HOME, "timeseries_config.json")
DEFAULT_LOG_FILE_LOCATION: str = os.path.join(HOME, "logs", "timeseries.log")

TIMESERIES_CONFIG: str = os.getenv("TIMESERIES_CONFIG", DEFAULT_CONFIG_LOCATION)


# @dataclass
# class Cfg:
#     name: str = "my-timeseries-config"
#     bucket: str = HOME
#     timeseries_root: str = ""
#     log_file: str = DEFAULT_LOG_FILE_LOCATION
#     configuration_file: str = DEFAULT_CONFIG_LOCATION


class Config:
    def __init__(self, configuration_file: str = "", **kwargs) -> None:

        self.log_file = kwargs.get(
            "log_file", os.path.join(HOME, "logs", "timeseries.log")
        )
        self.bucket = kwargs.get("bucket", HOME)
        self.timeseries_root = kwargs.get(
            "timeseries_root", os.path.join(self.bucket, "series_data")
        )
        self.product = kwargs.get("product", "")

        if fs.exists(configuration_file) and not kwargs:
            """If only the configuration_file is specified and exists, load it."""
            self.load(configuration_file)
            self.configuration_file = configuration_file

            self.save(configuration_file)
            os.environ["TIMESERIES_CONFIG"] = configuration_file
        else:
            pass

            self.save(TIMESERIES_CONFIG)
            os.environ["TIMESERIES_CONFIG"] = TIMESERIES_CONFIG

    @property
    def file_system_type(self) -> str:
        """Returns 'gcs' if Config.timeseries_root is on Google Cloud Storage,  otherwise'local'."""
        if self.timeseries_root.startswith("gs://"):
            return "gcs"
        else:
            return "local"

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    # @classmethod
    # def jovyan(cls):
    #     return cls(
    #         bucket="/home/jovyan",
    #         timeseries_root="/home/jovyan/series_data",
    #         log_file="/home/jovyan/logs/timeseries.log",
    #     )

    # @classmethod
    # def home(cls):
    #     if TIMESERIES_CONFIG:
    #         cfg = TIMESERIES_CONFIG
    #     else:
    #         cfg = os.path.join(HOME, ".configs", "timeseries", "config.json")

    #     return cls(
    #         timeseries_root=os.path.join(HOME, "series_data"), configuration_file=cfg
    #     )

    @classmethod
    def load(cls, path=TIMESERIES_CONFIG):
        from_file = json.loads(fs.read_json(path))
        # print(f"{path} red as {type(from_file)} \n{from_file}")
        return cls(
            timeseries_root=from_file.get("timeseries_root"),
            product=from_file.get("product", ""),
            log_file=from_file.get("log_file", ""),
        )

    def save(self, path=TIMESERIES_CONFIG):
        fs.write_json(content=self.toJSON(), path=path)
        os.environ["TIMESERIES_CONFIG"] = path


# def init():
#     pass


def main(*args):
    print(f"timeseries.config executed as main! {sys.argv[0]}")
    # for a, arg in enumerate(sys.argv[1:]):
    #    print(f"{a} - {arg}")

    option = sys.argv[1]
    print(option)
    match option:
        case "home":
            cfg = Config(
                bucket=HOME,
            )
        case "gcs":
            cfg = Config(
                bucket=GCS,
            )
        case "jovyan":
            cfg = Config(
                bucket=JOVYAN,
                timeseries_root=os.path.join(JOVYAN, "series_data"),
                log_file=os.path.join(JOVYAN, "logs", "timeseries.log"),
            )
    cfg.save()
    print(cfg.toJSON())


if __name__ == "__main__":
    # Execute when the module is not initialized from an import statement.
    print(f"Name of the script      : {sys.argv[0]=}")
    print(f"Arguments of the script : {sys.argv[1:]=}")

    main(sys.argv)
