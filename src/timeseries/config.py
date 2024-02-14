import os

DEFAULT = os.getcwd()


class Config:
    shared = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier"
    jovyan = "/home/jovyan"

    def __init__(
        self, root: str = "", product: str = "", dir: str = "series_data"
    ) -> None:

        self.product = product

        match root.lower():
            case "pwd" | "cwd":
                root = os.getcwd()
            case "jovyan":
                root = self.jovyan
            case "shared":
                root = self.shared
            case "home" | "~":
                root = "~"
            case "":
                from_env_variable = os.environ["BUCKET"]
                if from_env_variable:
                    root = from_env_variable
                else:
                    root = DEFAULT
            case _:
                pass

        self.bucket = root

        if product:
            # not sure this is a good idea
            self.timeseries_root = os.path.join(self.bucket, product, dir)
            self.log_location = os.path.join(self.bucket, product, "logs")
        else:
            self.timeseries_root = os.path.join(self.bucket, dir)
            self.log_location = os.path.join(self.bucket, "logs")

    def set_env(self):
        """Saves configurations into environment variables:
        BUCKET      The product bucket on GCS;  any convenient directory on a local filesystem.
        PRODUCT     Created only if the config value is not empty. (The default is empty.)
        TIMESERIES_ROOT     Must be a CATALOG within BUCKET.
        """
        os.environ["BUCKET"] = self.bucket
        if self.product:
            os.environ["PRODUCT"] = self.product
        os.environ["TIMESERIES_ROOT"] = self.timeseries_root
        os.environ["LOG_LOCATION"] = self.log_location

    def file_system_type(self) -> str:
        """Returns 'gcs' if Config.bucket is on Google Cloud Storage,  otherwise'local'."""
        if self.bucket.startswith("gs://"):
            return "gcs"
        else:
            return "local"
