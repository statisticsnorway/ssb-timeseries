import os


class Config:
    shared = "gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier"
    jovyan = "/home/jovyan"

    def __init__(self, root: str, product: str = "", dir: str = "series_data") -> None:

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
                root = os.environ["TIMESERIES_ROOT"]
            case _:
                pass

        self.bucket = root

        if product:
            # not sure this is a good idea
            self.timeseries_root = os.path.join(self.bucket, product, dir)
        else:
            self.timeseries_root = os.path.join(self.bucket, dir)

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

    def file_system_type(self) -> str:
        """Returns 'gcs' if Config.bucket is on Google Cloud Storage,  otherwise'local'."""
        if self.bucket.startswith("gs://"):
            return "gcs"
        else:
            return "local"
