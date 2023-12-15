import os
import json
import pandas
from timeseries import dates

from timeseries.logging import ts_logger
from timeseries import properties
from timeseries.dates import Interval, date_round
import datetime


TIMESERIES_ROOT: str = os.environ.get("TIMESERIES_ROOT", "/home/jovyan/series")


class DatasetDirectory:
    def __init__(
        self,
        set_name: str,
        set_type: properties.SeriesType,
        as_of_utc: datetime,
    ) -> None:
        self.set_name = set_name
        self.data_type = set_type

        if as_of_utc is None:
            pass
            # ecxception if not
        else:
            rounded_utc = as_of_utc
            self.as_of_utc: datetime = rounded_utc.isoformat()

    @property
    def root(self) -> str:
        return TIMESERIES_ROOT

    @property
    def set_type_dir(self) -> str:
        return f"{self.data_type.versioning}_{self.data_type.temporality}"

    @property
    def type_path(self) -> str:
        return os.path.join(TIMESERIES_ROOT, self.set_type_dir)

    @property
    def metadata_file(self) -> str:
        return f"{self.set_name}-metadata.json"

    @property
    def data_file(self) -> str:
        # def datafile_name(self) -> str:
        if "AS_OF" in self.set_type_dir:
            file_name = f"{self.set_name}-as_of_{self.as_of_utc}-data.parquet"
        elif "NONE" in self.set_type_dir:
            file_name = f"{self.set_name}-latest-data.parquet"
        elif "NAMED" in self.set_type_dir:
            file_name = f"{self.set_name}-NAMED-data.parquet"
        else:
            raise Exception("Unhandled versioning.")

        ts_logger.debug(file_name)
        return file_name

    @property
    def data_dir(self) -> str:
        return os.path.join(self.type_path, self.set_name)

    @property
    def data_fullpath(self) -> str:
        return os.path.join(self.data_dir, self.data_file)

    @property
    def metadata_dir(self) -> str:
        return os.path.join(self.type_path, self.set_name)

    @property
    def metadata_fullpath(self) -> str:
        return os.path.join(self.metadata_dir, self.metadata_file)

    def makedirs(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)

    def read_data(self, interval: Interval = Interval.all) -> pandas.DataFrame:
        ts_logger.debug(interval)
        if os.path.isfile(self.data_fullpath):
            ts_logger.debug(
                f"DATASET {self.set_name}: Reading data from file {self.data_fullpath}"
            )
            try:
                df = pandas.read_parquet(self.data_fullpath)
                ts_logger.info(f"DATASET {self.set_name}: Read data.")
            except FileNotFoundError:
                ts_logger.exception(
                    f"DATASET {self.set_name}: Read data failed. File not found: {self.data_fullpath}"
                )
                df = pandas.DataFrame()

        else:
            df = pandas.DataFrame()

        ts_logger.debug(f"DATASET {self.set_name}: read data:\n{df}")
        return df

    def write_data(self, data: pandas.DataFrame):
        ts_logger.info(
            f"DATASET {self.set_name}: write data to file {self.data_fullpath}."
        )
        os.makedirs(self.data_dir, exist_ok=True)
        try:
            ts_logger.debug(data)
            data.to_parquet(self.data_fullpath)
        except Exception as e:
            ts_logger.exception(
                f"DATASET {self.set_name}: writing data to {self.data_fullpath} returned exception: {e}."
            )
        ts_logger.info(
            f"DATASET {self.set_name}: writing data to file {self.data_fullpath}."
        )

        # TO DO: test for polars / do we need typed argument?
        # pq.write_table(table, 'file_name.parquet', compression='BROTLI')

    def read_metadata(self) -> dict:
        meta: dict = {"name": self.set_name}
        if os.path.isfile(self.metadata_fullpath):
            ts_logger.info(
                f"DATASET {self.set_name}: START: Reading metadata from file {self.metadata_fullpath}."
            )
            with open(self.metadata_fullpath, "r") as file:
                meta = json.load(file)
        return meta

    def write_metadata(self, meta) -> None:
        os.makedirs(self.metadata_dir, exist_ok=True)
        try:
            ts_logger.info(
                f"DATASET {self.set_name}: Writing metadata to file {self.metadata_fullpath}."
            )
            with open(self.metadata_fullpath, "w") as file:
                ts_logger.debug(meta)
                json.dump(meta, file, indent=4, ensure_ascii=False)
        except Exception as e:
            ts_logger.exception(
                f"DATASET {self.set_name}: ERROR: Writing metadata to file {self.metadata_fullpath} returned exception {e}."
            )

    def datafile_exists(self) -> bool:
        return os.path.isfile(self.data_fullpath)

    def metadatafile_exists(self) -> bool:
        return os.path.isfile(self.metadata_fullpath)

    def save(self, meta: dict, data: pandas.DataFrame = None) -> None:
        if meta:
            self.write_metadata(meta)
        else:
            ts_logger.warning(
                f"DATASET {self.set_name}: Metadata is empty. Nothing to write."
            )

        if not data.empty:
            self.write_data(data)
        else:
            ts_logger.warning(
                f"DATASET {self.set_name}: Data is empty. Nothing to write."
            )

    def purge(self):
        # method added to make early testing easier, remove for a "real" library?
        # # in case datadir == metadatadir, remove both data and metadata files first
        if os.path.isfile(self.data_fullpath):
            os.remove(self.data_fullpath)

        if os.path.isfile(self.metadata_fullpath):
            os.remove(self.metadata_fullpath)

        # remove datadir and metadatadir
        if os.path.isdir(self.data_dir):
            os.removedirs(self.data_dir)

        if os.path.isdir(self.metadata_dir):
            os.removedirs(self.metadata_dir)

    def find(self, pattern="*", *args, **kwargs):
        dir = [d[1] for d in os.walk(f"{TIMESERIES_ROOT}")]

        from glob import glob

        # dir = glob(f"{TIMESERIES_ROOT}/NONE_AT/*/")
        dir = []
        with os.scandir(TIMESERIES_ROOT) as root:
            for entry in root:
                if not entry.name.startswith(".") and entry.is_file():
                    dir.append(entry.name)

        ts_logger.debug(f"{dir}")
        return os.scandir(TIMESERIES_ROOT)


def validate_date_str(d: datetime) -> str:
    if d is None:
        # should not ever get here?
        rounded_d = "LATEST"
    else:
        rounded_d = date_round(d).isoformat()
    return rounded_d
