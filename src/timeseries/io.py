import os
import json
import pandas

from timeseries.logging import ts_logger as log
from timeseries import properties
from timeseries import dates
import datetime


TIMESERIES_ROOT = "/home/bernhard/code/arkitektur-poc-tidsserier/sample-data"


class DatasetDirectory:
    def __init__(
        self,
        set_name: str,
        set_type: properties.SeriesType,
        as_of_utc: datetime = dates.now_utc(),
    ) -> None:
        self.set_name: str = set_name
        self.type_path: str = self.datatype_path(set_type)
        self.as_of_utc: datetime = as_of_utc.isoformat()
        self.data_dir: str = f"{self.type_path}/{set_name}"
        self.data_file: str = self.__datafile_name(set_type)
        self.data_fullpath: str = f"{self.data_dir}/{self.data_file}"
        self.metadata_dir: str = f"{self.type_path}/{set_name}"
        self.metadata_file: str = self.__metafile_name()
        self.metadata_fullpath: str = f"{self.metadata_dir}/{self.metadata_file}"

    def datatype_path(self, set_type) -> str:
        return f"{TIMESERIES_ROOT}/{set_type.versioning}_{set_type.temporality}"

    def __datafile_name(self, type) -> str:
        match type:
            case ["estimate", "AS_OF"]:
                return f"{self.set_name}-v-{self.as_of_utc}-data.parquet"
            case _:
                return f"{self.set_name}-latest-data.parquet"

    def __metafile_name(self) -> str:
        return f"{self.set_name}-metadata.json"

    def makedirs(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)

    def read_data(self, init: bool = False) -> pandas.DataFrame:
        if os.path.isfile(self.data_fullpath):
            log.debug(
                f"DATASET {self.set_name}: Reading data from {self.data_fullpath}"
            )
            try:
                df = pandas.read_parquet(self.data_fullpath)
            except FileNotFoundError:
                log.debug(
                    f"Read data {self.set_name}: Data file not found: {self.data_fullpath}"
                )
                df = pandas.DataFrame()

        else:
            df = pandas.DataFrame()

        return df

    def write_data(self, data: pandas.DataFrame):
        log.info(f"DATASET {self.set_name}: write data to file {self.data_fullpath}.")
        os.makedirs(self.data_dir, exist_ok=True)
        try:
            log.debug(
                f"DATASET {self.set_name}: Writing data from {self.data_fullpath}"
            )
            log.debug(data)
            data.to_parquet(self.data_fullpath)
        except Exception as e:
            log.exception(
                f"DATASET {self.set_name}: writing to {self.data_fullpath} returned an exception: {e}"
            )
        log.info(f"DATASET {self.set_name}: writing data to file {self.data_fullpath}.")

        # TO DO: test for polars / do we need typed argument?
        # pq.write_table(table, 'file_name.parquet', compression='BROTLI')

    def read_metadata(self) -> dict:
        meta: dict = {"name": self.set_name}
        if os.path.isfile(self.metadata_fullpath):
            with open(self.metadata_fullpath, "r") as file:
                meta = json.load(file)
                log.info(
                    f"DATASET {self.set_name}: Reading metadata from file {self.metadata_fullpath} SUCCEDED."
                )
        log.debug(meta)
        return meta

    def write_metadata(self, meta) -> None:
        os.makedirs(self.metadata_dir, exist_ok=True)
        try:
            with open(self.metadata_fullpath, "w") as file:
                log.debug(meta)
                json.dump(meta, file, indent=4, ensure_ascii=False)
                log.info(
                    f"DATASET {self.set_name}: writing metadata to file {self.metadata_fullpath} SUCCEEDED."
                )
        except Exception as e:
            log.exception(
                f"DATASET {self.set_name}: writing metadata to file {self.metadata_fullpath} FAILED. {e}"
            )

    def datafile_exists(self) -> bool:
        return os.path.isfile(self.data_fullpath)

    def metadatafile_exists(self) -> bool:
        return os.path.isfile(self.metadata_fullpath)

    def save(self, data: pandas.DataFrame, meta: dict) -> None:
        if meta:
            self.write_metadata(meta)
        else:
            log.warning(
                f"DATASET {self.set_name}: Metadata is empty. Nothing to write."
            )

        if not data.empty:
            self.write_data(data)
        else:
            log.warning(f"DATASET {self.set_name}: Data is empty. Nothing to write.")

    def purge(self):
        # method added to make testing easier, remove for "real" library?
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
