import os
import json
import pandas

# import pyarrow
# import enum

from timeseries import logging as log, properties
from datetime import datetime


TIMESERIES_ROOT = "/home/bernhard/code/arkitektur-poc-tidsserier/sample-data"


class DatasetDirectory:
    def __init__(
        self,
        set_name: str,
        set_type: properties.SeriesType,
    ) -> None:
        self.set_name: str = set_name
        # self.set_type: properties.SeriesType = set_type
        self.type_path: str = self.datatype_path(set_type)
        self.data_dir: str = f"{self.type_path}/{set_name}"
        self.data_file: str = self.__datafile_name(set_name)
        self.data_fullpath: str = f"{self.data_dir}/{self.data_file}"
        self.metadata_dir: str = f"{self.type_path}/{set_name}"
        self.metadata_file: str = self.__metafile_name(set_name)
        self.metadata_fullpath: str = f"{self.metadata_dir}/{self.metadata_file}"

    def datatype_path(self, set_type) -> str:
        return f"{TIMESERIES_ROOT}/{set_type.versioning}_{set_type.temporality}"

    def __datafile_name(self, as_of: datetime = None) -> str:
        return f"{self.set_name}-data.parquet"

    def __metafile_name(self, as_of: datetime) -> str:
        return f"{self.set_name}-metadata.json"

    def makedirs(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)

    def read_data(self, init: bool = False) -> pandas.DataFrame:
        if os.path.isfile(self.data_fullpath):
            log.debug(
                f"Dataset {self.set_name}: Reading data from {self.data_fullpath}"
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

    def write_data(self, data: pandas.DataFrame, as_of: datetime = datetime.now()):
        # self.makedirs()
        os.makedirs(self.data_dir, exist_ok=True)
        if not data.empty:
            try:
                log.debug(
                    f"Dataset {self.set_name}: Writing data from {self.data_fullpath}"
                )
                log.debug(data)
                data.to_parquet(self.data_fullpath)
            except Exception as e:
                log.warn(
                    f"Dataset {self.set_name}: writing to {self.data_fullpath} returned an exception: {e}"
                )
        else:
            log.info(
                f"Dataset {self.set_name} is empty. Nothing to write to: {self.data_fullpath}"
            )

        # TO DO: test for polars / do we need typed argument?
        # pq.write_table(table, 'file_name.parquet', compression='BROTLI')

    def read_metadata(self) -> dict:
        meta: dict = {"name": self.set_name}
        if os.path.isfile(self.metadata_fullpath):
            log.debug(
                f"Dataset {self.set_name}: Reading metadata from {self.metadata_fullpath}"
            )
            with open(self.metadata_fullpath, "r") as file:
                meta = json.load(file)
        log.debug(meta)
        return meta
        # except FileNotFoundError:
        #    log.debug(f"Read metadata {self.set_name}: Metadata file not found: {self.metadata_fullpath}")

    def write_metadata(self, meta) -> None:
        # self.makedirs()
        os.makedirs(self.metadata_dir, exist_ok=True)
        if meta:
            try:
                with open(self.metadata_fullpath, "w") as file:
                    log.debug(meta)
                    json.dump(meta, file, indent=4, ensure_ascii=False)
            except Exception as e:
                log.debug(
                    f"Write metadata {self.set_name}: Could not write to: {self.metadata_fullpath}; {e}"
                )

    def datafile_exists(self) -> bool:
        return os.path.isfile(self.data_fullpath)

    def metadatafile_exists(self) -> bool:
        return os.path.isfile(self.metadata_fullpath)

    def save(self, data: pandas.DataFrame, meta: dict) -> None:
        # self.makedirs()
        if not data.empty:
            self.write_data(data)
        if meta:
            self.write_metadata(meta)
        # return self.datafile_exists() and self.metadatafile_exists()

    def purge(self):
        # remove both data and metadata files first, in case datadir == metadatadir
        if os.path.isfile(self.data_fullpath):
            os.remove(self.data_fullpath)

        if os.path.isfile(self.metadata_fullpath):
            os.remove(self.metadata_fullpath)

        if os.path.isdir(self.data_dir):
            os.removedirs(self.data_dir)

        if os.path.isdir(self.metadata_dir):
            os.removedirs(self.metadata_dir)
