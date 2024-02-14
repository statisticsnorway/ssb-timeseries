from abc import ABC, abstractmethod
import os
from dapla import FileClient
import pyarrow
import shutil
import glob
import json
import pandas
import datetime
import re
import contextlib


from timeseries import properties
from timeseries.dates import Interval, date_round, utc_iso
from timeseries.config import Config
from timeseries.logging import ts_logger


BUCKET: str = os.environ.get("BUCKET")
CONFIG = Config()


class FileSystem(ABC):
    def __init__(
        self,
        set_name: str,
        set_type: properties.SeriesType,
        as_of_utc: datetime,
        process_stage: str = "statistikk",
        sharing: dict = {},
    ) -> None:
        self.set_name = set_name
        self.data_type = set_type
        self.process_stage = process_stage
        self.sharing = sharing

        if as_of_utc is None:
            pass
            # ecxception if not
        else:
            rounded_utc = as_of_utc
            self.as_of_utc: datetime = rounded_utc.isoformat()

    def __new__(
        cls,
        set_name: str,
        set_type: properties.SeriesType,
        as_of_utc: datetime,
        process_stage: str = "statistikk",
        sharing: dict = {},
        type_name="local",
        *args,
        **kwargs,
    ):

        subclass_map = {
            subclass.type_name: subclass for subclass in cls.__subclasses__()
        }
        subclass = subclass_map[type_name]
        instance = super(FileSystem, subclass).__new__(subclass)
        instance.init_fs()
        return instance

    @property
    def root(self) -> str:
        # ts_root = os.environ["TIMESERIES_ROOT"]
        ts_root = CONFIG.timeseries_root
        ts_logger.warning(f"io.FileSystem.root:{ts_root}")
        return ts_root

    @property
    def set_type_dir(self) -> str:
        return f"{self.data_type.versioning}_{self.data_type.temporality}"

    @property
    def type_path(self) -> str:
        return os.path.join(self.root, self.set_type_dir)

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

    @abstractmethod
    def makedirs(self) -> None:
        pass

    @abstractmethod
    def read_data(
        self, interval: Interval = Interval.all, *args, **kwargs
    ) -> pandas.DataFrame:
        pass

    @abstractmethod
    def write_data(self, new: pandas.DataFrame):
        pass

    @abstractmethod
    def read_metadata(self) -> dict:
        pass

    @abstractmethod
    def write_metadata(self, meta) -> None:
        pass

    @abstractmethod
    def datafile_exists(self) -> bool:
        pass

    @abstractmethod
    def metadatafile_exists(self) -> bool:
        pass

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

    # def purge(self):
    #     # method added to make early testing easier, remove for a "real" library?
    #     # # in case datadir == metadatadir, remove both data and metadata files first
    #     if os.path.isfile(self.data_fullpath):
    #         os.remove(self.data_fullpath)

    #     if os.path.isfile(self.metadata_fullpath):
    #         os.remove(self.metadata_fullpath)

    #     # remove datadir and metadatadir
    #     if os.path.isdir(self.data_dir):
    #         os.removedirs(self.data_dir)

    #     if os.path.isdir(self.metadata_dir):
    #         os.removedirs(self.metadata_dir)

    @abstractmethod
    def last_version(self, dir: str, pattern: str = "*.parquet") -> str:
        # naive "version" check - simply use number of files
        # --> TO DO: use substring
        pass

    # def snapshot_directory(self, *args, **kwargs):
    @abstractmethod
    def snapshot_directory(self, product, process_stage: str = "statistikk"):
        # The "fixed" relationship between TEAM and STATISTICS PRODUCT in the Dapla
        # naming standard does not seem entirely "right". Not only does it force tech solution
        # to match org structure, but also creates tigther couplings betweeen otherwise unrelated code
        # as information about data content must be passed around.

        # --> Here: product as CONSTANT or PARAMETER?
        # def snapshot_directory(self, product, process_stage):
        # ... or access parent object using weakref?
        pass

    @abstractmethod
    def snapshot_filename(
        self,
        process_stage: str = "statistikk",
        as_of_utc=None,
        period_from: str = "",
        period_to: str = "",
    ) -> str:
        pass

    @abstractmethod
    def sharing_directory(
        self,
        product: str,
        team: str = "",
        bucket: str = CONFIG.bucket,
    ):
        pass

    @abstractmethod
    def snapshot(
        self, stage, sharing={}, as_of_tz=None, period_from=None, period_to=None
    ):
        """Copies snapshots to bucket(s) according to processing stage and sharing configuration.

        For this to work, .stage and sharing configurations should be set for the dataset, eg:
            .sharing = [{'team': 's123', 'path': '<s1234-bucket>'},
                        {'team': 's234', 'path': '<s234-bucket>'},
                        {'team': 's345': 'path': '<s345-bucket>'}]
            .stage = 'statistikk'
        """
        pass

    @abstractmethod
    def search(self, pattern="", *args, **kwargs):
        pass


class Local(FileSystem):
    type_name = "local"

    def init_fs(self) -> None:
        pass

    def makedirs(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)

    def read_data(
        self, interval: Interval = Interval.all, *args, **kwargs
    ) -> pandas.DataFrame:
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

    def write_data(self, new: pandas.DataFrame):
        ts_logger.info(
            f"DATASET {self.set_name}: write data to file {self.data_fullpath}."
        )
        os.makedirs(self.data_dir, exist_ok=True)
        if self.data_type.versioning == properties.Versioning.AS_OF:
            df = new
        else:
            old = self.read_data(self.set_name)
            if old.empty:
                df = new
            else:
                date_cols = list(
                    set(new.columns)
                    & set(old.columns)
                    & {"valid_at", "valid_from", "valid_to"}
                )
                df = pandas.concat(
                    [old, new],
                    axis=0,
                    ignore_index=True,
                ).drop_duplicates(date_cols, keep="last")

        try:
            ts_logger.debug(df)
            df.to_parquet(self.data_fullpath)
        except Exception as e:
            ts_logger.exception(
                f"DATASET {self.set_name}: writing data to {self.data_fullpath} returned exception: {e}."
            )
        ts_logger.info(
            f"DATASET {self.set_name}: writing data to file {self.data_fullpath}."
        )

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

    def last_version(self, dir: str, pattern: str = "*.parquet") -> str:
        # naive "version" check - simply use number of files
        # --> TO DO: use substring

        files = glob.glob(os.path.join(dir, pattern))
        number_of_files = len(files)

        vs = [int(re.search("(_v)([0-9]+)(.parquet)", f).group(2)) for f in files]
        ts_logger.warning(
            f"DATASET {self.set_name}: io.last_version regex identified versions {vs}."
        )
        if vs:
            read_from_filenames = max(vs)
            out = read_from_filenames
        else:
            read_from_filenames = 0
            out = number_of_files

        ts_logger.debug(
            f"DATASET {self.set_name}: io.last_version searched directory: \n\t{dir}\n\tfor '{pattern}' found {str(number_of_files)} files, regex identified version {str(read_from_filenames)} --> vs {str(out)}."
        )
        return out

    def snapshot_directory(self, product, process_stage: str = "statistikk"):
        return self.dir(
            CONFIG.bucket,
            product,
            process_stage,
            "series",  # to distinguish from other data types
            self.set_type_dir,
            self.set_name,
        )

        # dir = os.path.join(
        #     CONFIG.bucket,
        #     product,
        #     process_stage,
        #     "series",  # to distinguish from other data types
        #     self.set_type_dir,
        #     self.set_name,
        # )
        # ts_logger.debug(f"DATASET.IO.SNAPSHOT_DIRECTORY: {dir}")
        # os.makedirs(dir, exist_ok=True)
        # return dir

    def snapshot_filename(
        self,
        product: str,
        process_stage: str,
        as_of_utc=None,
        period_from: str = "",
        period_to: str = "",
    ) -> str:
        dir = self.snapshot_directory(product=product, process_stage=process_stage)

        last_vs = self.last_version(dir=dir, pattern="*.parquet")
        if as_of_utc:
            out = f"{self.set_name}_p{utc_iso(period_from)}_p{utc_iso(period_to)}_v{utc_iso(as_of_utc)}_v{last_vs+1}"
        else:
            out = f"{self.set_name}_p{utc_iso(period_from)}_p{utc_iso(period_to)}_v{last_vs+1}"

            # ouch! - to comply with the naming standard we need to know more about the data
            # than seems right for this module (tight coupling):
            ts_logger.warning(
                f"DATASET last version {last_vs+1} from {period_from} to {period_to}.')"
            )
        return out
        # return f"{self.set_name}_v{last_vs+1}"

    def sharing_directory(
        self,
        team: str,
        bucket: str,
    ):
        # if team:
        #     dir = os.path.join(bucket, team, self.set_name)
        # else:
        #     dir = os.path.join(bucket, self.set_name)

        dir = os.path.join(bucket, self.set_name)

        ts_logger.debug(f"DATASET.IO.SHARING_DIRECTORY: {dir}")
        os.makedirs(dir, exist_ok=True)
        return dir

    def snapshot(
        self,
        product: str,
        process_stage: str,
        sharing={},
        as_of_tz=None,
        period_from=None,
        period_to=None,
    ):
        """Copies snapshots to bucket(s) according to processing stage and sharing configuration.

        For this to work, .stage and sharing configurations should be set for the dataset, eg:
            .sharing = [{'team': 's123', 'path': '<s1234-bucket>'},
                        {'team': 's234', 'path': '<s234-bucket>'},
                        {'team': 's345': 'path': '<s345-bucket>'}]
            .stage = 'statistikk'
        """

        dir = self.snapshot_directory(product=product, process_stage=process_stage)
        snapshot_name = self.snapshot_filename(
            product=product,
            process_stage=process_stage,
            as_of_utc=as_of_tz,
            period_from=period_from,
            period_to=period_to,
        )

        data_publish_path = os.path.join(dir, f"{snapshot_name}.parquet")
        meta_publish_path = os.path.join(dir, f"{snapshot_name}.json")

        def copy(from_file, to_file):
            shutil.copy2(from_file, to_file)  # also copies file meta data
            ts_logger.debug(
                f"DATASET {self.set_name} shutil.copy2{from_file}, {to_file}')"
            )

        copy(self.data_fullpath, data_publish_path)
        copy(self.metadata_fullpath, meta_publish_path)

        if sharing:
            ts_logger.debug(f"Sharing configs: {sharing}")
            for s in sharing:
                ts_logger.debug(f"Sharing: {s}")
                copy(
                    data_publish_path,
                    self.sharing_directory(bucket=s["path"], team=s["team"]),
                )
                copy(
                    meta_publish_path,
                    self.sharing_directory(bucket=s["path"], team=s["team"]),
                )
                ts_logger.info(
                    f"DATASET {self.set_name}: share with {s['team']}, snapshot copied to {s['path']}."
                )

    def search(self, pattern="", *args, **kwargs):
        if pattern:
            pattern = f"*{pattern}*"
        else:
            pattern = "*"

        search_str = os.path.join(CONFIG.timeseries_root, "*", pattern)
        dirs = glob.glob(search_str)
        ts_logger.warning(f"DATASET.IO.SEARCH: {search_str} dirs{dirs}")
        search_results = [
            d.replace(CONFIG.timeseries_root, "root").split(os.path.sep) for d in dirs
        ]
        ts_logger.warning(f"DATASET.IO.SEARCH: search_results{search_results}")

        return [f[2] for f in search_results]

    @classmethod
    def dir(self, *args, **kwargs) -> str:
        """A convenience classmethod for os.makedirs(os.path.join(*args)).
        As long as the target is under BUCKET, it will create the target if it does not exist.
        """
        ts_logger.debug(f"{args}:")
        path = os.path.join(*args)
        # ts_root = str(self.root)
        ts_root = str(CONFIG.bucket)

        dir_is_in_series = os.path.commonpath([path, ts_root]) == ts_root
        if dir_is_in_series or kwargs.get(
            "force", False
        ):  # hidden feature: also for kwarg 'force' == True
            os.makedirs(path, exist_ok=True)
        else:
            raise DatasetIoException(
                f"Directory {path} must be below {BUCKET} in file tree."
            )

        return path


class GoogleCloudStorage(FileSystem):
    type_name = "gcs"

    def init_fs(self) -> None:
        try:
            # try: ... except: ... is a workaround for 23 warnings from dapla.FileClient.get_gcs_file_system():
            #  Implementing implicit namespace packages (as specified in PEP 420) is preferred to
            #   `pkg_resources.declare_namespace`.
            #   See https://setuptools.pypa.io/en/latest/references/keywords.html#keyword-namespace-packages
            self.gcs = FileClient.get_gcs_file_system()
        except:
            pass

    def makedirs(self) -> None:
        # not needed on GCS
        pass

    def read_data(
        self, interval: Interval = Interval.all, *args, **kwargs
    ) -> pandas.DataFrame:
        ts_logger.debug(interval)

        if self.gcs.exists(self.data_fullpath):
            ts_logger.debug(
                f"DATASET {self.set_name}: Reading data from file {self.data_fullpath}"
            )
            try:
                with self.gcs.open(self.data_fullpath, "rb"):
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

    def write_data(self, new: pandas.DataFrame):
        ts_logger.info(
            f"DATASET {self.set_name}: write data to file {self.data_fullpath}."
        )

        if self.data_type.versioning == properties.Versioning.AS_OF:
            df = new
        else:
            old = self.read_data(self.set_name)
            if old.empty:
                df = new
            else:
                date_cols = list(
                    set(new.columns)
                    & set(old.columns)
                    & {"valid_at", "valid_from", "valid_to"}
                )
                df = pandas.concat(
                    [old, new],
                    axis=0,
                    ignore_index=True,
                ).drop_duplicates(date_cols, keep="last")

        try:
            ts_logger.debug(df)
            with self.gcs.open(self.data_fullpath, "wb"):
                df.to_parquet(self.data_fullpath)
        except Exception as e:
            ts_logger.exception(
                f"DATASET {self.set_name}: writing data to {self.data_fullpath} returned exception: {e}."
            )
        ts_logger.info(
            f"DATASET {self.set_name}: writing data to file {self.data_fullpath}."
        )

    def read_metadata(self) -> dict:
        meta: dict = {"name": self.set_name}
        if self.gcs.exists(self.metadata_fullpath):
            ts_logger.info(
                f"DATASET {self.set_name}: START: Reading metadata from file {self.metadata_fullpath}."
            )
            with self.gcs.open(self.metadata_fullpath, "r") as file:
                meta = json.load(file)
        return meta

    def write_metadata(self, meta) -> None:
        try:
            ts_logger.info(
                f"DATASET {self.set_name}: Writing metadata to file {self.metadata_fullpath}."
            )
            with self.gcs.open(self.metadata_fullpath, "w") as file:
                ts_logger.debug(meta)
                json.dump(meta, file, indent=4, ensure_ascii=False)
        except Exception as e:
            ts_logger.exception(
                f"DATASET {self.set_name}: ERROR: Writing metadata to file {self.metadata_fullpath} returned exception {e}."
            )

    def datafile_exists(self) -> bool:
        return self.gcs.exists(self.data_fullpath)

    def metadatafile_exists(self) -> bool:
        return self.gcs.exists(self.metadata_fullpath)

    def purge(self):
        # method added to make early testing easier, remove for a "real" library?
        # ... not implemented for GCS
        pass

    def last_version(self, dir: str, pattern: str = "*.parquet") -> str:
        # naive "version" check - simply use number of files
        # --> TO DO: use substring

        files = self.gcs.glob(os.path.join(dir, pattern))
        number_of_files = len(files)

        vs = [int(re.search("(_v)([0-9]+)(.parquet)", f).group(2)) for f in files]
        ts_logger.warning(
            f"DATASET {self.set_name}: io.last_version regex identified versions {vs}."
        )
        if vs:
            read_from_filenames = max(vs)
            out = read_from_filenames
        else:
            read_from_filenames = 0
            out = number_of_files

        ts_logger.debug(
            f"DATASET {self.set_name}: io.last_version searched directory: \n\t{dir}\n\tfor '{pattern}' found {str(number_of_files)} files, regex identified version {str(read_from_filenames)} --> vs {str(out)}."
        )
        return out

    def snapshot_directory(self, product, process_stage: str = "statistikk"):
        return os.path.join(
            CONFIG.bucket,
            product,
            process_stage,
            "series",  # to distinguish from other data types
            self.set_type_dir,
            self.set_name,
        )

    def snapshot_filename(
        self,
        product: str,
        process_stage: str,
        as_of_utc=None,
        period_from: str = "",
        period_to: str = "",
    ) -> str:

        dir = self.snapshot_directory(product=product, process_stage=process_stage)
        last_vs = self.last_version(dir=dir, pattern="*.parquet")

        if as_of_utc:
            out = f"{self.set_name}_p{utc_iso(period_from)}_p{utc_iso(period_to)}_v{utc_iso(as_of_utc)}_v{last_vs+1}"
        else:
            out = f"{self.set_name}_p{utc_iso(period_from)}_p{utc_iso(period_to)}_v{last_vs+1}"

            # ouch! - to comply with the naming standard we need to know more about the data
            # than seems right for this module (tight coupling):
            ts_logger.warning(
                f"DATASET.IO.GCS last version {last_vs+1} from {period_from} to {period_to}.')"
            )
        return out

    def sharing_directory(
        self,
        team: str,
        bucket: str,
    ):
        # if team:
        #     dir = os.path.join(bucket, team, self.set_name)
        # else:
        #     dir = os.path.join(bucket, self.set_name)

        dir = os.path.join(bucket, self.set_name)
        ts_logger.debug(f"DATASET.IO.SHARING_DIRECTORY: {dir}")
        return dir

    def snapshot(
        self,
        product: str,
        process_stage: str,
        sharing={},
        as_of_tz=None,
        period_from=None,
        period_to=None,
    ):
        """Copies snapshots to bucket(s) according to processing stage and sharing configuration.

        For this to work, .stage and sharing configurations should be set for the dataset, eg:
            .sharing = [{'team': 's123', 'path': '<s1234-bucket>'},
                        {'team': 's234', 'path': '<s234-bucket>'},
                        {'team': 's345': 'path': '<s345-bucket>'}]
            .stage = 'statistikk'
        """

        dir = self.snapshot_directory(product=product, process_stage=process_stage)
        snapshot_name = self.snapshot_filename(
            product=product,
            process_stage=process_stage,
            as_of_utc=as_of_tz,
            period_from=period_from,
            period_to=period_to,
        )

        data_publish_path = os.path.join(dir, f"{snapshot_name}.parquet")
        meta_publish_path = os.path.join(dir, f"{snapshot_name}.json")

        def copy(from_file, to_file):
            self.gcs.copy(from_file, to_file)
            ts_logger.debug(f"DATASET {self.set_name} gcs.copy{from_file}, {to_file}')")

        copy(self.data_fullpath, data_publish_path)
        copy(self.metadata_fullpath, meta_publish_path)

        if sharing:
            ts_logger.debug(f"Sharing configs: {sharing}")
            for s in sharing:
                ts_logger.debug(f"Sharing: {s}")
                copy(
                    data_publish_path,
                    self.sharing_directory(bucket=s["path"], team=s["team"]),
                )
                copy(
                    meta_publish_path,
                    self.sharing_directory(bucket=s["path"], team=s["team"]),
                )
                ts_logger.info(
                    f"DATASET {self.set_name}: share with {s['team']}, snapshot copied to {s['path']}."
                )

    def search(self, pattern="", *args, **kwargs):
        if pattern:
            pattern = f"*{pattern}*"
        else:
            pattern = "*"

        search_str = os.path.join(CONFIG.timeseries_root, "*", pattern)
        dirs = self.gcs.glob(search_str)
        ts_logger.warning(f"DATASET.IO.SEARCH: {search_str} dirs{dirs}")
        search_results = [
            d.replace(CONFIG.timeseries_root, "root").split(os.path.sep) for d in dirs
        ]
        ts_logger.warning(f"DATASET.IO.SEARCH: search_results{search_results}")

        return [f[2] for f in search_results]

    @classmethod
    def dir(self, *args, **kwargs) -> str:
        """For GCS this method simply returns os.path.join(*args). This is required for consistency across file systems; for local file systems
        it is a convenience classmethod for os.makedirs(os.path.join(*args)).
        """
        ts_logger.debug(f"{args}:")
        path = os.path.join(*args)
        # ts_root = str(self.root)
        ts_root = str(CONFIG.bucket)

        dir_is_in_series = os.path.commonpath([path, ts_root]) == ts_root
        if dir_is_in_series:
            return path
        else:
            raise DatasetIoException(
                f"Directory {path} must be below {BUCKET} in file tree."
            )


# def validate_date_str(d: datetime) -> str:
#     if d is None:
#         # should not ever get here?
#         rounded_d = "LATEST"
#     else:
#         rounded_d = date_round(d).isoformat()
#     return rounded_d


@contextlib.contextmanager
def cd(path):
    """Temporary cd into a directory (create if not exists), like so:

    with cd(path):
        do_stuff()

    """
    CWD = os.getcwd()

    try:
        if os.path.isdir(path):
            os.chdir(path)
        else:
            os.makedirs(path, exist_ok=True)
            os.chdir(path)

        yield
    finally:
        os.chdir(CWD)


def init_root(
    path,
    products: list[str] = [],
    create_log_and_shared: bool = False,
    create_product_dirs: bool = False,
    create_all: bool = False,
):
    """init_root

    Args:
        path (str):
            Absolute or relative path to the top level root directory.
            It will be created if it does not exist, as will a 'series' inside it,
            and the TIMESERIES_ROOT env variable will be set to point to it.
        products list(str):
            If set, allows
        as_production_bucket (bool, optional): Create directory structure as if this was a production bucket. Defaults to False.

    """

    with cd(path):
        os.environ["BUCKET"] = os.getcwd()

        root = os.path.join(os.getcwd(), "series")
        os.makedirs(root, exist_ok=True)
        os.environ["TIMESERIES_ROOT"] = root

        if create_all:
            create_log_and_shared = True
            create_product_dirs = True

        if create_log_and_shared:
            os.makedirs("shared", exist_ok=True)
            os.makedirs("logs", exist_ok=True)

        if create_product_dirs and products:
            for p in products:
                with cd(p):
                    os.makedirs("inndata", exist_ok=True)
                    os.makedirs("klargjorte-data", exist_ok=True)
                    os.makedirs("statistikk", exist_ok=True)
                    os.makedirs("utdata", exist_ok=True)


class DatasetIoException(Exception):
    pass
