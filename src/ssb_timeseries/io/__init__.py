"""The IO modules define the read and write functionality that the Dataset module can access.

These modules are internal service modules.
They are not supposed to be called directly from user code.
Rather, for each time series repository,
the configuration must identify the module to be used along with any required parameters.
Thus, multiple time series repositories can be configured with different storage locations and technologies.
Data and metadata are conceptually separated,
so that a metadata catalog may be maintained per repository,
or common to all repositories.

Some basic interaction patterns are built in,
but the library can easily be extended with external IO modules.
"""

from __future__ import annotations

import importlib
import os
from datetime import datetime
from datetime import timezone
from functools import cache
from typing import Any

from narwhals.typing import IntoFrame

from ..config import Config
from ..config import FileBasedRepository
from ..dataset import Dataset
from ..dates import date_utc
from ..logging import logger
from ..properties import SeriesType
from . import protocols
from . import snapshot

# mypy: disable-error-code="no-any-return,no-untyped-def,return-value,assignment,attr-defined"
DEFAULT_PROCESS_STAGE = "Statistikk"  # TODO: control from config?

# TODO: consider: _HANDLER_CACHE = {}
_ACTIVE_CONFIG = Config.active()


# @cache
def _repo_config(
    target: Any,  # str | dict[str, FileBasedRepository],
) -> FileBasedRepository:
    """Get a repository configuration dictionary by name.

    A target that is already a dictionary will simply be passed through.
    """
    if isinstance(target, str):
        config = Config.active()
        # config = _ACTIVE_CONFIG #TODO: add Config.refresh() first
        repo = config.repositories[target]
    elif isinstance(target, dict):
        repo = target
    else:
        raise LookupError(f"Repository '{target}' was not found.")
    return repo


def _io_handler(**kwargs) -> protocols.DataHandler | protocols.MetadataHandler:
    """Dynamically import and instantiate an IO handler for reading and writing data."""
    repo_cfg = _repo_config(kwargs.get("repository"))
    handler_type = kwargs.pop("handler_type")
    match handler_type.lower():
        case "data":
            handler_config = repo_cfg["directory"]
        case "metadata":
            handler_config = repo_cfg["catalog"]
        case "archive":
            handler_config = repo_cfg["directory"]
        case _:
            raise ValueError("Unhandlked handler type.")
    handler = _handler_class(handler_config["handler"])
    instance = handler(**kwargs)
    return instance


def _handler_class(handler_name: str) -> type:
    """Dynamically imports and instantiates a handler from the config."""
    config = Config.active()
    # config = _ACTIVE_CONFIG  #TODO: add Config.refresh() first
    handler_conf = config.io_handlers[handler_name]
    handler_path = handler_conf["handler"]

    module_path, class_name = handler_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    handler_class = getattr(module, class_name)

    return handler_class


class DataIO:
    """Generic IO for data of a specific dataset."""

    def __init__(
        self,
        ds: Dataset,
    ) -> None:
        """Retrieve configuration and initiate data IO handler for Dataset."""
        self.ds = ds

    @property
    def dh(self) -> protocols.DataHandler:
        """Expose the IO handler."""
        return _io_handler(
            handler_type="data",
            repository=self.ds.repository,
            set_name=self.ds.name,
            set_type=self.ds.data_type,
            as_of_utc=date_utc(self.ds.as_of_utc),
        )


class MetaIO:
    """Generic IO for metadata of a specific dataset."""

    def __init__(
        self,
        ds: Dataset,
    ) -> None:
        """Retrieve configuration and initiate metadata IO handler for Dataset."""
        self.ds = ds

    @property
    def dh(self) -> protocols.MetadataHandler:
        """Expose the IO handler."""
        return _io_handler(
            handler_type="metadata",
            repository=self.ds.repository,
            set_name=self.ds.name,
        )


def save(ds: Dataset) -> None:
    """Write data and metadata using configured IO handlers."""
    DataIO(ds).dh.write(data=ds.data, tags=ds.tags)
    MetaIO(ds).dh.write(meta=ds.tags)


def read_metadata(
    repository: str | dict,
    set_name: str,
) -> dict:
    """Read metadata dict with configured IO Handlers."""
    # if "catalog" in repo_cfg:
    meta_io = _io_handler(
        handler_type="metadata",
        repository=repository,
        set_name=set_name,
    )
    if meta_io:
        return meta_io.read()
    else:
        return {}


def read_data(
    repository: str | dict,
    set_name: str,
    as_of_tz: datetime | None = None,
) -> IntoFrame:
    """Read data into >Arrow Table with configured IO Handlers."""
    # repo_config = _repo_config(repository)
    tags = read_metadata(repository, set_name)
    if tags:
        set_type = SeriesType(tags["versioning"], tags["temporality"])
        data_io = _io_handler(
            handler_type="data",
            repository=repository,
            set_name=set_name,
            set_type=set_type,
            as_of_utc=date_utc(as_of_tz),
        )
        data = data_io.read(repository=repository, set_name=set_name)
    else:
        raise LookupError(f"Could not find Dataset('{set_name}') in {repository=}.")

    return data


def find(
    set_name: str = "",
    repository: str | dict = "",
    require_one: bool = False,
    require_unique: bool = False,
    **kwargs,  # unused, but simplifies passing params from Dataset.__init__
) -> list[dict] | dict:
    """Search for datasets by name matching pattern in specified or all repositories.

    Returns:
         list[io.SearchResult] | Dataset | list[None]: The dataset for a single match, a list for no or multiple matches.

    Raises:
        LookupError: If `require_unique = True` and a unique result is not found.
    """
    if repository:
        repositories = [_repo_config(repository)]
    else:
        repositories = [
            v for k, v in Config.active().repositories.items() if "catalog" in v
        ]

    result = []
    for repo in repositories:
        meta_io = _io_handler(
            handler_type="metadata",
            repository=repo,
            set_name=set_name,
        )
        if meta_io.exists:
            tags = meta_io.read()
            tags["repository"] = repo
            result.append(dict(tags))

    match (len(result), require_one, require_unique):
        case (0, False, _):
            logger.debug("IO:find - found no sets!")
            out = {}
        case (1, _, _):
            logger.debug("IO:find - found single set!")
            out = result[0]
        case (_, False, False):
            logger.debug("IO:find - found multiple sets!")
            out = result
        case (0, True, _):
            raise LookupError(
                f"No results searching for {set_name=},{repository=} when one was required."
            )
        case (_, _, True):
            raise LookupError(
                f"Too many results in search for {set_name=},{repository=}; {len(result)} sets were found when a unique result was required."
            )

    return out


def list_versions(
    ds: Dataset,
    repository: str | dict = "",
    **kwargs,
) -> list[datetime | str]:
    """Get list of all series version markers (`as_of` dates or version names)."""
    data_io = DataIO(ds)
    versions = data_io.dh.list_versions(
        file_pattern="*.parquet",
        pattern=ds.data_type.versioning,  # type: ignore[union-attr]
    )
    return versions


def persist(
    ds: Dataset,
) -> None:
    """Hardcoded with snapshot.FileSystem; note dependency on other IO for providing path(s) to write to."""
    # TODO: rewrite to use _io_handler to dynamically define IO module from config
    snapshot_config = Config.active().snapshots
    if not snapshot_config:
        return
    process_stage = getattr(ds, "process_stage", DEFAULT_PROCESS_STAGE)
    config_item = snapshot_config.get(process_stage)
    if not config_item:
        config_item = snapshot_config.get("default", {})  # type: ignore[arg-type]

    if not config_item:
        return
    path = config_item["directory"]["path"]
    snap_io = snapshot.FileSystem(
        bucket=path,
        process_stage=process_stage,
        product=getattr(ds, "product", ""),
        set_name=ds.name,
        sharing=ds.sharing,
    )
    date_from = ds.data[ds.datetime_columns].min().min()
    date_to = ds.data[ds.datetime_columns].max().max()
    snap_io.write(
        product=getattr(ds, "product", ""),
        process_stage=getattr(ds, "process_stage", DEFAULT_PROCESS_STAGE),
        sharing=getattr(ds, "sharing", {}),
        as_of_tz=ds.as_of_utc,
        period_from=date_from,
        period_to=date_to,
        data_path=DataIO(ds).dh.data_fullpath,  # type: ignore[attr-defined]
        # meta_path=MetaIO(ds).dh.fullpath,
    )
