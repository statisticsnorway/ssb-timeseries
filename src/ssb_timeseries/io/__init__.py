"""The IO module provides the high-level facade for all data and metadata I/O.

This module serves as the single, authoritative entry point for all storage operations.
The functions exposed here are intended to be the **exclusive** interface used by the
rest of the library (such as the `Dataset` class) to interact with the storage layer.
This includes `read_data`, `read_metadata`, `save`, `search`, `find`, `persist`,
and `versions`.

This facade design decouples the core application logic from the specifics of the
storage backends.
The underlying implementation is a pluggable, configuration-driven system.
It dispatches tasks to the appropriate backend handler based on the active
project configuration.

Internal components like `Data_IO`, `Meta_IO`, and the concrete handler modules
(e.g., `ssb_timeseries.io.simple`) are considered implementation details of this
facade.
They should not be imported or used directly by other parts of the application.
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
from ..meta import TagDict
from ..properties import SeriesType
from . import protocols
from . import snapshot

# mypy: disable-error-code="no-any-return,no-untyped-def,return-value,assignment,attr-defined"
DEFAULT_PROCESS_STAGE = "Statistikk"  # TODO: control from config?
_ACTIVE_CONFIG = Config.active()


def _all_repos() -> list:
    """Get a list of all repository names."""
    return list(Config.active().repositories.keys())


def _repo_config(
    target: Any,  # str | dict[str, FileBasedRepository],
) -> FileBasedRepository:
    """Get a repository configuration dictionary by name.

    A target that is already a dictionary will simply be passed through.
    """
    if isinstance(target, str):
        config = Config.active()  #  _ACTIVE_CONFIG #TODO: add Config.refresh() first
        repo = config.repositories[target]
        repo.setdefault("name", target)
    elif isinstance(target, dict):
        repo = target
    else:
        raise TypeError(
            f"Repository must be provided either by name (str) or as full dict; was {type(target)}:\n{target}"
        )

    return repo


def _io_handler(**kwargs) -> protocols.DataReadWrite | protocols.MetadataReadWrite:
    """Dynamically import and instantiate an IO handler.

    The handler is determined by the 'repository' and 'handler_type' arguments.
    """
    repo_cfg = _repo_config(kwargs.pop("repository"))
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
    handler_options = handler_config.get("options", {})
    if kwargs:
        handler_options.update(kwargs)
        logger.warning("_IO_HANDLER() ... kwargs: %s", kwargs)
    instance = handler(repository=repo_cfg, **kwargs)
    return instance


def _handler_class(handler_name: str) -> type:
    """Dynamically import and return a handler class from the config."""
    config = Config.active()  #  _ACTIVE_CONFIG  #TODO: add Config.refresh() first
    handler_conf = config.io_handlers[handler_name]
    handler_path = handler_conf["handler"]

    module_path, class_name = handler_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    handler_class = getattr(module, class_name)

    return handler_class


class DataIO:
    """Provides a generic IO interface for the data of a specific dataset."""

    def __init__(
        self,
        ds: Dataset,
    ) -> None:
        """Initialize the data IO handler for the given Dataset."""
        self.ds = ds

    @property
    def dh(self) -> protocols.DataReadWrite:
        """Expose the configured IO handler for data operations."""
        return _io_handler(
            handler_type="data",
            repository=self.ds.repository,
            set_name=self.ds.name,
            set_type=self.ds.data_type,
            as_of_utc=date_utc(self.ds.as_of_utc),
        )


class MetaIO:
    """Provides a generic IO interface for the metadata of a specific dataset."""

    def __init__(
        self,
        ds: Dataset | None = None,
        repository: str = "",
    ) -> None:
        """Initialize the metadata IO handler.

        The handler can be bound to a Dataset instance or a repository name.
        """
        # dirty: either for Dataset or for repo --> target is repo only
        if isinstance(ds, Dataset):
            self.ds = ds
            self.repository = ds.repository
        elif repository:
            if isinstance(repository, dict):
                raise TypeError("WTF repo should be dict!")

            self.ds = None
            self.repository = repository
        else:
            raise ValueError("Either a dataset or a repository must be provided.")

    @property
    def dh(self) -> protocols.MetadataReadWrite:
        """Expose the configured IO handler for metadata operations."""
        return _io_handler(
            handler_type="metadata",
            repository=self.repository,
        )

    def search(
        self,
        **kwargs,
    ) -> list[dict]:
        """Search for datasets within a single repository."""
        kwargs.setdefault("datasets", True)
        kwargs.setdefault("series", False)
        return self.dh.search(**kwargs)

    def read(self, set_name: str = "") -> dict:
        """Read metadata for a given dataset."""
        if not set_name:
            set_name = self.ds.name
        return self.dh.read(set_name=set_name)

    def write(self, set_name: str = "", tags: TagDict | None = None) -> None:
        """Write metadata for a given dataset."""
        if not tags:
            tags = self.ds.tags
        else:
            raise ValueError(
                "MetaIO.write requires tags to be provided, eiher through dataset at init, or passed as 'tags' parameter."
            )
        self.dh.write(
            set_name=set_name,
            tags=self.ds.tags,
        )


def save(ds: Dataset) -> None:
    """Write a dataset's data and metadata to storage.

    Args:
        ds: The Dataset object to save.
    """
    DataIO(ds).dh.write(data=ds.data, tags=ds.tags)
    MetaIO(ds).dh.write(set_name=ds.name, tags=ds.tags)


def search(
    **kwargs,
) -> list[dict]:
    """Search for datasets or series across one or more repositories.

    Args:
        **kwargs: Search criteria such as 'equals', 'contains', 'pattern',
            'tags', and 'repositories'. See `JsonMetaIO.search` for details.
    """
    repositories = kwargs.pop("repositories", _all_repos())
    if isinstance(repositories, str):
        repos_to_check = [repositories]
    elif isinstance(repositories, dict):
        repos_to_check = list(repositories.keys())
    else:
        repos_to_check = repositories
    result = []
    for r in repos_to_check:
        meta_io = MetaIO(repository=r)
        found = meta_io.search(
            **kwargs,
        )
        for f in found:
            result.append(f)

    return result


def read_metadata(
    repository: str | dict,
    set_name: str,
) -> dict:
    """Read the metadata for a single dataset from the configured handler.

    Args:
        repository: The repository name or configuration dictionary.
        set_name: The name of the dataset.

    Returns:
        A dictionary containing the dataset's metadata.
    """
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
    """Read the data for a single dataset into a dataframe.

    Args:
        repository: The repository name or configuration dictionary.
        set_name: The name of the dataset.
        as_of_tz: The version timestamp if the dataset is versioned.

    Returns:
        A dataframe containing the dataset's data.
    """
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
        data = data_io.read()
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
    """Find dataset metadata by name in specified or all repositories.

    Args:
        set_name: The name of the dataset to find.
        repository: The specific repository to search in. If empty, searches all.
        require_one: If True, raises an error if no results are found.
        require_unique: If True, raises an error if more than one result is found.
        **kwargs: Unused, but present for compatibility.

    Returns:
        A single dictionary if one result is found, otherwise a list of dictionaries.

    Raises:
        LookupError: If `require_one` or `require_unique` is True and the
            number of results does not match the requirement.
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
            tags = meta_io.read(set_name=set_name)
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


def versions(
    ds: Dataset,
    **kwargs,
) -> list[datetime | str]:
    """Get a list of all available version markers for a dataset.

    Args:
        ds: The Dataset object to inspect.
        **kwargs: Additional arguments passed to the underlying IO handler.
    """
    data_io = DataIO(ds)
    versions = data_io.dh.versions(
        file_pattern="*.parquet",
        pattern=ds.data_type.versioning,
    )
    return versions


def persist(
    ds: Dataset,
) -> None:
    """Copy a dataset snapshot to its configured immutable and shared locations.

    This function relies on a `snapshots` section being defined in the project
    configuration. The dataset's `process_stage` and `sharing` attributes
    determine the exact destination paths.

    .. seealso::
        For detailed configuration examples, refer to the guide on
        :doc:`/configure-io`.

    Args:
        ds: The Dataset object to persist.
    """
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
    path = config_item["directory"]["options"]["path"]
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
        sharing=getattr(ds, "sharing", {}),
        as_of_tz=ds.as_of_utc,
        period_from=date_from,
        period_to=date_to,
        data_path=DataIO(ds).dh.fullpath,  # type: ignore[attr-defined]
        # meta_path=MetaIO(ds).dh.fullpath,
    )
