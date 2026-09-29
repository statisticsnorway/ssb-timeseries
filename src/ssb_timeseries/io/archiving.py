"""Archiving: where an archive goes, and what is kept of it.

This module holds only the parts of archiving that are archiving rather than
convention.
Where the file goes, what it is called and how its bytes are written are all
conventions, and belong to :py:mod:`ssb_timeseries.io.format`.
See :py:func:`ssb_timeseries.io.archive` for archiving a dataset.
"""

from __future__ import annotations

from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any

from ..logging import logger
from ..types import PathStr
from . import fs
from .dataset_ref import DatasetRef
from .format import ArchiveFormat
from .format import get_format


class Archive:
    """Writes a versioned copy of a dataset's data, and keeps every version.

    An archive is a destination, so it is told what to archive rather than asked
    for a path.
    A dataset is archived wherever its data repository keeps it, which need not
    be a filesystem at all.
    """

    def __init__(
        self,
        repository: str | dict,  # TODO: streamline - update to use dict config only
        path: PathStr = "",
        archive_format: str | ArchiveFormat | None = None,
        **options: Any,
    ) -> None:
        """Initialize the archive handler from the archive's configuration.

        Which dataset to archive is passed to each operation as a `DatasetRef`,
        so one instance can archive any number of datasets.

        Args:
            repository: The archive repository name or configuration.
            path: The root of the archive.
            archive_format: The name of the convention to follow, or a convention
                itself. Defaults to the convention in :py:data:`GENERIC`.
            **options: Any further parameters defined for the handler in the
                configuration.
        """
        self.repository = repository
        self.root = str(path)
        self.format = (
            archive_format
            if isinstance(archive_format, ArchiveFormat)
            else get_format(archive_format)
        )
        self.options = options

    # -- policy ---------------------------------------------------------------

    def next_version_number(self, ref: DatasetRef, directory: PathStr) -> int:
        """Find the version number to give the next archive of a dataset.

        Every version already archived is counted, because the convention is
        that an archive is never overwritten and never removed.

        Args:
            ref: The dataset being archived.
            directory: The folder its archives are in.

        Returns:
            The version number to use.
        """
        found = [
            number
            for number in (
                self.format.version_number(Path(path).name)
                for path in fs.ls(
                    str(directory), pattern=self.format.file_glob, create=True
                )
            )
            if number is not None
        ]
        logger.debug(
            "DATASET %s: archive format '%s' found versions %s in %s.",
            ref.name,
            self.format.name,
            sorted(found),
            directory,
        )
        highest = max(found) if found else self.format.first_version - 1
        return highest + 1

    def replicate(
        self,
        ref: DatasetRef,
        file_path: PathStr,
        destinations: list[PathStr],
        relative_folder: PathStr = "",
    ) -> None:
        """Copy an archived file to further destinations.

        The archive is written once and copied from, rather than written again
        per destination, so that replication costs a copy rather than a
        re-encoding of the data.

        Each destination gets the archive at the same place under it as it has
        under the archive's own root, so that a location holding several
        datasets still resolves them the same way the archive does.

        Args:
            ref: The dataset that was archived, for logging.
            file_path: The file to copy.
            destinations: The folders to copy it under.
            relative_folder: The dataset's folder, relative to the archive root.
        """
        for destination in destinations:
            directory = Path(destination) / relative_folder
            fs.mkdir(directory)
            fs.cp(file_path, directory)
            logger.debug("DATASET %s: archive copied to %s.", ref.name, directory)

    # -- writing --------------------------------------------------------------

    def naming_tokens(
        self,
        ref: DatasetRef,
        period_from: datetime | date | None = None,
        period_to: datetime | date | None = None,
    ) -> dict[str, str]:
        """Collect the tokens the convention names a file by.

        Args:
            ref: The dataset being archived.
            period_from: The start of the data's time period, if it has one.
            period_to: The end of the data's time period, if it has one.

        Returns:
            The tokens, with empty ones rendered as empty strings so that a
            template may interpolate them unconditionally.
        """
        render = self.format.render_timestamp
        return {
            "name": self.format.normalise(ref.name),
            "process_stage": ref.process_stage,
            "product": ref.product,
            "from": render(period_from) if period_from is not None else "",
            "to": render(period_to) if period_to is not None else "",
            "as_of": render(ref.as_of_utc) if ref.as_of_utc is not None else "",
            "version_prefix": self.format.version_prefix,
            "period_prefix": self.format.period_prefix,
        }

    def directory(self, ref: DatasetRef, tokens: dict[str, str]) -> PathStr:
        """Return the folder a dataset's archives belong in, creating it.

        Args:
            ref: The dataset being archived.
            tokens: The naming tokens, which carry the folder values.

        Returns:
            The folder path.
        """
        directory = self.format.folder_path(tokens, root=self.root)
        logger.debug("DATASET.IO.ARCHIVE_DIRECTORY: %s", directory)
        fs.mkdir(directory)
        return directory

    def write(
        self,
        ref: DatasetRef,
        data: Any,
        period_from: datetime | date | None = None,
        period_to: datetime | date | None = None,
        destinations: list[PathStr] | None = None,
    ) -> PathStr:
        """Write one version of a dataset's data, and keep every earlier one.

        The data is read by the caller rather than copied from a path, so that a
        dataset which is not stored as a file can be archived just as well.
        No version is ever overwritten: each call adds a new file.

        Args:
            ref: The dataset being archived.
            data: The dataset's data, as read from the data repository.
            period_from: The start of the data's time period, if it has one.
            period_to: The end of the data's time period, if it has one.
            destinations: Further folders to copy the archive into. Only used
                when the convention requires shared data to be archived.

        Returns:
            The path of the archive that was written.
        """
        tokens = self.naming_tokens(ref, period_from, period_to)
        directory = self.directory(ref, tokens)
        number = self.next_version_number(ref, directory)

        name = self.format.file_name(tokens, number)
        extension = self.format.extension or f".{self.format.data_format}"
        file_path = Path(directory) / f"{name}{extension}"

        fs.write_dataframe(
            data=data,
            path=file_path,
            file_format=self.format.data_format,
        )
        logger.info("DATASET %s: archived to %s.", ref.name, file_path)

        if destinations and self.format.replicate_sharing:
            self.replicate(
                ref,
                file_path,
                destinations,
                relative_folder=self.format.folder_path(tokens),
            )

        return file_path
