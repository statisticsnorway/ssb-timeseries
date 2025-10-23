"""Provides a file-based I/O handler for persisting dataset snapshots.

This handler stores data in a versioned directory structure that adheres to
the naming conventions of Statistics Norway.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from .. import fs
from ..logging import logger
from ..properties import Versioning
from ..types import PathStr

# mypy: disable-error-code="type-var, arg-type, type-arg, return-value, attr-defined, union-attr, operator, assignment,import-untyped, "


def version_from_file_name(
    file_name: str, pattern: str | Versioning = "as_of", group: int = 2
) -> str:
    """Extract a version marker from a filename using known patterns."""
    if isinstance(pattern, Versioning):
        pattern = str(pattern)

    match pattern.lower():
        case "persisted":
            regex = r"(_v)(\d+)(.parquet)"
        case "as_of":
            date_part = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{6}[+-][0-9]{4}"
            regex = f"(as_of_)({date_part})(-data.parquet)"
        case "names":
            # type is not implemented
            regex = "(_v)(*)(-data.parquet)"
        case "none":
            regex = "(.*)(latest)(-data.parquet)"
        case _:
            regex = pattern

    vs = re.search(regex, file_name).group(group)
    logger.debug(
        "file: %s pattern:%s, regex%s \n--> version: %s ",
        file_name,
        pattern,
        regex,
        vs,
    )
    return vs


class FileSystem:
    """A filesystem abstraction for writing dataset snapshots."""

    def __init__(
        self,
        set_name: str,
        bucket: PathStr,
        process_stage: str = "statistikk",
        product: str = "",
        sharing: dict | None = None,
    ) -> None:
        """Initialize the filesystem handler for a given dataset snapshot.

        This method calculates the necessary directory structure based on the
        dataset's name and other contextual attributes.
        """
        self.bucket = bucket
        self.process_stage = process_stage
        self.product = product
        self.set_name = set_name
        self.sharing = sharing

    def last_version_number_by_regex(self, directory: str, pattern: str = "*") -> str:
        """Return the max version number from files in a directory matching a pattern."""
        files = fs.ls(directory, pattern=pattern)
        number_of_files = len(files)

        vs = sorted(
            [int(version_from_file_name(fname, "persisted")) for fname in files]
        )
        logger.debug(
            "DATASET %s: io.last_version regex identified versions %s in %s.",
            self.set_name,
            vs,
            directory,
        )
        if vs:
            read_from_filenames = max(vs)
            out = read_from_filenames
        else:
            read_from_filenames = 0
            out = number_of_files

        logger.debug(
            "DATASET %s: io.last_version searched directory: \n\t%s\n\tfor '%s' found %s files, regex identified version %s --> vs %s.",
            self.set_name,
            directory,
            pattern,
            f"{number_of_files!s}",
            f"{read_from_filenames!s}",
            f"{out!s}",
        )
        return out

    @property
    def snapshot_directory(self) -> PathStr:
        """Return the directory path for the snapshot.

        The path is constructed from the configured bucket, process stage,
        product, and dataset name.
        """
        directory = (
            Path(self.bucket) / self.process_stage / self.product / self.set_name
        )
        logger.debug(
            "DATASET.IO.SHARING_DIRECTORY: %s",
            directory,
        )
        return directory

    def snapshot_filename(
        self,
        as_of_utc: datetime | None = None,
        period_from: str = "",
        period_to: str = "",
    ) -> PathStr:
        """Construct the full filename for the snapshot file.

        The name includes the dataset name, period range, version timestamp,
        and an incrementing version number.
        """
        directory = self.snapshot_directory
        next_vs = (
            self.last_version_number_by_regex(directory=directory, pattern="*.parquet")
            + 1
        )

        def iso_no_colon(dt: datetime) -> str:
            return dt.isoformat().replace(":", "")

        if as_of_utc:
            out = f"{self.set_name}_p{iso_no_colon(period_from)}_p{iso_no_colon(period_to)}_v{iso_no_colon(as_of_utc)}_v{next_vs}"
        else:
            out = f"{self.set_name}_p{iso_no_colon(period_from)}_p{iso_no_colon(period_to)}_v{next_vs}"

            logger.debug(
                "DATASET last version %s from %s to %s.')",
                next_vs,
                period_from,
                period_to,
            )
        return out

    def sharing_directory(self, path: str) -> PathStr:
        """Return the directory path for sharing, creating it if it does not exist."""
        directory = Path(path) / self.set_name

        logger.debug(
            "DATASET.IO.SHARING_DIRECTORY: %s",
            directory,
        )
        fs.mkdir(directory)
        return directory

    def write(
        self,
        sharing: dict | None = None,
        as_of_tz: datetime | None = None,
        period_from: datetime | None = None,
        period_to: datetime | None = None,
        data_path: str = "",
        meta_path: str = "",
    ) -> None:
        """Copy snapshot files to their primary and shared storage locations.

        Args:
            sharing: A dictionary defining sharing configurations.
            as_of_tz: The version timestamp of the snapshot.
            period_from: The start of the data's time period.
            period_to: The end of the data's time period.
            data_path: The source path of the data file to copy.
            meta_path: The source path of the metadata file to copy.
        """
        directory = self.snapshot_directory
        snapshot_name = self.snapshot_filename(
            as_of_utc=as_of_tz,
            period_from=period_from,
            period_to=period_to,
        )

        data_publish_path = Path(directory) / f"{snapshot_name}.parquet"
        meta_publish_path = Path(directory) / f"{snapshot_name}.json"

        if data_path:
            fs.cp(data_path, data_publish_path)

        if meta_path:
            fs.cp(meta_path, meta_publish_path)

        if sharing:
            logger.debug("Sharing configs: %s", sharing)
            for s in sharing:
                logger.debug("Sharing: %s", s)
                if "team" not in s.keys():
                    s["team"] = "no team specified"
                if data_path:
                    fs.cp(
                        data_publish_path,
                        self.sharing_directory(s["path"]),
                    )
                if meta_path:
                    fs.cp(
                        meta_publish_path,
                        self.sharing_directory(s["path"]),
                    )
                logger.debug(
                    "DATASET %s: sharing with %s, snapshot copied to %s.",
                    self.set_name,
                    s["team"],
                    s["path"],
                )
