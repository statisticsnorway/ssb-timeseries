"""Simple file based persisting of dataset data in stable stages.

Stores data in directory structure with named versioned files adhering to naming standards of Statistics Norway.
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
# ruff: noqa: D202


def version_from_file_name(
    file_name: str, pattern: str | Versioning = "as_of", group: int = 2
) -> str:
    """For known name patterns, extract version marker."""
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
    """A filesystem abstraction for Dataset IO."""

    def __init__(
        self,
        set_name: str,
        bucket: PathStr,
        process_stage: str = "statistikk",
        product: str = "",
        sharing: dict | None = None,
    ) -> None:
        """Initialise filesystem abstraction for dataset.

        Calculate directory structure based on dataset type and name.

        """
        self.bucket = bucket
        self.process_stage = process_stage
        self.product = product
        self.set_name = set_name
        self.sharing = sharing

    def last_version_number_by_regex(self, directory: str, pattern: str = "*") -> str:
        """Check directory and get max version number from files matching regex pattern."""
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
        """Get name of snapshot directory.

        Uses dataset parameters, configuration, product and process stage.
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
        """Get full path of snapshot file.

        Uses dataset parameters, configuration, product, process stage and as-of time.
        Relying on snapshot_directory() first to get the directory name.
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
        """Get name of sharing directory based on dataset parameters and configuration.

        Creates the directory if it does not exist.
        """
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
        """Copies snapshots to bucket(s) according to processing stage and sharing configuration.

        For this to work, .stage and sharing configurations should be set for the dataset, eg::

            .sharing = [
                {'team': 's123', 'path': '<s1234-bucket>'},
                {'team': 's234', 'path': '<s234-bucket>'},
                {'team': 's345': 'path': '<s345-bucket>'}
            ]
            .stage = 'statistikk'

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
