"""Definitions of the conventions an archive follows.

An archive is a file with a name, in a folder, written in a format.
Which name, which folder and which format are all conventions, and conventions
differ between organisations, so they are described here as data rather than
built into the archiving code.
See :py:mod:`ssb_timeseries.io.archiving` for the part that is not convention.

A convention is an :py:class:`ArchiveFormat`.
Several may be registered by name, and the archive is told which one to use.
The :py:data:`generic` definition is the fallback, and carries no assumptions
about any particular organisation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any

from ..types import PathStr


def iso(dt: datetime | date) -> str:
    """Render a date or datetime in ISO 8601, keeping the colons."""
    return dt.isoformat()


def iso_no_colon(dt: datetime | date) -> str:
    """Render a date or datetime in ISO 8601, with the colons removed."""
    return dt.isoformat().replace(":", "")


def dashes_ms(dt: datetime | date) -> str:
    """Render a date or datetime with dashes in place of colons, in milliseconds.

    The milliseconds are always written out, so that two file names differ only
    where their timestamps actually differ.
    """
    if isinstance(dt, datetime):
        rendered = dt.isoformat(timespec="milliseconds")
    else:
        rendered = dt.isoformat()
    return rendered.replace(":", "-")


TIMESTAMP_FORMS: dict[str, Any] = {
    "iso": iso,
    "iso_no_colon": iso_no_colon,
    "dashes_ms": dashes_ms,
}
"""Named renderers for a date or datetime inside a file name.

A convention names one of these rather than carrying a format string, so that
the definitions stay declarative and each renderer can be tested on its own.
"""


@dataclass(frozen=True)
class ArchiveFormat:
    """How one organisation names and lays out its archives.

    Every field is a convention rather than a mechanism.
    Two definitions that agree on all of them would produce identical archives
    for identical input, which is the only thing that distinguishes them.
    """

    name: str
    """The name this convention is selected by."""

    file_template: str
    """The file name, without its extension, as a template over naming segments.

    The template names whole segments rather than laying out characters, because
    the segments a convention wants are not all always wanted.
    A dataset with no period has no period to name, and a dataset with no as-of
    has no as-of marker, and a file name with a dangling separator in it says
    less than one without.
    The available segments are `name`, `period`, `as_of` and `version`, which
    are rendered from the conventions `period_template`, `as_of_template` and
    `version_template` below, and are each empty when the convention has nothing
    to say about them.
    """

    folder_order: tuple[str, ...]
    """Which tokens become folders, outermost first, under the archive's root.

    This is a field of its own because it is the one part of a layout that a
    template cannot express: ``{product}/{process_stage}`` and
    ``{process_stage}/{product}`` are the same tokens in a different order.
    A token that is empty contributes no folder.
    """

    data_format: str = "parquet"
    """The format the data is written in, as understood by :py:func:`fs.write_dataframe`."""

    version_pattern: str = r"_v(\d+)$"
    """Regex matching the archive's own version number in an existing file's name.

    Matched against the name without its extension, since a convention lays out
    the name and the format adds the extension.
    The capture group is the version number.
    """

    version_prefix: str = "_v"
    """The prefix that introduces a version number in a file name."""

    period_prefix: str = "_p"
    """The prefix that introduces a period in a file name."""

    period_template: str = "{period_prefix}{from}{period_prefix}{to}"
    """The period segment, from the data's own start and end.

    Rendered only when the data has both, since a period with one end is not a
    period.
    """

    as_of_template: str | None = None
    """The as-of segment, or None if a convention does not name the as-of.

    Whether the as-of belongs in a file name at all is a convention, as is the
    shape it takes when it does, and both are settled here rather than in the
    template.
    """

    version_template: str = "{version_prefix}{number}"
    """The version segment, carrying the archive's own version number."""

    timestamp_form: str = "iso_no_colon"
    """The name of the renderer used for the `from`, `to` and `as_of` tokens."""

    extension: str | None = None
    """The file's extension, if the convention requires a specific one.

    None takes the extension implied by `data_format`.
    """

    allowed_characters: str | None = None
    """The characters a dataset name may be built from, or None for no restriction.

    A name containing anything else is rejected, because a name that breaks the
    convention is worse than one that is refused.
    """

    character_folds: Mapping[str, str] = field(default_factory=dict)
    """Substitutions applied to a dataset name before it is checked or used.

    Each key is a single character, and is replaced wherever it occurs, so a fold
    may lengthen the name as well as shorten it.
    These are recommended transliterations rather than replacements that carry
    meaning, so a name that folds to something else still names the same dataset.
    """

    first_version: int = 1
    """The version number given to the first archive of a dataset."""

    replicate_sharing: bool = False
    """Whether archiving also copies the archive to the dataset's shared locations.

    Some conventions require that shared data is archived wherever it is shared,
    others do not, so it is a property of the convention rather than a rule.
    """

    def render_timestamp(self, value: datetime | date) -> str:
        """Render a date or datetime the way this convention does.

        Args:
            value: The date or datetime to render.

        Raises:
            ValueError: If the convention names a renderer that does not exist.
        """
        render = TIMESTAMP_FORMS.get(self.timestamp_form)
        if render is None:
            raise ValueError(
                f"Unknown timestamp form '{self.timestamp_form}' "
                f"in archive format '{self.name}'."
            )
        return render(value)

    def normalise(self, name: str) -> str:
        """Fold a dataset name into the characters this convention allows.

        Args:
            name: The dataset name as it is known to this library.

        Returns:
            The name to use in the archive's paths.

        Raises:
            ValueError: If the name still contains disallowed characters after
                folding, since a silently rewritten name would be ambiguous.
        """
        folded = name
        for source, target in self.character_folds.items():
            folded = folded.replace(source, target)
        if self.allowed_characters is None:
            return folded
        illegal = sorted({c for c in folded if c not in self.allowed_characters})
        if illegal:
            raise ValueError(
                f"The dataset name '{name}' contains characters that the "
                f"'{self.name}' archive format does not allow: "
                f"{' '.join(repr(c) for c in illegal)}. Allowed characters are "
                f"'{self.allowed_characters}'."
            )
        return folded

    def folder_path(self, tokens: dict[str, str], root: PathStr = "") -> PathStr:
        """Build the folder holding a dataset's archives.

        Args:
            tokens: The naming tokens, from which the folder tokens are taken.
            root: The archive's root, which the folders are placed under.

        Returns:
            The folder path, which may not yet exist.
        """
        segments = [tokens[name] for name in self.folder_order if tokens.get(name)]
        return str(Path(root, *segments)) if segments else str(root)

    def file_name(self, tokens: dict[str, str], number: int) -> str:
        """Build the name of one archived file, without its extension.

        Args:
            tokens: The naming tokens for the dataset.
            number: The version number to record in the name.

        Returns:
            The file name.
        """
        fields: dict[str, str | int] = {
            **tokens,
            "number": number,
            "version_prefix": self.version_prefix,
            "period_prefix": self.period_prefix,
        }
        segments = dict(tokens)
        segments["period"] = self._segment(
            self.period_template, fields, needs=("from", "to")
        )
        segments["as_of"] = self._segment(self.as_of_template, fields, needs=("as_of",))
        segments["version"] = self._segment(self.version_template, fields)
        return self.file_template.format(number=number, **segments)

    def _segment(
        self,
        template: str | None,
        fields: dict[str, str | int],
        needs: tuple[str, ...] = (),
    ) -> str:
        """Render one optional segment of a file name, or nothing at all.

        Args:
            template: The segment's template, or None if the convention has no
                such segment.
            fields: The naming fields available to the template.
            needs: The fields the segment is only meaningful with.

        Returns:
            The rendered segment, or an empty string if the convention has no
            such segment or the fields it needs are missing.
        """
        if template is None or not all(fields.get(field) for field in needs):
            return ""
        return template.format(**fields)

    def version_number(self, file_name: str) -> int | None:
        """Read the version number back out of an archived file's name.

        The extension is not part of the name a convention lays out, so it is
        taken off before matching.

        Args:
            file_name: The name of an archived file.

        Returns:
            The version number, or None if the name is not one of ours.
        """
        match = re.search(self.version_pattern, Path(file_name).stem)
        return int(match.group(1)) if match else None

    @property
    def file_glob(self) -> str:
        """The glob matching this convention's archived files."""
        extension = self.extension or f".{self.data_format}"
        return f"*{extension}"


GENERIC = ArchiveFormat(
    name="generic",
    file_template="{name}{version}",
    folder_order=("name",),
)
"""The default convention, which assumes nothing about any organisation.

A dataset's archives sit in a folder named after it, and its versions are
numbered.
"""

SSB = ArchiveFormat(
    name="ssb",
    # The as-of marker is carried in the region the standard leaves to a free
    # text description, because the standard's grammar has no slot for one.
    # That stretches the standard: a name of this shape has two version
    # markers, where the standard describes one. It is kept as it is until the
    # archiving conventions are revisited, since the as-of of a version is
    # otherwise recorded nowhere in the archive.
    as_of_template="{version_prefix}{as_of}{version_prefix}",
    file_template="{name}{period}{as_of}{version}",
    folder_order=("product", "process_stage", "name"),
    timestamp_form="dashes_ms",
    allowed_characters="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_",
    character_folds={
        "æ": "ae",
        "ø": "oe",
        "å": "aa",
        "Æ": "Ae",
        "Ø": "Oe",
        "Å": "Aa",
    },
    replicate_sharing=True,
)
"""The convention Statistics Norway's archiving standards describe.

The folder levels are the product's short name then the data state, as the
navnestandard requires.
The name is a short description, the data's period, the as-of, and the version.
"""

FORMATS: dict[str, ArchiveFormat] = {
    GENERIC.name: GENERIC,
    SSB.name: SSB,
}
"""The conventions this library knows, by name."""


def get_format(name: str | None = None) -> ArchiveFormat:
    """Look up a convention by name.

    Args:
        name: The name of the convention, or None for the default.

    Returns:
        The convention.

    Raises:
        ValueError: If no convention by that name is known.
    """
    if not name:
        return GENERIC
    try:
        return FORMATS[name]
    except KeyError:
        known = ", ".join(sorted(FORMATS))
        raise ValueError(
            f"Unknown archive format '{name}'. Known formats are: {known}."
        ) from None
