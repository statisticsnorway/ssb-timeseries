"""Export Marimo notebooks to Markdown for the Sphinx documentation.

The notebooks are executed and exported using the project's configured
minimal timeseries configuration.

Usage:
    poetry run python tools/export_all_guides.py

The exported Markdown files are written to ``docs/guides/``.
"""

import os
from pathlib import Path
import re
import sys
import subprocess

PROJECT_ROOT = Path(__file__).resolve().parent.parent

MAX_OUTPUT_BLOCK = 10_000

OUTPUT_BLOCK_RE = re.compile(r"<pre[^>]*>.*?</pre>", re.DOTALL)

NOTEBOOK_NAMES = [
        "quickstart",
        "basic-usage",
        "calc-basic-arithmetic",
        "calc-with-time",
        "calc-with-metadata",
        "data-archiving-and-sharing",
        "data-types-and-storage",
        "meta-basics",
        "meta-search-and-filtering",
        "meta-tag-maintenance",
]

NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
EXPORT_SCRIPT = PROJECT_ROOT / "tools" / "marimo_to_md.py"
TARGET_DIR = PROJECT_ROOT / "docs" / "guides"
CONFIG_FILE = NOTEBOOKS_DIR / "minimal_configuration.json"


def oversized_blocks(guide: Path) -> list[int]:
    """Return the sizes of output blocks in one guide that exceed the limit."""
    text = guide.read_text(encoding="utf-8")
    return [
        len(match.group(0))
        for match in OUTPUT_BLOCK_RE.finditer(text)
        if len(match.group(0)) > MAX_OUTPUT_BLOCK
    ]


def check_output_sizes(notebooks: list[Path]) -> None:
    """Fail the export when a guide renders an output block that is too large."""
    oversized = [
        f"{guide.name}: {size} characters"
        for guide in (TARGET_DIR / f"{notebook.stem}.md" for notebook in notebooks)
        if guide.is_file()
        for size in oversized_blocks(guide)
    ]
    if oversized:
        raise SystemExit(
            f"Output blocks larger than {MAX_OUTPUT_BLOCK} characters:\n"
            + "\n".join(oversized)
        )


def main():

    notebooks = [ NOTEBOOKS_DIR / f"{name}.py" for name in NOTEBOOK_NAMES ]

    paths_to_check = [EXPORT_SCRIPT, CONFIG_FILE, *notebooks]
    missing = [path for path in paths_to_check if not path.exists()]

    if missing:
        raise FileNotFoundError(
            "The following paths do not exist:\n"
            + "\n".join(map(str, missing))
        )

    environment = os.environ.copy()
    environment["TIMESERIES_CONFIG"] = str(CONFIG_FILE)

    subprocess.run(
        [sys.executable, str(EXPORT_SCRIPT), *notebooks, str(TARGET_DIR)],
        env=environment,
        check=True,
    )

    check_output_sizes(notebooks)

if __name__ == "__main__":
    main()
