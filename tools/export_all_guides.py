"""Export Marimo notebooks to Markdown for the Sphinx documentation.

The notebooks are executed and exported using the project's configured
minimal timeseries configuration.

Usage:
    poetry run python tools/export_all_guides.py

The exported Markdown files are written to ``docs/guides/``.
"""

import os
from pathlib import Path
import sys
import subprocess

PROJECT_ROOT = Path(__file__).resolve().parent.parent

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

if __name__ == "__main__":
    main()
