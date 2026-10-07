#!/usr/bin/env python3

"""Export Marimo notebooks to Markdown.

The export relies on the community package marimo-md-export,
so that both code and code output are included.
Any imagery is placed in a <exported>_resources/ directory next to the export file.

Usage:

poetry run python tools/marimo_to_md.py \
    marimo/basic_usage.py \
    marimo/datasets.py \
    md/

md/
├── basic_usage.md
└── datasets.md

Overwrites if files already exist.

Each notebook is exported with `TIMESERIES_CONFIG` pointing at
`notebooks/minimal_configuration.json` and the working directory anchored at the
repository root, so a single notebook can be rebuilt without any environment setup.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_FILE = PROJECT_ROOT / "notebooks" / "minimal_configuration.json"

PYTEST_REPORT_MARKERS = ("Passed Tests:", "Summary:")

OUTPUT_BLOCK_RE = re.compile(
    r"(?:<!-- @output:[^>\n]* -->\s*)?<pre[^>]*>.*?</pre>\s*",
    re.DOTALL,
)


def strip_pytest_report(text: str) -> str:
    """Drop marimo's pytest report blocks from exported Markdown.

    The report is written to stdout after the body of a ``test_`` cell, so no
    in-cell suppression exists.
    The assertion still decides whether the export succeeds; only the rendered
    report is removed.
    """
    def replace(match: re.Match[str]) -> str:
        block = match.group(0)
        if all(marker in block for marker in PYTEST_REPORT_MARKERS):
            return ""
        return block

    return OUTPUT_BLOCK_RE.sub(replace, text)


def export_notebook(notebook: Path, output: Path) -> None:
    """Export one Marimo notebook to a Markdown file."""
    if not CONFIG_FILE.is_file():
        raise FileNotFoundError(f"Notebook configuration does not exist: {CONFIG_FILE}")

    output.parent.mkdir(parents=True, exist_ok=True)
    notebook = notebook.resolve()
    output = output.resolve()

    # subprocess.run(
    #     [
    #         "marimo", "export", "md",
    #         str(notebook),
    #         "-o",
    #         str(output),
    #         "-f",
    #     ],
    #     check=True,
    # )

    environment = os.environ.copy()
    environment["TIMESERIES_CONFIG"] = str(CONFIG_FILE)

    subprocess.run(
        [
            "marimo-md-export",
            str(notebook),
            str(output),
        ],
        check=True,
        cwd=PROJECT_ROOT,
        env=environment,
    )

    exported = output.read_text(encoding="utf-8")
    stripped = strip_pytest_report(exported)
    if stripped != exported:
        output.write_text(stripped, encoding="utf-8")


def export_notebooks(notebooks: list[Path], target: Path) -> None:
    """Export notebooks to either a single file or a target directory."""
    if len(notebooks) == 1 and target.suffix:
        export_notebook(notebooks[0], target)
        return

    target.mkdir(parents=True, exist_ok=True)

    for notebook in notebooks:
        output = target / f"{notebook.stem}.md"
        export_notebook(notebook, output)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Marimo notebooks to Markdown."
    )
    parser.add_argument(
        "notebooks",
        type=Path,
        nargs="+",
        help="Marimo notebook(s) to export.",
    )
    parser.add_argument(
        "target",
        type=Path,
        help="Output Markdown file, or directory for multiple notebooks.",
    )

    args = parser.parse_args()

    if len(args.notebooks) > 1 and args.target.suffix:
        parser.error(
            "When exporting multiple notebooks, target must be a directory."
        )

    for notebook in args.notebooks:
        if not notebook.is_file():
            parser.error(f"Notebook does not exist: {notebook}")

    export_notebooks(args.notebooks, args.target)


if __name__ == "__main__":
    main()
