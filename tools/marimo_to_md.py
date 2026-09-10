#!/usr/bin/env python3

"""Export Marimo notebooks to Markdown.

Usage:

poetry run python tools/marimo_to_md.py \
    marimo/basic_usage.py \
    marimo/datasets.py \
    md/

md/
├── basic_usage.md
└── datasets.md

Overwrites if files already exist.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def export_notebook(notebook: Path, output: Path) -> None:
    """Export one Marimo notebook to a Markdown file."""
    output.parent.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "marimo",
            "export",
            "md",
            str(notebook),
            "-o",
            str(output),
            "-f",
        ],
        check=True,
    )


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
