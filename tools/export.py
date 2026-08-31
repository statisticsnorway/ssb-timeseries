#!/usr/bin/env python3
"""Export Marimo notebooks to HTML.

Usage:
    python tools/export_html.py <notebook> [<notebook> ...]
    python tools/export_html.py --all
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MARIMO_DIR = PROJECT_ROOT / "notebooks"
HTML_DIR = MARIMO_DIR / "html"


def export_html(notebook_name: str | Path) -> Path:
    """Export a Marimo notebook to the project's HTML directory.

    The notebook may be specified as a filename or as a Path. A ``.py``
    suffix is added when omitted. The generated HTML file has the same
    stem and is written to ``notebooks/html/``.

    Args:
        notebook_name: Name or path of the Marimo notebook. The path is
            interpreted relative to the notebooks directory.

    Returns:
        Path to the generated HTML file.

    Raises:
        ValueError: If the notebook does not have a ``.py`` suffix.
        FileNotFoundError: If the notebook does not exist.
    """
    notebook = MARIMO_DIR / Path(notebook_name)

    if notebook.suffix == "":
        notebook = notebook.with_suffix(".py")
    elif notebook.suffix != ".py":
        raise ValueError(f"Not a Python notebook: {notebook}")

    if not notebook.is_file():
        raise FileNotFoundError(f"Notebook not found: {notebook}")

    HTML_DIR.mkdir(parents=True, exist_ok=True)
    output = HTML_DIR / notebook.with_suffix(".html").name

    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(PROJECT_ROOT), environment.get("PYTHONPATH", "")]
    )

    subprocess.run(
        [
            sys.executable,
            "-m",
            "marimo",
            "export",
            "html",
            str(notebook),
            "-o",
            str(output),
            "--force",
        ],
        # check=True,  # Enable when export behaviour is stable.
        env=environment,
    )

    return output


def export_all() -> list[Path]:
    """Export all Marimo notebooks in the notebooks directory.

    Files whose names begin with ``_`` are ignored.

    Returns:
        Paths to the generated HTML files.
    """
    outputs = []

    for notebook in sorted(MARIMO_DIR.glob("*.py")):
        if notebook.name.startswith("_"):
            continue

        outputs.append(export_html(notebook))

    return outputs


def main() -> None:
    """Parse command-line arguments and export the requested notebooks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "notebooks",
        nargs="*",
        help="One or more notebook filenames in the notebooks directory",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Export all notebooks in the notebooks directory",
    )
    args = parser.parse_args()

    if args.all and args.notebooks:
        parser.error("--all cannot be combined with notebook names")

    if args.all:
        notebooks = [
            notebook
            for notebook in sorted(MARIMO_DIR.glob("*.py"))
            if not notebook.name.startswith("_")
        ]
    elif args.notebooks:
        notebooks = [MARIMO_DIR / name for name in args.notebooks]
    else:
        parser.error("specify one or more notebooks, or use --all")

    for notebook in notebooks:
        output = export_html(notebook)
        print(f"Exported {notebook.name} → {output.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
