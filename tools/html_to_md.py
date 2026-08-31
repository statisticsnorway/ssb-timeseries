#!/usr/bin/env python3
"""Convert an exported Marimo HTML file to Markdown.

Usage:
    python tools/html_to_md.py <html_file>
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HTML_DIR = PROJECT_ROOT / "notebooks" / "html"
DOCS_DIR = PROJECT_ROOT / "docs" / "guides"


def html_to_md(html_file: str | Path) -> Path:
    """Convert an HTML file to Markdown using Pandoc.

    The input may be specified as a filename or Path. Relative paths are
    interpreted relative to the project's notebooks/html directory.
    The resulting Markdown file is written to the project's docs directory.

    Args:
        html_file: Name or path of the HTML file to convert.

    Returns:
        Path to the generated Markdown file.

    Raises:
        FileNotFoundError: If the input HTML file does not exist.
        ValueError: If the input file does not have an ``.html`` suffix.
    """
    html = HTML_DIR / Path(html_file)

    if html.suffix != ".html":
        raise ValueError(f"Not an HTML file: {html}")

    if not html.is_file():
        raise FileNotFoundError(f"HTML file not found: {html}")

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    output = DOCS_DIR / html.with_suffix(".md").name

    subprocess.run(
        [
            "pandoc",
            str(html),
            "--from=html",
            "--to=gfm",
            "--output",
            str(output),
        ],
        check=True,
    )

    return output


def main() -> None:
    """Parse command-line arguments and convert the requested HTML file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "html_file",
        help="HTML file in the notebooks/html directory",
    )
    args = parser.parse_args()

    output = html_to_md(args.html_file)
    print(f"Converted {args.html_file} → {output.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
