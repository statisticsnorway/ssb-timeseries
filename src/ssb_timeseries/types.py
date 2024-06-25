"""Shortcuts to fix mypy issues with typing."""

from collections.abc import Callable
from os import PathLike
from typing import TypeAlias

PathStr: TypeAlias = str | PathLike[str]
F: TypeAlias = Callable
