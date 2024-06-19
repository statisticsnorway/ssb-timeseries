"""Shortcuts to fix mypy issues with typing."""

from collections.abc import Callable
from os import PathLike
from typing import TypeAlias

# --- typing aliases
# F = TypeVar("F", bound=Callable[..., Any])
# PathStr = TypeVar("PathStr", str, PathLike[str])
PathStr: TypeAlias = str | PathLike[str]
# F: TypeAlias = Callable[..., Any]
F: TypeAlias = Callable
