"""Shortcuts to fix mypy issues with typing."""

# from typing_extensions import Self
from collections.abc import Callable
from os import PathLike
from typing import Any
from typing import TypeVar

F = TypeVar("F", bound=Callable[..., Any])
# PathStr = TypeVar("PathStr", str, bytes, PathLike[str], PathLike[bytes])
PathStr = TypeVar("PathStr", str, PathLike[str])
# PathStr = TypeVar("PathStr", str, PathLike)
