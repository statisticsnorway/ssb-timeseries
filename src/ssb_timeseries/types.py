from collections.abc import Callable
from os import PathLike
from typing import TypeAlias

# from ssb_timeseries.logging import ts_logger

PathStr: TypeAlias = str | PathLike[str]
F: TypeAlias = Callable

# ruff: noqa: D202
