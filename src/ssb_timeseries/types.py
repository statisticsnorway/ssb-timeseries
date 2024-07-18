from collections.abc import Callable
from copy import deepcopy
from os import PathLike
from typing import Any
from typing import TypeAlias

import pyarrow

from ssb_timeseries import properties

# from ssb_timeseries.logging import ts_logger

PathStr: TypeAlias = str | PathLike[str]
F: TypeAlias = Callable

# ruff: noqa: D202
