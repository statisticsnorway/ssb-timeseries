import inspect
import textwrap
from collections.abc import Callable
from typing import Any, TypeVar
from tabulate import tabulate

F = TypeVar("F", bound=Callable[..., Any])
_tbl_format = 'simple'
_float_format=".2f"


def prompt(cmd, str=''):
    """print a prompt with command before output str"""
    if cmd:
        return f"\n```\n>>> {cmd}\n\n{str}\n```\n"
    else:
        return  f"\n```\n{str}\n```\n"


def tbl(df, cmd=''):
    """print a str formatted table"""
    tbl_str = tabulate(
        df,
        headers = df.columns,
        tablefmt = _tbl_format,
        floatfmt =_float_format,
        showindex=False,
    )
    return prompt(cmd, tbl_str)
