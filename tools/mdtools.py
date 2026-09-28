import inspect
import textwrap
import uuid
from collections.abc import Callable
from typing import Any, TypeVar
from tabulate import tabulate

F = TypeVar("F", bound=Callable[..., Any])
_tbl_format = 'simple'
_float_format=".2f"

def hex(prefix:str='',/, n:int = 8):
    if prefix:
        prefix = prefix+"_"
    return f"{prefix}{uuid.uuid4().hex[:n]}"

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

def show_code_and_result(func):
    """Decorator for outputting both code and result when executing."""
    source = textwrap.dedent(inspect.getsource(func))

    def wrapper(*args, **kwargs):
        tree = ast.parse(source)

        function = tree.body[0]
        assert isinstance(function, ast.FunctionDef)

        body = function.body

        # The return statement is Marimo plumbing, not guide code.
        if body and isinstance(body[-1], ast.Return):
            body = body[:-1]

        start = body[0].lineno - 1
        end = body[-1].end_lineno

        lines = source.splitlines()
        code = "\n".join(lines[start:end])

        print("```python")
        print(code)
        print("```")

        return func(*args, **kwargs)

    return wrapper

def catalog_item_list_to_df(cat_item_list):
    import pandas as pd

    def dict_placeholder(val, max_keys=0):
        if isinstance(val, dict):
            series_in_set = val.get('series', [])
            if len(series_in_set) > max_keys:
                return f"{{{len(val)-1} set tags + {len(series_in_set)} series}}"
            else:
                return f"{{{len(val)} series tags}}"
        return val

    if cat_item_list:
        #pd.set_option('display.max_rows', 12)
        df = pd.DataFrame(cat_item_list) #.truncate(before=5,after=5)
        return df.style.format(dict_placeholder, subset=["object_tags"]).hide(axis='index').hide(subset=['parent','children'],axis='columns').set_properties(**{
            'min-width': '90px',
            'max-width': '300px',
            'overflow': 'hidden',
            'text-overflow': 'ellipsis',
            'white-space': 'nowrap'
        })

        #.set_properties(subset=[col], **{'width': pixel_width})
    else:
        print("Empty!")
        return
