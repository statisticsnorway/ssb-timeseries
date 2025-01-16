import random
import re
from typing import Any

from ssb_timeseries.meta import Taxonomy

_words: list[str] = [
    "apple",
    "book",
    "desk",
    "pen",
    "cat",
    "dog",
    "tree",
    "house",
    "car",
    "phone",
    "computer",
    "laptop",
    "keyboard",
    "mouse",
    "chair",
    "table",
    "door",
    "window",
    "wall",
    "floor",
]


_aggregates: list[dict[str, Any]] = [
    {"code": "count", "name": "count", "parent": None},
    {"code": "sum", "name": "sum", "parent": None},
    {"code": "avg", "name": "avg", "parent": None},
    {"code": "min", "name": "min", "parent": None},
    {"code": "max", "name": "min", "parent": None},
    {"code": "perc01", "name": "perc01", "parent": None},
    {"code": "perc05", "name": "perc05", "parent": None},
    {"code": "perc10", "name": "perc10", "parent": None},
    {"code": "perc15", "name": "perc15", "parent": None},
    {"code": "perc20", "name": "perc20", "parent": None},
    {"code": "perc25", "name": "perc25", "parent": None},
    {"code": "perc30", "name": "perc30", "parent": None},
    {"code": "perc35", "name": "perc35", "parent": None},
    {"code": "perc40", "name": "perc40", "parent": None},
    {"code": "perc45", "name": "perc45", "parent": None},
    {"code": "perc50", "name": "perc50", "parent": None},
    {"code": "perc55", "name": "perc55", "parent": None},
    {"code": "perc60", "name": "perc60", "parent": None},
    {"code": "perc65", "name": "perc65", "parent": None},
    {"code": "perc70", "name": "perc70", "parent": None},
    {"code": "perc75", "name": "perc75", "parent": None},
    {"code": "perc80", "name": "perc80", "parent": None},
    {"code": "perc85", "name": "perc85", "parent": None},
    {"code": "perc90", "name": "perc90", "parent": None},
    {"code": "perc95", "name": "perc95", "parent": None},
    {"code": "perc99", "name": "perc99", "parent": None},
]

_nordic: list[dict[str, Any]] = [
    {"code": "nord", "name": "Nordic countries", "parent": None},
    {"code": "scan", "name": "Nordic countries", "parent": "nord"},
    {"code": "DK", "name": "Denmark", "parent": "scan"},
    {"code": "SE", "name": "Consumption", "parent": "scan"},
    {"code": "NO", "name": "Export", "parent": "scan"},
    {"code": "FI", "name": "Injections", "parent": "nord"},
    {"code": "IS", "name": "Supply", "parent": "nord"},
]
# Indclude province level?
# Sweden:
# SE-K	Blekinge län	Blekinge	[SE-10]
# SE-W	Dalarnas län	Dalarna	[SE-20]
# SE-I	Gotlands län	Gotland	[SE-09]
# SE-X	Gävleborgs län	Gävleborg	[SE-21]
# SE-N	Hallands län	Halland	[SE-13]
# SE-Z	Jämtlands län	Jämtland	[SE-23]
# SE-F	Jönköpings län	Jönköping	[SE-06]
# SE-H	Kalmar län	Kalmar	[SE-08]
# SE-G	Kronobergs län	Kronoberg	[SE-07]
# SE-BD	Norrbottens län	Norrbotten	[SE-25]
# SE-M	Skåne län	Scania	[SE-12]
# SE-AB	Stockholms län	Stockholm	[SE-01]
# SE-D	Södermanlands län	Södermanland	[SE-04]
# SE-C	Uppsala län	Uppsala	[SE-03]
# SE-S	Värmlands län	Värmland	[SE-17]
# SE-AC	Västerbottens län	Västerbotten	[SE-24]
# SE-Y	Västernorrlands län	Western Northland	[SE-22]
# SE-U	Västmanlands län	Västmanland	[SE-19]
# SE-O	Västra Götalands län	Västra Götaland	[SE-14]
# SE-T	Örebro län	Örebro	[SE-18]
# SE-E	Östergötlands län	Östergötland	[SE-05]

_econ: list[dict[str, str | None]] = [
    {"code": "B", "name": "Balance", "parent": None},
    {"code": "D", "name": "Demand", "parent": "B"},
    {"code": "C", "name": "Consumption", "parent": "D"},
    {"code": "Exp", "name": "Export", "parent": "D"},
    {"code": "Inj", "name": "Injections", "parent": "D"},
    {"code": "S", "name": "Supply", "parent": "B"},
    {"code": "P", "name": "Production", "parent": "S"},
    {"code": "Imp", "name": "Imports", "parent": "S"},
    {"code": "Wdr", "name": "Withdrawals", "parent": "S"},
]
"""Sample economic variables."""

_commodity: list[dict[str, str | None]] = [
    {"code": "petro", "name": "petroleum products", "parent": None},
    {"code": "oil", "name": "oil", "parent": "petro"},
    {"code": "gas", "name": "natural gas", "parent": "petro"},
    {"code": "lpg", "name": "liquefied petroleum gas", "parent": "petro"},
]
"""A list of petroleum products."""

_bio: list[dict[str, str | None]] = [
    {"code": "animals", "name": "animals", "parent": None},
    {"code": "plants", "name": "plants", "parent": None},
    {"code": "fungi", "name": "fungi", "parent": None},
    {"code": "microbes", "name": "microbes", "parent": None},
]
"""A list of biological categories."""


def random_words(some_list: list[str] | None = None, n: int = 1) -> list[str]:
    """Return a random word from 'some_list'."""
    if not some_list:
        some_list = _words
    return random.choices(some_list, k=n)


def tags_string(separator: str = "_", *args: list[str]) -> str:
    """Join one random element from each list in '*args*', separated by 'separator'."""
    return separator.join([random.choice(a) for a in args])


def lower_first_char(s: str) -> str:
    """Change first character of string to lowercase."""
    return s[0].lower() + s[1:]


def camel_case_keys(dictionary: dict) -> dict:
    """Rename dictionary keys to camel case."""
    out = {}

    for k, v in dictionary:
        pascal_k = "".join(word.title() for word in k.split("_"))
        camel_k = lower_first_char(pascal_k)
        if isinstance(v, dict):
            out[camel_k] = camel_case_keys(v)
        else:
            out[camel_k] = v
    return out


def snake_case_keys(dictionary: dict) -> dict:
    """Rename dictionary keys to snake case."""
    out = {}
    for k, v in dictionary:
        snake_k = re.sub(r"(?<!^)(?=[A-Z])", "_", k).lower()
        if isinstance(v, dict):
            out[snake_k] = snake_case_keys(v)
        else:
            out[snake_k] = v
    return out


def list_to_tax(list_of_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Use code as key, convert list of dicts to dict."""
    ...
    blanks = ["shortName", "presentationName", "validFrom", "validTo", "notes"]
    # dict_of_dicts = {d['code']: d for d in list_of_dicts}
    # for k in dict_of_dicts.keys():
    for d in list_of_dicts:
        # dict_of_dicts[k]['parentCode'] =  dict_of_dicts[k].pop('parent','')
        d["parentCode"] = d.pop("parent", "")
        for b in blanks:
            d[b] = ""
    return list_of_dicts


def balance() -> Taxonomy:
    """Return sample economic balance taxonomy."""
    d = list_to_tax(_econ)
    return Taxonomy(data=d)


def commodity() -> Taxonomy:
    """Return sample economic balance taxonomy."""
    d = list_to_tax(_commodity)
    return Taxonomy(data=d)


def nordic_countries() -> Taxonomy:
    """Return sample economic balance taxonomy."""
    d = list_to_tax(_nordic)
    return Taxonomy(data=d)


def aggregates() -> Taxonomy:
    """Return sample economic balance taxonomy."""
    d = list_to_tax(_aggregates)
    return Taxonomy(data=d)


# def words() -> Taxonomy:
#     """Return sample economic balance taxonomy."""
#     return Taxonomy(data=_words)


def biology() -> Taxonomy:
    """Return sample economic balance taxonomy."""
    d = list_to_tax(_bio)
    return Taxonomy(data=d)
