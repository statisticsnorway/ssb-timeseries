# ruff: noqa
# mypy: ignore-errors

import random

# import json
from klass import get_classification  # Import the utility-function


def words() -> list[str]:
    return [
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


def aggregates() -> list[str]:
    return [
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "perc01",
        "perc05",
        "perc10",
        "perc15",
        "perc20",
        "perc25",
        "perc30",
        "perc35",
        "perc40",
        "perc45",
        "perc50",
        "perc55",
        "perc60",
        "perc65",
        "perc70",
        "perc75",
        "perc80",
        "perc85",
        "perc90",
        "perc95",
        "perc99",
    ]


def random_word():
    return random.choice(words)


def tags_string(lists):
    return "_".join([random.choice(L) for L in lists])


def nus():
    return get_classification(36)
