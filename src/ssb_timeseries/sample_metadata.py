import random

from klass import KlassClassification
from klass import get_classification

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


_aggregates: list[str] = [
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


def random_word(some_list: list[str] = _words) -> str:
    """Return a random word from 'some_list'."""
    return random.choice(some_list)


def tags_string(separator: str = "_", *args: list[str]) -> str:
    """Join one random element from each list in '*args*', separated by 'separator'."""
    return separator.join([random.choice(a) for a in args])


def nus() -> KlassClassification:
    """Return the NUS classification (KLASS id 36)."""
    return get_classification("36")
