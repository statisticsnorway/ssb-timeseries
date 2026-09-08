"""Small helpers for profiling code during development."""

from collections.abc import Callable
import cProfile
import pstats
from typing import Any


def profile_call(
    func: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run ``func`` under cProfile and print cumulative statistics."""
    profiler = cProfile.Profile()

    profiler.enable()
    try:
        result = func(*args, **kwargs)
    finally:
        profiler.disable()

    stats = pstats.Stats(profiler)
    stats.sort_stats("cumulative")
    stats.print_stats(50)

    return result
