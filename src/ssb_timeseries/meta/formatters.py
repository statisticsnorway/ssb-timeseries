"""Utility functions for formatting metadata strings."""

import re


def camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case, handling acronyms.

    Example:
        >>> camel_to_snake('HTTPConnection')
        'http_connection'
    """
    # Insert underscore before uppercase letter preceded by lowercase letter or digit.
    #    e.g., 'CamelCase' -> 'Camel_Case', 'MyValue1' -> 'My_Value1'
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)

    # Insert an underscore before any uppercase letter that is followed
    #    by a lowercase letter, but only if it's preceded by another
    #    uppercase letter.
    #    e.g., 'HTTPConnection' -> 'HTTP_Connection'
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return name.lower()
