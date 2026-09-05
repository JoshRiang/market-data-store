"""Built-in strategies for the replay engine.

A strategy is any object with an `on_bar(bar, state) -> state` method. The
state object can be anything (a dict, a dataclass, an int counter). The
replay engine threads state between callbacks.
"""
from .naive import NaiveStrategy

__all__ = ["NaiveStrategy"]
