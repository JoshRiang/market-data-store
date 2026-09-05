"""Naive example strategy — prints each bar and tracks simple state."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..schema import Bar


@dataclass
class NaiveState:
    bars_seen: int = 0
    last_close: float | None = None
    max_close: float | None = None
    total_volume: int = 0


class NaiveStrategy:
    """Example strategy: print, count, and track high-water marks.

    `verbose=False` silences stdout for use in tests.
    """

    def __init__(self, verbose: bool = True) -> None:
        self.verbose = verbose
        self.state = NaiveState()

    def on_bar(self, bar: Bar, state: Any = None) -> NaiveState:
        # Allow the caller to pass state explicitly; fall back to self.state.
        if state is None:
            state = self.state
        state.bars_seen += 1
        state.last_close = bar.close
        state.max_close = bar.close if state.max_close is None else max(state.max_close, bar.close)
        state.total_volume += bar.volume
        if self.verbose:
            print(
                f"[{bar.timestamp.strftime('%Y-%m-%d')}] "
                f"{bar.ticker or '':6s} close={bar.close:.2f} vol={bar.volume}"
            )
        return state
