"""Deterministic replay engine.

Streams historical bars in chronological order to a strategy callback. The
callback signature is `on_bar(bar, state) -> state`; the engine threads
`state` between calls so strategies can accumulate internal state.

For testing speed: `speed=1.0` plays bars back at their original wall-clock
cadence (1s per bar). `speed=100.0` plays 100 bars per second of wall time.
Set `speed=0` to disable sleeping (full speed, useful in tests).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Optional

import pandas as pd

from .schema import Bar


# A strategy is anything with on_bar(bar, state) -> state.
StrategyCallback = Callable[[Bar, Any], Any]


@dataclass
class ReplayStats:
    """Stats captured during a replay run."""

    bars_emitted: int = 0
    elapsed_seconds: float = 0.0
    first_ts: Optional[pd.Timestamp] = None
    last_ts: Optional[pd.Timestamp] = None
    errors: int = 0


class ReplayEngine:
    """Iterate a bar sequence in chronological order, dispatching to a callback."""

    def __init__(
        self,
        bars: Iterable[Bar] | pd.DataFrame,
        speed: float = 1.0,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        """Args:
        bars: iterable of Bar dataclasses OR a pandas DataFrame produced by
            ParquetStore.read_bars.
        speed: wall-clock speed multiplier. `1.0` = one second per bar of
            calendar time. `0` runs as fast as possible.
        sleep_fn: injectable sleep function (handy in tests).
        """
        self._bars: list[Bar] = list(self._coerce(bars))
        self._bars.sort(key=lambda b: b.timestamp)
        self.speed = float(speed)
        self.sleep_fn = sleep_fn

    # ---------------------------------------------------------- public API

    def __iter__(self) -> Iterator[Bar]:
        return iter(self._bars)

    def __len__(self) -> int:
        return len(self._bars)

    def run(
        self,
        callback: StrategyCallback,
        initial_state: Any = None,
        stop_on_error: bool = False,
    ) -> tuple[Any, ReplayStats]:
        """Stream bars through `callback.on_bar(bar, state)`.

        Returns the final state and ReplayStats.
        """
        stats = ReplayStats()
        state = initial_state
        last_ts: Optional[pd.Timestamp] = None

        for bar in self._bars:
            try:
                state = callback(bar, state)
            except Exception:  # noqa: BLE001
                stats.errors += 1
                if stop_on_error:
                    raise
                continue

            stats.bars_emitted += 1
            ts_now = pd.Timestamp(bar.timestamp)
            if stats.first_ts is None:
                stats.first_ts = ts_now
            stats.last_ts = ts_now

            # Pacing — sleep proportional to calendar gap divided by speed.
            if self.speed > 0 and last_ts is not None:
                gap = (ts_now - last_ts).total_seconds()
                if gap > 0:
                    self.sleep_fn(gap / self.speed)
            last_ts = ts_now

        stats.elapsed_seconds = self._elapsed_since(stats)
        return state, stats

    # --------------------------------------------------------- diagnostics

    def head(self, n: int = 5) -> list[Bar]:
        return self._bars[:n]

    def tail(self, n: int = 5) -> list[Bar]:
        return self._bars[-n:]

    # ----------------------------------------------------------- internals

    @staticmethod
    def _coerce(bars: Iterable[Bar] | pd.DataFrame) -> Iterator[Bar]:
        if isinstance(bars, pd.DataFrame):
            return list(ReplayEngine._df_to_bars(bars))
        return list(bars)

    @staticmethod
    def _df_to_bars(df: pd.DataFrame) -> Iterator[Bar]:
        if df.empty:
            return
        df = df.sort_values("timestamp").reset_index(drop=True)
        for _, row in df.iterrows():
            yield Bar(
                timestamp=row["timestamp"].to_pydatetime(),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
                ticker=str(row.get("ticker", "")),
            )

    def _elapsed_since(self, stats: ReplayStats) -> float:
        # Not tracked internally — placeholder for future use; tests use
        # the injected sleep_fn to assert pacing behaviour.
        return 0.0
