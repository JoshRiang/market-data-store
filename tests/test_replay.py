"""Tests for the deterministic replay engine."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from store.replay import ReplayEngine
from store.schema import Bar
from store.storage import ParquetStore
from store.strategies.naive import NaiveState, NaiveStrategy


def _make_bars(n: int, start: datetime, ticker: str = "AAPL") -> list[Bar]:
    bars: list[Bar] = []
    for i in range(n):
        ts = start + timedelta(days=i)
        bars.append(
            Bar(
                timestamp=ts,
                open=100.0 + i,
                high=101.0 + i,
                low=99.0 + i,
                close=100.5 + i,
                volume=1000 * (i + 1),
                ticker=ticker,
            )
        )
    return bars


def test_replay_invokes_callback_in_order() -> None:
    bars = _make_bars(5, datetime(2024, 1, 1, tzinfo=timezone.utc))
    engine = ReplayEngine(bars, speed=0)  # speed=0 disables pacing
    seen: list[Bar] = []

    def cb(bar: Bar, state: list[Bar]) -> list[Bar]:
        state.append(bar)
        return state

    state, stats = engine.run(cb, initial_state=[])
    assert stats.bars_emitted == 5
    assert state == bars  # chronological order preserved


def test_replay_sorts_unsorted_input() -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bars = _make_bars(5, start)
    # Shuffle.
    shuffled = [bars[3], bars[0], bars[4], bars[1], bars[2]]
    engine = ReplayEngine(shuffled, speed=0)
    seen: list[Bar] = []

    def cb(bar: Bar, state: list[Bar]) -> list[Bar]:
        state.append(bar)
        return state

    state, _ = engine.run(cb, initial_state=[])
    assert state == bars  # engine must sort before emitting


def test_replay_threads_state() -> None:
    bars = _make_bars(3, datetime(2024, 1, 1, tzinfo=timezone.utc))
    engine = ReplayEngine(bars, speed=0)

    state, stats = engine.run(NaiveStrategy(verbose=False).on_bar, initial_state=NaiveState())
    assert state.bars_seen == 3
    assert state.last_close == pytest.approx(bars[-1].close)
    assert state.max_close == pytest.approx(max(b.close for b in bars))
    assert state.total_volume == sum(b.volume for b in bars)


def test_replay_respects_speed_via_sleep_fn() -> None:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    bars = _make_bars(3, start)
    sleeps: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)

    engine = ReplayEngine(bars, speed=2.0, sleep_fn=fake_sleep)
    state, _ = engine.run(NaiveStrategy(verbose=False).on_bar, initial_state=NaiveState())
    # 2 sleeps (one per gap between 3 bars). Each gap is 86400s / speed 2.0 = 43200s.
    assert len(sleeps) == 2
    for s in sleeps:
        assert s == pytest.approx(86400 / 2.0)


def test_replay_speed_zero_skips_sleep() -> None:
    bars = _make_bars(3, datetime(2024, 1, 1, tzinfo=timezone.utc))
    sleeps: list[float] = []

    engine = ReplayEngine(bars, speed=0, sleep_fn=lambda s: sleeps.append(s))
    engine.run(NaiveStrategy(verbose=False).on_bar, initial_state=NaiveState())
    assert sleeps == []


def test_replay_error_counts_and_continues() -> None:
    bars = _make_bars(3, datetime(2024, 1, 1, tzinfo=timezone.utc))

    def cb(bar: Bar, state: dict) -> dict:
        if bar.volume > 2000:
            raise ValueError("boom")
        state.setdefault("ok", []).append(bar)
        return state

    engine = ReplayEngine(bars, speed=0)
    state, stats = engine.run(cb, initial_state={})
    assert stats.errors == 1  # the 3rd bar has volume 3000
    assert stats.bars_emitted == 2  # only successful callbacks count as "emitted"
    assert len(state["ok"]) == 2


def test_replay_stop_on_error() -> None:
    bars = _make_bars(3, datetime(2024, 1, 1, tzinfo=timezone.utc))

    def cb(bar: Bar, state: list) -> list:
        if bar.volume > 2000:
            raise ValueError("boom")
        state.append(bar)
        return state

    engine = ReplayEngine(bars, speed=0)
    with pytest.raises(ValueError):
        engine.run(cb, initial_state=[], stop_on_error=True)


def test_replay_from_dataframe(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    bars = _make_bars(7, datetime(2024, 1, 1, tzinfo=timezone.utc))
    store.write_bars(bars)
    df = store.read_bars("AAPL")
    assert not df.empty

    engine = ReplayEngine(df, speed=0)
    seen = []

    def cb(bar: Bar, state: list) -> list:
        state.append(bar)
        return state

    state, stats = engine.run(cb, initial_state=[])
    assert stats.bars_emitted == 7
    assert [b.timestamp for b in state] == [b.timestamp for b in bars]


def test_replay_empty_input() -> None:
    engine = ReplayEngine([], speed=0)
    state, stats = engine.run(NaiveStrategy(verbose=False).on_bar, initial_state=NaiveState())
    assert stats.bars_emitted == 0
    assert state.bars_seen == 0


def test_replay_iter_protocol() -> None:
    bars = _make_bars(3, datetime(2024, 1, 1, tzinfo=timezone.utc))
    engine = ReplayEngine(bars, speed=0)
    emitted = [b for b in engine]
    assert emitted == bars
    assert len(engine) == 3
