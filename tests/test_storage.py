"""Round-trip tests for the Parquet store."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from store.schema import BAR_COLUMNS, SCHEMA_VERSION, Bar
from store.storage import ParquetStore


def _make_bars(ticker: str, n: int, start: datetime) -> list[Bar]:
    bars: list[Bar] = []
    for i in range(n):
        ts = start + timedelta(days=i)
        bars.append(
            Bar(
                timestamp=ts,
                open=100.0 + i * 0.1,
                high=101.0 + i * 0.1,
                low=99.0 + i * 0.1,
                close=100.5 + i * 0.1,
                volume=1_000_000 + i * 100,
                ticker=ticker,
            )
        )
    return bars


def test_partition_layout(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    start = datetime(2024, 1, 2, tzinfo=timezone.utc)
    bars = _make_bars("AAPL", 5, start)
    written = store.write_bars(bars)
    assert written == 5

    partition = tmp_path / "ticker=AAPL" / "year=2024" / "data.parquet"
    assert partition.exists(), f"expected partition at {partition}"
    assert partition.is_file()


def test_round_trip_preserves_values(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    start = datetime(2023, 6, 1, tzinfo=timezone.utc)
    bars = _make_bars("SPY", 10, start)
    store.write_bars(bars)

    df = store.read_bars("SPY")
    assert len(df) == 10
    for i, bar in enumerate(bars):
        row = df.iloc[i]
        assert row["timestamp"] == pd.Timestamp(bar.timestamp)
        assert row["open"] == pytest.approx(bar.open)
        assert row["high"] == pytest.approx(bar.high)
        assert row["low"] == pytest.approx(bar.low)
        assert row["close"] == pytest.approx(bar.close)
        assert int(row["volume"]) == bar.volume
        assert row["ticker"] == "SPY"


def test_year_partitioning(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    bars_2023 = _make_bars("QQQ", 3, datetime(2023, 12, 29, tzinfo=timezone.utc))
    bars_2024 = _make_bars("QQQ", 3, datetime(2024, 1, 1, tzinfo=timezone.utc))
    store.write_bars(bars_2023 + bars_2024)

    p23 = tmp_path / "ticker=QQQ" / "year=2023" / "data.parquet"
    p24 = tmp_path / "ticker=QQQ" / "year=2024" / "data.parquet"
    assert p23.exists()
    assert p24.exists()

    df = store.read_bars("QQQ")
    assert len(df) == 6
    assert df["timestamp"].dt.year.tolist() == [2023, 2023, 2023, 2024, 2024, 2024]


def test_incremental_is_idempotent(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    bars = _make_bars("AAPL", 5, datetime(2024, 1, 1, tzinfo=timezone.utc))
    store.write_bars(bars)
    # Write the same bars again — should not duplicate.
    store.write_bars(bars)
    df = store.read_bars("AAPL")
    assert len(df) == 5


def test_incremental_adds_new_bars(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    initial = _make_bars("AAPL", 5, datetime(2024, 1, 1, tzinfo=timezone.utc))
    store.write_bars(initial)
    extra = _make_bars("AAPL", 3, datetime(2024, 1, 6, tzinfo=timezone.utc))
    store.write_bars(extra)
    df = store.read_bars("AAPL")
    assert len(df) == 8


def test_schema_version_stamped(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    store.write_bars(_make_bars("MSFT", 2, datetime(2024, 1, 1, tzinfo=timezone.utc)))
    partition = tmp_path / "ticker=MSFT" / "year=2024" / "data.parquet"
    table = pq.read_table(partition)
    assert "schema_version" in table.schema.names
    assert table.column("schema_version").to_pylist() == [SCHEMA_VERSION, SCHEMA_VERSION]


def test_columns_present(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    store.write_bars(_make_bars("GOOG", 2, datetime(2024, 1, 1, tzinfo=timezone.utc)))
    df = store.read_bars("GOOG")
    for c in BAR_COLUMNS:
        assert c in df.columns, f"missing column: {c}"


def test_list_tickers(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    assert store.list_tickers() == []
    store.write_bars(_make_bars("AAPL", 2, datetime(2024, 1, 1, tzinfo=timezone.utc)))
    store.write_bars(_make_bars("MSFT", 2, datetime(2024, 1, 1, tzinfo=timezone.utc)))
    assert sorted(store.list_tickers()) == ["AAPL", "MSFT"]


def test_latest_timestamp(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    assert store.latest_timestamp("EMPTY") is None
    bars = _make_bars("SPY", 4, datetime(2024, 1, 1, tzinfo=timezone.utc))
    store.write_bars(bars)
    latest = store.latest_timestamp("SPY")
    assert latest == pd.Timestamp(bars[-1].timestamp)


def test_empty_read(tmp_path: Path) -> None:
    store = ParquetStore(root=tmp_path)
    df = store.read_bars("NOPE")
    assert df.empty
    assert list(df.columns) == BAR_COLUMNS
