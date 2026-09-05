"""Schema definitions for market data records.

`Bar` is the canonical OHLCV record used by the Parquet store and replay engine.
`Tick` and `Quote` are defined for future tick-level and L1-quote ingestion paths.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

# Bump whenever the on-disk parquet schema changes. The store stamps every
# write with this so readers can detect schema drift and reject incompatible
# files (or migrate them).
SCHEMA_VERSION: int = 1


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(frozen=True)
class Bar:
    """One OHLCV bar (candle) for a single ticker."""

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    ticker: str = ""
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        # Ensure timestamp is timezone-aware UTC so parquet round-trips are stable.
        ts = self.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
            object.__setattr__(self, "timestamp", ts)
        elif ts.tzinfo != timezone.utc:
            ts = ts.astimezone(timezone.utc)
            object.__setattr__(self, "timestamp", ts)
        # Cast volume to int (yfinance returns numpy ints sometimes).
        if not isinstance(self.volume, int):
            object.__setattr__(self, "volume", int(self.volume))

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # Strip tz for parquet; pyarrow will store as tz-aware timestamp.
        return d


@dataclass(frozen=True)
class Tick:
    """One trade tick (future use)."""

    timestamp: datetime
    ticker: str
    price: float
    size: int
    side: str = ""  # 'buy' / 'sell' / '' (unknown)
    schema_version: int = SCHEMA_VERSION


@dataclass(frozen=True)
class Quote:
    """One top-of-book quote (future use)."""

    timestamp: datetime
    ticker: str
    bid: float
    ask: float
    bid_size: int = 0
    ask_size: int = 0
    schema_version: int = SCHEMA_VERSION


# Ordered column list for parquet writes/reads. Keep this stable across versions.
BAR_COLUMNS: list[str] = [
    "timestamp",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "schema_version",
]
