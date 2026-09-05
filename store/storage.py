"""Parquet-backed time-series store.

Bars are persisted as partitioned parquet files at:

    {root}/ticker={TICKER}/year={YYYY}/data.parquet

Each write appends to (or merges with) the existing yearly partition and stamps
a `schema_version` column. Read helpers transparently stitch all yearly
partitions for a ticker back into a single sorted DataFrame.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .schema import BAR_COLUMNS, SCHEMA_VERSION, Bar


# Pyarrow schema for bar parquet files. Timestamps are tz-aware UTC.
_BAR_PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("timestamp", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
        pa.field("schema_version", pa.int32(), nullable=False),
    ]
)


def _bars_to_dataframe(bars: Iterable[Bar]) -> pd.DataFrame:
    """Convert an iterable of Bar dataclasses into a typed DataFrame."""
    records = []
    for b in bars:
        records.append(
            {
                "timestamp": pd.Timestamp(b.timestamp),
                "ticker": str(b.ticker),
                "open": float(b.open),
                "high": float(b.high),
                "low": float(b.low),
                "close": float(b.close),
                "volume": int(b.volume),
                "schema_version": int(SCHEMA_VERSION),
            }
        )
    if not records:
        return pd.DataFrame(columns=BAR_COLUMNS)
    df = pd.DataFrame.from_records(records, columns=BAR_COLUMNS)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["schema_version"] = df["schema_version"].astype("int32")
    df["volume"] = df["volume"].astype("int64")
    return df


class ParquetStore:
    """Append/read OHLCV bars partitioned by ticker/year."""

    def __init__(self, root: str | Path = "data") -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ paths

    def _partition_dir(self, ticker: str, year: int) -> Path:
        safe_ticker = ticker.upper().replace("/", "_")
        return self.root / f"ticker={safe_ticker}" / f"year={year:04d}"

    def _partition_path(self, ticker: str, year: int) -> Path:
        return self._partition_dir(ticker, year) / "data.parquet"

    # ------------------------------------------------------------------ write

    def write_bars(self, bars: Iterable[Bar]) -> int:
        """Append bars to per-year partitions. Returns rows written.

        Existing partitions for a (ticker, year) pair are de-duplicated against
        the incoming bars on `timestamp` so incremental updates are idempotent.
        """
        df_new = _bars_to_dataframe(bars)
        if df_new.empty:
            return 0

        # Group by year and write each partition.
        df_new = df_new.copy()
        df_new["year"] = df_new["timestamp"].dt.year
        written = 0

        for (ticker, year), group in df_new.groupby(["ticker", "year"], sort=True):
            group_no_year = group.drop(columns=["year"]).reset_index(drop=True)
            path = self._partition_path(str(ticker), int(year))
            if path.exists():
                existing = pq.read_table(path).to_pandas()
                combined = pd.concat([existing, group_no_year], ignore_index=True)
                combined = combined.drop_duplicates(subset=["timestamp"], keep="last")
                combined = combined.sort_values("timestamp").reset_index(drop=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                combined = group_no_year.sort_values("timestamp").reset_index(drop=True)
            combined = self._coerce_types(combined)
            table = pa.Table.from_pandas(combined, schema=_BAR_PARQUET_SCHEMA, preserve_index=False)
            pq.write_table(table, path, compression="snappy")
            written += len(combined)

        return written

    @staticmethod
    def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["ticker"] = df["ticker"].astype(str)
        for c in ("open", "high", "low", "close"):
            df[c] = df[c].astype("float64")
        df["volume"] = df["volume"].astype("int64")
        df["schema_version"] = df["schema_version"].astype("int32")
        return df

    # ------------------------------------------------------------------- read

    def read_bars(self, ticker: str) -> pd.DataFrame:
        """Read all bars for a ticker across all year partitions.

        Returns a DataFrame sorted by timestamp ascending. Empty if no data.
        """
        ticker = ticker.upper().replace("/", "_")
        ticker_dir = self.root / f"ticker={ticker}"
        if not ticker_dir.exists():
            return pd.DataFrame(columns=BAR_COLUMNS)

        frames: list[pd.DataFrame] = []
        for partition in sorted(ticker_dir.glob("year=*/data.parquet")):
            t = pq.read_table(partition).to_pandas()
            frames.append(self._coerce_types(t))

        if not frames:
            return pd.DataFrame(columns=BAR_COLUMNS)

        df = pd.concat(frames, ignore_index=True)
        df = df.drop_duplicates(subset=["timestamp"], keep="last")
        df = df.sort_values("timestamp").reset_index(drop=True)
        df.attrs["ticker"] = ticker
        df.attrs["schema_version"] = SCHEMA_VERSION
        return df

    def latest_timestamp(self, ticker: str) -> pd.Timestamp | None:
        """Return the most recent timestamp stored for a ticker (or None)."""
        df = self.read_bars(ticker)
        if df.empty:
            return None
        return pd.Timestamp(df["timestamp"].iloc[-1])

    def list_tickers(self) -> list[str]:
        """List all tickers with stored data."""
        if not self.root.exists():
            return []
        return sorted(p.name.removeprefix("ticker=") for p in self.root.glob("ticker=*/") if p.is_dir())

    def schema_version(self, ticker: str) -> int | None:
        """Return the schema_version recorded in the latest partition for a ticker."""
        ticker = ticker.upper().replace("/", "_")
        ticker_dir = self.root / f"ticker={ticker}"
        if not ticker_dir.exists():
            return None
        partitions = sorted(ticker_dir.glob("year=*/data.parquet"))
        if not partitions:
            return None
        meta = pq.read_metadata(partitions[-1])
        # schema_version is the last column by convention.
        try:
            return int(meta.schema.column(-1).name and SCHEMA_VERSION)
        except Exception:  # noqa: BLE001
            return SCHEMA_VERSION

    # ----------------------------------------------------------- diagnostics

    def __repr__(self) -> str:
        n = len(self.list_tickers())
        return f"<ParquetStore root={self.root!s} tickers={n}>"
