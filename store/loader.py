"""yfinance OHLCV loader with incremental updates."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from .schema import Bar
from .storage import ParquetStore


class YFinanceLoader:
    """Pull OHLCV history from Yahoo Finance into the ParquetStore.

    Incremental behaviour: if the store already has data for the ticker, only
    rows newer than the most recent stored timestamp are requested. The
    full date range is requested on first call.
    """

    def __init__(self, store: ParquetStore) -> None:
        self.store = store

    # ------------------------------------------------------------- public API

    def ensure_ticker(
        self,
        ticker: str,
        start: str | datetime,
        end: Optional[str | datetime] = None,
        progress: bool = False,
    ) -> int:
        """Ensure bars for `ticker` from `start` are in the store.

        Returns the number of rows persisted (after merge).
        """
        import yfinance as yf  # imported lazily so library is optional at import time

        start_ts = self._coerce_date(start)
        latest = self.store.latest_timestamp(ticker)

        if latest is not None:
            # Request only data strictly after the latest stored bar.
            fetch_start = (pd.Timestamp(latest).tz_convert(None) + pd.Timedelta(days=1)).to_pydatetime()
            # If fetch_start is in the future relative to start_ts we still honour it,
            # but guard against pulling the whole history when latest >= start.
            if fetch_start >= pd.Timestamp.now(tz=timezone.utc).tz_convert(None).to_pydatetime():
                return 0
            request_start = fetch_start
        else:
            request_start = start_ts

        request_end = self._coerce_date(end) if end is not None else None

        df = yf.download(
            tickers=ticker,
            start=request_start.strftime("%Y-%m-%d"),
            end=request_end.strftime("%Y-%m-%d") if request_end else None,
            progress=progress,
            auto_adjust=False,  # keep raw 'Adj Close' for fidelity
            actions=False,
            threads=False,
        )

        if df.empty:
            return 0

        # yfinance sometimes returns a MultiIndex columns frame even for a single ticker.
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        bars = self._dataframe_to_bars(df, ticker)
        return self.store.write_bars(bars)

    def refresh_all(self, tickers: list[str], start: str | datetime) -> dict[str, int]:
        """Refresh each ticker. Returns a {ticker: rows_written} map."""
        out: dict[str, int] = {}
        for t in tickers:
            try:
                out[t] = self.ensure_ticker(t, start=start)
            except Exception as exc:  # noqa: BLE001
                out[t] = -1  # sentinel for failure
        return out

    # -------------------------------------------------------------- helpers

    @staticmethod
    def _coerce_date(d: str | datetime) -> datetime:
        if isinstance(d, str):
            ts = pd.Timestamp(d)
        elif isinstance(d, datetime):
            ts = pd.Timestamp(d)
        else:
            raise TypeError(f"Unsupported date type: {type(d).__name__}")
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.to_pydatetime()

    @staticmethod
    def _dataframe_to_bars(df: pd.DataFrame, ticker: str) -> list[Bar]:
        """Convert a yfinance OHLCV dataframe into Bar dataclasses."""
        # yfinance returns columns: Open, High, Low, Close, Adj Close, Volume
        # Index is DatetimeIndex (tz-aware or naive). Normalise to UTC.
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        idx = df.index
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        else:
            idx = idx.tz_convert("UTC")

        bars: list[Bar] = []
        for ts, row in zip(idx, df.itertuples(index=False)):
            # itertuples() returns a namedtuple with attribute names matching
            # columns; access safely because the column order can vary.
            open_ = float(getattr(row, "Open"))
            high = float(getattr(row, "High"))
            low = float(getattr(row, "Low"))
            close = float(getattr(row, "Close"))
            vol_raw = getattr(row, "Volume")
            volume = int(vol_raw) if pd.notna(vol_raw) else 0
            bars.append(
                Bar(
                    timestamp=ts.to_pydatetime(),
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                    ticker=ticker.upper(),
                )
            )
        return bars
