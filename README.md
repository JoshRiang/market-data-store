# market-data-store

Parquet-based time-series market data store with a deterministic replay engine.

## Features

- **Partitioned Parquet storage** — bars are persisted under `data/ticker=<TICKER>/year=<YYYY>/data.parquet`, enabling efficient time- and symbol-scoped reads.
- **Schema versioning** — every parquet write records a `SCHEMA_VERSION` field so future readers can detect schema drift.
- **yfinance loader** — pulls OHLCV history from Yahoo Finance with incremental updates (only fetches data newer than the latest stored bar).
- **Deterministic replay engine** — streams stored bars in chronological order to a strategy callback, with a configurable speed multiplier and bar index.
- **Simple CLI** — `python -m store --ticker SPY --start 2020-01-01 --replay strategy=naive`.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Layout

```
market-data-store/
├── store/
│   ├── __init__.py
│   ├── __main__.py        # CLI entrypoint
│   ├── schema.py          # Bar / Tick / Quote dataclasses
│   ├── storage.py         # Parquet read/write, partitioning
│   ├── loader.py          # yfinance loader with incremental updates
│   ├── replay.py          # Deterministic replay engine
│   └── strategies/
│       ├── __init__.py
│       └── naive.py       # Example strategy for replay
├── tests/
│   ├── test_storage.py
│   └── test_replay.py
├── data/                  # Created at runtime (parquet partitions live here)
├── requirements.txt
└── .gitignore
```

## Usage

### Load data from yfinance (and persist to parquet)

```bash
python -m store --ticker SPY --start 2020-01-01
```

This will:
1. Fetch OHLCV bars from `2020-01-01` to today via yfinance.
2. Persist them under `data/ticker=SPY/year=<YYYY>/data.parquet`.
3. Re-run the command later — only new bars since the latest stored bar are fetched.

### Replay historical bars through a strategy

```bash
python -m store --ticker SPY --start 2020-01-01 --replay strategy=naive --speed 100
```

The naive strategy prints each bar's date and close price.

### Programmatic usage

```python
from store.storage import ParquetStore
from store.loader import YFinanceLoader
from store.replay import ReplayEngine
from store.strategies.naive import NaiveStrategy

# 1) Load + persist
store = ParquetStore(root="data")
loader = YFinanceLoader(store)
loader.ensure_ticker("AAPL", start="2020-01-01")

# 2) Replay
bars = store.read_bars("AAPL")
engine = ReplayEngine(bars, speed=50.0)
engine.run(NaiveStrategy())
```

## Schema

`Bar` is the canonical record:

| Field      | Type        | Notes                          |
|------------|-------------|--------------------------------|
| timestamp  | datetime64[ns, UTC] | Bar close time (UTC)    |
| ticker     | str         | Ticker symbol                  |
| open       | float64     | Open price                     |
| high       | float64     | High price                     |
| low        | float64     | Low price                      |
| close      | float64     | Close price                    |
| volume     | int64       | Bar volume                     |
| schema_version | int     | Storage schema version         |

`Tick` and `Quote` dataclasses are also defined for future use (tick-level and L1 quote data); the current Parquet store focuses on bar data.

## Testing

```bash
python -m pytest tests/ -v
```

## License

MIT
