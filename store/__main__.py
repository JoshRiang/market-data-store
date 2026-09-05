"""CLI for the market-data-store package.

Examples:
    python -m store --ticker SPY --start 2020-01-01
    python -m store --ticker AAPL --start 2021-01-01 --end 2021-12-31
    python -m store --ticker SPY --start 2020-01-01 --replay strategy=naive --speed 100
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .loader import YFinanceLoader
from .replay import ReplayEngine
from .storage import ParquetStore
from .strategies import NaiveStrategy


_STRATEGIES = {
    "naive": NaiveStrategy,
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="store",
        description="Parquet market-data store + replay engine",
    )
    parser.add_argument("--ticker", required=True, help="Ticker symbol, e.g. SPY")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--data-root",
        default="data",
        help="Parquet store root (default: ./data)",
    )
    parser.add_argument(
        "--replay",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Replay options, e.g. strategy=naive. Repeatable.",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Replay speed multiplier (default: 1.0; 0 = no pacing)",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Skip yfinance fetch and only replay what's already on disk.",
    )
    return parser.parse_args(argv)


def _parse_kv(pairs: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for p in pairs:
        if "=" not in p:
            raise SystemExit(f"--replay expects KEY=VALUE, got: {p!r}")
        k, v = p.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    store = ParquetStore(root=args.data_root)
    if not args.no_fetch:
        print(f"[store] fetching {args.ticker} from {args.start}…", file=sys.stderr)
        loader = YFinanceLoader(store)
        n = loader.ensure_ticker(args.ticker, start=args.start, end=args.end)
        print(f"[store] persisted {n} rows for {args.ticker}", file=sys.stderr)

    if args.replay:
        replay_opts = _parse_kv(args.replay)
        strategy_name = replay_opts.get("strategy", "naive")
        if strategy_name not in _STRATEGIES:
            print(f"unknown strategy: {strategy_name}", file=sys.stderr)
            print(f"available: {sorted(_STRATEGIES)}", file=sys.stderr)
            return 2
        strategy_cls = _STRATEGIES[strategy_name]

        df = store.read_bars(args.ticker)
        if df.empty:
            print(f"[store] no data on disk for {args.ticker}; nothing to replay", file=sys.stderr)
            return 1
        engine = ReplayEngine(df, speed=args.speed)
        print(
            f"[store] replaying {len(engine)} bars for {args.ticker} "
            f"with strategy={strategy_name} speed={args.speed}",
            file=sys.stderr,
        )
        strategy = strategy_cls(verbose=True)
        state, stats = engine.run(strategy.on_bar)
        print(
            f"[store] replay done: {stats.bars_emitted} bars, {stats.errors} errors",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
