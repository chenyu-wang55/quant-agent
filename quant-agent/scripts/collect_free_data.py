from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.ingestion.free_dataset import collect_free_dataset, verify_free_dataset  # noqa: E402
from services.ingestion.free_sources import (  # noqa: E402
    SECClient,
    download_stooq_bars,
    download_yahoo_bars,
    normalized_ticker,
)
from services.ingestion.point_in_time_validator import parse_zoned_timestamp  # noqa: E402
from services.ingestion.sec_facts import FreeDataError, normalized_cik  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect free SEC/Yahoo research data; never enables production trading.")
    parser.add_argument("--security", action="append", metavar="TICKER:CIK", help="explicit research issuer; repeat")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--price-source", choices=("yfinance", "stooq"), default="yfinance",
                        help="explicit free daily-price source; never switches silently")
    parser.add_argument("--start", help="first daily bar date, YYYY-MM-DD")
    parser.add_argument("--end", help="last daily bar date, YYYY-MM-DD")
    parser.add_argument("--decision-at", help="decision cutoff with explicit timezone")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts" / "free_data")
    parser.add_argument("--sec-user-agent", default=os.getenv(
        "SEC_USER_AGENT", "QuantAgentResearch/0.1 (+https://github.com/chenyu-wang55/quant-agent)"),
        help="client identity for SEC fair access; configure contact details in SEC_USER_AGENT")
    parser.add_argument("--verify", type=Path, help="verify an existing immutable snapshot offline, without any network")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.verify:
            if args.security or args.start or args.end or args.decision_at:
                raise FreeDataError("--verify cannot be combined with collection arguments")
            report = verify_free_dataset(args.verify)
            print(json.dumps({"archive_integrity": "verified", "status": report["status"],
                              "production_ready": False, "dataset_fingerprint": report["dataset_fingerprint"]}))
            return 0
        if not args.security or not args.start or not args.end or not args.decision_at:
            raise FreeDataError("collection requires --security, --start, --end and --decision-at")
        securities: dict[str, str] = {}
        for item in args.security:
            ticker, cik = item.split(":", 1)
            ticker = normalized_ticker(ticker)
            if ticker in securities:
                raise FreeDataError("duplicate --security ticker")
            securities[ticker] = normalized_cik(cik)
        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
        decision_at = parse_zoned_timestamp(args.decision_at, field="--decision-at")
        if args.price_source == "yfinance":
            import yfinance as yf

            # Keep vendor caches within the designated research directory.
            yf.set_tz_cache_location(str(args.output_dir.expanduser().resolve() / ".yahoo-cache"))
        fetch_bars = download_stooq_bars if args.price_source == "stooq" else download_yahoo_bars
        with SECClient(args.sec_user_agent) as sec:
            path, report = collect_free_dataset(
                securities=securities, benchmark=args.benchmark, start=start, end=end,
                decision_at=decision_at, output_dir=args.output_dir, fetch_sec=sec.issuer, fetch_bars=fetch_bars,
            )
        print(json.dumps({"status": report["status"], "snapshot_path": str(path),
                          "dataset_fingerprint": report["dataset_fingerprint"], "production_ready": False,
                          "paid_api_calls": 0, "summary": report["summary"], "errors": report["errors"],
                          "missing_requirements": report["missing_requirements"]}, ensure_ascii=False, indent=2))
        return 2 if report["errors"] else 0
    except (ValueError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({"status": "blocked", "error_type": type(exc).__name__, "production_ready": False,
                          "error": str(exc) if isinstance(exc, FreeDataError) else "invalid input or archive"}),
              file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
