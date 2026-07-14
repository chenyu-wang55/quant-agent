from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from services.ingestion.point_in_time_validator import (
    PointInTimeDatasetPaths,
    PointInTimeValidationError,
    parse_zoned_timestamp,
    validate_point_in_time_dataset,
)


def _path_argument(value: str | None, env_name: str) -> Path:
    configured = (value or os.getenv(env_name) or "").strip()
    if not configured:
        raise PointInTimeValidationError(
            f"provide --{env_name.removeprefix('POINT_IN_TIME_').removesuffix('_CSV').lower().replace('_', '-')} "
            f"or set {env_name}"
        )
    return Path(configured)


def _required_universes(values: list[str] | None) -> dict[str, int]:
    if values:
        result: dict[str, int] = {}
        for value in values:
            try:
                name, minimum_text = value.split(":", 1)
                minimum = int(minimum_text)
            except (TypeError, ValueError) as exc:
                raise PointInTimeValidationError(
                    f"invalid --required-universe value {value!r}; use NAME:MINIMUM"
                ) from exc
            if not name.strip() or minimum < 1:
                raise PointInTimeValidationError(f"invalid --required-universe value {value!r}; use NAME:MINIMUM")
            result[name.strip().upper()] = minimum
        return result

    configured = os.getenv("POINT_IN_TIME_REQUIRED_UNIVERSES", "SP500,NASDAQ100")
    defaults = {"SP500": 450, "NASDAQ100": 90}
    result = {}
    for raw_name in configured.split(","):
        name = raw_name.strip().upper()
        if not name:
            continue
        minimum = int(os.getenv(f"POINT_IN_TIME_MIN_{name}_CONSTITUENTS", str(defaults.get(name, 1))))
        if minimum < 1:
            raise PointInTimeValidationError(f"minimum constituent count for {name} must be positive")
        result[name] = minimum
    if not result:
        raise PointInTimeValidationError("POINT_IN_TIME_REQUIRED_UNIVERSES cannot be empty")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fail-closed validation for licensed point-in-time quant CSV inputs.")
    parser.add_argument("--start", required=True, help="backtest range start with explicit timezone")
    parser.add_argument("--end", required=True, help="backtest range end with explicit timezone")
    parser.add_argument("--universe", help="universe membership CSV path")
    parser.add_argument("--fundamentals", help="fundamentals CSV path")
    parser.add_argument("--events", help="events CSV path")
    parser.add_argument("--earnings", help="earnings calendar CSV path")
    parser.add_argument(
        "--required-universe",
        action="append",
        metavar="NAME:MINIMUM",
        help="required universe and minimum active constituents; repeat for multiple universes",
    )
    parser.add_argument(
        "--max-fundamental-age-days",
        type=int,
        default=550,
        help="maximum age of the latest available fundamental record on every trading date",
    )
    parser.add_argument("--output", help="optional path for the immutable JSON validation report")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        start = parse_zoned_timestamp(args.start, field="--start")
        end = parse_zoned_timestamp(args.end, field="--end")
        paths = PointInTimeDatasetPaths(
            universe=_path_argument(args.universe, "POINT_IN_TIME_UNIVERSE_CSV"),
            fundamentals=_path_argument(args.fundamentals, "POINT_IN_TIME_FUNDAMENTALS_CSV"),
            events=_path_argument(args.events, "POINT_IN_TIME_EVENTS_CSV"),
            earnings=_path_argument(args.earnings, "POINT_IN_TIME_EARNINGS_CSV"),
        )
        report = validate_point_in_time_dataset(
            paths=paths,
            start=start,
            end=end,
            required_universes=_required_universes(args.required_universe),
            max_fundamental_age_days=args.max_fundamental_age_days,
        )
    except (PointInTimeValidationError, ValueError) as exc:
        print(json.dumps({"status": "blocked", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2

    payload = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
