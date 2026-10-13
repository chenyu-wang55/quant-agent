from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.ingestion.sec_facts import FreeDataError  # noqa: E402
from services.ingestion.twelve_prices import TwelvePriceClient, read_price_key  # noqa: E402
from services.research.price_data import canonical, digest, load_price_dataset, save_price_dataset  # noqa: E402
from services.research.price_only import PriceResearchConfig, run_price_study  # noqa: E402


def _private_atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        handle.write(raw)
        temporary = Path(handle.name)
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _report_markdown(report: dict) -> str:
    base = report["backtest"]
    lines = ["# 日频价格研究结果", "", f"数据截止：{report['selection']['data_date']}。策略：{report['config']['strategy']}。",
             "", "这是明确股票池的技术研究版本，存在股票池选择和幸存者偏差；不能用作完整指数策略或自动交易验收。",
             "", "| 指标 | 策略 | SPY 基准 |", "|---|---:|---:|"]
    for title, key in (("累计收益", "total_return"), ("年化收益（252交易日）", "annualized_return"),
                       ("最大回撤", "max_drawdown"), ("年化波动率", "annualized_volatility")):
        lines.append(f"| {title} | {base['strategy'][key]:.2%} | {base['benchmark'][key]:.2%} |")
    lines += ["", f"策略零无风险利率 Sharpe：{base['strategy']['sharpe_zero_risk_free']:.3f}。",
              f"模拟费用/滑点：${base['total_costs']:.2f}；卖出成交腿：{base['sell_legs']}，不是独立完整交易次数。",
              "", "## 最近一期候选", "", "| 标的 | 板块 | 规则分 | 63日超额动量 | 目标权重 |", "|---|---|---:|---:|---:|"]
    for row in report["selection"]["candidates"]:
        if row["target_weight"] > 0:
            lines.append(f"| {row['ticker']} | {row['sector']} | {row['rule_score']:.4f} | {row['relative_strength_63d']:.2%} | {row['target_weight']:.0%} |")
    if report["selection"]["selected_count"] == 0:
        lines.append("| 无合格候选 | 风险状态或趋势门槛未满足 | — | — | 0% |")
    lines += ["", f"现金权重：{report['selection']['cash_weight']:.0%}。所有候选只用于研究，不生成可执行订单。",
              "", "## 成本压力", "", "| 倍数 | 策略年化 | 策略回撤 |", "|---|---:|---:|"]
    for multiplier, result in report["cost_stress"].items():
        lines.append(f"| {multiplier}× | {result['strategy']['annualized_return']:.2%} | {result['strategy']['max_drawdown']:.2%} |")
    lines += ["", "详细分段、日净值、模拟成交和输入指纹保存在 report.json；方法和限制见 docs/price_research.md。", ""]
    return "\n".join(lines)


def _csv_bytes(rows: list[dict]) -> bytes:
    buffer = io.StringIO()
    if rows:
        writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return buffer.getvalue().encode()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Frozen free-data daily research; never enables trading")
    parser.add_argument("--config", type=Path, default=ROOT / "docs/manifests/price_research_v1.json")
    parser.add_argument("--dataset", type=Path, help="reuse an immutable full dataset without network")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/price_research")
    args = parser.parse_args(argv)
    attempt_path = args.output_dir / "latest_attempt.json"
    try:
        _private_atomic_write(attempt_path, canonical({"status": "collecting", "execution_allowed": False}))
        config_raw = args.config.read_bytes()
        config = PriceResearchConfig.model_validate_json(config_raw)
        if config.selection_at > datetime.now(timezone.utc):
            raise FreeDataError("selection cutoff is in the future")
        config_hash = digest(config.model_dump(mode="json"))
        frozen = args.output_dir / "frozen" / f"{config_hash}.json"
        if frozen.exists() and frozen.read_bytes() != canonical(config.model_dump(mode="json")):
            raise FreeDataError("frozen price strategy config changed")
        if not frozen.exists():
            _private_atomic_write(frozen, canonical(config.model_dump(mode="json")))
        if args.dataset:
            dataset = load_price_dataset(args.dataset)
        else:
            key = read_price_key(ROOT)
            raw_by_symbol = {}
            with TwelvePriceClient(key, quota_path=args.output_dir / "quota.sqlite") as client:
                for symbol in sorted(set(config.securities) | {config.benchmark}):
                    raw_by_symbol[symbol] = client.daily(symbol, config.data_start, config.data_end)
                    print(json.dumps({"phase": "collecting", "symbol": symbol, "completed": len(raw_by_symbol),
                                      "required": len(config.securities) + 1}), file=sys.stderr)
            _, dataset = save_price_dataset(raw_by_symbol, start=config.data_start, end=config.data_end,
                                            output_dir=args.output_dir / "datasets")
        if dataset.manifest["source_kind"] != "real_twelvedata":
            raise FreeDataError("synthetic or demo sources cannot produce a real research report")
        report = run_price_study(dataset, config)
        if args.config.read_bytes() != config_raw:
            raise FreeDataError("price strategy config changed during collection or calculation")
        report_dir = args.output_dir / "runs" / report["run_id"]
        raw = canonical(report)
        if report_dir.exists():
            if (report_dir / "report.json").read_bytes() != raw:
                raise FreeDataError("existing study ID has different report bytes")
        else:
            report_dir.mkdir(parents=True, mode=0o700)
            _private_atomic_write(report_dir / "report.json", raw)
            _private_atomic_write(report_dir / "report.md", _report_markdown(report).encode())
            _private_atomic_write(report_dir / "trades.csv", _csv_bytes(report["backtest"]["trades"]))
            _private_atomic_write(report_dir / "equity.csv", _csv_bytes(report["backtest"]["equity_curve"]))
        _private_atomic_write(args.output_dir / "latest.json", raw)
        _private_atomic_write(attempt_path, canonical({"status": "completed_research", "run_id": report["run_id"],
                                                       "execution_allowed": False}))
        print(json.dumps({"status": report["status"], "report_path": str(report_dir / "report.md"),
                          "run_id": report["run_id"], "dataset_fingerprint": report["dataset_fingerprint"],
                          "selection_data_date": report["selection"]["data_date"],
                          "selected_count": report["selection"]["selected_count"],
                          "backtest_metrics": report["backtest"]["strategy"], "execution_allowed": False}, indent=2))
        return 0
    except Exception as exc:
        reason = str(exc) if isinstance(exc, FreeDataError) else type(exc).__name__
        blocked = {"status": "blocked", "reason": reason, "execution_allowed": False}
        try:
            _private_atomic_write(attempt_path, canonical(blocked))
        except OSError:
            pass
        print(json.dumps(blocked, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
