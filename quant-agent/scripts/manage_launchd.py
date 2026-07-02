from __future__ import annotations

import argparse
import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_LABEL = "com.quant-agent.system-cycle-loop"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def launch_agents_dir(home: Path | None = None) -> Path:
    return (home or Path.home()) / "Library" / "LaunchAgents"


def default_plist_path(label: str = DEFAULT_LABEL, home: Path | None = None) -> Path:
    return launch_agents_dir(home=home) / f"{label}.plist"


def default_log_dir(home: Path | None = None) -> Path:
    return (home or Path.home()) / "Library" / "Logs"


def build_program_arguments(args: argparse.Namespace) -> list[str]:
    program_args = [
        args.python,
        "-m",
        "apps.worker.main",
        "system_cycle_loop",
        "--top-n",
        str(args.top_n),
        "--interval-seconds",
        str(args.interval_seconds),
        "--max-consecutive-errors",
        str(args.max_consecutive_errors),
        "--max-broker-sync-items",
        str(args.max_broker_sync_items),
        "--position-reconciliation-qty-tolerance",
        str(args.position_reconciliation_qty_tolerance),
        "--auto-execution-mode",
        args.auto_execution_mode,
        "--auto-approve-min-confidence",
        str(args.auto_approve_min_confidence),
        "--auto-approve-min-composite",
        str(args.auto_approve_min_composite),
        "--max-auto-approvals",
        str(args.max_auto_approvals),
        "--max-auto-buys",
        str(args.max_auto_buys),
        "--max-auto-sells",
        str(args.max_auto_sells),
        "--order-dedupe-minutes",
        str(args.order_dedupe_minutes),
        "--sell-alert-cooldown-minutes",
        str(args.sell_alert_cooldown_minutes),
        "--rebuy-cooldown-minutes",
        str(args.rebuy_cooldown_minutes),
        "--min-snapshot-bar-coverage",
        str(args.min_snapshot_bar_coverage),
        "--min-snapshot-fundamental-coverage",
        str(args.min_snapshot_fundamental_coverage),
        "--max-snapshot-bar-age-minutes",
        str(args.max_snapshot_bar_age_minutes),
        "--account-equity",
        str(args.account_equity),
        "--max-open-risk-pct",
        str(args.max_open_risk_pct),
        "--max-daily-realized-loss-pct",
        str(args.max_daily_realized_loss_pct),
        "--max-auto-buy-price-drift-pct",
        str(args.max_auto_buy_price_drift_pct),
        "--max-position-reconciliation-age-minutes",
        str(args.max_position_reconciliation_age_minutes),
        "--risk-per-trade-pct",
        str(args.risk_per_trade_pct),
        "--max-position-pct",
        str(args.max_position_pct),
        "--max-gross-exposure-pct",
        str(args.max_gross_exposure_pct),
        "--max-sector-exposure-pct",
        str(args.max_sector_exposure_pct),
    ]
    if args.min_confidence is not None:
        program_args.extend(["--min-confidence", str(args.min_confidence)])
    if args.use_autopilot_policy:
        program_args.append("--use-autopilot-policy")
    if args.disable_auto_broker_sync:
        program_args.append("--disable-auto-broker-sync")
    if args.disable_auto_position_reconciliation:
        program_args.append("--disable-auto-position-reconciliation")
    if args.auto_execute_approved:
        program_args.append("--auto-execute-approved")
    if args.auto_approve_recommendations:
        program_args.append("--auto-approve-recommendations")
    if args.require_position_reconciliation:
        program_args.append("--require-position-reconciliation")
    if args.consume_events:
        program_args.append("--consume-events")
    if args.stop_on_error:
        program_args.append("--stop-on-error")
    return program_args


def build_plist(args: argparse.Namespace, home: Path | None = None) -> dict[str, Any]:
    label = args.label
    logs = default_log_dir(home=home)
    env = {
        "PYTHONUNBUFFERED": "1",
        "DATA_PROVIDER": args.data_provider,
    }
    if args.access_password:
        env["QUANT_AGENT_ACCESS_PASSWORD"] = args.access_password

    return {
        "Label": label,
        "ProgramArguments": build_program_arguments(args),
        "WorkingDirectory": str(Path(args.repo_root).resolve()),
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(logs / f"{label}.out.log"),
        "StandardErrorPath": str(logs / f"{label}.err.log"),
        "EnvironmentVariables": env,
    }


def write_plist(plist: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(plist, sort_keys=False))


def install(args: argparse.Namespace) -> Path:
    path = Path(args.plist_path).expanduser() if args.plist_path else default_plist_path(args.label)
    default_log_dir().mkdir(parents=True, exist_ok=True)
    write_plist(build_plist(args), path)
    if args.load:
        subprocess.run(["launchctl", "load", str(path)], check=True)
    return path


def uninstall(args: argparse.Namespace) -> Path:
    path = Path(args.plist_path).expanduser() if args.plist_path else default_plist_path(args.label)
    if args.unload and path.exists():
        subprocess.run(["launchctl", "unload", str(path)], check=False)
    if path.exists():
        path.unlink()
    return path


def status(args: argparse.Namespace) -> int:
    result = subprocess.run(
        ["launchctl", "list", args.label],
        check=False,
        text=True,
        capture_output=True,
    )
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip(), file=sys.stderr)
    return result.returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage quant-agent macOS launchd worker service")
    parser.add_argument("action", choices=["render", "install", "uninstall", "status"])
    parser.add_argument("--label", default=DEFAULT_LABEL)
    parser.add_argument("--plist-path", default=None, help="override plist path; defaults to ~/Library/LaunchAgents")
    parser.add_argument("--repo-root", default=str(repo_root()))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--data-provider", default=os.getenv("DATA_PROVIDER", "yfinance"))
    parser.add_argument("--access-password", default=os.getenv("QUANT_AGENT_ACCESS_PASSWORD"))
    parser.add_argument("--top-n", type=int, default=8)
    parser.add_argument("--min-confidence", type=float, default=0.0)
    parser.add_argument("--interval-seconds", type=float, default=300.0)
    parser.add_argument("--max-consecutive-errors", type=int, default=3)
    parser.add_argument("--disable-auto-broker-sync", action="store_true")
    parser.add_argument("--max-broker-sync-items", type=int, default=50)
    parser.add_argument("--disable-auto-position-reconciliation", action="store_true")
    parser.add_argument("--position-reconciliation-qty-tolerance", type=float, default=1e-6)
    parser.add_argument("--use-autopilot-policy", action="store_true")
    parser.add_argument("--auto-approve-recommendations", action="store_true")
    parser.add_argument("--auto-approve-min-confidence", type=float, default=0.72)
    parser.add_argument("--auto-approve-min-composite", type=float, default=0.0)
    parser.add_argument("--max-auto-approvals", type=int, default=1)
    parser.add_argument("--auto-execute-approved", action="store_true")
    parser.add_argument("--auto-execution-mode", choices=["paper", "live_dry_run"], default="paper")
    parser.add_argument("--max-auto-buys", type=int, default=1)
    parser.add_argument("--max-auto-sells", type=int, default=10)
    parser.add_argument("--order-dedupe-minutes", type=int, default=1440)
    parser.add_argument("--sell-alert-cooldown-minutes", type=int, default=60)
    parser.add_argument("--rebuy-cooldown-minutes", type=int, default=240)
    parser.add_argument("--min-snapshot-bar-coverage", type=float, default=1.0)
    parser.add_argument("--min-snapshot-fundamental-coverage", type=float, default=1.0)
    parser.add_argument("--max-snapshot-bar-age-minutes", type=int, default=4320)
    parser.add_argument("--account-equity", type=float, default=100_000.0)
    parser.add_argument("--max-open-risk-pct", type=float, default=0.06)
    parser.add_argument("--max-daily-realized-loss-pct", type=float, default=0.03)
    parser.add_argument("--max-auto-buy-price-drift-pct", type=float, default=0.03)
    parser.add_argument("--require-position-reconciliation", action="store_true")
    parser.add_argument("--max-position-reconciliation-age-minutes", type=int, default=1440)
    parser.add_argument("--risk-per-trade-pct", type=float, default=0.01)
    parser.add_argument("--max-position-pct", type=float, default=0.10)
    parser.add_argument("--max-gross-exposure-pct", type=float, default=1.0)
    parser.add_argument("--max-sector-exposure-pct", type=float, default=0.30)
    parser.add_argument("--consume-events", action="store_true")
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--load", action="store_true", help="load service after install")
    parser.add_argument("--unload", action="store_true", help="unload service before uninstall")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.action == "render":
        plist = build_plist(args)
        sys.stdout.buffer.write(plistlib.dumps(plist, sort_keys=False))
        return 0
    if args.action == "install":
        path = install(args)
        print(path)
        return 0
    if args.action == "uninstall":
        path = uninstall(args)
        print(path)
        return 0
    if args.action == "status":
        return status(args)
    raise ValueError(f"Unsupported action: {args.action}")


if __name__ == "__main__":
    raise SystemExit(main())
