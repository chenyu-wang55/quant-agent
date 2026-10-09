from __future__ import annotations

import argparse
import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any

DEFAULT_LABEL = "com.quant-agent.system-cycle-loop"


def env_text(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_python() -> Path:
    return repo_root() / ".venv" / "bin" / "python"


def launch_agents_dir(home: Path | None = None) -> Path:
    return (home or Path.home()) / "Library" / "LaunchAgents"


def default_plist_path(label: str = DEFAULT_LABEL, home: Path | None = None) -> Path:
    return launch_agents_dir(home=home) / f"{label}.plist"


def default_log_dir(home: Path | None = None) -> Path:
    return (home or Path.home()) / "Library" / "Logs"


def _database_url(args: argparse.Namespace) -> str:
    if args.database_url:
        prefix = "sqlite:///"
        if not args.database_url.startswith(prefix):
            return args.database_url
        configured_path = Path(args.database_url[len(prefix) :]).expanduser()
        if not configured_path.is_absolute():
            configured_path = Path(args.repo_root).expanduser() / configured_path
        return f"sqlite:///{configured_path.resolve()}"
    database_path = Path(args.repo_root).expanduser().resolve() / "quant_agent_v2.db"
    return f"sqlite:///{database_path}"


def _set_optional(env: dict[str, str], name: str, value: str | None) -> None:
    if value:
        env[name] = value


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
        "--min-paper-shadow-trading-days",
        str(args.min_paper_shadow_trading_days),
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
    if args.allow_auto_live_execution:
        program_args.append("--allow-auto-live-execution")
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
    root = Path(args.repo_root).expanduser().resolve()
    logs = default_log_dir(home=home)
    structured_log = (
        Path(args.log_file).expanduser().resolve()
        if args.log_file
        else logs / f"{label}.json.log"
    )
    env = {
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": str(root),
        "DATABASE_URL": _database_url(args),
        "DATA_PROVIDER": args.data_provider,
        "QUANT_AGENT_LOG_FORMAT": "json",
        "QUANT_AGENT_LOG_FILE": str(structured_log),
        "QUANT_AGENT_LOG_MAX_BYTES": str(args.log_max_bytes),
        "QUANT_AGENT_LOG_BACKUP_COUNT": str(args.log_backup_count),
    }
    if args.access_password:
        env["QUANT_AGENT_ACCESS_PASSWORD"] = args.access_password
    _set_optional(env, "QUANT_AGENT_AUTH_SIGNING_SECRET", args.auth_signing_secret)
    _set_optional(env, "POINT_IN_TIME_UNIVERSE_CSV", args.point_in_time_universe_csv)
    _set_optional(env, "POINT_IN_TIME_FUNDAMENTALS_CSV", args.point_in_time_fundamentals_csv)
    _set_optional(env, "POINT_IN_TIME_EVENTS_CSV", args.point_in_time_events_csv)
    _set_optional(env, "POINT_IN_TIME_EARNINGS_CSV", args.point_in_time_earnings_csv)
    _set_optional(env, "POINT_IN_TIME_VALIDATION_REPORT", args.point_in_time_validation_report)
    _set_optional(env, "QUANT_BROKER_ADAPTER", args.broker_adapter)
    _set_optional(env, "ALPACA_BASE_URL", args.alpaca_base_url)
    _set_optional(env, "ALPACA_API_KEY", args.alpaca_api_key)
    _set_optional(env, "ALPACA_SECRET_KEY", args.alpaca_secret_key)

    return {
        "Label": label,
        "ProgramArguments": build_program_arguments(args),
        "WorkingDirectory": str(root),
        "RunAtLoad": True,
        "KeepAlive": True,
        # Worker events are written through RotatingFileHandler. Discard raw
        # stdout/stderr so launchd cannot grow unbounded log files.
        "StandardOutPath": "/dev/null",
        "StandardErrorPath": "/dev/null",
        "EnvironmentVariables": env,
    }


def write_plist(plist: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(plist, sort_keys=False))
    path.chmod(0o600)


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
    parser.add_argument("--python", default=str(default_python()))
    parser.add_argument(
        "--database-url",
        default=env_text("DATABASE_URL") or None,
        help="explicit database override; defaults to <repo-root>/quant_agent_v2.db",
    )
    parser.add_argument("--data-provider", default=env_text("DATA_PROVIDER", "yfinance"))
    parser.add_argument("--access-password", default=env_text("QUANT_AGENT_ACCESS_PASSWORD") or None)
    parser.add_argument(
        "--auth-signing-secret",
        default=env_text("QUANT_AGENT_AUTH_SIGNING_SECRET") or None,
    )
    parser.add_argument(
        "--point-in-time-universe-csv",
        default=env_text("POINT_IN_TIME_UNIVERSE_CSV") or None,
    )
    parser.add_argument(
        "--point-in-time-fundamentals-csv",
        default=env_text("POINT_IN_TIME_FUNDAMENTALS_CSV") or None,
    )
    parser.add_argument(
        "--point-in-time-events-csv",
        default=env_text("POINT_IN_TIME_EVENTS_CSV") or None,
    )
    parser.add_argument(
        "--point-in-time-earnings-csv",
        default=env_text("POINT_IN_TIME_EARNINGS_CSV") or None,
    )
    parser.add_argument(
        "--point-in-time-validation-report",
        default=env_text("POINT_IN_TIME_VALIDATION_REPORT") or None,
    )
    parser.add_argument("--broker-adapter", default=env_text("QUANT_BROKER_ADAPTER") or None)
    parser.add_argument("--alpaca-base-url", default=env_text("ALPACA_BASE_URL") or None)
    parser.add_argument("--alpaca-api-key", default=env_text("ALPACA_API_KEY") or None)
    parser.add_argument("--alpaca-secret-key", default=env_text("ALPACA_SECRET_KEY") or None)
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--log-max-bytes", type=int, default=20 * 1024 * 1024)
    parser.add_argument("--log-backup-count", type=int, default=10)
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
    parser.add_argument("--auto-execution-mode", choices=["paper", "live_dry_run", "live"], default="paper")
    parser.add_argument("--allow-auto-live-execution", action="store_true")
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
    parser.add_argument("--min-paper-shadow-trading-days", type=int, default=20)
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
