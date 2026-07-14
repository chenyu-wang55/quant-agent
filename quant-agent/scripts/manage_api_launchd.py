from __future__ import annotations

import argparse
import os
import plistlib
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

DEFAULT_LABEL = "com.quant-agent.api"
LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def env_text(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def launch_agents_dir(home: Path | None = None) -> Path:
    return (home or Path.home()) / "Library" / "LaunchAgents"


def default_plist_path(label: str = DEFAULT_LABEL, home: Path | None = None) -> Path:
    return launch_agents_dir(home=home) / f"{label}.plist"


def default_log_file(label: str = DEFAULT_LABEL, home: Path | None = None) -> Path:
    return (home or Path.home()) / "Library" / "Logs" / f"{label}.json.log"


def launchd_domain() -> str:
    return f"gui/{os.getuid()}"


def service_target(label: str) -> str:
    return f"{launchd_domain()}/{label}"


def validate_args(args: argparse.Namespace) -> None:
    if args.host not in LOOPBACK_HOSTS:
        raise ValueError("API LaunchAgent host must be loopback-only (127.0.0.1, ::1, or localhost)")
    if not 1 <= args.port <= 65535:
        raise ValueError("API port must be between 1 and 65535")
    if not args.access_password or len(args.access_password) < 12:
        raise ValueError("QUANT_AGENT_ACCESS_PASSWORD must contain at least 12 characters")
    if not args.auth_signing_secret or len(args.auth_signing_secret) < 32:
        raise ValueError("QUANT_AGENT_AUTH_SIGNING_SECRET must contain at least 32 characters")


def build_program_arguments(args: argparse.Namespace) -> list[str]:
    return [
        args.python,
        "-m",
        "uvicorn",
        "apps.api.main:app",
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]


def _database_url(args: argparse.Namespace) -> str:
    if args.database_url:
        return args.database_url
    database_path = Path(args.repo_root).expanduser().resolve() / "quant_agent_v2.db"
    return f"sqlite:///{database_path}"


def _set_optional(env: dict[str, str], name: str, value: str | None) -> None:
    if value:
        env[name] = value


def build_plist(args: argparse.Namespace, home: Path | None = None) -> dict[str, Any]:
    validate_args(args)
    root = Path(args.repo_root).expanduser().resolve()
    structured_log = (
        Path(args.log_file).expanduser().resolve()
        if args.log_file
        else default_log_file(args.label, home=home)
    )
    env = {
        "PYTHONUNBUFFERED": "1",
        "PYTHONPATH": str(root),
        "DATABASE_URL": _database_url(args),
        "DATA_PROVIDER": args.data_provider,
        "QUANT_AGENT_ACCESS_PASSWORD": args.access_password,
        "QUANT_AGENT_AUTH_SIGNING_SECRET": args.auth_signing_secret,
        "QUANT_AGENT_COOKIE_SECURE": "0",
        "QUANT_AGENT_LOG_FORMAT": "json",
        "QUANT_AGENT_LOG_FILE": str(structured_log),
        "QUANT_AGENT_LOG_MAX_BYTES": str(args.log_max_bytes),
        "QUANT_AGENT_LOG_BACKUP_COUNT": str(args.log_backup_count),
    }
    _set_optional(env, "POINT_IN_TIME_UNIVERSE_CSV", args.point_in_time_universe_csv)
    _set_optional(env, "POINT_IN_TIME_FUNDAMENTALS_CSV", args.point_in_time_fundamentals_csv)
    _set_optional(env, "POINT_IN_TIME_EVENTS_CSV", args.point_in_time_events_csv)
    _set_optional(env, "POINT_IN_TIME_EARNINGS_CSV", args.point_in_time_earnings_csv)
    _set_optional(env, "QUANT_BROKER_ADAPTER", args.broker_adapter)
    _set_optional(env, "ALPACA_BASE_URL", args.alpaca_base_url)
    _set_optional(env, "ALPACA_API_KEY", args.alpaca_api_key)
    _set_optional(env, "ALPACA_SECRET_KEY", args.alpaca_secret_key)

    return {
        "Label": args.label,
        "ProgramArguments": build_program_arguments(args),
        "WorkingDirectory": str(root),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
        "ThrottleInterval": 5,
        # Application logs use RotatingFileHandler. Discard launchd's raw
        # streams so they cannot grow without rotation.
        "StandardOutPath": "/dev/null",
        "StandardErrorPath": "/dev/null",
        "EnvironmentVariables": env,
    }


def write_plist(plist: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(plistlib.dumps(plist, sort_keys=False))
    path.chmod(0o600)


def _unload(label: str) -> None:
    subprocess.run(
        ["launchctl", "bootout", service_target(label)],
        check=False,
        text=True,
        capture_output=True,
    )
    # `launchctl submit` jobs do not always accept bootout by service target.
    subprocess.run(
        ["launchctl", "remove", label],
        check=False,
        text=True,
        capture_output=True,
    )


def _wait_until_loaded(label: str, attempts: int = 20) -> bool:
    for _ in range(attempts):
        result = subprocess.run(
            ["launchctl", "print", service_target(label)],
            check=False,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            return True
        time.sleep(0.1)
    return False


def install(args: argparse.Namespace) -> Path:
    validate_args(args)
    path = Path(args.plist_path).expanduser() if args.plist_path else default_plist_path(args.label)
    default_log_file(args.label).parent.mkdir(parents=True, exist_ok=True)
    write_plist(build_plist(args), path)
    if args.load:
        _unload(args.label)
        # launchd can retain the just-removed label briefly and return EIO for
        # an otherwise valid replacement plist.
        time.sleep(1.0)
        bootstrap = subprocess.run(
            ["launchctl", "bootstrap", launchd_domain(), str(path)],
            check=False,
            text=True,
            capture_output=True,
        )
        if bootstrap.returncode != 0:
            # Some supported macOS user domains return EIO for `bootstrap`
            # while the compatibility loader accepts the same valid plist.
            subprocess.run(
                ["launchctl", "load", "-w", str(path)],
                check=False,
                text=True,
                capture_output=True,
            )
        if not _wait_until_loaded(args.label):
            raise RuntimeError(f"launchd did not register {args.label} after installation")
    return path


def uninstall(args: argparse.Namespace) -> Path:
    path = Path(args.plist_path).expanduser() if args.plist_path else default_plist_path(args.label)
    if args.unload:
        _unload(args.label)
    if path.exists():
        path.unlink()
    return path


def status(args: argparse.Namespace) -> int:
    result = subprocess.run(
        ["launchctl", "print", service_target(args.label)],
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
    parser = argparse.ArgumentParser(description="Manage the loopback quant-agent API LaunchAgent")
    parser.add_argument("action", choices=["render", "install", "uninstall", "status"])
    parser.add_argument("--label", default=DEFAULT_LABEL)
    parser.add_argument("--plist-path", default=None, help="override ~/Library/LaunchAgents plist path")
    parser.add_argument("--repo-root", default=str(repo_root()))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--database-url",
        default=None,
        help="explicit database override; defaults to <repo-root>/quant_agent_v2.db",
    )
    parser.add_argument("--data-provider", default=env_text("DATA_PROVIDER", "yfinance"))
    parser.add_argument("--access-password", default=env_text("QUANT_AGENT_ACCESS_PASSWORD") or None)
    parser.add_argument(
        "--auth-signing-secret",
        default=env_text("QUANT_AGENT_AUTH_SIGNING_SECRET") or None,
    )
    parser.add_argument("--log-file", default=None)
    parser.add_argument("--log-max-bytes", type=int, default=20 * 1024 * 1024)
    parser.add_argument("--log-backup-count", type=int, default=10)
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
    parser.add_argument("--broker-adapter", default=env_text("QUANT_BROKER_ADAPTER") or None)
    parser.add_argument("--alpaca-base-url", default=env_text("ALPACA_BASE_URL") or None)
    parser.add_argument("--alpaca-api-key", default=env_text("ALPACA_API_KEY") or None)
    parser.add_argument("--alpaca-secret-key", default=env_text("ALPACA_SECRET_KEY") or None)
    parser.add_argument("--load", action="store_true", help="bootstrap the service after install")
    parser.add_argument("--unload", action="store_true", help="boot out the service before uninstall")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.action == "render":
            sys.stdout.buffer.write(plistlib.dumps(build_plist(args), sort_keys=False))
            return 0
        if args.action == "install":
            print(install(args))
            return 0
        if args.action == "uninstall":
            print(uninstall(args))
            return 0
        if args.action == "status":
            return status(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    raise ValueError(f"Unsupported action: {args.action}")


if __name__ == "__main__":
    raise SystemExit(main())
