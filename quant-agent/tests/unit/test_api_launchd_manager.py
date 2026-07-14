from __future__ import annotations

import plistlib
import stat
import subprocess
from pathlib import Path

import pytest

from scripts import manage_api_launchd


def _args(*extra: str):
    return manage_api_launchd.build_parser().parse_args(
        [
            "render",
            "--repo-root",
            "/tmp/quant-agent",
            "--python",
            "/usr/bin/python3",
            "--data-provider",
            "mock",
            "--access-password",
            "local-password-123",
            "--auth-signing-secret",
            "signing-secret-with-at-least-32-characters",
            *extra,
        ]
    )


def test_build_secure_loopback_api_plist(tmp_path: Path) -> None:
    args = _args(
        "--point-in-time-universe-csv",
        "/licensed/universe.csv",
        "--broker-adapter",
        "alpaca",
        "--alpaca-base-url",
        "https://paper-api.alpaca.markets",
    )

    plist = manage_api_launchd.build_plist(args, home=tmp_path)
    round_trip = plistlib.loads(plistlib.dumps(plist))

    assert round_trip["Label"] == manage_api_launchd.DEFAULT_LABEL
    assert round_trip["ProgramArguments"] == [
        "/usr/bin/python3",
        "-m",
        "uvicorn",
        "apps.api.main:app",
        "--host",
        "127.0.0.1",
        "--port",
        "8000",
    ]
    assert round_trip["WorkingDirectory"] == str(Path("/tmp/quant-agent").resolve())
    assert round_trip["RunAtLoad"] is True
    assert round_trip["KeepAlive"] is True
    assert round_trip["StandardOutPath"] == "/dev/null"
    assert round_trip["StandardErrorPath"] == "/dev/null"
    environment = round_trip["EnvironmentVariables"]
    assert environment["DATABASE_URL"] == (
        f"sqlite:///{Path('/tmp/quant-agent').resolve() / 'quant_agent_v2.db'}"
    )
    assert environment["QUANT_AGENT_ACCESS_PASSWORD"] == "local-password-123"
    assert environment["QUANT_AGENT_AUTH_SIGNING_SECRET"].startswith("signing-secret")
    assert environment["QUANT_AGENT_COOKIE_SECURE"] == "0"
    assert "QUANT_AGENT_DISABLE_AUTH" not in environment
    assert environment["POINT_IN_TIME_UNIVERSE_CSV"] == "/licensed/universe.csv"
    assert environment["QUANT_BROKER_ADAPTER"] == "alpaca"
    assert environment["ALPACA_BASE_URL"] == "https://paper-api.alpaca.markets"


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        (("--host", "0.0.0.0"), "loopback-only"),
        (("--access-password", "short"), "at least 12"),
        (("--auth-signing-secret", "short"), "at least 32"),
        (("--port", "70000"), "between 1 and 65535"),
    ],
)
def test_api_plist_rejects_unsafe_configuration(extra: tuple[str, ...], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        manage_api_launchd.build_plist(_args(*extra))


def test_install_writes_private_plist_and_bootstraps(tmp_path: Path, monkeypatch) -> None:
    plist_path = tmp_path / "LaunchAgents" / "com.example.quant-api.plist"
    log_file = tmp_path / "Logs" / "api.json.log"
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        _ = kwargs
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(manage_api_launchd.subprocess, "run", fake_run)
    monkeypatch.setattr(manage_api_launchd.time, "sleep", lambda _seconds: None)
    args = manage_api_launchd.build_parser().parse_args(
        [
            "install",
            "--label",
            "com.example.quant-api",
            "--plist-path",
            str(plist_path),
            "--repo-root",
            str(tmp_path / "repo"),
            "--python",
            "/usr/bin/python3",
            "--access-password",
            "local-password-123",
            "--auth-signing-secret",
            "signing-secret-with-at-least-32-characters",
            "--log-file",
            str(log_file),
            "--load",
        ]
    )

    installed = manage_api_launchd.install(args)

    assert installed == plist_path
    assert stat.S_IMODE(plist_path.stat().st_mode) == 0o600
    assert ["launchctl", "bootout", manage_api_launchd.service_target(args.label)] in calls
    assert ["launchctl", "remove", args.label] in calls
    assert ["launchctl", "bootstrap", manage_api_launchd.launchd_domain(), str(plist_path)] in calls


def test_install_falls_back_to_compatibility_loader_when_bootstrap_returns_eio(
    tmp_path: Path,
    monkeypatch,
) -> None:
    plist_path = tmp_path / "LaunchAgents" / "com.example.quant-api.plist"
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        _ = kwargs
        calls.append(command)
        returncode = 5 if command[1:2] == ["bootstrap"] else 0
        return subprocess.CompletedProcess(command, returncode, stdout="", stderr="")

    monkeypatch.setattr(manage_api_launchd.subprocess, "run", fake_run)
    monkeypatch.setattr(manage_api_launchd.time, "sleep", lambda _seconds: None)
    args = manage_api_launchd.build_parser().parse_args(
        [
            "install",
            "--label",
            "com.example.quant-api",
            "--plist-path",
            str(plist_path),
            "--repo-root",
            str(tmp_path / "repo"),
            "--access-password",
            "local-password-123",
            "--auth-signing-secret",
            "signing-secret-with-at-least-32-characters",
            "--load",
        ]
    )

    manage_api_launchd.install(args)

    assert ["launchctl", "load", "-w", str(plist_path)] in calls
