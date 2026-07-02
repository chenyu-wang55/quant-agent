from __future__ import annotations

import plistlib
from pathlib import Path

from scripts import manage_launchd


def _args(*extra: str):
    return manage_launchd.build_parser().parse_args(
        [
            "render",
            "--repo-root",
            "/tmp/quant-agent",
            "--python",
            "/usr/bin/python3",
            "--data-provider",
            "mock",
            *extra,
        ]
    )


def test_build_launchd_plist_for_system_cycle_loop(tmp_path: Path) -> None:
    args = _args(
        "--use-autopilot-policy",
        "--auto-approve-recommendations",
        "--auto-approve-min-confidence",
        "0.8",
        "--auto-execute-approved",
        "--auto-execution-mode",
        "live",
        "--allow-auto-live-execution",
        "--interval-seconds",
        "60",
        "--max-consecutive-errors",
        "4",
        "--max-broker-sync-items",
        "12",
        "--disable-auto-broker-sync",
        "--position-reconciliation-qty-tolerance",
        "0.001",
        "--disable-auto-position-reconciliation",
        "--max-auto-buys",
        "2",
        "--order-dedupe-minutes",
        "720",
        "--sell-alert-cooldown-minutes",
        "30",
        "--rebuy-cooldown-minutes",
        "180",
        "--min-snapshot-bar-coverage",
        "0.95",
        "--min-snapshot-fundamental-coverage",
        "0.9",
        "--max-snapshot-bar-age-minutes",
        "720",
        "--max-open-risk-pct",
        "0.04",
        "--max-daily-realized-loss-pct",
        "0.02",
        "--max-auto-buy-price-drift-pct",
        "0.015",
        "--require-position-reconciliation",
        "--max-position-reconciliation-age-minutes",
        "120",
    )

    plist = manage_launchd.build_plist(args, home=tmp_path)
    round_trip = plistlib.loads(plistlib.dumps(plist))

    assert round_trip["Label"] == manage_launchd.DEFAULT_LABEL
    assert round_trip["WorkingDirectory"] == str(Path("/tmp/quant-agent").resolve())
    assert round_trip["RunAtLoad"] is True
    assert round_trip["KeepAlive"] is True
    assert round_trip["EnvironmentVariables"]["DATA_PROVIDER"] == "mock"
    assert round_trip["ProgramArguments"][:4] == [
        "/usr/bin/python3",
        "-m",
        "apps.worker.main",
        "system_cycle_loop",
    ]
    assert "--use-autopilot-policy" in round_trip["ProgramArguments"]
    assert "--auto-execute-approved" in round_trip["ProgramArguments"]
    assert "--auto-execution-mode" in round_trip["ProgramArguments"]
    assert "live" in round_trip["ProgramArguments"]
    assert "--allow-auto-live-execution" in round_trip["ProgramArguments"]
    assert "--auto-approve-recommendations" in round_trip["ProgramArguments"]
    assert "--auto-approve-min-confidence" in round_trip["ProgramArguments"]
    assert "0.8" in round_trip["ProgramArguments"]
    assert "--interval-seconds" in round_trip["ProgramArguments"]
    assert "60.0" in round_trip["ProgramArguments"]
    assert "--max-consecutive-errors" in round_trip["ProgramArguments"]
    assert "4" in round_trip["ProgramArguments"]
    assert "--max-broker-sync-items" in round_trip["ProgramArguments"]
    assert "12" in round_trip["ProgramArguments"]
    assert "--disable-auto-broker-sync" in round_trip["ProgramArguments"]
    assert "--position-reconciliation-qty-tolerance" in round_trip["ProgramArguments"]
    assert "0.001" in round_trip["ProgramArguments"]
    assert "--disable-auto-position-reconciliation" in round_trip["ProgramArguments"]
    assert "--rebuy-cooldown-minutes" in round_trip["ProgramArguments"]
    assert "--order-dedupe-minutes" in round_trip["ProgramArguments"]
    assert "720" in round_trip["ProgramArguments"]
    assert "--sell-alert-cooldown-minutes" in round_trip["ProgramArguments"]
    assert "30" in round_trip["ProgramArguments"]
    assert "180" in round_trip["ProgramArguments"]
    assert "--min-snapshot-bar-coverage" in round_trip["ProgramArguments"]
    assert "0.95" in round_trip["ProgramArguments"]
    assert "--min-snapshot-fundamental-coverage" in round_trip["ProgramArguments"]
    assert "0.9" in round_trip["ProgramArguments"]
    assert "--max-snapshot-bar-age-minutes" in round_trip["ProgramArguments"]
    assert "720" in round_trip["ProgramArguments"]
    assert "--max-open-risk-pct" in round_trip["ProgramArguments"]
    assert "0.04" in round_trip["ProgramArguments"]
    assert "--max-daily-realized-loss-pct" in round_trip["ProgramArguments"]
    assert "0.02" in round_trip["ProgramArguments"]
    assert "--max-auto-buy-price-drift-pct" in round_trip["ProgramArguments"]
    assert "0.015" in round_trip["ProgramArguments"]
    assert "--require-position-reconciliation" in round_trip["ProgramArguments"]
    assert "--max-position-reconciliation-age-minutes" in round_trip["ProgramArguments"]
    assert "120" in round_trip["ProgramArguments"]
    assert str(tmp_path / "Library" / "Logs") in round_trip["StandardOutPath"]


def test_install_and_uninstall_launchd_plist(tmp_path: Path, monkeypatch) -> None:
    plist_path = tmp_path / "LaunchAgents" / "com.example.quant.plist"
    log_dir = tmp_path / "Logs"
    monkeypatch.setattr(manage_launchd, "default_log_dir", lambda home=None: log_dir)
    args = manage_launchd.build_parser().parse_args(
        [
            "install",
            "--label",
            "com.example.quant",
            "--plist-path",
            str(plist_path),
            "--repo-root",
            str(tmp_path / "repo"),
            "--python",
            "/usr/bin/python3",
            "--data-provider",
            "mock",
        ]
    )

    installed_path = manage_launchd.install(args)
    assert installed_path == plist_path
    assert plist_path.exists()
    assert log_dir.exists()
    assert plistlib.loads(plist_path.read_bytes())["Label"] == "com.example.quant"

    uninstall_args = manage_launchd.build_parser().parse_args(
        ["uninstall", "--label", "com.example.quant", "--plist-path", str(plist_path)]
    )
    removed_path = manage_launchd.uninstall(uninstall_args)
    assert removed_path == plist_path
    assert not plist_path.exists()
