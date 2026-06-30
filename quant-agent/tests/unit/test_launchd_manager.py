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
        "paper",
        "--interval-seconds",
        "60",
        "--max-auto-buys",
        "2",
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
    assert "--auto-approve-recommendations" in round_trip["ProgramArguments"]
    assert "--auto-approve-min-confidence" in round_trip["ProgramArguments"]
    assert "0.8" in round_trip["ProgramArguments"]
    assert "--interval-seconds" in round_trip["ProgramArguments"]
    assert "60.0" in round_trip["ProgramArguments"]
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
