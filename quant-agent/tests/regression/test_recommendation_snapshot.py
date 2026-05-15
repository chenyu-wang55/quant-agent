from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from domain.entities.models import PublicationConfig, ResearchRunRequest, RiskPolicy, RunType, UniverseRules
from services.ingestion.mock_provider import MockMarketDataProvider
from services.ranking.pipeline import ResearchPipeline


SNAPSHOT_FILE = Path(__file__).parent / "snapshots" / "recommendations_sp500_2026-04-10.json"


def _projection() -> list[dict]:
    pipeline = ResearchPipeline(provider=MockMarketDataProvider())
    request = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="regression snapshot run",
        as_of=datetime(2026, 4, 10, 9, 30, tzinfo=timezone.utc),
        universe="SP500",
        universe_rules=UniverseRules(
            min_price=5,
            min_avg_dollar_volume=5_000_000,
            max_spread_bps=100,
            min_market_cap_usd=1_000_000_000,
            allowed_sectors=[],
            max_candidates_after_filter=100,
        ),
        risk_policy=RiskPolicy(
            min_confidence=0.0,
            earnings_blackout_minutes=15,
            max_name_weight=0.10,
            max_sector_weight=0.30,
            max_gross_exposure=1.0,
            max_correlated_cluster_weight=0.35,
            reject_on_material_evidence_conflict=False,
            event_trading_enabled=True,
        ),
        publication=PublicationConfig(top_n=3, output_channels=["api"]),
    )

    output = pipeline.run(request)
    rows: list[dict] = []
    for rec in output.result.recommendations:
        rows.append(
            {
                "ticker": rec.ticker,
                "composite": round(rec.score_vector["composite"], 6),
                "entry_zone_low": round(rec.entry_zone_low, 4),
                "entry_zone_high": round(rec.entry_zone_high, 4),
                "stop_loss": round(rec.stop_loss, 4),
                "tp1": round(rec.tp1, 4),
                "tp2": round(rec.tp2, 4),
                "confidence": round(rec.confidence, 6),
            }
        )
    return rows


def test_recommendation_snapshot_regression() -> None:
    current = _projection()

    if os.getenv("UPDATE_SNAPSHOTS") == "1":
        SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_FILE.write_text(json.dumps(current, indent=2), encoding="utf-8")
        return

    assert SNAPSHOT_FILE.exists(), "Snapshot file missing; run with UPDATE_SNAPSHOTS=1 once."
    expected = json.loads(SNAPSHOT_FILE.read_text(encoding="utf-8"))
    assert current == expected
