from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from apps.api.dependencies import get_app_state
from domain.entities.models import (
    BacktestRunRequest,
    PublicationConfig,
    ResearchRunRequest,
    RiskPolicy,
    RunType,
    SnapshotMode,
    SystemCycleRun,
)
from infra.queue.events import EventType


def _run_research_job(job_name: str) -> None:
    state = get_app_state()
    request = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective=f"Scheduled {job_name} recommendation generation",
        as_of=datetime.now(timezone.utc),
    )
    output = state.pipeline.run(request)
    state.ingest_run_output(request, output)
    print(f"{job_name}: generated {len(output.result.recommendations)} recommendations")


def pre_market_ingestion() -> None:
    _run_research_job("pre_market_ingestion")


def intraday_refresh() -> None:
    _run_research_job("intraday_refresh")


def end_of_day_reconciliation() -> None:
    _run_research_job("end_of_day_reconciliation")


def nightly_backtest_batch() -> None:
    state = get_app_state()
    now = datetime.now(timezone.utc)
    request = BacktestRunRequest(
        run_name="nightly_backtest",
        start_date=now.replace(year=max(2000, now.year - 1)),
        end_date=now,
        top_n=10,
    )
    template = state.last_research_request or ResearchRunRequest(
        run_type=RunType.BACKTEST_EVALUATION,
        objective="Nightly backtest template",
        as_of=now,
    )
    result = state.backtest_engine.run(request, state.pipeline, template)
    state.backtest_runs.append(result)
    state.publish_event(
        EventType.MODEL_EVALUATION,
        {
            "run_id": result.run_id,
            "config_hash": result.config_hash,
            "metrics": result.metrics,
        },
    )
    print(f"nightly_backtest_batch: run_id={result.run_id}")


def daily_metrics_aggregation() -> None:
    state = get_app_state()
    metrics = state.metrics_store.dump()
    print(f"daily_metrics_aggregation: {metrics}")


def process_event_queue() -> None:
    state = get_app_state()
    events = state.consume_events(limit=1000)
    type_counts: dict[str, int] = {}
    for event in events:
        key = event.event_type.value
        type_counts[key] = type_counts.get(key, 0) + 1
    print(f"process_event_queue: consumed={len(events)} by_type={type_counts}")


def monitor_positions_alerts() -> None:
    state = get_app_state()
    alerts = state.monitor_sell_alerts()
    print(f"monitor_positions_alerts: alert_count={len(alerts)}")
    for alert in alerts:
        print(f"{alert.ticker} | {alert.level.value} | {alert.reason_code} | {alert.message_cn}")


def _event_type_counts(events: list[Any]) -> dict[str, int]:
    type_counts: dict[str, int] = {}
    for event in events:
        key = event.event_type.value
        type_counts[key] = type_counts.get(key, 0) + 1
    return type_counts


def system_cycle(
    top_n: int = 8,
    min_confidence: float | None = None,
    consume_events: bool = False,
) -> dict[str, Any]:
    state = get_app_state()
    started_at = datetime.now(timezone.utc)
    request = ResearchRunRequest(
        run_type=RunType.RESEARCH_BATCH,
        objective="Scheduled full system cycle",
        as_of=started_at,
        snapshot_mode=SnapshotMode.LATEST,
        publication=PublicationConfig(top_n=top_n, output_channels=["api", "worker"]),
        risk_policy=RiskPolicy(min_confidence=min_confidence) if min_confidence is not None else RiskPolicy(),
    )
    output = state.pipeline.run(request)
    state.ingest_run_output(request, output)
    alerts = state.monitor_sell_alerts(as_of=started_at)
    consumed_events = state.consume_events(limit=1000) if consume_events else []
    pending_event_count = state.event_queue.size()
    finished_at = datetime.now(timezone.utc)
    top_recommendations = [
        {
            "id": rec.id,
            "ticker": rec.ticker,
            "confidence": rec.confidence,
            "entry_zone": rec.entry_zone,
            "stop_loss": rec.stop_loss,
            "tp1": rec.tp1,
            "tp2": rec.tp2,
        }
        for rec in output.result.recommendations[:top_n]
    ]
    sell_alerts = [
        {
            "ticker": alert.ticker,
            "level": alert.level.value,
            "reason_code": alert.reason_code,
            "current_price": alert.current_price,
            "message_cn": alert.message_cn,
            "suggested_action_cn": alert.suggested_action_cn,
        }
        for alert in alerts
    ]
    consumed_type_counts = _event_type_counts(consumed_events)
    run = SystemCycleRun(
        id=uuid4().hex[:16],
        started_at=started_at,
        finished_at=finished_at,
        source_snapshot_id=output.result.source_snapshot_id,
        strategy_config_id=output.result.strategy_config_id,
        recommendation_count=len(output.result.recommendations),
        sell_alert_count=len(alerts),
        consumed_event_count=len(consumed_events),
        pending_event_count=pending_event_count,
        auto_execution_enabled=False,
        top_recommendations=top_recommendations,
        sell_alerts=sell_alerts,
        consumed_event_type_counts=consumed_type_counts,
        metrics=state.metrics_store.dump(),
    )
    state.record_system_cycle_run(run)
    summary = {
        "system_cycle_run_id": run.id,
        "job": "system_cycle",
        "generated_at": finished_at.isoformat(),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "status": run.status,
        "source_snapshot_id": output.result.source_snapshot_id,
        "strategy_config_id": output.result.strategy_config_id,
        "recommendation_count": len(output.result.recommendations),
        "top_recommendations": top_recommendations,
        "sell_alert_count": len(alerts),
        "sell_alerts": sell_alerts,
        "auto_execution_enabled": False,
        "consumed_event_count": len(consumed_events),
        "consumed_event_type_counts": consumed_type_counts,
        "pending_event_count": pending_event_count,
        "metrics": run.metrics,
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Quant agent worker jobs")
    parser.add_argument(
        "job",
        choices=[
            "pre_market_ingestion",
            "intraday_refresh",
            "end_of_day_reconciliation",
            "nightly_backtest_batch",
            "daily_metrics_aggregation",
            "process_event_queue",
            "monitor_positions_alerts",
            "system_cycle",
        ],
    )
    parser.add_argument("--top-n", type=int, default=8, help="system_cycle recommendation publication size")
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help="optional system_cycle risk-policy min confidence override",
    )
    parser.add_argument(
        "--consume-events",
        action="store_true",
        help="system_cycle should consume pending events after publishing its summary inputs",
    )
    args = parser.parse_args()

    dispatch = {
        "pre_market_ingestion": pre_market_ingestion,
        "intraday_refresh": intraday_refresh,
        "end_of_day_reconciliation": end_of_day_reconciliation,
        "nightly_backtest_batch": nightly_backtest_batch,
        "daily_metrics_aggregation": daily_metrics_aggregation,
        "process_event_queue": process_event_queue,
        "monitor_positions_alerts": monitor_positions_alerts,
        "system_cycle": lambda: system_cycle(
            top_n=args.top_n,
            min_confidence=args.min_confidence,
            consume_events=args.consume_events,
        ),
    }
    dispatch[args.job]()


if __name__ == "__main__":
    main()
