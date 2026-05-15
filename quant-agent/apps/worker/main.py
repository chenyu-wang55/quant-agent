from __future__ import annotations

import argparse
from datetime import datetime, timezone

from apps.api.dependencies import get_app_state
from domain.entities.models import BacktestRunRequest, ResearchRunRequest, RunType
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
        ],
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
    }
    dispatch[args.job]()


if __name__ == "__main__":
    main()
