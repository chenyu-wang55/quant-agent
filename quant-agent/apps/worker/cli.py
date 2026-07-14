from __future__ import annotations

import argparse
from datetime import datetime, timezone

from apps.worker.main import (
    daily_metrics_aggregation,
    end_of_day_reconciliation,
    intraday_refresh,
    monitor_positions_alerts,
    nightly_backtest_batch,
    pre_market_ingestion,
    process_event_queue,
    system_cycle,
    system_cycle_loop,
)


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
            "system_cycle_loop",
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
    parser.add_argument(
        "--as-of",
        default=None,
        help="optional ISO timestamp for deterministic system_cycle replay, defaults to now",
    )
    parser.add_argument(
        "--disable-auto-broker-sync",
        action="store_true",
        help="disable automatic broker status polling at the start of system_cycle",
    )
    parser.add_argument(
        "--max-broker-sync-items",
        type=int,
        default=50,
        help="maximum pending live buy orders and live sell executions to poll per cycle",
    )
    parser.add_argument(
        "--disable-auto-position-reconciliation",
        action="store_true",
        help="disable automatic broker position reconciliation at the start of system_cycle",
    )
    parser.add_argument(
        "--position-reconciliation-qty-tolerance",
        type=float,
        default=1e-6,
        help="quantity tolerance for automatic broker position reconciliation",
    )
    parser.add_argument(
        "--use-autopilot-policy",
        action="store_true",
        help="load the latest persisted autopilot policy and use it for auto approval/execution controls",
    )
    parser.add_argument(
        "--auto-execute-approved",
        action="store_true",
        help="system_cycle should auto-route approved buys and active sell alerts through existing execution gates",
    )
    parser.add_argument(
        "--auto-approve-recommendations",
        action="store_true",
        help="system_cycle should auto-approve recommendations that pass the configured thresholds",
    )
    parser.add_argument(
        "--auto-approve-min-confidence",
        type=float,
        default=0.72,
        help="minimum recommendation confidence for automatic approval",
    )
    parser.add_argument(
        "--auto-approve-min-composite",
        type=float,
        default=0.0,
        help="minimum composite score for automatic approval",
    )
    parser.add_argument("--max-auto-approvals", type=int, default=1, help="maximum auto approvals per cycle")
    parser.add_argument(
        "--auto-execution-mode",
        choices=["paper", "live_dry_run", "live"],
        default="paper",
        help="execution mode for automatic actions",
    )
    parser.add_argument(
        "--allow-auto-live-execution",
        action="store_true",
        help="explicitly allow autopilot live broker order submission when auto-execution-mode=live",
    )
    parser.add_argument("--max-auto-buys", type=int, default=1, help="maximum approved buys per cycle")
    parser.add_argument("--max-auto-sells", type=int, default=10, help="maximum sell alerts to execute per cycle")
    parser.add_argument(
        "--order-dedupe-minutes",
        type=int,
        default=1440,
        help="minutes to block repeat auto buys for the same recommendation or ticker after a routed buy order",
    )
    parser.add_argument(
        "--sell-alert-cooldown-minutes",
        type=int,
        default=60,
        help="minutes to block repeat auto sells for the same ticker and alert reason",
    )
    parser.add_argument(
        "--rebuy-cooldown-minutes",
        type=int,
        default=240,
        help="minutes to block automatic rebuy after the latest sell for the same ticker; set 0 to disable",
    )
    parser.add_argument(
        "--min-snapshot-bar-coverage",
        type=float,
        default=1.0,
        help="minimum source snapshot bar coverage required before automatic approval/execution",
    )
    parser.add_argument(
        "--min-snapshot-fundamental-coverage",
        type=float,
        default=1.0,
        help="minimum source snapshot fundamental coverage required before automatic approval/execution",
    )
    parser.add_argument(
        "--max-snapshot-bar-age-minutes",
        type=int,
        default=4320,
        help="maximum age of the latest captured source snapshot bar before automatic approval/execution is blocked",
    )
    parser.add_argument(
        "--account-equity", type=float, default=100_000.0, help="account equity for auto buy risk sizing"
    )
    parser.add_argument(
        "--max-open-risk-pct",
        type=float,
        default=0.06,
        help="maximum current open risk to stop as a fraction of account equity before auto buys are blocked",
    )
    parser.add_argument(
        "--max-daily-realized-loss-pct",
        type=float,
        default=0.03,
        help="maximum same-day realized loss as a fraction of account equity before auto approvals/buys are blocked",
    )
    parser.add_argument(
        "--max-auto-buy-price-drift-pct",
        type=float,
        default=0.03,
        help="maximum latest-price drift from the recommendation entry zone before auto buys are blocked",
    )
    parser.add_argument(
        "--require-position-reconciliation",
        action="store_true",
        help="require the latest position reconciliation report to pass before automatic execution",
    )
    parser.add_argument(
        "--max-position-reconciliation-age-minutes",
        type=int,
        default=1440,
        help="maximum age of the latest position reconciliation report before automatic execution is blocked",
    )
    parser.add_argument(
        "--min-paper-shadow-trading-days",
        type=int,
        default=20,
        help="minimum successful paper shadow trading days required before automatic live execution",
    )
    parser.add_argument(
        "--risk-per-trade-pct",
        type=float,
        default=0.01,
        help="fractional per-trade risk budget for auto buy sizing",
    )
    parser.add_argument(
        "--max-position-pct",
        type=float,
        default=0.10,
        help="fractional per-ticker position cap for auto buy sizing",
    )
    parser.add_argument(
        "--max-gross-exposure-pct",
        type=float,
        default=1.0,
        help="fractional gross exposure cap for auto buy sizing",
    )
    parser.add_argument(
        "--max-sector-exposure-pct",
        type=float,
        default=0.30,
        help="fractional sector exposure cap for auto buy sizing",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=300.0,
        help="system_cycle_loop sleep interval between cycles",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="optional system_cycle_loop cycle cap; omit for continuous operation",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="system_cycle_loop should stop after the first cycle error",
    )
    parser.add_argument(
        "--max-consecutive-errors",
        type=int,
        default=0,
        help="activate kill switch and stop system_cycle_loop after this many consecutive errors; set 0 to disable",
    )
    args = parser.parse_args()
    as_of = datetime.fromisoformat(args.as_of) if args.as_of else None
    if as_of is not None and as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)

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
            as_of=as_of,
            auto_sync_broker_statuses=not args.disable_auto_broker_sync,
            max_broker_sync_items=args.max_broker_sync_items,
            auto_reconcile_broker_positions=not args.disable_auto_position_reconciliation,
            position_reconciliation_qty_tolerance=args.position_reconciliation_qty_tolerance,
            use_autopilot_policy=args.use_autopilot_policy,
            auto_execute_approved=args.auto_execute_approved,
            auto_approve_recommendations=args.auto_approve_recommendations,
            auto_approve_min_confidence=args.auto_approve_min_confidence,
            auto_approve_min_composite=args.auto_approve_min_composite,
            max_auto_approvals=args.max_auto_approvals,
            auto_execution_mode=args.auto_execution_mode,
            allow_auto_live_execution=True if args.allow_auto_live_execution else None,
            max_auto_buys=args.max_auto_buys,
            max_auto_sells=args.max_auto_sells,
            order_dedupe_minutes=args.order_dedupe_minutes,
            sell_alert_cooldown_minutes=args.sell_alert_cooldown_minutes,
            rebuy_cooldown_minutes=args.rebuy_cooldown_minutes,
            min_snapshot_bar_coverage=args.min_snapshot_bar_coverage,
            min_snapshot_fundamental_coverage=args.min_snapshot_fundamental_coverage,
            max_snapshot_bar_age_minutes=args.max_snapshot_bar_age_minutes,
            account_equity=args.account_equity,
            max_open_risk_pct=args.max_open_risk_pct,
            max_daily_realized_loss_pct=args.max_daily_realized_loss_pct,
            max_auto_buy_price_drift_pct=args.max_auto_buy_price_drift_pct,
            require_position_reconciliation=args.require_position_reconciliation,
            max_position_reconciliation_age_minutes=args.max_position_reconciliation_age_minutes,
            min_paper_shadow_trading_days=args.min_paper_shadow_trading_days,
            risk_per_trade_pct=args.risk_per_trade_pct,
            max_position_pct=args.max_position_pct,
            max_gross_exposure_pct=args.max_gross_exposure_pct,
            max_sector_exposure_pct=args.max_sector_exposure_pct,
        ),
        "system_cycle_loop": lambda: system_cycle_loop(
            interval_seconds=args.interval_seconds,
            max_cycles=args.max_cycles,
            stop_on_error=args.stop_on_error,
            max_consecutive_errors=args.max_consecutive_errors,
            top_n=args.top_n,
            min_confidence=args.min_confidence,
            consume_events=args.consume_events,
            as_of=as_of,
            auto_sync_broker_statuses=not args.disable_auto_broker_sync,
            max_broker_sync_items=args.max_broker_sync_items,
            auto_reconcile_broker_positions=not args.disable_auto_position_reconciliation,
            position_reconciliation_qty_tolerance=args.position_reconciliation_qty_tolerance,
            use_autopilot_policy=args.use_autopilot_policy,
            auto_execute_approved=args.auto_execute_approved,
            auto_approve_recommendations=args.auto_approve_recommendations,
            auto_approve_min_confidence=args.auto_approve_min_confidence,
            auto_approve_min_composite=args.auto_approve_min_composite,
            max_auto_approvals=args.max_auto_approvals,
            auto_execution_mode=args.auto_execution_mode,
            allow_auto_live_execution=True if args.allow_auto_live_execution else None,
            max_auto_buys=args.max_auto_buys,
            max_auto_sells=args.max_auto_sells,
            order_dedupe_minutes=args.order_dedupe_minutes,
            sell_alert_cooldown_minutes=args.sell_alert_cooldown_minutes,
            rebuy_cooldown_minutes=args.rebuy_cooldown_minutes,
            min_snapshot_bar_coverage=args.min_snapshot_bar_coverage,
            min_snapshot_fundamental_coverage=args.min_snapshot_fundamental_coverage,
            max_snapshot_bar_age_minutes=args.max_snapshot_bar_age_minutes,
            account_equity=args.account_equity,
            max_open_risk_pct=args.max_open_risk_pct,
            max_daily_realized_loss_pct=args.max_daily_realized_loss_pct,
            max_auto_buy_price_drift_pct=args.max_auto_buy_price_drift_pct,
            require_position_reconciliation=args.require_position_reconciliation,
            max_position_reconciliation_age_minutes=args.max_position_reconciliation_age_minutes,
            min_paper_shadow_trading_days=args.min_paper_shadow_trading_days,
            risk_per_trade_pct=args.risk_per_trade_pct,
            max_position_pct=args.max_position_pct,
            max_gross_exposure_pct=args.max_gross_exposure_pct,
            max_sector_exposure_pct=args.max_sector_exposure_pct,
        ),
    }
    dispatch[args.job]()


if __name__ == "__main__":
    main()
