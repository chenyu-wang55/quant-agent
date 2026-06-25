# Daily Operations Runbook

## Jobs

1. Pre-market ingestion
2. Intraday refresh
3. End-of-day reconciliation
4. Nightly backtest batch
5. Daily metrics aggregation

## Commands

```bash
python -m apps.worker.main pre_market_ingestion
python -m apps.worker.main intraday_refresh
python -m apps.worker.main end_of_day_reconciliation
python -m apps.worker.main nightly_backtest_batch
python -m apps.worker.main daily_metrics_aggregation
python -m apps.worker.main process_event_queue
python -m apps.worker.main monitor_positions_alerts
python -m apps.worker.main system_cycle --top-n 8 --min-confidence 0.0
```

`system_cycle` is the preferred schedulable loop for local operation. It records/replays
the latest market snapshot, generates recommendations, refreshes sell alerts, prints a
JSON health summary, and leaves order execution to the approval/dashboard flow. Add
`--consume-events` only when a downstream event sink has already captured the printed
summary and you want the in-memory queue drained.

## API Checks

```bash
curl http://localhost:8000/health
curl http://localhost:8000/recommendations/latest
curl http://localhost:8000/recommendations/<id>/evidence
curl http://localhost:8000/source-snapshots
curl http://localhost:8000/source-snapshots/<source_snapshot_id>
curl "http://localhost:8000/source-snapshots/<source_snapshot_id>/bars/MSFT?limit=5"
curl http://localhost:8000/strategy-configs
curl http://localhost:8000/strategy-configs/tuning-report
curl http://localhost:8000/strategy-configs/<strategy_config_id>
curl http://localhost:8000/dashboard
curl http://localhost:8000/dashboard/realtime-data
curl http://localhost:8000/metrics
curl http://localhost:8000/execution/kill-switch
curl http://localhost:8000/events/pending
curl http://localhost:8000/paper-orders
curl http://localhost:8000/portfolio/holdings
curl http://localhost:8000/portfolio/holdings?status=closed
curl http://localhost:8000/portfolio/summary
curl http://localhost:8000/portfolio/performance
curl http://localhost:8000/portfolio/recommendation-attribution
curl http://localhost:8000/portfolio/trades
curl http://localhost:8000/portfolio/alerts
```

Replay a previous research snapshot by reusing its `source_snapshot_id` in
`POST /research/run`. Check `universe_summary.snapshot.operation`; it should show
`replayed` when the database snapshot was used instead of the live provider.
For operator replay without rebuilding the full request, call the dedicated endpoint:

```bash
curl -X POST http://localhost:8000/source-snapshots/<source_snapshot_id>/replay \
  -H "Content-Type: application/json" \
  -d '{
    "objective": "ops snapshot replay",
    "publication": {"top_n": 5, "output_channels": ["api"]},
    "risk_policy": {
      "min_confidence": 0,
      "earnings_blackout_minutes": 0,
      "max_name_weight": 0.10,
      "max_sector_weight": 0.30,
      "max_gross_exposure": 1.0,
      "max_correlated_cluster_weight": 0.35,
      "reject_on_material_evidence_conflict": false,
      "event_trading_enabled": true
    }
  }'
```

记录你实际买入的股票（用于卖出提醒）：

```bash
curl -X POST http://localhost:8000/portfolio/buys \
  -H "Content-Type: application/json" \
  -d '{
    "ticker": "MSFT",
    "qty": 100,
    "buy_price": 380,
    "note": "manual swing position"
  }'
```

部分或全部卖出并记录已实现盈亏：

```bash
curl -X POST http://localhost:8000/portfolio/holdings/MSFT/sell \
  -H "Content-Type: application/json" \
  -d '{
    "qty": 50,
    "sell_price": 410,
    "reason": "trim_at_target"
  }'
```

如果省略 `qty`，系统会按当前剩余数量全部卖出并关闭持仓。

复盘组合状态与交易流水：

```bash
curl http://localhost:8000/portfolio/summary
curl http://localhost:8000/portfolio/performance
curl http://localhost:8000/portfolio/recommendation-attribution
curl "http://localhost:8000/portfolio/trades?limit=20"
curl "http://localhost:8000/portfolio/trades?ticker=MSFT&side=sell"
```

`/portfolio/recommendation-attribution` shows realized P&L grouped by both
`recommendation_id` and `source_snapshot_id`, which is the daily check for whether
a replayable market/news snapshot produced useful recommendations after exits.
Use each snapshot row's `performance_score`, `quality_grade`, `expectancy_per_sell`,
`win_rate`, and `profit_factor` to decide whether a signal configuration should be
kept, relaxed, or tightened before the next research batch.
Use `by_strategy_config` in the same response to compare parameter versions across
many snapshots; this is the check for whether a risk policy, signal weight set, or
price-plan version deserves to remain the default.
Then use `/strategy-configs/tuning-report` as the operator-facing summary. It converts
strategy attribution into an action (`collect_more_data`, `keep`, `tighten`, `relax`,
or `review`) and includes concrete suggested parameter deltas when the action is to
tighten or relax.

按当前卖出提醒执行建议动作：

```bash
curl -X POST http://localhost:8000/portfolio/alerts/MSFT/execute \
  -H "Content-Type: application/json" \
  -d '{"reason_code":"stop_loss_breach"}'
```

默认动作由提醒原因决定：止损和第二目标位会清仓，第一目标位和 risk-off 会先减半。
需要覆盖默认动作时可传 `qty`、`sell_price` 或 `sell_all`。

## Approval Gate

Paper-order routing requires recommendation approval first.

```bash
curl -X POST http://localhost:8000/recommendations/<id>/approval \
  -H "Content-Type: application/json" \
  -d '{"decision":"approved","approver":"ops","notes":"approved for paper trading"}'

curl -X POST http://localhost:8000/paper-orders/risk-plan \
  -H "Content-Type: application/json" \
  -d '{"recommendation_id":"<id>","side":"BUY","qty":10,"limit_price":null,"account_equity":100000,"risk_per_trade_pct":0.01,"max_position_pct":0.10}'

curl -X POST http://localhost:8000/paper-orders \
  -H "Content-Type: application/json" \
  -d '{"recommendation_id":"<id>","side":"BUY","qty":10,"limit_price":null,"account_equity":100000,"risk_per_trade_pct":0.01,"max_position_pct":0.10}'

curl -X POST http://localhost:8000/paper-orders \
  -H "Content-Type: application/json" \
  -d '{"recommendation_id":"<id>","side":"BUY","qty":10,"limit_price":null,"execution_mode":"live","dry_run":true,"account_equity":100000,"risk_per_trade_pct":0.01,"max_position_pct":0.10}'

curl "http://localhost:8000/paper-orders?recommendation_id=<id>&status=filled"
```

Filled BUY paper orders automatically create/update the monitored holding and write a
buy row to `/portfolio/trades`, so sell alerts and later recommendation attribution
start from the approved order fill instead of a separate manual entry.
`/paper-orders/risk-plan` shows `recommended_qty`, stop-loss risk, position percentage,
and any violations. `/paper-orders` enforces the same limits unless `enforce_risk_limits`
is explicitly set to `false`.
On the dashboard, use each recommendation row's `建议股数` button to calculate and fill
the current risk-adjusted buy quantity before pressing `买入`.
Set `Exec Mode` to `Live Dry Run` only for broker-adapter rehearsals. It records a
submitted audit order but does not send anything to a broker or create a monitored
holding.

Sell controls use the same execution gate:

```bash
curl -X POST http://localhost:8000/portfolio/holdings/<ticker>/sell \
  -H "Content-Type: application/json" \
  -d '{"sell_price":125.50,"execution_mode":"live","dry_run":true,"reason":"adapter_rehearsal"}'

curl -X POST http://localhost:8000/portfolio/alerts/<ticker>/execute \
  -H "Content-Type: application/json" \
  -d '{"reason_code":"stop_loss_breach","execution_mode":"live","dry_run":true}'
```

Live sell dry-runs emit `sell_routed` with `applied_to_ledger=false`; they do not close
holdings or create sell trades. Real live sells intentionally return `501` until a broker
adapter is configured and reviewed.

Kill switch can pause all execution:

```bash
curl -X POST http://localhost:8000/execution/kill-switch \
  -H "Content-Type: application/json" \
  -d '{"enabled":true,"reason":"incident","updated_by":"ops"}'
```

## Incident Actions

- If recommendation count is unexpectedly low, inspect `rejected_recommendations` reason codes.
- If sector or correlated-cluster exposure is the limiter, inspect
  `universe_summary.portfolio_exposure` and `universe_summary.rejection_counts`.
- If missing data rate spikes, verify ingestion provider health and snapshot generation timestamps.
- If paper-order flow fails, check recommendation ID validity and router logs.
