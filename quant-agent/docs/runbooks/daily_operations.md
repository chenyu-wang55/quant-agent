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
```

## API Checks

```bash
curl http://localhost:8000/health
curl http://localhost:8000/recommendations/latest
curl http://localhost:8000/recommendations/<id>/evidence
curl http://localhost:8000/dashboard
curl http://localhost:8000/dashboard/realtime-data
curl http://localhost:8000/metrics
curl http://localhost:8000/execution/kill-switch
curl http://localhost:8000/events/pending
curl http://localhost:8000/portfolio/holdings
curl http://localhost:8000/portfolio/alerts
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

## Approval Gate

Paper-order routing requires recommendation approval first.

```bash
curl -X POST http://localhost:8000/recommendations/<id>/approval \
  -H "Content-Type: application/json" \
  -d '{"decision":"approved","approver":"ops","notes":"approved for paper trading"}'
```

Kill switch can pause all execution:

```bash
curl -X POST http://localhost:8000/execution/kill-switch \
  -H "Content-Type: application/json" \
  -d '{"enabled":true,"reason":"incident","updated_by":"ops"}'
```

## Incident Actions

- If recommendation count is unexpectedly low, inspect `rejected_recommendations` reason codes.
- If missing data rate spikes, verify ingestion provider health and snapshot generation timestamps.
- If paper-order flow fails, check recommendation ID validity and router logs.
