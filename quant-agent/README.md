# Quant Agent

PRD-aligned quant research and trading recommendation platform.

## Scope

- US equities recommendation workflow
- Deterministic signal scoring and price-plan generation
- Per-recommendation stock analysis content (summary + technical/event/fundamental/execution/risk views)
- Chinese-first analysis narratives with explicit reasons (`analysis.report_cn`, `why_to_buy_cn`, `why_to_sell_cn`)
- Risk/policy guardrails with rejection reason codes
- Recommendation publishing for API, dashboard, and reports
- Paper-trading simulation and backtest hooks

## Repository Layout

- `apps/` API, worker, dashboard
- `services/` ingestion, features, ranking, risk, execution, research, llm
- `domain/` entities and policies
- `infra/` db, cache, observability
- `tests/` unit, integration, regression
- `docs/` PRD and runbooks
- `prompts/` implementation prompts

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,vendors]"
export QUANT_AGENT_ACCESS_PASSWORD="replace-with-a-local-secret"
uvicorn apps.api.main:app --reload
```

API health endpoint: `GET /health`

## Data Provider

- Default provider is real-market mode: `DATA_PROVIDER=yfinance`
- For deterministic/local testing, set `DATA_PROVIDER=mock`

Example:

```bash
DATA_PROVIDER=yfinance uvicorn apps.api.main:app --reload
```

## Source Snapshots and Replay

Research runs persist the exact input snapshot used by the pipeline: universe metadata,
historical bars, fundamentals, news/events, and earnings-blackout timing. The first
run with a new `source_snapshot_id` records the provider inputs; later runs with the
same `source_snapshot_id` replay from the database instead of calling the live vendor.

The run output reports this under `universe_summary.snapshot.operation`:
- `recorded`: provider data was captured into a new snapshot
- `replayed`: provider data was loaded from an existing snapshot
- `disabled`: the pipeline was constructed without a snapshot repository

Snapshot audit and replay endpoints:
- `GET /source-snapshots`
- `GET /source-snapshots/{source_snapshot_id}`
- `GET /source-snapshots/{source_snapshot_id}/bars/{ticker}`
- `POST /source-snapshots/{source_snapshot_id}/replay`
- `POST /source-snapshots/{source_snapshot_id}/replay/compare`

The replay endpoint rebuilds recommendations from the stored market/news inputs and
returns `operation=replayed`, making a live recommendation set reproducible by its
`source_snapshot_id`.
The compare endpoint performs the same replay without writing the replayed run into
the latest recommendation store, then returns a deterministic diff against stored
recommendations for that snapshot. Use `baseline_strategy_config_id` to compare
against a specific strategy version when the same snapshot has been replayed with
multiple parameter sets.

Snapshot performance is reported through `GET /portfolio/recommendation-attribution`.
Each `by_snapshot` row includes `performance_score`, `quality_grade`,
`expectancy_per_sell`, win rate, profit factor, and average recommendation confidence,
so operators can rank which captured market/news states later produced useful exits.

## Strategy Config Versions

Every research run also persists a stable `strategy_config_id` derived from the
non-temporal strategy parameters: universe rules, signal weights, price-plan config,
risk policy, publication settings, and execution mode. It intentionally excludes
`as_of`, `source_snapshot_id`, and the free-form objective, so the same parameter set
can be compared across many market/news snapshots.

Endpoints:
- `GET /strategy-configs`
- `GET /strategy-configs/tuning-report`
- `GET /strategy-configs/{strategy_config_id}`

Recommendations carry both `source_snapshot_id` and `strategy_config_id`; attribution
reports include `by_strategy_config` so operators can compare which parameter versions
produce better realized exits.
`GET /strategy-configs/tuning-report` turns that attribution into conservative
parameter actions: collect more data, keep, tighten, relax, or review. Tighten
recommendations include concrete suggested deltas for confidence thresholds, entry-gap
limits, ATR stop ranges, and signal weights.

## Database and Migrations

Recommended bootstrap (idempotent):

```bash
python -m infra.db.init_db
```

If a local SQLite schema is stale, back up the database first. Automatic rebuild is opt-in:

```bash
DB_RESET_ON_SCHEMA_CONFLICT=1 python -m infra.db.init_db
```

Direct Alembic usage (fresh database preferred):

```bash
alembic upgrade head
```

## Approval and Execution Controls

- Recommendations must be approved before paper-order routing.
- Kill switch endpoint blocks all execution routing when enabled.

Endpoints:
- `POST /recommendations/{id}/approval`
- `GET /recommendations/{id}/approval`
- `GET /recommendations/{id}/evidence`
- `GET /source-snapshots`
- `POST /source-snapshots/{source_snapshot_id}/replay`
- `POST /source-snapshots/{source_snapshot_id}/replay/compare`
- `GET /operations/control-center`
- `GET /strategy-configs`
- `GET /strategy-configs/tuning-report`
- `GET /paper-orders`
- `POST /paper-orders/risk-plan`
- `POST /paper-orders`
- `GET /execution/kill-switch`
- `POST /execution/kill-switch`

Filled BUY paper orders are automatically synchronized into portfolio monitoring and
the trade ledger, so approved dashboard buys flow into stop/take-profit alerts and
later P&L attribution. Manual buys remain available for trades placed outside the
paper router.
Use `GET /paper-orders` to audit recent submitted, filled, or canceled paper orders
separately from the trade ledger and P&L records.
`POST /paper-orders/risk-plan` computes the maximum and recommended order quantity
from account equity, per-trade risk, max position size, max gross exposure, max
sector exposure, entry price, and stop-loss distance. `POST /paper-orders` enforces
the same plan by default before filling a paper order.
Order requests now carry `execution_mode`: `paper` fills through the simulator, while
`live` is accepted only as `dry_run=true` unless a future broker adapter is explicitly
configured. Live dry-runs return a submitted audit order with `broker_order_id` and
`adapter_message`, but do not create holdings or send anything to a broker.
Sell requests and alert execution use the same gate. `execution_mode=live` with
`dry_run=true` validates the exit, emits a `sell_routed` event, and returns adapter
metadata without closing the holding or writing a sell trade. Confirmed live sells return
`501` until a real broker adapter is configured.

Default risk guardrails:
- `min_confidence=0.72`
- `max_entry_gap_pct=0.30` (reject plans too far from current spot)
- `max_name_weight=0.10`, `max_sector_weight=0.30`, and `max_correlated_cluster_weight=0.35`
- In live mode (`snapshot_mode=latest`), engine applies calibrated confidence relaxation to preserve coverage (`effective_min_confidence = max(0.0, min_confidence - 0.08)`).

Portfolio risk is applied after candidates are ranked by composite score. The run output includes
`universe_summary.portfolio_exposure` so operators can inspect the published name, sector, and
correlated-cluster weights that shaped the final recommendation set.

Default publication:
- `top_n=8`

Default real-data universe:
- `SP500` preset expanded to 20 liquid large-cap names for better candidate coverage.

## Dashboard and Event Queue

- Realtime dashboard (auto-refresh by session: market open 1m, closed 30m): `GET /dashboard`
- Realtime dashboard payload: `GET /dashboard/realtime-data`
- Recommendation detail page: `GET /dashboard/recommendations/{id}`
- Operations control center: `GET /operations/control-center`
- Pending events: `GET /events/pending`
- Consumed events: `GET /events/consumed`
- Consume events: `POST /events/consume?limit=100`

`/operations/control-center` is the machine-readable daily cockpit: it combines the
kill switch, pending approvals, approved recommendations ready for buy routing, active
sell alerts, pending events, and recent execution audit counts into prioritized next
actions. It is read-only and is safe to poll from scripts.
System events are persisted in the database, so pending and consumed event audit trails
survive API or worker restarts.

## Worker Automation

The conservative full-system loop is:

```bash
python -m apps.worker.main system_cycle --top-n 8 --min-confidence 0.0
```

It captures or replays the latest source snapshot, generates recommendations, refreshes
portfolio sell alerts, and prints a JSON operational summary with recommendation,
alert, event, and metric counts. By default it does not auto-buy or auto-sell.

Explicit automatic execution mode:

```bash
python -m apps.worker.main system_cycle --top-n 8 --min-confidence 0.0 \
  --auto-execute-approved --auto-execution-mode paper
```

Automatic execution is conservative: sell alerts are handled first, buys only route
recommendations that already have an `approved` decision, and all actions still pass
through the existing kill switch, risk sizing, paper-order, live-dry-run, sell audit,
trade-ledger, and event gates. Use `--auto-execution-mode live_dry_run` to validate
broker-shaped orders without mutating holdings. Use `--max-auto-buys`,
`--max-auto-sells`, `--account-equity`, `--risk-per-trade-pct`,
`--max-position-pct`, `--max-gross-exposure-pct`, and
`--max-sector-exposure-pct` to tune the automatic sizing envelope.

Use `--consume-events` only when the printed summary is your audit sink and you want
pending in-memory events drained after the cycle.
Every successful cycle is persisted as a durable heartbeat and can be reviewed with
`GET /operations/system-runs`; the JSON output includes `system_cycle_run_id` and
an `auto_execution` action report.

For unattended local operation, run the bounded or continuous loop wrapper:

```bash
python -m apps.worker.main system_cycle_loop --interval-seconds 300 \
  --auto-execute-approved --auto-execution-mode paper
```

`system_cycle_loop` repeatedly calls the same audited `system_cycle` path. Add
`--max-cycles 2` for smoke tests or launchd/cron probes; omit it for continuous
operation. The loop prints a final JSON report with cycle counts, errors, and the
last `system_cycle_run_id`.

## Backtest (Real Historical Data)

- Backtest engine now uses walk-forward historical bars (not synthetic TP-proxy math).
- Period metrics include annualized return, benchmark return, alpha, information ratio, fill rate, and max drawdown.

Endpoint:
- `POST /backtests/runs`

Example one-year run:

```bash
curl -X POST "http://127.0.0.1:8000/backtests/runs" \
  -H "Content-Type: application/json" \
  -H "x-access-password: $QUANT_AGENT_ACCESS_PASSWORD" \
  -d '{
    "run_name": "one_year_real_data",
    "start_date": "2025-04-13T00:00:00Z",
    "end_date": "2026-04-13T00:00:00Z",
    "benchmark": "SPY",
    "top_n": 10,
    "rebalance_frequency": "monthly",
    "transaction_cost_bps": 10
  }'
```

## Manual Buy Tracking and Sell Alerts

- Record a manual buy: `POST /portfolio/buys`
- List holdings: `GET /portfolio/holdings?status=open|closed|all`
- Portfolio summary: `GET /portfolio/summary`
- Portfolio performance review: `GET /portfolio/performance`
- Recommendation attribution: `GET /portfolio/recommendation-attribution`
- Trade ledger: `GET /portfolio/trades`
- Update holding stop/target controls: `PATCH /portfolio/holdings/{ticker}/controls`
- Holding control audit: `GET /portfolio/holding-control-audits`
- Sell execution audit: `GET /portfolio/sell-executions`
- Sell part or all of a holding: `POST /portfolio/holdings/{ticker}/sell`
- Execute an active sell alert: `POST /portfolio/alerts/{ticker}/execute`
- Close a holding: `POST /portfolio/holdings/{ticker}/close`
- Get sell alerts: `GET /portfolio/alerts`
- Sell alert history: `GET /portfolio/alert-history`

Sell alerts are Chinese-first and reason-based (stop-loss breach, target hit, regime risk-off).
Alert execution uses the alert reason to choose a default action: stop-loss and second target sell all,
first target and risk-off reduce half unless `qty`, `sell_price`, or `sell_all` is supplied.
Scheduled `system_cycle` runs persist every generated sell alert into alert history with
`monitor_run_id`, so the monitoring trail survives process restarts. Dashboard refreshes
still compute current alerts without writing new history rows.
Sell controls record sell price, quantity, reason, realized P&L, and whether the holding remains open.
Holding controls can be tightened or relaxed while a position is open; every stop-loss,
target, or note change writes a durable holding-control audit row and emits a
`holding_controls_updated` event.
Every manual buy and sell also writes an immutable trade-ledger entry so repeated ticker cycles remain auditable
even when the current holding watch row is reopened or overwritten.
Every sell route also writes a sell-execution audit row. Paper sells are marked
`applied_to_ledger=true`; live dry-runs are marked `applied_to_ledger=false` and remain
queryable after restart even though they do not create a sell trade.
Performance review is derived from the ledger and reports win rate, profit factor, expectancy per sell,
best/worst realized trade, and per-ticker attribution.
Recommendation attribution connects sell results back to the original `recommendation_id` and
`source_snapshot_id`, so a replayed research snapshot can be compared against later realized P&L.
Snapshot rows also expose a 0-100 `performance_score` and `quality_grade` derived from
realized P&L, win rate, profit factor, and expectancy per sell.
Strategy rows expose the same metrics grouped by `strategy_config_id`, letting operators
separate market-regime effects from parameter-version effects.
