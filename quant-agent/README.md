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
- `GET /execution/kill-switch`
- `POST /execution/kill-switch`

Default risk guardrails:
- `min_confidence=0.72`
- `max_entry_gap_pct=0.30` (reject plans too far from current spot)
- `max_name_weight=0.10`, `max_sector_weight=0.30`, and `max_correlated_cluster_weight=0.35`
- In live mode (`snapshot_mode=latest`), engine applies calibrated confidence relaxation to preserve coverage (`effective_min_confidence = max(0.60, min_confidence - 0.08)`).

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
- Pending events: `GET /events/pending`
- Consume events: `POST /events/consume?limit=100`

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
- List open holdings: `GET /portfolio/holdings`
- Close a holding: `POST /portfolio/holdings/{ticker}/close`
- Get sell alerts: `GET /portfolio/alerts`

Sell alerts are Chinese-first and reason-based (stop-loss breach, target hit, regime risk-off).
