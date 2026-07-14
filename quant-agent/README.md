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

Runtime environment parsing is centralized in `infra/config.py`. API state behavior is split into
focused mixins (`state_orders`, `state_autopilot`, `state_trading`, `state_sells`, and
`state_portfolio`); Worker broker synchronization and automatic execution live in separate modules,
and the Dashboard HTML asset is isolated from its data API.

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.lock
pip install --no-deps -e .
export QUANT_AGENT_ACCESS_PASSWORD="replace-with-a-local-secret"
uvicorn apps.api.main:app --reload
```

Health endpoints:

- `GET /health` and `GET /health/live`: process liveness
- `GET /health/ready`: database/provider/broker/snapshot/reconciliation readiness plus a separate
  `trading_ready` decision and explicit blockers

For a persistent loopback-only API on macOS, install the authenticated LaunchAgent:

```bash
export QUANT_AGENT_ACCESS_PASSWORD="a-long-local-password"
export QUANT_AGENT_AUTH_SIGNING_SECRET="$(openssl rand -hex 32)"
python scripts/manage_api_launchd.py install --load
python scripts/manage_api_launchd.py status
```

The generated plist is mode `0600`, binds only to `127.0.0.1`, keeps authentication
enabled, uses an absolute production database path, starts at login, and writes through
the rotating structured-log handler. A non-loopback host, short password, or short signing
secret is rejected. Use `python scripts/manage_api_launchd.py uninstall --unload` to remove it.

## Authentication and execution roles

Browser access uses `POST /auth/login` and a signed, expiring HttpOnly session cookie. Passwords in
URLs are rejected; do not use the former `?pwd=` pattern. Dashboard write requests also send the
session-bound `quant_csrf` token in `x-csrf-token`.

The legacy `x-access-password` header remains available for local scripts. A single
`QUANT_AGENT_ACCESS_PASSWORD` grants the highest role for backward compatibility. For separated
permissions, configure distinct values:

```bash
export QUANT_AGENT_READ_PASSWORD="..."
export QUANT_AGENT_APPROVAL_PASSWORD="..."
export QUANT_AGENT_EXECUTION_PASSWORD="..."
export QUANT_AGENT_AUTH_SIGNING_SECRET="a-separate-long-random-secret"
export QUANT_AGENT_COOKIE_SECURE=1  # default outside tests; terminate TLS before the API
```

Set `QUANT_AGENT_COOKIE_SECURE=0` only for a loopback-only HTTP deployment.

- `read`: dashboards, GET APIs, metrics, and protected OpenAPI documentation
- `approve`: read access plus research/backtest writes and recommendation approval
- `execute`: approval access plus order, portfolio, execution-control, and operations writes

Automatic execution is restricted to the NYSE (`XNYS`) regular session by default. The calendar
uses the maintained `exchange_calendars` XNYS schedule for exchange holidays, observed holidays,
one-off closures, and 13:00 ET early closes; dates outside its supported 1990–2050 range fail
closed instead of falling back to hand-written rules.

## Order idempotency and recovery

Buy and sell execution requests accept `idempotency_key`. Confirmed live orders require it. The
system persists an intent before broker I/O, derives a stable `client_order_id`, and uses unique
database indexes to collapse concurrent retries. Ambiguous broker responses are reconciled by
`client_order_id`; the Worker automatically revisits unknown or stale submissions. Local order,
holding, position, trade-ledger, audit, and event effects commit atomically.

Database maintenance commands always operate on an explicit database/backup path:

```bash
python3 scripts/manage_database.py backup \
  --output backups/quant_agent_v2-before-maintenance.db
python3 scripts/manage_database.py restore \
  --backup backups/quant_agent_v2-before-maintenance.db \
  --output /tmp/quant-agent-restored.db
python3 scripts/manage_database.py retention-plan \
  --created-before "2026-01-01T00:00:00Z" --keep-latest 500
python3 scripts/manage_database.py archive-and-prune --help
python3 scripts/manage_database.py compact --help
python3 scripts/manage_database.py inspect-test-snapshots --help
python3 scripts/manage_database.py cleanup-test-snapshots --help
```

The retention workflow is deliberately two-step: inspect a plan, then archive the complete
database and prune only the exact expected snapshot count. It refuses to prune snapshots with
operational order, trade, holding, or approval references. `compact` requires a verified backup
before it runs SQLite `VACUUM`. Keep daily backups for 14 days, weekly backups for 12 weeks, and
monthly backups for 12 months; test a restore into a new path at least monthly.

## Data Provider

- Default provider is real-market mode: `DATA_PROVIDER=yfinance`
- For deterministic/local testing, set `DATA_PROVIDER=mock`
- Real-provider runs require point-in-time universe membership via
  `POINT_IN_TIME_UNIVERSE_CSV`. Historical runs additionally require
  `POINT_IN_TIME_FUNDAMENTALS_CSV`, `POINT_IN_TIME_EVENTS_CSV`, and
  `POINT_IN_TIME_EARNINGS_CSV`; the system fails closed when these are absent.

Example:

```bash
DATA_PROVIDER=yfinance uvicorn apps.api.main:app --reload
```

Point-in-time CSV contracts:

- universe: `universe,ticker,effective_from,effective_to,sector,market_cap_usd,spread_bps,source`
- fundamentals: `ticker,period_end,available_at,pe_ttm,roe,revenue_growth_yoy,eps_revision_30d,source`
- events: `source_id,published_at,ingested_at,headline,normalized_text,tickers,event_type,sentiment,relevance,horizon,source_url,source`
- earnings: `ticker,known_at,earnings_at,source`

Before a historical backtest, validate the complete licensed export offline and retain the
JSON report with the run evidence:

```bash
python scripts/validate_point_in_time_data.py \
  --start 2021-01-01T00:00:00Z --end 2025-12-31T00:00:00Z \
  --output artifacts/pit-validation-2021-2025.json
```

The command reads the four `POINT_IN_TIME_*_CSV` variables by default. It checks every XNYS
trading point in the requested range, minimum constituent coverage, overlapping memberships,
explicit timestamp timezones, source/provenance fields, fundamental availability and age,
event publication/ingestion ordering, and earnings-known ordering. The default maximum
fundamental age is 550 days; configure `POINT_IN_TIME_MAX_FUNDAMENTAL_AGE_DAYS` or pass
`--max-fundamental-age-days` only when the licensed feed's publication cadence justifies it.
The report records each input file's SHA-256 and a content-derived `dataset_fingerprint`.

Production validation requires both `SP500` and `NASDAQ100` by default and rejects active
membership sets below 450 and 90 securities respectively, overlapping rows for the same ticker,
blank sources, or incomplete sector/capitalization/spread metadata. Configure a smaller intended
scope explicitly with `POINT_IN_TIME_REQUIRED_UNIVERSES` and the corresponding
`POINT_IN_TIME_MIN_<UNIVERSE>_CONSTITUENTS`; do not lower these thresholds merely to make a
partial export pass readiness.

Use an official or licensed historical constituent feed. S&P DJI describes constituent,
weight, GICS, and composition-event delivery as subscription index data:
<https://www.spglobal.com/spdji/en/documents/index-policies/index-data-capabilities-brochure.pdf>.
Nasdaq exposes NDX weighting data through its index portal, with additional history requiring
full-access login: <https://indexes.nasdaqomx.com/Index/Weighting/NDX>. Current constituent pages
or reconstructed static ticker lists are not acceptable substitutes for point-in-time history.

All timestamps must include a timezone. Every returned bar, fundamental record, event, and
universe row is checked against the run's `as_of`. A future timestamp raises an error; vendor
failures are recorded in snapshot quality metadata. Fallback or unverified data sets
`live_execution_allowed=false`, so it cannot pass the Worker execution gate. `MarketBar`
exports include adjusted close, dividends, splits, source, and quality status.
Real-provider backtests invoke the same full-range validator automatically before any
simulation work. The dataset fingerprint is included in the backtest configuration hash and
daily snapshot IDs, so changed source files cannot silently replay snapshots from an older
export with the same strategy parameters.

## Source Snapshots and Replay

Research runs persist the exact input snapshot used by the pipeline: universe metadata,
historical bars, fundamentals, news/events, and earnings-blackout timing. The first
run with a new `source_snapshot_id` records the provider inputs; later runs with the
same `source_snapshot_id` replay from the database instead of calling the live vendor.
Historical bars are content-addressed across every persisted price, corporate-action, quality,
and provenance field in `market_bars`; a vendor revision creates a new immutable bar version
instead of rewriting older snapshot evidence. Snapshots store compact integer references, so
identical repeated 260-day histories are not copied again. Snapshot reads use a
`(ticker, timestamp)` index and the snapshot-reference primary key.

The run output reports this under `universe_summary.snapshot.operation`:
- `recorded`: provider data was captured into a new snapshot
- `replayed`: provider data was loaded from an existing snapshot
- `disabled`: the pipeline was constructed without a snapshot repository

Snapshot summaries include `data_quality` coverage and freshness metrics for captured bars,
fundamentals, and news/event tickers. `system_cycle` checks this before any
automatic approval or automatic execution. If bar or fundamental coverage is below
the configured threshold, or the latest captured bar is too old, the cycle records
`snapshot_quality_gate.passed=false` and skips automatic actions for that run.

Snapshot audit and replay endpoints:
- `GET /source-snapshots`
- `GET /source-snapshots/{source_snapshot_id}`
- `GET /source-snapshots/{source_snapshot_id}/export`
- `GET /source-snapshots/{source_snapshot_id}/bars/{ticker}`
- `POST /source-snapshots/{source_snapshot_id}/replay`
- `POST /source-snapshots/{source_snapshot_id}/replay/compare`

The replay endpoint rebuilds recommendations from the stored market/news inputs and
returns `operation=replayed`, making a live recommendation set reproducible by its
`source_snapshot_id`.
The export endpoint returns the full captured input evidence package for audit:
universe securities, bars grouped by ticker, fundamentals grouped by ticker,
news/events, and snapshot metadata/data-quality fields.
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
- `GET /source-snapshots/{source_snapshot_id}/export`
- `POST /source-snapshots/{source_snapshot_id}/replay`
- `POST /source-snapshots/{source_snapshot_id}/replay/compare`
- `GET /operations/control-center`
- `POST /operations/system-cycle`
- `GET /strategy-configs`
- `GET /strategy-configs/tuning-report`
- `GET /paper-orders`
- `POST /paper-orders/risk-plan`
- `POST /paper-orders`
- `POST /paper-orders/broker-sync`
- `POST /paper-orders/{order_id}/fill`
- `POST /paper-orders/{order_id}/cancel`
- `POST /portfolio/sell-executions/broker-sync`
- `GET /execution/kill-switch`
- `POST /execution/kill-switch`
- `GET /execution/autopilot-policy`
- `POST /execution/autopilot-policy`
- `GET /execution/market-session`

Filled BUY paper orders are automatically synchronized into portfolio monitoring and
the trade ledger, so approved dashboard buys flow into stop/take-profit alerts and
later P&L attribution. Manual buys remain available for trades placed outside the
paper router.
Use `GET /paper-orders` to audit recent submitted, filled, or canceled paper orders
separately from the trade ledger and P&L records. Submitted broker or dry-run orders
can be resolved with `POST /paper-orders/{order_id}/fill` or
`POST /paper-orders/{order_id}/cancel`; for confirmed live broker BUY orders, cancel
first calls the configured broker adapter and only then marks the local order canceled.
Broker pollers/webhooks can batch the same lifecycle updates through
`POST /paper-orders/broker-sync`.
`POST /paper-orders/risk-plan` computes the maximum and recommended order quantity
from account equity, per-trade risk, max position size, max gross exposure, max
sector exposure, entry price, and stop-loss distance. `POST /paper-orders` enforces
the same plan by default before filling a paper order.
Order requests now carry `execution_mode`: `paper` fills through the simulator, while
`live` supports either `dry_run=true` rehearsals or `confirm_live=true` confirmed
broker submissions when `QUANT_BROKER_ADAPTER=alpaca` and Alpaca credentials are
configured. Live dry-runs return a submitted audit order with `broker_order_id` and
`adapter_message`, but do not create holdings or send anything to a broker.
Confirmed live BUY orders still require approval and risk-plan checks; filled broker
responses create monitored holdings and trade-ledger rows through the same fill path.
Alpaca live BUY submissions use a broker-native bracket order: the recommendation's
second target is the take-profit limit and its stop-loss is the stop order. The exit
protection therefore remains active at the broker if this process is offline.
Sell requests and alert execution use the same gate. `execution_mode=live` with
`dry_run=true` validates the exit, emits a `sell_routed` event, and returns adapter
metadata without closing the holding or writing a sell trade. Confirmed live SELL
orders use the same configured Alpaca adapter; immediate broker fills update holdings
and trade-ledger rows, while submitted or rejected responses are audited without
mutating local holdings.

Alpaca adapter environment:

```bash
export QUANT_BROKER_ADAPTER=alpaca
export ALPACA_BASE_URL=https://paper-api.alpaca.markets
export ALPACA_API_KEY=<paper-key>
export ALPACA_SECRET_KEY=<paper-secret>
```

Default risk guardrails:
- `min_confidence=0.72`
- `max_entry_gap_pct=0.30` (reject plans too far from current spot)
- `max_name_weight=0.10`, `max_sector_weight=0.30`, and `max_correlated_cluster_weight=0.35`
- `max_gross_exposure=1.00`, `max_portfolio_beta=1.50`,
  `max_portfolio_volatility=0.45`, and `max_liquidation_days=5.0`
- Live and point-in-time modes apply the configured `min_confidence` without an unvalidated
  automatic relaxation.

Portfolio risk is applied after candidates are ranked by composite score. The run output includes
`universe_summary.portfolio_exposure` so operators can inspect published name and sector weights,
return-derived correlation clusters, gross exposure, portfolio beta and volatility, maximum
liquidation days, and liquidity stress loss. BUY requests reserve gross exposure inside a
serialized SQLite transaction before broker I/O; concurrent requests cannot both spend the same
remaining capacity. Reservations are committed on fill, released on definitive cancel/rejection,
and retained after ambiguous broker responses until reconciliation resolves the order.

Default publication:
- `top_n=8`

Default real-data universe:
- `SP500` preset expanded to 20 liquid large-cap names for better candidate coverage.

## Dashboard and Event Queue

- Realtime dashboard (auto-refresh by session: market open 1m, closed 30m): `GET /dashboard`
- Realtime dashboard payload: `GET /dashboard/realtime-data`
- Recommendation detail page: `GET /dashboard/recommendations/{id}`
- Paper-shadow readiness (visible even while Autopilot is off): `GET /execution/paper-shadow-readiness`
- Operations control center: `GET /operations/control-center`
- Pending events: `GET /events/pending`
- Consumed events: `GET /events/consumed`
- Consume events: `POST /events/consume?limit=100`

`/operations/control-center` is the machine-readable daily cockpit: it combines the
kill switch, autopilot policy, pending approvals, approved recommendations ready for
buy routing, active sell alerts, pending events, and recent execution audit counts
into prioritized next actions. It is read-only and is safe to poll from scripts.
`POST /operations/system-cycle` runs one audited `system_cycle` from the API and
defaults to `use_autopilot_policy=true`, which is the dashboard's manual "run now"
button for recommendation generation, monitoring, and any policy-enabled automatic
actions. The response includes `autopilot_preflight`; if the preflight is `blocked`
or `off`, automatic approval and execution are forced off for that cycle even when
policy flags are enabled.
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
through the existing kill switch, risk sizing, paper-order, broker live, sell audit,
trade-ledger, event, source-snapshot quality, and portfolio open-risk gates. Use
`--auto-execution-mode live_dry_run` to validate broker-shaped orders without mutating
holdings. Use `--auto-execution-mode live` only after configuring a broker adapter,
passing position reconciliation, and explicitly adding `--allow-auto-live-execution`
or setting `QUANT_ALLOW_AUTOPILOT_LIVE=1`; otherwise the live gate blocks automatic
broker submissions with `auto_live_execution_not_allowed`. When live automatic
execution is allowed, the worker also pulls a broker account snapshot before routing
orders; blocked/non-active accounts stop the cycle with `broker_account_gate_failed`,
live automatic buys size risk from broker `equity` or `portfolio_value`, and automatic
buys are skipped when broker `buying_power` cannot cover the risk-plan notional. If
broker equity is missing, live automatic buys are skipped instead of falling back to a
stale static balance. Use `--max-auto-buys`, `--max-auto-sells`, `--account-equity`,
`--risk-per-trade-pct`,
`--max-position-pct`, `--max-gross-exposure-pct`, and
`--max-sector-exposure-pct` to tune the automatic sizing envelope. Use
`--max-open-risk-pct` to stop new automatic buys when current portfolio risk to stop
is already too high while still allowing automatic sell alerts to run. Use
`--max-auto-buy-price-drift-pct` to skip automatic buys when the latest price has
moved too far away from the recommendation entry zone. Use
`--min-snapshot-bar-coverage` and `--min-snapshot-fundamental-coverage` to tune the
minimum data completeness required before automatic actions can run. Use
`--max-snapshot-bar-age-minutes` to tune how old the latest captured bar may be
before automatic actions are blocked.

Full autopilot mode also auto-approves qualifying recommendations before execution:

```bash
python -m apps.worker.main system_cycle --top-n 8 --min-confidence 0.0 \
  --auto-approve-recommendations --auto-approve-min-confidence 0.72 \
  --auto-execute-approved --auto-execution-mode paper
```

Auto-approval is disabled by default. When enabled it writes the same approval audit
record as the API, caps approvals with `--max-auto-approvals`, skips open holdings,
and requires both `--auto-approve-min-confidence` and
`--auto-approve-min-composite`.

For unattended operation, persist those controls as an auditable autopilot policy and
let the worker read it each cycle:

```bash
curl -X POST http://localhost:8000/execution/autopilot-policy \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": true,
    "auto_approve_recommendations": true,
    "auto_execute_approved": true,
    "auto_execution_mode": "paper",
    "auto_approve_min_confidence": 0.72,
    "max_auto_approvals": 1,
    "max_auto_buys": 1,
    "max_auto_sells": 10,
    "updated_by": "ops",
    "reason": "paper autopilot"
  }'

python -m apps.worker.main system_cycle --top-n 8 --min-confidence 0.0 \
  --use-autopilot-policy
```

For real broker autopilot, set the policy `auto_execution_mode` to `live` and run the
cycle with `--allow-auto-live-execution` after the broker adapter, credentials, kill
switch, source snapshot, risk, daily budget, broker account, and reconciliation gates
are all green.
Without that runtime allow switch, `autopilot_preflight` remains blocked even if the
policy is otherwise enabled.

The policy has a global `enabled` switch. When it is false, `--use-autopilot-policy`
forces automatic approval and execution off even if older CLI flags are present.
When it is true, each cycle still runs an `autopilot_preflight` gate. Kill switch,
zero-capacity approval/execution settings, or disabled action types make the cycle
skip unsafe automatic actions while still recording the research run and monitoring
results. Set `restrict_auto_execution_to_regular_hours=true` when automatic buys and
sells should be blocked outside the XNYS regular session
(`09:30-16:00 America/New_York`, with holidays and early closes). `GET /execution/market-session` exposes the same
session status used by preflight. Daily automatic action budgets
(`max_daily_auto_approvals`, `max_daily_auto_buys`, `max_daily_auto_sells`) are also
checked from persisted `system_cycle` history so a fast loop cannot keep spending the
same per-cycle budget all day. The same preflight also evaluates
`max_daily_realized_loss_pct` against the trade ledger for the current US trading day;
when the loss gate trips, automatic approvals and automatic buys stop while sell-alert
execution can still reduce risk. `order_dedupe_minutes` blocks repeat automatic buy
orders for the same recommendation or ticker after a recent routed buy order, which
keeps fast unattended loops from duplicating broker submissions.
`max_auto_buy_price_drift_pct` blocks automatic buys when the current market price
has drifted beyond the allowed entry-zone tolerance, reducing stale-snapshot chasing.
Submitted buy orders also block new automatic buys for the same recommendation or
ticker until the order is filled or canceled, independent of the time-based dedupe
window. Use `POST /paper-orders/{order_id}/cancel` to cancel a submitted dry-run or
broker-submitted order; confirmed live broker BUY cancellation calls the broker adapter
before updating the local order. Use `POST /paper-orders/{order_id}/fill` to record a
broker fill and release the pending-order gate. Pending live broker SELL orders also
block automatic sell-alert execution for the same ticker until broker sync marks the
sell filled, canceled, rejected, or expired; this prevents repeated sell submissions
when the previous broker sell is still open. At the start of every `system_cycle`, the
worker automatically polls the configured broker adapter for submitted live buy orders
and live sell executions, then applies the same sync path used by
`POST /paper-orders/broker-sync` and `POST /portfolio/sell-executions/broker-sync`.
Use `--disable-auto-broker-sync` to turn that polling off, or `--max-broker-sync-items`
to cap the number of pending buys and sells queried per cycle. External webhooks or
manual operators can still send broker status snapshots to the sync endpoints;
`filled` snapshots update holdings and trades, while `canceled` or `rejected`
snapshots resolve submitted orders as canceled.
`sell_alert_cooldown_minutes` similarly prevents the same ticker/reason sell alert
from repeatedly selling partial positions on every loop.

Live autopilot also requires at least `min_paper_shadow_trading_days` successful paper
shadow cycles on distinct XNYS trading days (default `20`). Paper mode is allowed to
bootstrap before this threshold; only live mode is blocked by the threshold. A day counts
only when a policy-driven cycle runs during the regular XNYS session with the autopilot
policy and paper auto-execution enabled, snapshot/daily-loss/reconciliation gates passing,
the execution path actually evaluated, and no execution errors. Each run persists explicit
`paper_shadow_evidence`; missing or legacy inferred metrics fail closed, duplicate cycles on
one trading date count once, and synthetic/backdated rows are not operational evidence.
The dashboard always displays the distinct-day progress, and
`GET /execution/paper-shadow-readiness` returns the policy-derived required, observed,
remaining, first, and latest qualifying dates without changing the policy or execution state.
Unit/integration tests bypass only the elapsed-time result where needed while still
exercising the other execution controls.
For live automatic execution, the same cycle records `broker_account_gate` inside
`auto_execution`: the gate includes normalized cash, buying power, equity, account
status, and block flags. Broker account or trading blocks stop all live automatic
orders. `account_equity_gate` records whether live buy sizing used broker `equity` or
`portfolio_value`; if neither is available, live buys are skipped. Missing or
insufficient buying power skips automatic buys before broker submit.
If the configured broker adapter supports positions, each `system_cycle` also pulls a
broker position snapshot and records a reconciliation report before the execution gate
is evaluated. Use `--disable-auto-position-reconciliation` to skip this, or
`--position-reconciliation-qty-tolerance` to tune quantity matching. Manual snapshots
can still be posted to `POST /portfolio/reconciliation`. Reconciliation responses are
persisted in `position_reconciliations`, emit a `position_reconciliation` event, and
set `blocks_auto_execution=true` whenever local and broker quantities disagree.
Live automatic execution always requires the latest reconciliation to be `matched` or
`empty` and still within `max_position_reconciliation_age_minutes`, even if the policy
field is false. For paper automation, set `require_position_reconciliation=true` in
the autopilot policy, or run `system_cycle --require-position-reconciliation`, to make
the same gate mandatory.

Use `--consume-events` only when the printed summary is your audit sink and you want
pending in-memory events drained after the cycle.
Every successful cycle is persisted as a durable heartbeat and can be reviewed with
`GET /operations/system-runs`; the JSON output includes `system_cycle_run_id` and
an `auto_execution` action report.

For unattended local operation, run the bounded or continuous loop wrapper:

```bash
python -m apps.worker.main system_cycle_loop --interval-seconds 300 \
  --use-autopilot-policy
```

`system_cycle_loop` repeatedly calls the same audited `system_cycle` path. Add
`--max-cycles 2` for smoke tests or launchd/cron probes; omit it for continuous
operation. The loop prints a final JSON report with cycle counts, errors, and the
last `system_cycle_run_id`. Failed cycles are also persisted in `system_cycle_runs`
with `status=error`. Set `--max-consecutive-errors N` to activate the kill switch and
stop the loop after repeated failures.

On macOS, render or install a user LaunchAgent for the same loop:

```bash
python scripts/manage_launchd.py render --use-autopilot-policy --data-provider yfinance
python scripts/manage_launchd.py install --use-autopilot-policy --data-provider yfinance --load
python scripts/manage_launchd.py install --auto-approve-recommendations \
  --auto-execute-approved --data-provider yfinance --load
python scripts/manage_launchd.py status
python scripts/manage_launchd.py uninstall --unload
```

The LaunchAgent writes structured JSON to
`~/Library/Logs/com.quant-agent.system-cycle-loop.json.log`, rotates at 20 MiB, and keeps ten
archives by default. Override this with `--log-file`, `--log-max-bytes`, and
`--log-backup-count`. Raw launchd stdout/stderr is discarded so it cannot grow without bounds.
The worker remains managed by `launchd`.

## Observability and alerts

Logs are JSON by default and include `correlation_id`, `run_id`, and `order_id` where applicable.
API responses echo `x-correlation-id`; password, authorization, API-key, and secret values are
redacted before output. Configure an application log file with:

```bash
export QUANT_AGENT_LOG_FILE="$HOME/Library/Logs/quant-agent.json.log"
export QUANT_AGENT_LOG_MAX_BYTES=$((20 * 1024 * 1024))
export QUANT_AGENT_LOG_BACKUP_COUNT=10
```

Operational counters and gauges are persisted in SQLite rather than reset on process restart.
`GET /metrics` returns the JSON operational view and `GET /metrics/prometheus` returns Prometheus
text exposition. `GET /operations/alerts` lists active persisted alerts. Readiness evaluation and
each Worker cycle activate or resolve alerts for provider failures, consecutive cycle failures,
database size, and broker-position reconciliation differences. Relevant thresholds are
`QUANT_AGENT_DB_SIZE_ALERT_BYTES` and `QUANT_AGENT_CONSECUTIVE_ERROR_ALERT_COUNT`.

## Development checks and CI

The repository contains an exact `requirements.lock` and GitHub Actions runs on Python 3.11 and
3.13 for every pull request. Run the same gates locally:

```bash
ruff check .
mypy
python scripts/check_core_module_size.py --max-lines 850
pytest -q tests/unit/test_migrations.py
pytest -q
pytest -q tests/unit/test_yfinance_provider.py tests/unit/test_data_provenance.py \
  tests/unit/test_point_in_time_validator.py \
  --cov=services.ingestion.vendors.yfinance_provider \
  --cov=services.ingestion.point_in_time_validator \
  --cov-report=term-missing --cov-fail-under=70
```

Migration tests require a single Alembic head, build a fresh database through head, exercise the
0031-to-head downgrade/upgrade path, and run SQLite integrity checks. The real yfinance adapter's
offline contract suite covers live and historical bars, point-in-time CSVs, fundamentals, events,
earnings, retries, caching, and explicit fallback behavior; CI enforces at least 70% coverage.
CI also caps the API state mixins, Worker modules, and dashboard Python entrypoint at 850
lines. The large dashboard HTML/CSS template is deliberately kept in `home_page.py` and is
not counted as a Python business-service module.

## Backtest (Real Historical Data)

- The event-driven portfolio engine uses XNYS trading days, cash, overlapping positions,
  position/gross limits, volume-constrained partial fills, slippage, bilateral fees, and turnover.
- Entry/stop/target fills are gap-aware; dividends and splits adjust the portfolio explicitly.
- Every run has a deterministic configuration hash and produces an equity curve and trade ledger.
- Results are split into training, validation, and out-of-sample segments and include confidence
  calibration (Brier score, expected calibration error, and confidence bins).
- Period metrics include annualized return, benchmark return, alpha, information ratio, fill rate,
  turnover, total fees, and maximum drawdown.

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
    "transaction_cost_bps": 10,
    "slippage_bps": 5,
    "initial_cash": 100000,
    "max_position_pct": 0.10,
    "max_gross_exposure_pct": 1.0,
    "max_volume_participation_pct": 0.05,
    "train_fraction": 0.60,
    "validation_fraction": 0.20
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
Paper orders, trade-ledger rows, sell alerts, sell executions, and holding-control
audits persist `source_snapshot_id` and `strategy_config_id` directly when they can be
derived from the originating recommendation.
Sell controls record sell price, quantity, reason, realized P&L, and whether the holding remains open.
Holding controls can be tightened or relaxed while a position is open; every stop-loss,
target, or note change writes a durable holding-control audit row and emits a
`holding_controls_updated` event.
Autopilot TP1 partial sells also tighten the remaining holding stop toward breakeven
through the same audited holding-control path.
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
