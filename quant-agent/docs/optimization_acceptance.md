# Quant System Optimization Acceptance

Last audited: 2026-07-20 (Asia/Shanghai)
Runtime repair verified: 2026-10-09 (America/Toronto)

This document is the authoritative closeout checklist for the P0/P1/P2 optimization
program. `Implemented` is not treated as equivalent to `Accepted`: an item is accepted
only when the production state or a test that exercises the full invariant supplies the
required evidence.

## Current production safety state

- Running SQLite revision: `20260411_0035`
- Code Alembic head: `20260721_0037` (pending reviewed production migration)
- SQLite `PRAGMA quick_check`: `ok`
- Kill switch: enabled (`maintenance safety lock after test database recovery`)
- Autopilot: disabled
- Execution mode: `paper`
- Regular-session restriction: enabled
- Minimum paper-shadow evidence: 20 distinct XNYS trading days
- Observed qualifying paper-shadow days: 0
- Active portfolio-risk reservations: 0
- LaunchAgent `com.quant-agent.api`: running from the repository `.venv/bin/python`, loopback-only,
  authenticated, plist mode `0600`; installed dependencies match `requirements.lock`
- LaunchAgent `com.quant-agent.system-cycle-loop`: not loaded

The 2026-10-09 runtime repair replaced string comparisons in the native Broker protection
check with `Direction.BUY` and `OrderExecutionMode.LIVE`. The complete strict test suite passes
(`185 passed`), along with Ruff, mypy, the core-module size check, and `pip check`.
The API LaunchAgent was restarted using the locked repository environment. Liveness and
authenticated OpenAPI returned HTTP 200, unauthenticated data access returned HTTP 401, and
the running process was verified to load its libraries from the repository `.venv`.
The production database's SHA-256, revision, kill switch, and Autopilot policy were unchanged.
A verified SQLite backup was saved as
`backups/quant_agent_v2-pre-runtime-alignment-20261009-112306.db`.
PIT data and its validation report remain unconfigured, and production migrations 0036/0037
remain outstanding; the overall paper preflight therefore remains `blocked`.

Do not disable the kill switch or enable live autopilot while either point-in-time data
validation or paper-shadow readiness is incomplete.

## Requirement audit

| Priority | Requirement | Status | Authoritative evidence |
|---|---|---|---|
| P0 | Test isolation and state restoration | Accepted | 181 tests pass with `ResourceWarning` and `PytestUnraisableExceptionWarning` promoted to errors. Tests use a temporary database and dispose the SQLAlchemy engine. |
| P0 | Atomic and idempotent buy/sell execution | Accepted | Unique idempotency and client-order identifiers, intent-first submission, atomic fill units of work, optimistic holding updates, broker lookup recovery, concurrent retry tests, injected rollback tests, and stale-intent worker recovery tests are present. |
| P0 | Authentication and live-trading safety | Accepted | Signed HttpOnly session cookie, double-submit CSRF protection, read/approve/execute roles, protected OpenAPI, URL-password rejection, correlation IDs, maintained XNYS calendar tests, and regular-session defaults all pass. Confirmed/automatic live buy and sell paths, including recovery, share one fail-closed readiness gate and Mock data cannot authorize live Broker I/O. |
| P1 | Deduplicated snapshot storage and database operations | Accepted | Production contains 594 snapshots, 36,660 immutable content-addressed bars, and 2,611,180 compact references. Listing 500 snapshots took 0.283 seconds. Repeated identical bars are stored once; revised corporate-action/provenance content creates a new immutable version. Backup, restore, retention, pruning, compaction, migration upgrade/downgrade, and integrity checks pass. |
| P1 | Point-in-time data trust | Implemented; external data pending | Future bars/fundamentals/events are rejected; provider failures and fallback fields block live execution; adjusted close, dividends, splits, source, quality, and provenance persist. Production membership readiness rejects incomplete constituent counts, overlaps, blank sources, and incomplete metadata. Real-backtest preflight additionally rejects timestamps without explicit timezones, invalid publication/availability ordering, and stale fundamentals. A full-range offline validator emits input SHA-256 evidence and a content-derived dataset fingerprint. A licensed historical constituent export has not been supplied. |
| P1 | Backtest and strategy validation | Implemented; operational acceptance pending | The engine models cash, overlapping positions, XNYS sessions, volume-limited fills, gap exits, bilateral fees/slippage, corporate actions, sector caps, immutable snapshots, train/validation/out-of-sample segments, calibration, and market regimes. Out-of-sample acceptance enforces ≥100 trades, drawdown ≤15%, Profit Factor ≥1.20, Sharpe ≥0.8, payoff ratio ≥1.5, positive after-cost expectancy, name ≤10%, and sector ≤30%. Complete results are retained in `backtest_runs`; live readiness requires a passing record with the same strategy ID and dataset fingerprint. A real licensed-data run remains outstanding. |
| P1 | Portfolio risk model | Accepted for code invariants | Gross, sector, timestamp-aligned correlation, beta, volatility, liquidation-time, and liquidity-stress gates are exercised. Broker submissions use native bracket protection. Serialized SQLite risk reservations prove concurrent requests cannot bypass the gross limit. |
| P2 | Observability and operations | Accepted | JSON logs redact secrets and carry correlation/run/order IDs. Persistent metrics, Prometheus export, readiness separation, alerts, and rotating logs are tested. A fail-closed CLI preflight checks the locked `.venv`, auth, PIT hashes/fingerprint, database integrity/revision/kill switch, Broker target, and logging. Both private LaunchAgent plists default to `.venv/bin/python` and carry a shared absolute database plus PIT/report/Broker settings. No running service was replaced during this audit. |
| P2 | Code split and CI | Accepted | Orders vs. snapshot/risk, sell execution vs. broker sync, autopilot preflight vs. runtime gates, Worker CLI, and execution helpers are separate modules; dashboard HTML and environment reads are also separated. CI enforces an 850-line limit across 15 API-state/Worker/dashboard Python modules, runs Ruff, mypy across 25 source files, migration tests, the strict 155-test suite on Python 3.11/3.13, and provider/validator coverage. Current YFinance adapter coverage is 94.17%, the point-in-time validator is 85%, and their combined focused coverage is 91.08%. |

## Remaining operational plan

1. Obtain licensed point-in-time membership data for every configured universe. S&P DJI
   describes constituent, GICS, weight, and composition-event delivery as subscription
   index data: <https://www.spglobal.com/spdji/en/documents/index-policies/index-data-capabilities-brochure.pdf>.
   Nasdaq exposes NDX weighting data through its full-access index portal:
   <https://indexes.nasdaqomx.com/Index/Weighting/NDX>.
2. Populate `POINT_IN_TIME_UNIVERSE_CSV` and the historical fundamentals, events, and
   earnings files. Keep the full default thresholds unless the configured universe is
   intentionally different from SP500/NDX. Run `scripts/validate_point_in_time_data.py`
   over the entire proposed backtest range and retain the JSON report.
   Export the reviewed runtime configuration and require
   `.venv/bin/python scripts/preflight.py --target paper` to return `ready` before installing the
   worker LaunchAgent.
3. Run a real historical backtest and retain its dataset fingerprint, source snapshots,
   configuration hash, out-of-sample metrics, trade ledger, equity curve, turnover,
   drawdown, and calibration.
   Review `acceptance.checks`, regime metrics, and the complete retained trade ledger. Do not lower
   the fixed acceptance thresholds when a run fails.
4. After data readiness passes, keep the policy strictly in `paper` mode and do not pass
   the live runtime-allow flag. During each supervised paper window, intentionally release
   the global kill switch so the paper execution path can actually be evaluated, then
   restore it when the window ends. Accumulate one explicit, gate-passing, error-free
   `paper_shadow_evidence` record during the regular session on 20 distinct XNYS trading
   days; synthetic, backdated, legacy-inferred, or duplicate-date rows do not count.
5. Review alerts, reconciliation, broker account status, and readiness after day 20.
   Live enablement remains a separate human approval action.

## Revalidation commands

```bash
.venv/bin/ruff check .
.venv/bin/mypy
.venv/bin/python scripts/check_core_module_size.py --max-lines 850
.venv/bin/pytest -q -W error::ResourceWarning -W error::pytest.PytestUnraisableExceptionWarning
.venv/bin/pytest -q tests/unit/test_yfinance_provider.py tests/unit/test_data_provenance.py \
  tests/unit/test_point_in_time_validator.py \
  --cov=services.ingestion.vendors.yfinance_provider \
  --cov=services.ingestion.point_in_time_validator --cov-fail-under=70
sqlite3 quant_agent_v2.db "PRAGMA quick_check; SELECT version_num FROM alembic_version;"
.venv/bin/python scripts/preflight.py --target paper
.venv/bin/python -m alembic upgrade head  # only after backup and reviewed migration approval
.venv/bin/python scripts/manage_launchd.py status
```
