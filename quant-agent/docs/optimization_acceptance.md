# Quant System Optimization Acceptance

Last audited: 2026-07-14 (Asia/Shanghai)

This document is the authoritative closeout checklist for the P0/P1/P2 optimization
program. `Implemented` is not treated as equivalent to `Accepted`: an item is accepted
only when the production state or a test that exercises the full invariant supplies the
required evidence.

## Current production safety state

- Alembic revision: `20260411_0035`
- SQLite `PRAGMA quick_check`: `ok`
- Kill switch: enabled (`maintenance safety lock after test database recovery`)
- Autopilot: disabled
- Execution mode: `paper`
- Regular-session restriction: enabled
- Minimum paper-shadow evidence: 20 distinct XNYS trading days
- Observed qualifying paper-shadow days: 0
- Active portfolio-risk reservations: 0
- LaunchAgent `com.quant-agent.api`: loaded, loopback-only, authenticated, plist mode `0600`
- LaunchAgent `com.quant-agent.system-cycle-loop`: not loaded

Do not disable the kill switch or enable live autopilot while either point-in-time data
validation or paper-shadow readiness is incomplete.

## Requirement audit

| Priority | Requirement | Status | Authoritative evidence |
|---|---|---|---|
| P0 | Test isolation and state restoration | Accepted | 155 tests pass with `ResourceWarning` and `PytestUnraisableExceptionWarning` promoted to errors. The production database SHA-256, size, and mtime remained unchanged across the test run. Test configuration uses a temporary database and disposes the SQLAlchemy engine. |
| P0 | Atomic and idempotent buy/sell execution | Accepted | Unique idempotency and client-order identifiers, intent-first submission, atomic fill units of work, optimistic holding updates, broker lookup recovery, concurrent retry tests, injected rollback tests, and stale-intent worker recovery tests are present. |
| P0 | Authentication and live-trading safety | Accepted | Signed HttpOnly session cookie, double-submit CSRF protection, read/approve/execute roles, protected OpenAPI, URL-password rejection, correlation IDs on success and rejection responses, maintained `exchange_calendars` XNYS holiday/ad-hoc-closure/early-close tests, and regular-session execution defaults all pass. |
| P1 | Deduplicated snapshot storage and database operations | Accepted | Production contains 594 snapshots, 36,660 immutable content-addressed bars, and 2,611,180 compact references. Listing 500 snapshots took 0.283 seconds. Repeated identical bars are stored once; revised corporate-action/provenance content creates a new immutable version. Backup, restore, retention, pruning, compaction, migration upgrade/downgrade, and integrity checks pass. |
| P1 | Point-in-time data trust | Implemented; external data pending | Future bars/fundamentals/events are rejected; provider failures and fallback fields block live execution; adjusted close, dividends, splits, source, quality, and provenance persist. Production membership readiness rejects incomplete constituent counts, overlaps, blank sources, and incomplete metadata. Real-backtest preflight additionally rejects timestamps without explicit timezones, invalid publication/availability ordering, and stale fundamentals. A full-range offline validator emits input SHA-256 evidence and a content-derived dataset fingerprint. A licensed historical constituent export has not been supplied. |
| P1 | Backtest and strategy validation | Implemented; operational acceptance pending | The engine models cash, overlapping positions, chronological XNYS sessions, volume-limited partial fills, entry/exit gaps, bilateral fees/slippage, splits, dividends, immutable snapshots, train/validation/out-of-sample segments, drawdown/turnover, a buy/sell trade ledger, and confidence calibration. Real-provider runs must pass full-range data validation first, and the dataset fingerprint is included in the configuration hash and daily snapshot IDs so changed files cannot replay an older export. Determinism and focused mechanics tests pass. Paper mode can bootstrap before the live threshold; only explicit policy-driven, regular-session, gate-passing, error-free `paper_shadow_evidence` counts, and duplicate dates count once. A real point-in-time out-of-sample run and 20 live-calendar paper-shadow days remain outstanding. |
| P1 | Portfolio risk model | Accepted for code invariants | Gross, sector, timestamp-aligned correlation, beta, volatility, liquidation-time, and liquidity-stress gates are exercised. Broker submissions use native bracket protection. Serialized SQLite risk reservations prove concurrent requests cannot bypass the gross limit. |
| P2 | Observability and operations | Accepted | JSON logs redact secrets and carry correlation/run/order IDs. Persistent metrics, Prometheus export, liveness/readiness/trading-readiness separation, provider/broker/database/snapshot/reconciliation checks, persistent alerts, and rotating LaunchAgent logs are tested. API state creation is thread-safe: a cold-start probe completed 64 concurrent first requests without duplicate initialization or HTTP 500 responses. Paper-shadow progress is exposed through a protected read-only endpoint and the Dashboard even when Autopilot is off. A loopback-only authenticated API LaunchAgent now survives login/reboot, uses a mode-`0600` plist, an absolute production database path, bounded rotating logs, and verified post-load registration. |
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
3. Run a real historical backtest and retain its dataset fingerprint, source snapshots,
   configuration hash, out-of-sample metrics, trade ledger, equity curve, turnover,
   drawdown, and calibration.
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
ruff check .
mypy
python scripts/check_core_module_size.py --max-lines 850
pytest -q -W error::ResourceWarning -W error::pytest.PytestUnraisableExceptionWarning
pytest -q tests/unit/test_yfinance_provider.py tests/unit/test_data_provenance.py \
  tests/unit/test_point_in_time_validator.py \
  --cov=services.ingestion.vendors.yfinance_provider \
  --cov=services.ingestion.point_in_time_validator --cov-fail-under=70
sqlite3 quant_agent_v2.db "PRAGMA quick_check; SELECT version_num FROM alembic_version;"
PYTHONPATH=. python3 scripts/manage_launchd.py status
```
