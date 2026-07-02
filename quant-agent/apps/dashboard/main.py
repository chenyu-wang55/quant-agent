from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse

from apps.api.dependencies import AppState, get_app_state
from domain.entities.models import Recommendation


router = APIRouter(tags=["dashboard"])


def _escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _as_text(value: object) -> str:
    if hasattr(value, "value"):
        return str(getattr(value, "value"))
    return str(value)


def _is_price_plan_reasonable(rec: Recommendation, current_price: float | None) -> bool:
    if current_price is None or current_price <= 0:
        return True
    mid = (float(rec.entry_zone_low) + float(rec.entry_zone_high)) / 2.0
    gap = abs(mid - current_price) / current_price
    return gap <= 0.40


def _select_recommendations_for_dashboard(
    recommendations: list[Recommendation],
    price_lookup: dict[str, float | None],
) -> list[Recommendation]:
    chosen_by_ticker: dict[str, Recommendation] = {}
    for rec in recommendations:
        ticker = rec.ticker.upper()
        if ticker in chosen_by_ticker:
            continue

        if _is_price_plan_reasonable(rec, price_lookup.get(ticker)):
            chosen_by_ticker[ticker] = rec
            continue

        replacement = None
        for candidate in recommendations:
            if candidate.ticker.upper() != ticker:
                continue
            if _is_price_plan_reasonable(candidate, price_lookup.get(ticker)):
                replacement = candidate
                break
        if replacement is not None:
            chosen_by_ticker[ticker] = replacement

    return list(chosen_by_ticker.values())


def _build_price_lookup(state: AppState, recommendations: list[Recommendation], as_of: datetime) -> dict[str, float | None]:
    lookup: dict[str, float | None] = {}
    for rec in recommendations:
        ticker = rec.ticker.upper()
        if ticker in lookup:
            continue
        try:
            price = state.provider.get_latest_price(ticker=ticker, as_of=as_of)
            lookup[ticker] = round(float(price), 4) if price is not None else None
        except Exception:
            lookup[ticker] = None
    return lookup


def _rec_payload(state: AppState, rec: Recommendation, current_price: float | None) -> dict:
    approval = state.get_latest_approval(rec.id)
    approval_status = approval.decision.value if approval else "pending"
    return {
        "id": rec.id,
        "ticker": rec.ticker,
        "strategy_config_id": rec.strategy_config_id,
        "direction": _as_text(rec.direction),
        "composite": round(float(rec.score_vector.get("composite", 0.0)), 4),
        "confidence": round(float(rec.confidence), 4),
        "entry_zone_low": rec.entry_zone_low,
        "entry_zone_high": rec.entry_zone_high,
        "stop_loss": rec.stop_loss,
        "tp1": rec.tp1,
        "tp2": rec.tp2,
        "holding_period": rec.holding_period,
        "approval_status": approval_status,
        "report_title": rec.analysis.report_title,
        "report_cn": rec.analysis.report_cn,
        "why_to_buy_cn": rec.analysis.why_to_buy_cn,
        "why_to_sell_cn": rec.analysis.why_to_sell_cn,
        "action_guidance_cn": rec.analysis.action_guidance_cn,
        "current_price": current_price,
    }


@router.get("/dashboard/realtime-data")
def dashboard_realtime_data(
    refresh_alerts: bool = Query(default=True),
    state: AppState = Depends(get_app_state),
) -> dict:
    raw_recommendations = (
        state.latest_run.recommendations
        if state.latest_run is not None
        else state.recommendation_repo.list_latest(limit=200)
    )
    holdings = state.list_open_holdings()
    alerts = state.monitor_sell_alerts() if refresh_alerts else state.recent_sell_alerts

    now = datetime.now(timezone.utc)
    portfolio_summary = state.get_portfolio_summary(as_of=now)
    portfolio_performance = state.get_portfolio_performance()
    recommendation_attribution = state.get_recommendation_attribution()
    strategy_tuning = state.get_strategy_tuning_report(attribution=recommendation_attribution)
    recent_trades = state.list_trade_ledger(limit=10)
    recent_holding_control_audits = state.list_holding_control_audits(limit=10)
    holding_control_audit_count = len(state.list_holding_control_audits(limit=10_000))
    recent_sell_executions = state.list_sell_execution_audits(limit=10)
    sell_execution_count = len(state.list_sell_execution_audits(limit=10_000))
    recent_position_reconciliations = state.list_position_reconciliations(limit=10)
    position_reconciliation_count = len(state.list_position_reconciliations(limit=10_000))
    recent_system_runs = state.list_system_cycle_runs(limit=10)
    system_run_count = len(state.list_system_cycle_runs(limit=10_000))
    latest_auto_execution = (
        recent_system_runs[0].metrics.get("auto_execution", {})
        if recent_system_runs
        else {}
    )
    latest_auto_approval = (
        recent_system_runs[0].metrics.get("auto_approval", {})
        if recent_system_runs
        else {}
    )
    latest_auto_approval_count = int(latest_auto_approval.get("action_count") or 0)
    latest_auto_action_count = int(latest_auto_execution.get("action_count") or 0)
    recent_alert_history = state.list_sell_alert_audits(limit=10)
    alert_history_count = len(state.list_sell_alert_audits(limit=10_000))
    recent_paper_orders = state.list_paper_orders(limit=10)
    paper_order_count = len(state.list_paper_orders(limit=10_000))
    recent_source_snapshots = state.list_source_snapshots(limit=10)
    source_snapshot_count = len(state.list_source_snapshots(limit=10_000))
    recent_strategy_configs = state.list_strategy_configs(limit=10)
    strategy_config_count = len(state.list_strategy_configs(limit=10_000))
    autopilot_policy = state.get_autopilot_policy()
    autopilot_preflight = state.build_autopilot_preflight(autopilot_policy)
    market_session = state.get_market_session_status(as_of=now)
    price_lookup = _build_price_lookup(state=state, recommendations=raw_recommendations, as_of=now)
    recommendations = _select_recommendations_for_dashboard(raw_recommendations, price_lookup)
    return {
        "timestamp": now.isoformat(),
        "provider": type(state.provider).__name__,
        "source_snapshot_id": state.latest_run.source_snapshot_id if state.latest_run else None,
        "kill_switch": {
            "enabled": state.kill_switch.enabled,
            "reason": state.kill_switch.reason,
            "updated_at": state.kill_switch.updated_at.isoformat(),
        },
        "autopilot_policy": autopilot_policy.model_dump(mode="json"),
        "autopilot_preflight": autopilot_preflight.model_dump(mode="json"),
        "market_session": market_session.model_dump(mode="json"),
        "summary": {
            "recommendation_count": len(recommendations),
            "open_holding_count": len(holdings),
            "sell_alert_count": len(alerts),
            "alert_history_count": alert_history_count,
            "paper_order_count": paper_order_count,
            "holding_control_audit_count": holding_control_audit_count,
            "sell_execution_count": sell_execution_count,
            "position_reconciliation_count": position_reconciliation_count,
            "source_snapshot_count": source_snapshot_count,
            "strategy_config_count": strategy_config_count,
            "strategy_tuning_count": strategy_tuning.recommendation_count,
            "system_run_count": system_run_count,
            "latest_auto_approval_count": latest_auto_approval_count,
            "latest_auto_action_count": latest_auto_action_count,
            "pending_event_count": state.pending_event_count(),
        },
        "portfolio_summary": portfolio_summary.model_dump(mode="json"),
        "portfolio_performance": portfolio_performance.model_dump(mode="json"),
        "recommendation_attribution": recommendation_attribution.model_dump(mode="json"),
        "recommendations": [_rec_payload(state, rec, price_lookup.get(rec.ticker.upper())) for rec in recommendations],
        "holdings": [
            {
                "ticker": holding.ticker,
                "qty": holding.qty,
                "avg_buy_price": holding.avg_buy_price,
                "bought_at": holding.bought_at.isoformat(),
                "stop_loss": holding.stop_loss,
                "take_profit1": holding.take_profit1,
                "take_profit2": holding.take_profit2,
                "source_recommendation_id": holding.source_recommendation_id,
                "note": holding.note,
                "status": _as_text(holding.status),
                "realized_pnl": holding.realized_pnl,
                "closed_at": holding.closed_at.isoformat() if holding.closed_at else None,
                "last_sell_price": holding.last_sell_price,
                "last_sell_reason": holding.last_sell_reason,
            }
            for holding in holdings
        ],
        "recent_paper_orders": [order.model_dump(mode="json") for order in recent_paper_orders],
        "source_snapshots": [snapshot.model_dump(mode="json") for snapshot in recent_source_snapshots],
        "strategy_configs": [config.model_dump(mode="json") for config in recent_strategy_configs],
        "strategy_tuning": strategy_tuning.model_dump(mode="json"),
        "recent_trades": [trade.model_dump(mode="json") for trade in recent_trades],
        "recent_holding_control_audits": [
            audit.model_dump(mode="json") for audit in recent_holding_control_audits
        ],
        "recent_sell_executions": [execution.model_dump(mode="json") for execution in recent_sell_executions],
        "recent_position_reconciliations": [
            item.model_dump(mode="json") for item in recent_position_reconciliations
        ],
        "recent_system_runs": [run.model_dump(mode="json") for run in recent_system_runs],
        "recent_alert_history": [item.model_dump(mode="json") for item in recent_alert_history],
        "alerts": [
            {
                "ticker": alert.ticker,
                "level": _as_text(alert.level),
                "reason_code": alert.reason_code,
                "message_cn": alert.message_cn,
                "suggested_action_cn": alert.suggested_action_cn,
                "current_price": alert.current_price,
                "stop_loss": alert.stop_loss,
                "take_profit1": alert.take_profit1,
                "take_profit2": alert.take_profit2,
                "generated_at": alert.generated_at.isoformat(),
            }
            for alert in alerts
        ],
        "recent_events": [event.model_dump() for event in state.list_pending_events(limit=5)],
    }


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard_home() -> str:
    return """
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Quant 实时看板</title>
  <style>
    :root {
      --ink: #12253a;
      --ink-soft: #4d6278;
      --line: #d7e1ec;
      --line-strong: #c2cfde;
      --surface: rgba(255, 255, 255, 0.88);
      --surface-solid: #ffffff;
      --brand: #0f766e;
      --brand-2: #1d4ed8;
      --ok: #166534;
      --warn: #a16207;
      --danger: #b91c1c;
      --shadow: 0 14px 36px rgba(16, 31, 48, 0.12);
    }
    * { box-sizing: border-box; }

    @keyframes rise {
      from { opacity: 0; transform: translateY(12px); }
      to { opacity: 1; transform: translateY(0); }
    }

    body {
      margin: 0;
      color: var(--ink);
      font-family: "Avenir Next", "PingFang SC", "Noto Sans SC", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(920px 560px at -12% -22%, rgba(15, 118, 110, 0.24), transparent 72%),
        radial-gradient(1020px 620px at 112% -20%, rgba(29, 78, 216, 0.2), transparent 70%),
        linear-gradient(180deg, #eef4fb 0%, #f7f9fc 56%, #edf2f8 100%);
      min-height: 100vh;
    }

    .wrap {
      max-width: 1380px;
      margin: 0 auto;
      padding: 24px 18px 28px;
    }

    .hero {
      position: relative;
      overflow: hidden;
      background: linear-gradient(140deg, rgba(8, 47, 73, 0.96), rgba(29, 78, 216, 0.88));
      color: #ebf3ff;
      border: 1px solid rgba(255, 255, 255, 0.2);
      border-radius: 20px;
      padding: 18px 20px;
      margin-bottom: 14px;
      box-shadow: 0 18px 42px rgba(9, 25, 42, 0.3);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      animation: rise 0.45s ease-out both;
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: 0;
      background:
        radial-gradient(520px 240px at 0% 0%, rgba(45, 212, 191, 0.2), transparent 68%),
        radial-gradient(620px 320px at 100% 0%, rgba(147, 197, 253, 0.18), transparent 65%);
      pointer-events: none;
    }

    .hero > * {
      position: relative;
      z-index: 1;
    }

    .eyebrow {
      font-size: 11px;
      letter-spacing: 1.1px;
      text-transform: uppercase;
      font-weight: 700;
      opacity: 0.82;
      margin-bottom: 5px;
    }

    .hero h1 { margin: 0; font-size: 24px; letter-spacing: 0.2px; }

    .hero .desc {
      margin-top: 7px;
      font-size: 13px;
      opacity: 0.95;
      max-width: 760px;
      line-height: 1.45;
    }

    .hero .right {
      text-align: right;
      font-size: 12px;
      opacity: 0.95;
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 8px;
    }

    .hero-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 7px 12px;
      border: 1px solid rgba(255, 255, 255, 0.34);
      background: rgba(255, 255, 255, 0.12);
      backdrop-filter: blur(6px);
    }

    .hero-pill.soft {
      background: rgba(15, 23, 42, 0.24);
      border-color: rgba(255, 255, 255, 0.22);
      max-width: 420px;
    }

    .btn {
      border: 1px solid rgba(255, 255, 255, 0.35);
      border-radius: 11px;
      padding: 10px 14px;
      background: linear-gradient(135deg, #dff4ff, #c7dcff);
      color: #0d2945;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.18s ease, box-shadow 0.18s ease;
    }

    .btn:hover {
      transform: translateY(-1px);
      box-shadow: 0 10px 18px rgba(15, 23, 42, 0.2);
    }

    .btn-mini {
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 7px 9px;
      background: #f8fbff;
      color: #15304a;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
      margin: 0 5px 5px 0;
      white-space: nowrap;
    }

    .btn-mini:hover { border-color: var(--brand); background: #ecfdf5; }
    .btn-mini.danger:hover { border-color: var(--danger); background: #fef2f2; color: var(--danger); }

    .control-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(110px, 1fr));
      gap: 9px;
      align-items: end;
    }

    .autopilot-grid {
      margin-top: 11px;
      grid-template-columns: repeat(5, minmax(118px, 1fr));
    }

    .check-field {
      min-height: 35px;
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 7px 9px;
      background: #fbfdff;
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--ink);
      font-size: 13px;
      font-weight: 700;
    }

    .check-field input {
      width: 16px;
      height: 16px;
      accent-color: var(--brand);
    }

    .autopilot-head {
      margin-top: 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }

    .field label {
      display: block;
      color: var(--ink-soft);
      font-size: 11px;
      font-weight: 700;
      margin-bottom: 5px;
      text-transform: uppercase;
      letter-spacing: 0.25px;
    }

    .field input,
    .field select {
      width: 100%;
      min-height: 35px;
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 7px 9px;
      color: var(--ink);
      background: #fbfdff;
      font: inherit;
      font-size: 13px;
    }

    .status-line {
      margin-top: 8px;
      min-height: 18px;
      color: var(--ink-soft);
      font-size: 12px;
    }

    .stats {
      display: grid;
      gap: 11px;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      margin-bottom: 13px;
    }

    .stat {
      position: relative;
      overflow: hidden;
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 15px;
      padding: 13px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(6px);
      animation: rise 0.5s ease-out both;
      transition: transform 0.18s ease, border-color 0.2s ease;
    }

    .stat::before {
      content: "";
      position: absolute;
      left: 0;
      top: 0;
      width: 100%;
      height: 3px;
      background: linear-gradient(90deg, var(--brand), var(--brand-2));
      opacity: 0.95;
    }

    .stat:nth-child(2) { animation-delay: 0.03s; }
    .stat:nth-child(3) { animation-delay: 0.06s; }
    .stat:nth-child(4) { animation-delay: 0.09s; }
    .stat:nth-child(5) { animation-delay: 0.12s; }

    .stat:hover {
      transform: translateY(-2px);
      border-color: var(--line-strong);
    }

    .stat .k { color: var(--ink-soft); font-size: 12px; margin-bottom: 8px; }

    .stat .v {
      font-size: 25px;
      font-weight: 800;
      color: #0f1f32;
      letter-spacing: 0.2px;
    }

    .panel {
      background: var(--surface-solid);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      margin-bottom: 12px;
      box-shadow: var(--shadow);
      overflow: auto;
      animation: rise 0.55s ease-out both;
    }

    .panel:nth-of-type(3) { animation-delay: 0.05s; }
    .panel:nth-of-type(4) { animation-delay: 0.1s; }
    .panel:nth-of-type(5) { animation-delay: 0.15s; }

    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      margin-bottom: 9px;
    }

    .panel h3 {
      margin: 0;
      font-size: 15px;
      color: #0f2236;
      letter-spacing: 0.2px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 980px;
    }

    th, td {
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }

    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: #f5f9ff;
      color: var(--ink-soft);
      font-weight: 700;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.45px;
    }

    tbody tr:nth-child(2n) {
      background: #fbfdff;
    }

    tbody tr:hover {
      background: rgba(15, 118, 110, 0.08);
    }

    .badge {
      display: inline-block;
      padding: 4px 9px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.25px;
    }

    .b-buy { color: #0f5132; background: #d1fae5; }
    .b-ok { color: var(--ok); background: #dcfce7; }
    .b-warn { color: var(--warn); background: #fef3c7; }
    .b-danger { color: var(--danger); background: #fee2e2; }
    .b-neutral { color: #2f4a68; background: #dbeafe; }

    .mono {
      font-family: "SF Mono", "IBM Plex Mono", "Menlo", "Consolas", monospace;
      font-size: 11px;
      color: #576b82;
    }

    .small { font-size: 12px; color: var(--ink-soft); }
    .why { margin: 0; padding-left: 15px; }

    .link {
      color: var(--brand-2);
      text-decoration: none;
      font-weight: 700;
    }

    a { color: var(--brand-2); text-decoration: none; font-weight: 600; }
    a:hover { text-decoration: underline; }

    @media (max-width: 1140px) {
      .stats { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .control-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .autopilot-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }

    @media (max-width: 980px) {
      .hero { flex-direction: column; align-items: flex-start; }
      .hero .right { text-align: left; align-items: flex-start; width: 100%; }
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .control-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .autopilot-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }

    @media (max-width: 680px) {
      .wrap { padding: 14px 10px 18px; }
      .hero h1 { font-size: 20px; }
      .stats { grid-template-columns: 1fr 1fr; gap: 8px; }
      .stat { padding: 11px 10px; }
      .stat .v { font-size: 21px; }
      .panel { padding: 10px; border-radius: 13px; }
      table { font-size: 12px; }
      th, td { padding: 8px 6px; }
      .control-grid { grid-template-columns: 1fr; }
      .autopilot-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <div class="eyebrow">US EQUITY QUANT DESK</div>
        <h1>Quant 实时交易看板</h1>
        <div class="desc" id="meta">加载中...</div>
      </div>
      <div class="right">
        <div class="hero-pill" id="refreshPlan">刷新策略: 交易时段 1 分钟 / 非交易时段 30 分钟</div>
        <div class="hero-pill soft" id="providerNote">数据源可靠性: 加载中...</div>
        <div><button class="btn" onclick="refreshNow(true)">立即刷新</button></div>
      </div>
    </div>

    <div class="stats">
      <div class="stat"><div class="k">数据源</div><div class="v" id="provider">-</div></div>
      <div class="stat"><div class="k">推荐数量</div><div class="v" id="recCount">0</div></div>
      <div class="stat"><div class="k">快照</div><div class="v" id="snapshotCount">0</div></div>
      <div class="stat"><div class="k">策略版本</div><div class="v" id="strategyConfigCount">0</div></div>
      <div class="stat"><div class="k">持仓监控</div><div class="v" id="holdingCount">0</div></div>
      <div class="stat"><div class="k">卖出提醒</div><div class="v" id="alertCount">0</div></div>
      <div class="stat"><div class="k">提醒历史</div><div class="v" id="alertHistoryCount">0</div></div>
      <div class="stat"><div class="k">纸单</div><div class="v" id="paperOrderCount">0</div></div>
      <div class="stat"><div class="k">风控审计</div><div class="v" id="holdingControlAuditCount">0</div></div>
      <div class="stat"><div class="k">卖出审计</div><div class="v" id="sellExecutionCount">0</div></div>
      <div class="stat"><div class="k">仓位核对</div><div class="v" id="positionReconciliationCount">0</div></div>
      <div class="stat"><div class="k">已实现盈亏</div><div class="v" id="realizedPnl">0.00</div></div>
      <div class="stat"><div class="k">开放风险</div><div class="v" id="openRisk">0.00</div></div>
      <div class="stat"><div class="k">交易笔数</div><div class="v" id="tradeCount">0</div></div>
      <div class="stat"><div class="k">胜率</div><div class="v" id="winRate">0%</div></div>
      <div class="stat"><div class="k">Profit Factor</div><div class="v" id="profitFactor">-</div></div>
      <div class="stat"><div class="k">归因推荐</div><div class="v" id="attributionCount">0</div></div>
      <div class="stat"><div class="k">调参建议</div><div class="v" id="strategyTuningCount">0</div></div>
      <div class="stat"><div class="k">自动循环</div><div class="v" id="systemRunCount">0</div></div>
      <div class="stat"><div class="k">自动审批</div><div class="v" id="autoApprovalCount">0</div></div>
      <div class="stat"><div class="k">自动执行</div><div class="v" id="autoActionCount">0</div></div>
      <div class="stat"><div class="k">Autopilot</div><div class="v" id="autopilotStatus">-</div></div>
      <div class="stat"><div class="k">Kill Switch</div><div class="v" id="killSwitch">-</div></div>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>交易控制</h3>
        <span class="small" id="actionStatus">待命</span>
      </div>
      <div class="control-grid">
        <div class="field"><label for="topN">Top N</label><input id="topN" type="number" min="1" max="20" step="1" value="5" /></div>
        <div class="field"><label for="minConfidence">Min Confidence</label><input id="minConfidence" type="number" min="0" max="1" step="0.05" value="0" /></div>
        <div class="field"><label for="buyQty">Buy Qty</label><input id="buyQty" type="number" min="0.0001" step="1" value="10" /></div>
        <div class="field"><label for="buyPrice">Buy Price</label><input id="buyPrice" type="number" min="0.0001" step="0.01" placeholder="entry high" /></div>
        <div class="field"><label for="accountEquity">Account Equity</label><input id="accountEquity" type="number" min="1" step="1000" value="100000" /></div>
        <div class="field"><label for="riskPct">Risk %</label><input id="riskPct" type="number" min="0.01" max="100" step="0.1" value="1" /></div>
        <div class="field"><label for="maxPositionPct">Max Position %</label><input id="maxPositionPct" type="number" min="0.01" max="100" step="0.5" value="10" /></div>
        <div class="field"><label for="maxGrossPct">Max Gross %</label><input id="maxGrossPct" type="number" min="0.01" max="500" step="1" value="100" /></div>
        <div class="field"><label for="maxSectorPct">Max Sector %</label><input id="maxSectorPct" type="number" min="0.01" max="500" step="1" value="30" /></div>
        <div class="field"><label for="executionMode">Exec Mode</label><select id="executionMode"><option value="paper">Paper</option><option value="live_dry_run">Live Dry Run</option></select></div>
        <div class="field"><label for="sellQty">Sell Qty</label><input id="sellQty" type="number" min="0.0001" step="1" placeholder="all" /></div>
        <div class="field"><label for="sellPrice">Sell Price</label><input id="sellPrice" type="number" min="0.0001" step="0.01" /></div>
        <div class="field"><label for="controlStopLoss">Stop</label><input id="controlStopLoss" type="number" min="0.0001" step="0.01" /></div>
        <div class="field"><label for="controlTp1">TP1</label><input id="controlTp1" type="number" min="0.0001" step="0.01" /></div>
        <div class="field"><label for="controlTp2">TP2</label><input id="controlTp2" type="number" min="0.0001" step="0.01" /></div>
        <div class="field"><label for="sourceSnapshotId">Snapshot ID</label><input id="sourceSnapshotId" type="text" placeholder="source_snapshot_id" /></div>
      </div>
      <div style="margin-top:9px; display:flex; gap:8px; flex-wrap:wrap;">
        <button class="btn-mini" onclick="runResearch()">运行推荐</button>
        <button class="btn-mini" onclick="runSystemCycle()">运行系统循环</button>
        <button class="btn-mini" onclick="replaySnapshotByInput()">回放快照</button>
        <button class="btn-mini" onclick="refreshNow(true)">刷新状态</button>
        <input id="tradeReason" style="flex:1; min-width:220px; border:1px solid var(--line-strong); border-radius:8px; padding:7px 9px; font:inherit; font-size:13px;" placeholder="reason" />
      </div>
      <div class="autopilot-head">
        <div>
          <span id="autopilotPolicyBadge" class="badge b-neutral">Autopilot Policy</span>
          <span id="autopilotPolicyMeta" class="small"></span>
        </div>
        <div>
          <button class="btn-mini" onclick="saveAutopilotPolicy()">保存 Autopilot</button>
          <button class="btn-mini danger" onclick="disableAutopilotPolicy()">关闭 Autopilot</button>
        </div>
      </div>
      <div class="control-grid autopilot-grid">
        <label class="check-field"><input id="autopilotEnabled" type="checkbox" /><span>Enabled</span></label>
        <label class="check-field"><input id="autopilotAutoApprove" type="checkbox" /><span>Auto Approve</span></label>
        <label class="check-field"><input id="autopilotAutoExecute" type="checkbox" /><span>Auto Execute</span></label>
        <label class="check-field"><input id="autopilotRegularHours" type="checkbox" /><span>Regular Hours</span></label>
        <div class="field"><label for="autopilotExecutionMode">Policy Exec</label><select id="autopilotExecutionMode"><option value="paper">Paper</option><option value="live_dry_run">Live Dry Run</option><option value="live">Live</option></select></div>
        <div class="field"><label for="autopilotMinConfidence">Auto Min Conf</label><input id="autopilotMinConfidence" type="number" min="0" max="1" step="0.01" value="0.72" /></div>
        <div class="field"><label for="autopilotMinComposite">Min Composite</label><input id="autopilotMinComposite" type="number" min="0" step="0.01" value="0" /></div>
        <div class="field"><label for="autopilotMaxApprovals">Max Approvals</label><input id="autopilotMaxApprovals" type="number" min="0" step="1" value="1" /></div>
        <div class="field"><label for="autopilotMaxBuys">Max Buys</label><input id="autopilotMaxBuys" type="number" min="0" step="1" value="1" /></div>
        <div class="field"><label for="autopilotMaxSells">Max Sells</label><input id="autopilotMaxSells" type="number" min="0" step="1" value="10" /></div>
        <div class="field"><label for="autopilotDailyApprovals">Daily Approvals</label><input id="autopilotDailyApprovals" type="number" min="0" step="1" value="3" /></div>
        <div class="field"><label for="autopilotDailyBuys">Daily Buys</label><input id="autopilotDailyBuys" type="number" min="0" step="1" value="3" /></div>
        <div class="field"><label for="autopilotDailySells">Daily Sells</label><input id="autopilotDailySells" type="number" min="0" step="1" value="10" /></div>
        <div class="field"><label for="autopilotOrderDedupeMinutes">Order Dedupe</label><input id="autopilotOrderDedupeMinutes" type="number" min="0" step="60" value="1440" /></div>
        <div class="field"><label for="autopilotSellAlertCooldownMinutes">Sell Cooldown</label><input id="autopilotSellAlertCooldownMinutes" type="number" min="0" step="15" value="60" /></div>
        <div class="field"><label for="autopilotCooldownMinutes">Rebuy Cooldown</label><input id="autopilotCooldownMinutes" type="number" min="0" step="15" value="240" /></div>
        <div class="field"><label for="autopilotMinSnapshotBars">Min Bars %</label><input id="autopilotMinSnapshotBars" type="number" min="0" max="100" step="1" value="100" /></div>
        <div class="field"><label for="autopilotMinSnapshotFundamentals">Min Fundamentals %</label><input id="autopilotMinSnapshotFundamentals" type="number" min="0" max="100" step="1" value="100" /></div>
        <div class="field"><label for="autopilotMaxSnapshotBarAge">Max Bar Age</label><input id="autopilotMaxSnapshotBarAge" type="number" min="0" step="60" value="4320" /></div>
        <div class="field"><label for="autopilotAccountEquity">Policy Equity</label><input id="autopilotAccountEquity" type="number" min="1" step="1000" value="100000" /></div>
        <div class="field"><label for="autopilotMaxOpenRiskPct">Policy Open Risk %</label><input id="autopilotMaxOpenRiskPct" type="number" min="0" max="100" step="0.5" value="6" /></div>
        <div class="field"><label for="autopilotMaxDailyLossPct">Daily Loss %</label><input id="autopilotMaxDailyLossPct" type="number" min="0" max="100" step="0.5" value="3" /></div>
        <div class="field"><label for="autopilotMaxBuyDriftPct">Buy Drift %</label><input id="autopilotMaxBuyDriftPct" type="number" min="0" max="100" step="0.5" value="3" /></div>
        <label class="check-field"><input id="autopilotRequireReconciliation" type="checkbox" /><span>Require Reconcile</span></label>
        <div class="field"><label for="autopilotMaxReconciliationAge">Reconcile Age</label><input id="autopilotMaxReconciliationAge" type="number" min="0" step="60" value="1440" /></div>
        <div class="field"><label for="autopilotRiskPct">Policy Risk %</label><input id="autopilotRiskPct" type="number" min="0.01" max="100" step="0.1" value="1" /></div>
        <div class="field"><label for="autopilotMaxPositionPct">Policy Position %</label><input id="autopilotMaxPositionPct" type="number" min="0.01" max="100" step="0.5" value="10" /></div>
        <div class="field"><label for="autopilotMaxGrossPct">Policy Gross %</label><input id="autopilotMaxGrossPct" type="number" min="0.01" max="500" step="1" value="100" /></div>
        <div class="field"><label for="autopilotMaxSectorPct">Policy Sector %</label><input id="autopilotMaxSectorPct" type="number" min="0.01" max="500" step="1" value="30" /></div>
        <div class="field"><label for="autopilotReason">Policy Reason</label><input id="autopilotReason" type="text" placeholder="policy update reason" /></div>
      </div>
      <div class="status-line" id="operationLog"></div>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>行情快照</h3>
        <span class="small">行情、基本面、新闻和推荐输入的可回放记录</span>
      </div>
      <table>
        <thead><tr><th>Snapshot</th><th>时间</th><th>Provider</th><th>质量</th><th>股票</th><th>Bars</th><th>News</th><th>评分</th><th>期望值</th><th>推荐</th><th>操作</th></tr></thead>
        <tbody id="sourceSnapshotBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>策略版本</h3>
        <span class="small">每次推荐使用的风险、信号和价格计划参数</span>
      </div>
      <table>
        <thead><tr><th>Strategy</th><th>Universe</th><th>Pattern</th><th>Min Conf</th><th>Signal Weights</th><th>Top N</th><th>Created</th></tr></thead>
        <tbody id="strategyConfigBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>调参建议</h3>
        <span class="small">基于 strategy 级卖出归因生成的保留、收紧或放宽建议</span>
      </div>
      <table>
        <thead><tr><th>Strategy</th><th>动作</th><th>优先级</th><th>评分</th><th>样本</th><th>当前参数</th><th>建议改动</th><th>理由</th></tr></thead>
        <tbody id="strategyTuningBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>自动循环历史</h3>
        <span class="small">system_cycle 的持久心跳、推荐数量、提醒数量和事件处理情况</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>状态</th><th>推荐</th><th>提醒</th><th>自动审批</th><th>自动执行</th><th>事件</th><th>Snapshot</th><th>Strategy</th><th>Top</th></tr></thead>
        <tbody id="systemRunBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>推荐列表（原因导向）</h3>
        <span class="small">以最新快照为主，关注入场区与纪律止损</span>
      </div>
      <table>
        <thead><tr><th>股票</th><th>现价</th><th>方向</th><th>信号</th><th>交易计划</th><th>为什么买</th><th>什么时候卖</th><th>操作</th></tr></thead>
        <tbody id="recsBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>持仓监控</h3>
        <span class="small">纸单成交和手工买入会同步进入止损/止盈监控</span>
      </div>
      <table>
        <thead><tr><th>股票</th><th>数量</th><th>成本</th><th>止损</th><th>止盈1</th><th>止盈2</th><th>已实现盈亏</th><th>最近卖出</th><th>操作</th></tr></thead>
        <tbody id="holdingBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>持仓风控审计</h3>
        <span class="small">止损、目标位和持仓备注的调整轨迹</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>股票</th><th>止损</th><th>止盈1</th><th>止盈2</th><th>操作人</th><th>理由</th></tr></thead>
        <tbody id="holdingControlAuditBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>纸单记录</h3>
        <span class="small">审批后的下单、成交和取消状态审计轨迹</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>Order</th><th>推荐</th><th>Exec</th><th>方向</th><th>数量</th><th>限价</th><th>状态</th><th>成交价</th><th>Adapter</th><th>操作</th></tr></thead>
        <tbody id="orderBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>交易流水</h3>
        <span class="small">买入、卖出和已实现盈亏审计轨迹</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>股票</th><th>方向</th><th>数量</th><th>价格</th><th>已实现盈亏</th><th>原因</th></tr></thead>
        <tbody id="tradeBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>卖出执行审计</h3>
        <span class="small">记录 paper sell 与 live dry-run，区分是否真正写入交易流水</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>Exec</th><th>股票</th><th>数量</th><th>价格</th><th>状态</th><th>Ledger</th><th>预计/实际盈亏</th><th>原因</th><th>Adapter</th></tr></thead>
        <tbody id="sellExecutionBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>仓位核对</h3>
        <span class="small">券商持仓快照与本地 open holdings 的一致性审计</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>Broker</th><th>状态</th><th>阻断</th><th>本地/券商</th><th>差异</th><th>备注</th></tr></thead>
        <tbody id="positionReconciliationBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>交易复盘</h3>
        <span class="small">按股票归因的已实现盈亏、胜率与盈亏质量</span>
      </div>
      <table>
        <thead><tr><th>股票</th><th>卖出笔数</th><th>已实现盈亏</th><th>胜率</th><th>均赢</th><th>均亏</th><th>Profit Factor</th></tr></thead>
        <tbody id="performanceBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>推荐归因</h3>
        <span class="small">把卖出结果回挂到 recommendation_id 和 source_snapshot_id</span>
      </div>
      <table>
        <thead><tr><th>股票</th><th>推荐</th><th>Snapshot</th><th>卖出笔数</th><th>已实现盈亏</th><th>胜率</th><th>Profit Factor</th><th>推荐分</th></tr></thead>
        <tbody id="attributionBody"></tbody>
      </table>
      <table style="margin-top:12px;">
        <thead><tr><th>Snapshot</th><th>评分</th><th>推荐数</th><th>卖出笔数</th><th>已实现盈亏</th><th>期望值</th><th>胜率</th><th>Profit Factor</th></tr></thead>
        <tbody id="snapshotAttributionBody"></tbody>
      </table>
      <table style="margin-top:12px;">
        <thead><tr><th>Strategy</th><th>评分</th><th>推荐数</th><th>卖出笔数</th><th>已实现盈亏</th><th>期望值</th><th>胜率</th><th>Profit Factor</th></tr></thead>
        <tbody id="strategyAttributionBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>卖出提醒</h3>
        <span class="small">触发条件: 止损失效 / 目标触达 / 风险偏好切换</span>
      </div>
      <table>
        <thead><tr><th>级别</th><th>股票</th><th>原因</th><th>提醒内容</th><th>建议动作</th><th>时间</th><th>操作</th></tr></thead>
        <tbody id="alertBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>卖出提醒历史</h3>
        <span class="small">由 system_cycle 持久记录的监控触发历史，避免重启后丢失提醒证据</span>
      </div>
      <table>
        <thead><tr><th>时间</th><th>Run</th><th>级别</th><th>股票</th><th>原因</th><th>价格</th><th>提醒内容</th><th>建议动作</th></tr></thead>
        <tbody id="alertHistoryBody"></tbody>
      </table>
    </div>
  </div>

  <script>
    const searchParams = new URLSearchParams(window.location.search);
    const accessPwd = searchParams.get('pwd');
    const pwdSuffix = accessPwd ? `?pwd=${encodeURIComponent(accessPwd)}` : '';
    let refreshTimer = null;
    let currentRecommendations = [];
    let currentHoldings = [];
    let currentAlerts = [];
    let currentSnapshots = [];
    let currentStrategyConfigs = [];
    let currentStrategyTuning = [];
    let currentAutopilotPolicy = {};
    let currentAutopilotPreflight = {};

    function esc(v) {
      return String(v ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function badge(level) {
      if (level === "urgent") return '<span class="badge b-danger">紧急</span>';
      if (level === "warn") return '<span class="badge b-warn">关注</span>';
      return '<span class="badge b-ok">提示</span>';
    }

    function directionBadge(direction) {
      const side = String(direction ?? "").toUpperCase();
      if (side === "BUY") return '<span class="badge b-buy">BUY</span>';
      if (side === "SELL") return '<span class="badge b-danger">SELL</span>';
      return `<span class="badge b-neutral">${esc(direction || '-')}</span>`;
    }

    function fmtNum(v, digits) {
      const n = Number(v);
      if (!Number.isFinite(n)) return '-';
      return n.toFixed(digits);
    }

    function fmtTime(value) {
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return esc(value || '-');
      return date.toLocaleString('zh-CN', { hour12: false });
    }

    function numberValue(id) {
      const raw = document.getElementById(id)?.value;
      if (raw === undefined || raw === null || raw === '') return null;
      const value = Number(raw);
      return Number.isFinite(value) ? value : null;
    }

    function checkedValue(id) {
      return Boolean(document.getElementById(id)?.checked);
    }

    function setChecked(id, value) {
      const el = document.getElementById(id);
      if (el) el.checked = Boolean(value);
    }

    function setFieldValue(id, value) {
      const el = document.getElementById(id);
      if (!el || value === undefined || value === null) return;
      el.value = String(value);
    }

    function pctInputValue(value, fallback) {
      const n = Number(value);
      const safe = Number.isFinite(n) ? n : fallback;
      return Number((safe * 100).toFixed(4)).toString();
    }

    function reasonValue(fallback) {
      const value = document.getElementById('tradeReason')?.value?.trim();
      return value || fallback;
    }

    function setActionStatus(message, isError = false) {
      const status = document.getElementById('actionStatus');
      const log = document.getElementById('operationLog');
      const messageText = String(message || '');
      status.textContent = isError ? '操作失败' : (messageText.toLowerCase().includes('dry-run') ? 'Dry-run完成' : '操作完成');
      status.style.color = isError ? 'var(--danger)' : 'var(--ok)';
      log.textContent = messageText;
      log.style.color = isError ? 'var(--danger)' : 'var(--ink-soft)';
    }

    function apiUrl(path) {
      const u = new URL(path, window.location.origin);
      if (accessPwd) u.searchParams.set('pwd', accessPwd);
      return u.toString();
    }

    async function postJson(path, payload, method = 'POST') {
      const res = await fetch(apiUrl(path), {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const text = await res.text();
      let data = {};
      if (text) {
        try { data = JSON.parse(text); } catch (_) { data = { detail: text }; }
      }
      if (!res.ok) {
        const detail = data.detail || `HTTP ${res.status}`;
        const message = Array.isArray(detail)
          ? detail.map((x) => x.msg || String(x)).join('; ')
          : (typeof detail === 'object' ? JSON.stringify(detail) : String(detail));
        throw new Error(message);
      }
      return data;
    }

    function renderAutopilotPolicy(policy, preflight = {}) {
      currentAutopilotPolicy = policy || {};
      currentAutopilotPreflight = preflight || {};
      const enabled = Boolean(currentAutopilotPolicy.enabled);
      const status = currentAutopilotPreflight.status || (enabled ? 'ready' : 'off');
      const statusClass = status === 'ready' ? 'b-ok' : (status === 'blocked' ? 'b-danger' : 'b-neutral');
      document.getElementById('autopilotStatus').innerHTML = `<span class="badge ${statusClass}">${esc(status.toUpperCase())}</span>`;
      document.getElementById('autopilotPolicyBadge').className = enabled
        ? `badge ${statusClass}`
        : 'badge b-neutral';
      document.getElementById('autopilotPolicyBadge').textContent = enabled
        ? `Autopilot ${status.toUpperCase()}`
        : 'Autopilot OFF';
      const reasons = (currentAutopilotPreflight.reasons || []).join(', ');
      const daily = currentAutopilotPreflight.daily_usage || {};
      const budgetText = daily.trading_day
        ? ` | left A/B/S ${daily.remaining_approvals ?? '-'} / ${daily.remaining_buys ?? '-'} / ${daily.remaining_sells ?? '-'}`
        : '';
      document.getElementById('autopilotPolicyMeta').textContent =
        ` #${currentAutopilotPolicy.policy_id || '-'} | ${currentAutopilotPolicy.updated_by || 'system'} | ${fmtTime(currentAutopilotPolicy.updated_at)}${budgetText}${reasons ? ` | ${reasons}` : ''}`;

      setChecked('autopilotEnabled', enabled);
      setChecked('autopilotAutoApprove', currentAutopilotPolicy.auto_approve_recommendations);
      setChecked('autopilotAutoExecute', currentAutopilotPolicy.auto_execute_approved);
      setChecked('autopilotRegularHours', currentAutopilotPolicy.restrict_auto_execution_to_regular_hours);
      setFieldValue('autopilotExecutionMode', currentAutopilotPolicy.auto_execution_mode || 'paper');
      setFieldValue('autopilotMinConfidence', currentAutopilotPolicy.auto_approve_min_confidence ?? 0.72);
      setFieldValue('autopilotMinComposite', currentAutopilotPolicy.auto_approve_min_composite ?? 0);
      setFieldValue('autopilotMaxApprovals', currentAutopilotPolicy.max_auto_approvals ?? 1);
      setFieldValue('autopilotMaxBuys', currentAutopilotPolicy.max_auto_buys ?? 1);
      setFieldValue('autopilotMaxSells', currentAutopilotPolicy.max_auto_sells ?? 10);
      setFieldValue('autopilotDailyApprovals', currentAutopilotPolicy.max_daily_auto_approvals ?? 3);
      setFieldValue('autopilotDailyBuys', currentAutopilotPolicy.max_daily_auto_buys ?? 3);
      setFieldValue('autopilotDailySells', currentAutopilotPolicy.max_daily_auto_sells ?? 10);
      setFieldValue('autopilotOrderDedupeMinutes', currentAutopilotPolicy.order_dedupe_minutes ?? 1440);
      setFieldValue('autopilotSellAlertCooldownMinutes', currentAutopilotPolicy.sell_alert_cooldown_minutes ?? 60);
      setFieldValue('autopilotCooldownMinutes', currentAutopilotPolicy.rebuy_cooldown_minutes ?? 240);
      setFieldValue('autopilotMinSnapshotBars', pctInputValue(currentAutopilotPolicy.min_snapshot_bar_coverage, 1.0));
      setFieldValue('autopilotMinSnapshotFundamentals', pctInputValue(currentAutopilotPolicy.min_snapshot_fundamental_coverage, 1.0));
      setFieldValue('autopilotMaxSnapshotBarAge', currentAutopilotPolicy.max_snapshot_bar_age_minutes ?? 4320);
      setFieldValue('autopilotAccountEquity', currentAutopilotPolicy.account_equity ?? 100000);
      setFieldValue('autopilotMaxOpenRiskPct', pctInputValue(currentAutopilotPolicy.max_open_risk_pct, 0.06));
      setFieldValue('autopilotMaxDailyLossPct', pctInputValue(currentAutopilotPolicy.max_daily_realized_loss_pct, 0.03));
      setFieldValue('autopilotMaxBuyDriftPct', pctInputValue(currentAutopilotPolicy.max_auto_buy_price_drift_pct, 0.03));
      setChecked('autopilotRequireReconciliation', currentAutopilotPolicy.require_position_reconciliation);
      setFieldValue('autopilotMaxReconciliationAge', currentAutopilotPolicy.max_position_reconciliation_age_minutes ?? 1440);
      setFieldValue('autopilotRiskPct', pctInputValue(currentAutopilotPolicy.risk_per_trade_pct, 0.01));
      setFieldValue('autopilotMaxPositionPct', pctInputValue(currentAutopilotPolicy.max_position_pct, 0.10));
      setFieldValue('autopilotMaxGrossPct', pctInputValue(currentAutopilotPolicy.max_gross_exposure_pct, 1.0));
      setFieldValue('autopilotMaxSectorPct', pctInputValue(currentAutopilotPolicy.max_sector_exposure_pct, 0.30));
      setFieldValue('autopilotReason', currentAutopilotPolicy.reason || '');
    }

    function autopilotPolicyPayload(forceEnabled = null) {
      const enabled = forceEnabled === null ? checkedValue('autopilotEnabled') : Boolean(forceEnabled);
      return {
        enabled,
        auto_approve_recommendations: checkedValue('autopilotAutoApprove'),
        auto_execute_approved: checkedValue('autopilotAutoExecute'),
        restrict_auto_execution_to_regular_hours: checkedValue('autopilotRegularHours'),
        auto_execution_mode: document.getElementById('autopilotExecutionMode')?.value || 'paper',
        auto_approve_min_confidence: numberValue('autopilotMinConfidence') ?? 0.72,
        auto_approve_min_composite: numberValue('autopilotMinComposite') ?? 0,
        max_auto_approvals: Math.max(0, Math.floor(numberValue('autopilotMaxApprovals') ?? 1)),
        max_auto_buys: Math.max(0, Math.floor(numberValue('autopilotMaxBuys') ?? 1)),
        max_auto_sells: Math.max(0, Math.floor(numberValue('autopilotMaxSells') ?? 10)),
        max_daily_auto_approvals: Math.max(0, Math.floor(numberValue('autopilotDailyApprovals') ?? 3)),
        max_daily_auto_buys: Math.max(0, Math.floor(numberValue('autopilotDailyBuys') ?? 3)),
        max_daily_auto_sells: Math.max(0, Math.floor(numberValue('autopilotDailySells') ?? 10)),
        order_dedupe_minutes: Math.max(0, Math.floor(numberValue('autopilotOrderDedupeMinutes') ?? 1440)),
        sell_alert_cooldown_minutes: Math.max(0, Math.floor(numberValue('autopilotSellAlertCooldownMinutes') ?? 60)),
        rebuy_cooldown_minutes: Math.max(0, Math.floor(numberValue('autopilotCooldownMinutes') ?? 240)),
        min_snapshot_bar_coverage: Math.max(0, Math.min(100, numberValue('autopilotMinSnapshotBars') ?? 100)) / 100,
        min_snapshot_fundamental_coverage: Math.max(0, Math.min(100, numberValue('autopilotMinSnapshotFundamentals') ?? 100)) / 100,
        max_snapshot_bar_age_minutes: Math.max(0, Math.floor(numberValue('autopilotMaxSnapshotBarAge') ?? 4320)),
        account_equity: numberValue('autopilotAccountEquity') ?? 100000,
        max_open_risk_pct: Math.max(0, Math.min(100, numberValue('autopilotMaxOpenRiskPct') ?? 6)) / 100,
        max_daily_realized_loss_pct: Math.max(0, Math.min(100, numberValue('autopilotMaxDailyLossPct') ?? 3)) / 100,
        max_auto_buy_price_drift_pct: Math.max(0, Math.min(100, numberValue('autopilotMaxBuyDriftPct') ?? 3)) / 100,
        require_position_reconciliation: checkedValue('autopilotRequireReconciliation'),
        max_position_reconciliation_age_minutes: Math.max(0, Math.floor(numberValue('autopilotMaxReconciliationAge') ?? 1440)),
        risk_per_trade_pct: (numberValue('autopilotRiskPct') ?? 1) / 100,
        max_position_pct: (numberValue('autopilotMaxPositionPct') ?? 10) / 100,
        max_gross_exposure_pct: (numberValue('autopilotMaxGrossPct') ?? 100) / 100,
        max_sector_exposure_pct: (numberValue('autopilotMaxSectorPct') ?? 30) / 100,
        reason: document.getElementById('autopilotReason')?.value?.trim() || (
          enabled ? 'dashboard_autopilot_policy' : 'dashboard_autopilot_disabled'
        ),
        updated_by: 'dashboard',
      };
    }

    async function saveAutopilotPolicy(forceEnabled = null) {
      try {
        const result = await postJson('/execution/autopilot-policy', autopilotPolicyPayload(forceEnabled));
        renderAutopilotPolicy(result, currentAutopilotPreflight);
        setActionStatus(`Autopilot policy 已保存: ${result.enabled ? 'ON' : 'OFF'} #${result.policy_id}`);
        await loadData();
      } catch (err) {
        setActionStatus(`保存 Autopilot policy 失败: ${err.message || err}`, true);
      }
    }

    function disableAutopilotPolicy() {
      saveAutopilotPolicy(false);
    }

    function providerReliability(provider) {
      if (provider === 'YFinanceProvider') return '研究级实时（聚合源，非交易所直连）';
      if (provider === 'MockMarketDataProvider') return '模拟数据（仅用于测试）';
      return '外部适配器（请确认授权等级）';
    }

    function nyMarketOpenStatus() {
      const fmt = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        weekday: 'short',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      });
      const parts = fmt.formatToParts(new Date());
      const obj = {};
      for (const p of parts) obj[p.type] = p.value;
      const weekday = obj.weekday;
      const hour = Number(obj.hour || '0');
      const minute = Number(obj.minute || '0');
      const dayOpen = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'].includes(weekday);
      const mins = hour * 60 + minute;
      const isOpen = dayOpen && mins >= (9 * 60 + 30) && mins < (16 * 60);
      return { isOpen, weekday, hour, minute };
    }

    function getRefreshMs() {
      const status = nyMarketOpenStatus();
      return status.isOpen ? 60 * 1000 : 30 * 60 * 1000;
    }

    function refreshLabel() {
      const status = nyMarketOpenStatus();
      return status.isOpen
        ? '当前美股开盘时段，自动每 1 分钟刷新一次'
        : '当前美股闭市时段，自动每 30 分钟刷新一次';
    }

    function renderRecommendations(items) {
      currentRecommendations = items || [];
      const body = document.getElementById("recsBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="8" class="small">暂无推荐</td></tr>';
        return;
      }
      body.innerHTML = items.map((r, idx) => `
        <tr>
          <td><strong>${esc(r.ticker)}</strong><div class="mono">${esc(r.id)}</div><div class="small mono">${esc(shortId(r.strategy_config_id, 14))}</div></td>
          <td><strong>${fmtNum(r.current_price, 2)}</strong></td>
          <td>${directionBadge(r.direction)}</td>
          <td>composite=${fmtNum(r.composite, 4)}<br/>confidence=${fmtNum(r.confidence, 3)}</td>
          <td>入场 ${fmtNum(r.entry_zone_low, 2)}-${fmtNum(r.entry_zone_high, 2)}<br/>止损 ${fmtNum(r.stop_loss, 2)}<br/>目标 ${fmtNum(r.tp1, 2)} / ${fmtNum(r.tp2, 2)}</td>
          <td><ul class="why">${(r.why_to_buy_cn || []).map((x) => `<li>${esc(x)}</li>`).join("")}</ul></td>
          <td><ul class="why">${(r.why_to_sell_cn || []).map((x) => `<li>${esc(x)}</li>`).join("")}</ul></td>
          <td>
            <div class="small">审批: ${esc(r.approval_status || 'pending')}</div>
            <button class="btn-mini" onclick="approveRecommendation(${idx})">审批</button>
            <button class="btn-mini" onclick="planBuyRecommendation(${idx})">建议股数</button>
            <button class="btn-mini" onclick="buyRecommendation(${idx})">买入</button>
            <a class="link" href="/dashboard/recommendations/${encodeURIComponent(r.id)}${pwdSuffix}" target="_blank">查看</a>
          </td>
        </tr>
      `).join("");
    }

    function snapshotScoreCell(score) {
      if (!score) return '<span class="small">unscored</span>';
      return `${fmtNum(score.performance_score, 1)}<div class="small">${esc(score.quality_grade || '')}</div>`;
    }

    function snapshotQualityCell(snapshot) {
      const quality = snapshot?.data_quality || {};
      const status = quality.status || 'unknown';
      const cls = status === 'complete' ? 'b-ok' : (status === 'partial' ? 'b-warn' : 'b-neutral');
      return `
        <span class="badge ${cls}">${esc(status)}</span>
        <div class="small">B ${fmtPct(quality.bar_coverage)} / F ${fmtPct(quality.fundamental_coverage)}</div>
        <div class="small">N ${fmtPct(quality.event_ticker_coverage)}</div>
      `;
    }

    function renderSourceSnapshots(items, snapshotScores) {
      currentSnapshots = items || [];
      const body = document.getElementById("sourceSnapshotBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="11" class="small">暂无行情快照。</td></tr>';
        return;
      }
      const scoresBySnapshot = new Map((snapshotScores || []).map((row) => [row.source_snapshot_id, row]));
      body.innerHTML = items.map((snapshot, idx) => `
        <tr>
          ${(() => {
            const score = scoresBySnapshot.get(snapshot.source_snapshot_id);
            return `
          <td class="mono" title="${esc(snapshot.source_snapshot_id)}">${esc(shortId(snapshot.source_snapshot_id, 18))}</td>
          <td class="small">${fmtTime(snapshot.as_of)}</td>
          <td>${esc(snapshot.provider_name)}</td>
          <td>${snapshotQualityCell(snapshot)}</td>
          <td>${esc(snapshot.ticker_count)}</td>
          <td>${esc(snapshot.bar_count)}</td>
          <td>${esc(snapshot.event_count)}</td>
          <td>${snapshotScoreCell(score)}</td>
          <td>${score ? fmtNum(score.expectancy_per_sell, 2) : '-'}</td>
          <td>${esc(snapshot.recommendation_count)}</td>
          <td><button class="btn-mini" onclick="replaySnapshot(${idx})">回放</button></td>
            `;
          })()}
        </tr>
      `).join("");
    }

    function signalWeightsText(config) {
      const weights = config?.signal_config || {};
      const parts = [
        ['T', weights.technical_weight],
        ['N', weights.event_news_weight],
        ['RS', weights.relative_strength_weight],
        ['F', weights.fundamental_weight],
        ['X', weights.execution_quality_weight],
      ];
      return parts.map(([label, value]) => `${label}:${fmtNullableNum(value, 2)}`).join(' ');
    }

    function renderStrategyConfigs(items) {
      currentStrategyConfigs = items || [];
      const body = document.getElementById("strategyConfigBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="7" class="small">暂无策略版本。</td></tr>';
        return;
      }
      body.innerHTML = items.map((config) => `
        <tr>
          <td class="mono" title="${esc(config.strategy_config_id)}">${esc(shortId(config.strategy_config_id, 18))}</td>
          <td>${esc(config.universe)}</td>
          <td>${esc(config.price_plan_config?.strategy_pattern || '-')}</td>
          <td>${fmtNullableNum(config.risk_policy?.min_confidence, 2)}</td>
          <td class="mono">${esc(signalWeightsText(config))}</td>
          <td>${esc(config.publication?.top_n ?? '-')}</td>
          <td class="small">${fmtTime(config.created_at)}</td>
        </tr>
      `).join("");
    }

    function tuningActionBadge(action) {
      const labels = {
        collect_more_data: '收集样本',
        keep: '保留',
        tighten: '收紧',
        relax: '放宽',
        review: '复盘',
      };
      const cls = {
        collect_more_data: 'b-neutral',
        keep: 'b-ok',
        tighten: 'b-danger',
        relax: 'b-buy',
        review: 'b-warn',
      }[action] || 'b-neutral';
      return `<span class="badge ${cls}">${esc(labels[action] || action || '-')}</span>`;
    }

    function parameterSummary(params) {
      if (!params || Object.keys(params).length === 0) return '<span class="small">无参数快照</span>';
      const stopRange = Array.isArray(params.stop_atr_range)
        ? params.stop_atr_range.map((x) => fmtNullableNum(x, 2)).join('-')
        : '-';
      return [
        `conf ${fmtNullableNum(params.min_confidence, 2)}`,
        `gap ${fmtNullableNum(params.max_entry_gap_pct, 2)}`,
        `news ${fmtNullableNum(params.event_news_weight, 2)}`,
        `stop ${stopRange}`,
        `top ${esc(params.top_n ?? '-')}`,
      ].join('<br/>');
    }

    function tuningChangeText(changes) {
      const entries = Object.entries(changes || {});
      if (!entries.length) return '<span class="small">保持当前参数</span>';
      return entries.map(([key, change]) => {
        const current = Array.isArray(change?.current) ? change.current.join(' / ') : change?.current;
        const suggested = Array.isArray(change?.suggested) ? change.suggested.join(' / ') : change?.suggested;
        return `<div><span class="mono">${esc(key)}</span><br/><span class="small">${esc(current)} → ${esc(suggested)}</span></div>`;
      }).join('');
    }

    function renderStrategyTuning(report) {
      currentStrategyTuning = report?.items || [];
      const body = document.getElementById("strategyTuningBody");
      if (!currentStrategyTuning.length) {
        body.innerHTML = '<tr><td colspan="8" class="small">暂无调参建议。</td></tr>';
        return;
      }
      body.innerHTML = currentStrategyTuning.map((item) => {
        const metrics = item.metric_snapshot || {};
        return `
          <tr>
            <td class="mono" title="${esc(item.strategy_config_id)}">${esc(shortId(item.strategy_config_id, 18))}</td>
            <td>${tuningActionBadge(item.action)}</td>
            <td>${esc(item.priority ?? '-')}</td>
            <td>${snapshotScoreCell(metrics)}</td>
            <td>rec ${esc(metrics.recommendation_count ?? 0)}<br/>sell ${esc(metrics.sell_trade_count ?? 0)}<br/>win ${fmtPct(metrics.win_rate)}</td>
            <td>${parameterSummary(item.current_parameters)}</td>
            <td>${tuningChangeText(item.recommended_changes)}</td>
            <td>${esc(item.rationale_cn || '-')}</td>
          </tr>
        `;
      }).join("");
    }

    function autoExecutionCell(run) {
      const auto = run.metrics?.auto_execution || {};
      if (!auto.enabled) return '<span class="badge b-neutral">off</span>';
      const errors = Number(auto.error_count || 0);
      const buys = Number(auto.buy_order_count || 0);
      const sells = Number(auto.sell_order_count || 0);
      const skipped = Number(auto.skipped_count || 0);
      const cls = errors > 0 ? 'b-danger' : (buys + sells > 0 ? 'b-ok' : 'b-warn');
      const mode = auto.mode || 'paper';
      return `
        <span class="badge ${cls}">${esc(mode)}</span>
        <div class="small">buy ${buys} / sell ${sells}</div>
        <div class="small">skip ${skipped} / err ${errors}</div>
      `;
    }

    function autoApprovalCell(run) {
      const auto = run.metrics?.auto_approval || {};
      if (!auto.enabled) return '<span class="badge b-neutral">off</span>';
      const errors = Number(auto.error_count || 0);
      const approved = Number(auto.approved_count || 0);
      const skipped = Number(auto.skipped_count || 0);
      const cls = errors > 0 ? 'b-danger' : (approved > 0 ? 'b-ok' : 'b-warn');
      return `
        <span class="badge ${cls}">approval</span>
        <div class="small">approved ${approved}</div>
        <div class="small">skip ${skipped} / err ${errors}</div>
      `;
    }

    function renderSystemRuns(items) {
      const body = document.getElementById("systemRunBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="10" class="small">暂无自动循环历史。</td></tr>';
        return;
      }
      body.innerHTML = items.map((run) => {
        const top = (run.top_recommendations || []).slice(0, 3).map((item) => item.ticker).join(', ');
        const statusCell = run.status === 'success'
          ? '<span class="badge b-ok">success</span>'
          : `<span class="badge b-danger">${esc(run.status)}</span>`;
        return `
          <tr>
            <td class="small">${fmtTime(run.finished_at || run.started_at)}</td>
            <td>${statusCell}</td>
            <td>${esc(run.recommendation_count ?? 0)}</td>
            <td>${esc(run.sell_alert_count ?? 0)}</td>
            <td>${autoApprovalCell(run)}</td>
            <td>${autoExecutionCell(run)}</td>
            <td class="small">consumed ${esc(run.consumed_event_count ?? 0)}<br/>pending ${esc(run.pending_event_count ?? 0)}</td>
            <td class="mono" title="${esc(run.source_snapshot_id || '')}">${esc(shortId(run.source_snapshot_id, 16))}</td>
            <td class="mono" title="${esc(run.strategy_config_id || '')}">${esc(shortId(run.strategy_config_id, 16))}</td>
            <td class="small">${esc(top || '-')}</td>
          </tr>
        `;
      }).join("");
    }

    function replayPayload(objective) {
      const topN = numberValue('topN') || 5;
      const minConfidence = numberValue('minConfidence') ?? 0;
      return {
        objective,
        universe_rules: {
          min_price: 1,
          min_avg_dollar_volume: 1000000,
          max_spread_bps: 100,
          min_market_cap_usd: 100000000,
          allowed_sectors: [],
          max_candidates_after_filter: 50,
        },
        risk_policy: {
          min_confidence: minConfidence,
          earnings_blackout_minutes: 0,
          max_name_weight: 0.10,
          max_sector_weight: 0.30,
          max_gross_exposure: 1.0,
          max_correlated_cluster_weight: 0.35,
          reject_on_material_evidence_conflict: false,
          event_trading_enabled: true,
        },
        publication: { top_n: topN, output_channels: ['api'] },
        execution_mode: 'research_only',
      };
    }

    async function replaySnapshot(index) {
      const snapshot = currentSnapshots[index];
      if (!snapshot) return;
      document.getElementById('sourceSnapshotId').value = snapshot.source_snapshot_id;
      try {
        const result = await postJson(
          `/source-snapshots/${encodeURIComponent(snapshot.source_snapshot_id)}/replay`,
          replayPayload('dashboard snapshot replay')
        );
        const op = result.universe_summary?.snapshot?.operation || 'unknown';
        setActionStatus(`已回放 ${result.source_snapshot_id}，operation=${op}，推荐 ${result.recommendations?.length || 0} 条`);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    async function replaySnapshotByInput() {
      const snapshotId = document.getElementById('sourceSnapshotId')?.value?.trim();
      if (!snapshotId) {
        setActionStatus('请输入 snapshot id', true);
        return;
      }
      try {
        const result = await postJson(
          `/source-snapshots/${encodeURIComponent(snapshotId)}/replay`,
          replayPayload('dashboard snapshot replay')
        );
        const op = result.universe_summary?.snapshot?.operation || 'unknown';
        setActionStatus(`已回放 ${result.source_snapshot_id}，operation=${op}，推荐 ${result.recommendations?.length || 0} 条`);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    function renderHoldings(items) {
      currentHoldings = items || [];
      const body = document.getElementById("holdingBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="9" class="small">暂无监控持仓。审批后纸单买入或 POST /portfolio/buys 会进入这里。</td></tr>';
        return;
      }
      body.innerHTML = items.map((h, idx) => `
        <tr>
          <td>${esc(h.ticker)}</td>
          <td>${fmtNum(h.qty, 2)}</td>
          <td>${fmtNum(h.avg_buy_price, 2)}</td>
          <td>${fmtNum(h.stop_loss, 2)}</td>
          <td>${fmtNum(h.take_profit1, 2)}</td>
          <td>${fmtNum(h.take_profit2, 2)}</td>
          <td>${fmtNum(h.realized_pnl, 2)}</td>
          <td>${fmtNum(h.last_sell_price, 2)}<div class="small">${esc(h.last_sell_reason || "")}</div></td>
          <td>
            <button class="btn-mini" onclick="loadHoldingControls(${idx})">载入</button>
            <button class="btn-mini" onclick="updateHoldingControls(${idx})">风控</button>
            <button class="btn-mini danger" onclick="sellHolding(${idx}, false)">卖出</button>
            <button class="btn-mini danger" onclick="sellHolding(${idx}, true)">清仓</button>
            <div class="small">${esc(h.note || "")}</div>
          </td>
        </tr>
      `).join("");
    }

    async function runResearch() {
      const topN = numberValue('topN') || 5;
      const minConfidence = numberValue('minConfidence') ?? 0;
      const payload = {
        run_type: 'research_batch',
        objective: 'dashboard research run',
        snapshot_mode: 'latest',
        universe: 'SP500',
        universe_rules: {
          min_price: 1,
          min_avg_dollar_volume: 1000000,
          max_spread_bps: 100,
          min_market_cap_usd: 100000000,
          allowed_sectors: [],
          max_candidates_after_filter: 50,
        },
        risk_policy: {
          min_confidence: minConfidence,
          earnings_blackout_minutes: 0,
          max_name_weight: 0.10,
          max_sector_weight: 0.30,
          max_gross_exposure: 1.0,
          max_correlated_cluster_weight: 0.35,
          max_entry_gap_pct: 0.40,
          reject_on_material_evidence_conflict: false,
          event_trading_enabled: true,
        },
        publication: { top_n: topN, output_channels: ['api', 'dashboard'] },
        execution_mode: 'research_only',
      };
      try {
        const result = await postJson('/research/run', payload);
        setActionStatus(`已生成 ${result.recommendations?.length || 0} 条推荐，snapshot=${result.source_snapshot_id}`);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    async function runSystemCycle() {
      const topN = numberValue('topN') || 5;
      const minConfidence = numberValue('minConfidence') ?? 0;
      try {
        const result = await postJson('/operations/system-cycle', {
          top_n: topN,
          min_confidence: minConfidence,
          consume_events: false,
          use_autopilot_policy: true,
        });
        const approval = result.auto_approval || {};
        const execution = result.auto_execution || {};
        setActionStatus(
          `系统循环完成: 推荐 ${result.recommendation_count || 0}，提醒 ${result.sell_alert_count || 0}，` +
          `自动审批 ${approval.approved_count || 0}，买入 ${execution.buy_order_count || 0}，卖出 ${execution.sell_order_count || 0}`
        );
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    async function approveRecommendation(index) {
      const rec = currentRecommendations[index];
      if (!rec) return;
      try {
        const result = await postJson(`/recommendations/${encodeURIComponent(rec.id)}/approval`, {
          decision: 'approved',
          approver: 'dashboard',
          notes: reasonValue('dashboard approval'),
        });
        setActionStatus(`已审批 ${result.recommendation_id}`);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    function executionModePayload() {
      const executionChoice = document.getElementById('executionMode')?.value || 'paper';
      const isLiveDryRun = executionChoice === 'live_dry_run';
      return {
        execution_mode: isLiveDryRun ? 'live' : 'paper',
        dry_run: isLiveDryRun,
        confirm_live: false,
      };
    }

    function paperOrderPayload(rec, qty) {
      const buyPrice = numberValue('buyPrice') || Number(rec.entry_zone_high);
      return {
        recommendation_id: rec.id,
        side: rec.direction || 'BUY',
        qty,
        limit_price: buyPrice,
        ...executionModePayload(),
        account_equity: numberValue('accountEquity') || 100000,
        risk_per_trade_pct: (numberValue('riskPct') || 1) / 100,
        max_position_pct: (numberValue('maxPositionPct') || 10) / 100,
        max_gross_exposure_pct: (numberValue('maxGrossPct') || 100) / 100,
        max_sector_exposure_pct: (numberValue('maxSectorPct') || 30) / 100,
      };
    }

    async function planBuyRecommendation(index) {
      const rec = currentRecommendations[index];
      if (!rec) return;
      const qty = numberValue('buyQty') || 1;
      try {
        const plan = await postJson('/paper-orders/risk-plan', paperOrderPayload(rec, qty));
        if (!plan.recommended_qty || plan.recommended_qty <= 0) {
          setActionStatus(plan.message_cn || '当前账户风控下无可买股数', true);
          return;
        }
        const suggestedQty = Math.max(1, Math.floor(Number(plan.recommended_qty)));
        document.getElementById('buyQty').value = String(suggestedQty);
        setActionStatus(
          `建议 ${rec.ticker} 买入 ${suggestedQty} 股；单笔风险预算 ${fmtNum(plan.risk_budget, 2)}，预计止损风险 ${fmtNum(plan.risk_per_share * suggestedQty, 2)}。`
        );
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    async function buyRecommendation(index) {
      const rec = currentRecommendations[index];
      if (!rec) return;
      const qty = numberValue('buyQty');
      if (!qty || qty <= 0) {
        setActionStatus('买入数量必须大于 0', true);
        return;
      }
      const payload = paperOrderPayload(rec, qty);
      try {
        const plan = await postJson('/paper-orders/risk-plan', payload);
        if (!plan.is_within_limits) {
          setActionStatus(plan.message_cn || '风险校验未通过', true);
          return;
        }
        const order = await postJson('/paper-orders', payload);
        const executionText = order.dry_run
          ? `已完成 ${rec.ticker} ${order.execution_mode} dry-run ${qty} 股`
          : `已提交纸单买入 ${rec.ticker} x ${qty} @ ${fmtNum(order.simulated_fill_price || payload.limit_price, 2)}`;
        setActionStatus(`${executionText}；${order.adapter_message || plan.message_cn}`);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    function loadHoldingControls(index) {
      const holding = currentHoldings[index];
      if (!holding) return;
      document.getElementById('controlStopLoss').value = String(holding.stop_loss ?? '');
      document.getElementById('controlTp1').value = String(holding.take_profit1 ?? '');
      document.getElementById('controlTp2').value = String(holding.take_profit2 ?? '');
      setActionStatus(`${holding.ticker} 风控参数已载入`);
    }

    async function updateHoldingControls(index) {
      const holding = currentHoldings[index];
      if (!holding) return;
      const stopLoss = numberValue('controlStopLoss');
      const tp1 = numberValue('controlTp1');
      const tp2 = numberValue('controlTp2');
      const payload = {
        stop_loss: stopLoss || Number(holding.stop_loss),
        take_profit1: tp1 || Number(holding.take_profit1),
        take_profit2: tp2 || Number(holding.take_profit2),
        note: reasonValue(holding.note || 'dashboard_control_update'),
        reason: reasonValue('dashboard_control_update'),
        updated_by: 'dashboard',
      };
      try {
        const result = await postJson(
          `/portfolio/holdings/${encodeURIComponent(holding.ticker)}/controls`,
          payload,
          'PATCH'
        );
        setActionStatus(result.message_cn || `${holding.ticker} 风控参数已更新`);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    async function sellHolding(index, sellAll) {
      const holding = currentHoldings[index];
      if (!holding) return;
      const price = numberValue('sellPrice');
      if (!price || price <= 0) {
        setActionStatus('卖出价必须大于 0', true);
        return;
      }
      const qty = sellAll ? null : numberValue('sellQty');
      const payload = {
        sell_price: price,
        reason: reasonValue(sellAll ? 'dashboard_exit_all' : 'dashboard_sell'),
        ...executionModePayload(),
      };
      if (qty) payload.qty = qty;
      try {
        const result = await postJson(`/portfolio/holdings/${encodeURIComponent(holding.ticker)}/sell`, payload);
        const adapterText = result.adapter_message ? `；${result.adapter_message}` : '';
        setActionStatus((result.message_cn || `已卖出 ${holding.ticker}`) + adapterText);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    async function executeAlert(index) {
      const alert = currentAlerts[index];
      if (!alert) return;
      const qty = numberValue('sellQty');
      const sellPrice = numberValue('sellPrice') || Number(alert.current_price);
      const payload = {
        reason_code: alert.reason_code,
        sell_price: sellPrice,
        note: reasonValue(`alert:${alert.reason_code}`),
        ...executionModePayload(),
      };
      if (qty) payload.qty = qty;
      try {
        const result = await postJson(`/portfolio/alerts/${encodeURIComponent(alert.ticker)}/execute`, payload);
        const adapterText = result.execution?.adapter_message ? `；${result.execution.adapter_message}` : '';
        setActionStatus((result.execution?.message_cn || `${alert.ticker} 已按提醒执行卖出`) + adapterText);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    function renderAlerts(items) {
      currentAlerts = items || [];
      const body = document.getElementById("alertBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="7" class="small">当前无卖出提醒。</td></tr>';
        return;
      }
      body.innerHTML = items.map((a, idx) => `
        <tr>
          <td>${badge(a.level)}</td>
          <td>${esc(a.ticker)}</td>
          <td class="mono">${esc(a.reason_code)}</td>
          <td>${esc(a.message_cn)}</td>
          <td>${esc(a.suggested_action_cn)}</td>
          <td class="small">${fmtTime(a.generated_at)}</td>
          <td><button class="btn-mini danger" onclick="executeAlert(${idx})">执行建议</button></td>
        </tr>
      `).join("");
    }

    function renderAlertHistory(items) {
      const body = document.getElementById("alertHistoryBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="8" class="small">暂无卖出提醒历史。</td></tr>';
        return;
      }
      body.innerHTML = items.map((item) => `
        <tr>
          <td class="small">${fmtTime(item.generated_at)}</td>
          <td class="mono" title="${esc(item.monitor_run_id || '')}">${esc(shortId(item.monitor_run_id, 12))}</td>
          <td>${badge(item.level)}</td>
          <td>${esc(item.ticker)}</td>
          <td class="mono">${esc(item.reason_code)}</td>
          <td>${fmtNum(item.current_price, 2)}</td>
          <td>${esc(item.message_cn)}</td>
          <td>${esc(item.suggested_action_cn)}</td>
        </tr>
      `).join("");
    }

    function renderPaperOrders(items) {
      const body = document.getElementById("orderBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="11" class="small">暂无纸单记录。</td></tr>';
        return;
      }
      body.innerHTML = items.map((order) => `
        <tr>
          <td class="small">${fmtTime(order.submitted_at)}</td>
          <td class="mono" title="${esc(order.id)}">${esc(shortId(order.id, 12))}</td>
          <td class="mono" title="${esc(order.recommendation_id)}">${esc(shortId(order.recommendation_id, 12))}</td>
          <td>${esc(order.execution_mode || 'paper')}${order.dry_run ? '<div class="small">dry-run</div>' : ''}</td>
          <td>${esc(order.side)}</td>
          <td>${fmtNum(order.qty, 2)}</td>
          <td>${fmtNullableNum(order.limit_price, 2)}</td>
          <td>${esc(order.status)}</td>
          <td>${fmtNullableNum(order.simulated_fill_price, 2)}</td>
          <td class="small" title="${esc(order.broker_order_id || '')}">${esc(order.adapter_message || '-')}</td>
          <td>${order.status === 'submitted' ? `
            <button class="btn-mini" onclick="fillPaperOrder('${esc(order.id)}', '${esc(order.simulated_fill_price || order.limit_price || '')}')">成交</button>
            <button class="btn-mini danger" onclick="cancelPaperOrder('${esc(order.id)}')">取消</button>
          ` : '<span class="small">-</span>'}</td>
        </tr>
      `).join("");
    }

    async function fillPaperOrder(orderId, defaultPrice) {
      const suggestedPrice = Number(defaultPrice) > 0 ? String(defaultPrice) : '';
      const rawPrice = window.prompt('成交价', suggestedPrice);
      if (rawPrice === null) return;
      const fillPrice = Number(rawPrice);
      if (!Number.isFinite(fillPrice) || fillPrice <= 0) {
        setActionStatus('成交价必须大于 0', true);
        return;
      }
      const applyToLedger = window.confirm('写入持仓和交易流水？dry-run 通常选择取消。');
      try {
        const order = await postJson(`/paper-orders/${encodeURIComponent(orderId)}/fill`, {
          fill_price: fillPrice,
          filled_by: 'dashboard',
          apply_to_ledger: applyToLedger,
        });
        const ledgerText = applyToLedger ? '已写入持仓/流水' : '仅更新订单状态';
        setActionStatus(`已回写成交 ${shortId(order.id, 12)} @ ${fmtNum(order.simulated_fill_price, 2)}；${ledgerText}`);
        await loadData(false);
      } catch (err) {
        setActionStatus(`回写成交失败: ${err.message || err}`, true);
      }
    }

    async function cancelPaperOrder(orderId) {
      const reason = window.prompt('取消原因', 'operator_cancel') || 'operator_cancel';
      try {
        const order = await postJson(`/paper-orders/${encodeURIComponent(orderId)}/cancel`, {
          reason,
          canceled_by: 'dashboard',
        });
        setActionStatus(`已取消订单 ${shortId(order.id, 12)}；${order.cancel_reason || reason}`);
        await loadData(false);
      } catch (err) {
        setActionStatus(`取消订单失败: ${err.message || err}`, true);
      }
    }

    function renderTrades(items) {
      const body = document.getElementById("tradeBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="7" class="small">暂无交易流水。</td></tr>';
        return;
      }
      body.innerHTML = items.map((t) => `
        <tr>
          <td class="small">${fmtTime(t.executed_at)}</td>
          <td>${esc(t.ticker)}</td>
          <td>${esc(t.side)}</td>
          <td>${fmtNum(t.qty, 2)}</td>
          <td>${fmtNum(t.price, 2)}</td>
          <td>${fmtNum(t.realized_pnl_delta, 2)}</td>
          <td>${esc(t.reason || '')}</td>
        </tr>
      `).join("");
    }

    function renderHoldingControlAudits(items) {
      const body = document.getElementById("holdingControlAuditBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="7" class="small">暂无风控调整记录。</td></tr>';
        return;
      }
      body.innerHTML = items.map((item) => `
        <tr>
          <td class="small">${fmtTime(item.updated_at)}</td>
          <td>${esc(item.ticker)}</td>
          <td>${fmtNum(item.old_stop_loss, 2)} -> ${fmtNum(item.new_stop_loss, 2)}</td>
          <td>${fmtNum(item.old_take_profit1, 2)} -> ${fmtNum(item.new_take_profit1, 2)}</td>
          <td>${fmtNum(item.old_take_profit2, 2)} -> ${fmtNum(item.new_take_profit2, 2)}</td>
          <td>${esc(item.updated_by || '-')}</td>
          <td class="small">${esc(item.reason || item.new_note || '')}</td>
        </tr>
      `).join("");
    }

    function renderSellExecutions(items) {
      const body = document.getElementById("sellExecutionBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="10" class="small">暂无卖出执行审计记录。</td></tr>';
        return;
      }
      body.innerHTML = items.map((item) => {
        const pnl = item.applied_to_ledger ? item.realized_pnl_delta : item.estimated_realized_pnl_delta;
        return `
          <tr>
            <td class="small">${fmtTime(item.submitted_at)}</td>
            <td>${esc(item.execution_mode || 'paper')}${item.dry_run ? '<div class="small">dry-run</div>' : ''}</td>
            <td>${esc(item.ticker)}</td>
            <td>${fmtNum(item.qty, 2)}</td>
            <td>${fmtNum(item.sell_price, 2)}</td>
            <td>${esc(item.status)}</td>
            <td>${item.applied_to_ledger ? '<span class="badge b-ok">yes</span>' : '<span class="badge b-warn">no</span>'}</td>
            <td>${fmtNullableNum(pnl, 2)}</td>
            <td class="small">${esc(item.reason || '')}</td>
            <td class="small" title="${esc(item.broker_order_id || '')}">${esc(item.adapter_message || '-')}</td>
          </tr>
        `;
      }).join("");
    }

    function renderPositionReconciliations(items) {
      const body = document.getElementById("positionReconciliationBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="7" class="small">暂无仓位核对记录。</td></tr>';
        return;
      }
      body.innerHTML = items.map((item) => {
        const cls = item.status === 'matched' || item.status === 'empty'
          ? 'b-ok'
          : 'b-danger';
        return `
          <tr>
            <td class="small">${fmtTime(item.checked_at)}</td>
            <td>${esc(item.broker || '-')}</td>
            <td><span class="badge ${cls}">${esc(item.status || '-')}</span></td>
            <td>${item.blocks_auto_execution ? '<span class="badge b-danger">yes</span>' : '<span class="badge b-ok">no</span>'}</td>
            <td>${esc(item.local_position_count ?? 0)} / ${esc(item.broker_position_count ?? 0)}</td>
            <td>${esc(item.mismatch_count ?? 0)}</td>
            <td class="small">${esc(item.note || '')}</td>
          </tr>
        `;
      }).join("");
    }

    function fmtPct(v) {
      const n = Number(v);
      if (!Number.isFinite(n)) return '-';
      return `${(n * 100).toFixed(1)}%`;
    }

    function fmtNullableNum(v, digits) {
      if (v === null || v === undefined) return '-';
      return fmtNum(v, digits);
    }

    function renderPerformance(perf) {
      const body = document.getElementById("performanceBody");
      const rows = perf?.by_ticker || [];
      if (!rows.length) {
        body.innerHTML = '<tr><td colspan="7" class="small">暂无可复盘的已实现交易。</td></tr>';
        return;
      }
      body.innerHTML = rows.map((row) => `
        <tr>
          <td>${esc(row.ticker)}</td>
          <td>${esc(row.sell_trade_count)}</td>
          <td>${fmtNum(row.total_realized_pnl, 2)}</td>
          <td>${fmtPct(row.win_rate)}</td>
          <td>${fmtNum(row.avg_win, 2)}</td>
          <td>${fmtNum(row.avg_loss, 2)}</td>
          <td>${fmtNullableNum(row.profit_factor, 2)}</td>
        </tr>
      `).join("");
    }

    function shortId(value, size = 10) {
      const text = String(value || '');
      return text.length > size ? `${text.slice(0, size)}...` : text || '-';
    }

    function renderAttribution(report) {
      const recBody = document.getElementById("attributionBody");
      const snapshotBody = document.getElementById("snapshotAttributionBody");
      const strategyBody = document.getElementById("strategyAttributionBody");
      const recRows = report?.by_recommendation || [];
      const snapshotRows = report?.by_snapshot || [];
      const strategyRows = report?.by_strategy_config || [];

      if (!recRows.length) {
        recBody.innerHTML = '<tr><td colspan="8" class="small">暂无可归因的推荐卖出结果。</td></tr>';
      } else {
        recBody.innerHTML = recRows.map((row) => `
          <tr>
            <td>${esc(row.ticker)}</td>
            <td class="mono" title="${esc(row.recommendation_id)}">${esc(shortId(row.recommendation_id, 12))}</td>
            <td class="mono" title="${esc(row.source_snapshot_id || '')}">${esc(shortId(row.source_snapshot_id, 12))}</td>
            <td>${esc(row.sell_trade_count)}</td>
            <td>${fmtNum(row.total_realized_pnl, 2)}</td>
            <td>${fmtPct(row.win_rate)}</td>
            <td>${fmtNullableNum(row.profit_factor, 2)}</td>
            <td>conf=${fmtNullableNum(row.confidence, 2)}<br/>comp=${fmtNullableNum(row.composite, 3)}</td>
          </tr>
        `).join("");
      }

      if (!snapshotRows.length) {
        snapshotBody.innerHTML = '<tr><td colspan="8" class="small">暂无 snapshot 级归因。</td></tr>';
      } else {
        snapshotBody.innerHTML = snapshotRows.map((row) => `
          <tr>
            <td class="mono" title="${esc(row.source_snapshot_id)}">${esc(shortId(row.source_snapshot_id, 18))}</td>
            <td>${snapshotScoreCell(row)}</td>
            <td>${esc(row.recommendation_count)}</td>
            <td>${esc(row.sell_trade_count)}</td>
            <td>${fmtNum(row.total_realized_pnl, 2)}</td>
            <td>${fmtNum(row.expectancy_per_sell, 2)}</td>
            <td>${fmtPct(row.win_rate)}</td>
            <td>${fmtNullableNum(row.profit_factor, 2)}</td>
          </tr>
        `).join("");
      }

      if (!strategyRows.length) {
        strategyBody.innerHTML = '<tr><td colspan="8" class="small">暂无 strategy 级归因。</td></tr>';
        return;
      }
      strategyBody.innerHTML = strategyRows.map((row) => `
        <tr>
          <td class="mono" title="${esc(row.strategy_config_id)}">${esc(shortId(row.strategy_config_id, 18))}</td>
          <td>${snapshotScoreCell(row)}</td>
          <td>${esc(row.recommendation_count)}</td>
          <td>${esc(row.sell_trade_count)}</td>
          <td>${fmtNum(row.total_realized_pnl, 2)}</td>
          <td>${fmtNum(row.expectancy_per_sell, 2)}</td>
          <td>${fmtPct(row.win_rate)}</td>
          <td>${fmtNullableNum(row.profit_factor, 2)}</td>
        </tr>
      `).join("");
    }

    function dashboardDataUrl() {
      const u = new URL('/dashboard/realtime-data', window.location.origin);
      u.searchParams.set('refresh_alerts', 'true');
      if (accessPwd) u.searchParams.set('pwd', accessPwd);
      return u.toString();
    }

    async function loadData() {
      const res = await fetch(dashboardDataUrl(), { cache: 'no-store' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      const provider = data.provider || '-';
      document.getElementById('provider').textContent = provider;
      document.getElementById('recCount').textContent = String(data.summary?.recommendation_count ?? 0);
      document.getElementById('snapshotCount').textContent = String(data.summary?.source_snapshot_count ?? 0);
      document.getElementById('strategyConfigCount').textContent = String(data.summary?.strategy_config_count ?? 0);
      document.getElementById('holdingCount').textContent = String(data.summary?.open_holding_count ?? 0);
      document.getElementById('alertCount').textContent = String(data.summary?.sell_alert_count ?? 0);
      document.getElementById('alertHistoryCount').textContent = String(data.summary?.alert_history_count ?? 0);
      document.getElementById('paperOrderCount').textContent = String(data.summary?.paper_order_count ?? 0);
      document.getElementById('holdingControlAuditCount').textContent = String(data.summary?.holding_control_audit_count ?? 0);
      document.getElementById('sellExecutionCount').textContent = String(data.summary?.sell_execution_count ?? 0);
      document.getElementById('positionReconciliationCount').textContent = String(
        data.summary?.position_reconciliation_count ?? 0
      );
      document.getElementById('realizedPnl').textContent = fmtNum(data.portfolio_summary?.total_realized_pnl, 2);
      document.getElementById('openRisk').textContent = fmtNum(data.portfolio_summary?.open_risk_to_stop, 2);
      document.getElementById('tradeCount').textContent = String(data.portfolio_summary?.trade_count ?? 0);
      document.getElementById('winRate').textContent = fmtPct(data.portfolio_performance?.win_rate);
      document.getElementById('profitFactor').textContent = fmtNullableNum(data.portfolio_performance?.profit_factor, 2);
      document.getElementById('attributionCount').textContent = String(
        data.recommendation_attribution?.recommendation_count ?? 0
      );
      document.getElementById('strategyTuningCount').textContent = String(
        data.strategy_tuning?.recommendation_count ?? 0
      );
      document.getElementById('systemRunCount').textContent = String(data.summary?.system_run_count ?? 0);
      document.getElementById('autoApprovalCount').textContent = String(data.summary?.latest_auto_approval_count ?? 0);
      document.getElementById('autoActionCount').textContent = String(data.summary?.latest_auto_action_count ?? 0);
      document.getElementById('killSwitch').innerHTML = data.kill_switch?.enabled
        ? '<span class="badge b-danger">ON</span>'
        : '<span class="badge b-ok">OFF</span>';
      renderAutopilotPolicy(data.autopilot_policy || {}, data.autopilot_preflight || {});
      const updateTime = data.timestamp ? fmtTime(data.timestamp) : '-';
      document.getElementById('meta').textContent =
        `最后刷新: ${updateTime} | snapshot: ${data.source_snapshot_id || 'N/A'} | pending events: ${data.summary?.pending_event_count ?? 0}`;
      document.getElementById('refreshPlan').textContent = refreshLabel();
      document.getElementById('providerNote').textContent = `数据源可靠性: ${providerReliability(provider)}`;

      renderRecommendations(data.recommendations || []);
      renderSourceSnapshots(
        data.source_snapshots || [],
        data.recommendation_attribution?.by_snapshot || []
      );
      renderStrategyConfigs(data.strategy_configs || []);
      renderStrategyTuning(data.strategy_tuning || {});
      renderSystemRuns(data.recent_system_runs || []);
      renderHoldings(data.holdings || []);
      renderPaperOrders(data.recent_paper_orders || []);
      renderTrades(data.recent_trades || []);
      renderHoldingControlAudits(data.recent_holding_control_audits || []);
      renderSellExecutions(data.recent_sell_executions || []);
      renderPositionReconciliations(data.recent_position_reconciliations || []);
      renderPerformance(data.portfolio_performance || {});
      renderAttribution(data.recommendation_attribution || {});
      renderAlerts(data.alerts || []);
      renderAlertHistory(data.recent_alert_history || []);
    }

    function scheduleNext() {
      if (refreshTimer) clearTimeout(refreshTimer);
      refreshTimer = setTimeout(() => refreshNow(false), getRefreshMs());
    }

    async function refreshNow(isManual) {
      try {
        await loadData();
      } catch (err) {
        document.getElementById('meta').textContent = `刷新失败: ${err}`;
      } finally {
        scheduleNext();
      }
      if (isManual) {
        document.getElementById('refreshPlan').textContent = `${refreshLabel()}（手动刷新已执行）`;
      }
    }

    refreshNow(false);
  </script>
</body>
</html>
"""


@router.get("/dashboard/recommendations/{recommendation_id}", response_class=HTMLResponse)
def dashboard_recommendation_detail(
    recommendation_id: str,
    request: Request,
    state: AppState = Depends(get_app_state),
) -> str:
    rec = state.recommendations_by_id.get(recommendation_id) or state.recommendation_repo.get(recommendation_id)
    if rec is None:
        return "<html><body><h3>Recommendation not found</h3></body></html>"

    approval = state.get_latest_approval(recommendation_id)
    pwd = request.query_params.get("pwd")
    back_url = "/dashboard"
    if pwd:
        back_url = f"/dashboard?pwd={_escape(pwd)}"
    html = (
        "<html><head><title>Recommendation Detail</title><meta charset='utf-8' />"
        "<meta name='viewport' content='width=device-width, initial-scale=1' />"
        "<style>"
        ":root{--ink:#12253a;--muted:#4d6278;--line:#d7e1ec;--surface:#fff;--brand:#0f766e;}"
        "*{box-sizing:border-box}"
        "body{margin:0;padding:26px;background:radial-gradient(760px 420px at -10% -25%, rgba(15,118,110,0.16), transparent 75%),"
        "radial-gradient(760px 420px at 110% -20%, rgba(29,78,216,0.14), transparent 72%),"
        "linear-gradient(180deg,#eef4fb 0%,#f7f9fc 56%,#edf2f8 100%);"
        "font-family:'Avenir Next','PingFang SC','Noto Sans SC','Microsoft YaHei',sans-serif;color:var(--ink)}"
        ".card{max-width:960px;margin:0 auto;background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:20px 18px;"
        "box-shadow:0 14px 36px rgba(16,31,48,0.12)}"
        ".back{color:var(--brand);text-decoration:none;font-weight:700}"
        ".back:hover{text-decoration:underline}"
        "h2{margin:10px 0 12px;font-size:28px;letter-spacing:0.2px}"
        ".grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:6px 16px}"
        "p{margin:8px 0;line-height:1.55}"
        ".section{margin-top:12px;padding-top:12px;border-top:1px dashed var(--line)}"
        ".muted{color:var(--muted)}"
        "@media (max-width:760px){body{padding:14px}.grid{grid-template-columns:1fr}h2{font-size:24px}}"
        "</style></head><body><div class='card'>"
        f"<a class='back' href='{back_url}'>← 返回看板</a>"
        f"<h2>{_escape(rec.ticker)} Recommendation</h2>"
        "<div class='grid'>"
        f"<p><strong>ID:</strong> {_escape(rec.id)}</p>"
        f"<p><strong>方向:</strong> {_escape(_as_text(rec.direction))}</p>"
        f"<p><strong>入场区间:</strong> {rec.entry_zone_low:.2f} - {rec.entry_zone_high:.2f}</p>"
        f"<p><strong>止损位:</strong> {rec.stop_loss:.2f}</p>"
        f"<p><strong>止盈位:</strong> {rec.tp1:.2f} / {rec.tp2:.2f}</p>"
        f"<p><strong>置信度:</strong> {rec.confidence:.3f}</p>"
        f"<p><strong>审批状态:</strong> {_escape(approval.decision.value) if approval else 'pending'}</p>"
        "</div>"
        "<div class='section'>"
        f"<p><strong>中文分析:</strong> {_escape(rec.analysis.report_cn)}</p>"
        f"<p><strong>为什么买:</strong> {_escape('；'.join(rec.analysis.why_to_buy_cn))}</p>"
        f"<p><strong>什么时候卖:</strong> {_escape('；'.join(rec.analysis.why_to_sell_cn))}</p>"
        f"<p><strong>操作建议:</strong> {_escape(rec.analysis.action_guidance_cn)}</p>"
        f"<p class='muted'><strong>补充说明:</strong> 详细信号与特征快照可通过 /recommendations/{_escape(rec.id)}/evidence 查询。</p>"
        "</div></div></body></html>"
    )
    return html
