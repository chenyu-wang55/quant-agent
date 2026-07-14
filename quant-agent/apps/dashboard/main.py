from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse

from apps.api.dependencies import AppState, get_app_state
from apps.dashboard.home_page import HOME_PAGE
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
    paper_shadow_readiness = state.build_paper_shadow_readiness(as_of=now)
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
        "paper_shadow_readiness": paper_shadow_readiness.model_dump(mode="json"),
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
    return HOME_PAGE


@router.get("/dashboard/recommendations/{recommendation_id}", response_class=HTMLResponse)
def dashboard_recommendation_detail(
    recommendation_id: str,
    state: AppState = Depends(get_app_state),
) -> str:
    rec = state.recommendations_by_id.get(recommendation_id) or state.recommendation_repo.get(recommendation_id)
    if rec is None:
        return "<html><body><h3>Recommendation not found</h3></body></html>"

    approval = state.get_latest_approval(recommendation_id)
    back_url = "/dashboard"
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
