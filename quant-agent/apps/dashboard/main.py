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
        chosen_by_ticker[ticker] = replacement or rec

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
        "summary": {
            "recommendation_count": len(recommendations),
            "open_holding_count": len(holdings),
            "sell_alert_count": len(alerts),
            "pending_event_count": state.event_queue.size(),
        },
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
        "recent_events": [event.model_dump() for event in state.event_queue.pending(limit=5)],
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

    .field label {
      display: block;
      color: var(--ink-soft);
      font-size: 11px;
      font-weight: 700;
      margin-bottom: 5px;
      text-transform: uppercase;
      letter-spacing: 0.25px;
    }

    .field input {
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
    }

    @media (max-width: 980px) {
      .hero { flex-direction: column; align-items: flex-start; }
      .hero .right { text-align: left; align-items: flex-start; width: 100%; }
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .control-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
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
      <div class="stat"><div class="k">持仓监控</div><div class="v" id="holdingCount">0</div></div>
      <div class="stat"><div class="k">卖出提醒</div><div class="v" id="alertCount">0</div></div>
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
        <div class="field"><label for="sellQty">Sell Qty</label><input id="sellQty" type="number" min="0.0001" step="1" placeholder="all" /></div>
        <div class="field"><label for="sellPrice">Sell Price</label><input id="sellPrice" type="number" min="0.0001" step="0.01" /></div>
      </div>
      <div style="margin-top:9px; display:flex; gap:8px; flex-wrap:wrap;">
        <button class="btn-mini" onclick="runResearch()">运行推荐</button>
        <button class="btn-mini" onclick="refreshNow(true)">刷新状态</button>
        <input id="tradeReason" style="flex:1; min-width:220px; border:1px solid var(--line-strong); border-radius:8px; padding:7px 9px; font:inherit; font-size:13px;" placeholder="reason" />
      </div>
      <div class="status-line" id="operationLog"></div>
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
        <span class="small">手工买入会同步进入止损/止盈监控</span>
      </div>
      <table>
        <thead><tr><th>股票</th><th>数量</th><th>成本</th><th>止损</th><th>止盈1</th><th>止盈2</th><th>已实现盈亏</th><th>最近卖出</th><th>操作</th></tr></thead>
        <tbody id="holdingBody"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-head">
        <h3>卖出提醒</h3>
        <span class="small">触发条件: 止损失效 / 目标触达 / 风险偏好切换</span>
      </div>
      <table>
        <thead><tr><th>级别</th><th>股票</th><th>原因</th><th>提醒内容</th><th>建议动作</th><th>时间</th></tr></thead>
        <tbody id="alertBody"></tbody>
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

    function reasonValue(fallback) {
      const value = document.getElementById('tradeReason')?.value?.trim();
      return value || fallback;
    }

    function setActionStatus(message, isError = false) {
      const status = document.getElementById('actionStatus');
      const log = document.getElementById('operationLog');
      status.textContent = isError ? '操作失败' : '操作完成';
      status.style.color = isError ? 'var(--danger)' : 'var(--ok)';
      log.textContent = message;
      log.style.color = isError ? 'var(--danger)' : 'var(--ink-soft)';
    }

    function apiUrl(path) {
      const u = new URL(path, window.location.origin);
      if (accessPwd) u.searchParams.set('pwd', accessPwd);
      return u.toString();
    }

    async function postJson(path, payload) {
      const res = await fetch(apiUrl(path), {
        method: 'POST',
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
        throw new Error(Array.isArray(detail) ? detail.map((x) => x.msg || String(x)).join('; ') : String(detail));
      }
      return data;
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
          <td><strong>${esc(r.ticker)}</strong><div class="mono">${esc(r.id)}</div></td>
          <td><strong>${fmtNum(r.current_price, 2)}</strong></td>
          <td>${directionBadge(r.direction)}</td>
          <td>composite=${fmtNum(r.composite, 4)}<br/>confidence=${fmtNum(r.confidence, 3)}</td>
          <td>入场 ${fmtNum(r.entry_zone_low, 2)}-${fmtNum(r.entry_zone_high, 2)}<br/>止损 ${fmtNum(r.stop_loss, 2)}<br/>目标 ${fmtNum(r.tp1, 2)} / ${fmtNum(r.tp2, 2)}</td>
          <td><ul class="why">${(r.why_to_buy_cn || []).map((x) => `<li>${esc(x)}</li>`).join("")}</ul></td>
          <td><ul class="why">${(r.why_to_sell_cn || []).map((x) => `<li>${esc(x)}</li>`).join("")}</ul></td>
          <td>
            <div class="small">审批: ${esc(r.approval_status || 'pending')}</div>
            <button class="btn-mini" onclick="approveRecommendation(${idx})">审批</button>
            <button class="btn-mini" onclick="buyRecommendation(${idx})">买入</button>
            <a class="link" href="/dashboard/recommendations/${encodeURIComponent(r.id)}${pwdSuffix}" target="_blank">查看</a>
          </td>
        </tr>
      `).join("");
    }

    function renderHoldings(items) {
      currentHoldings = items || [];
      const body = document.getElementById("holdingBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="9" class="small">暂无监控持仓。你可通过 POST /portfolio/buys 记录买入。</td></tr>';
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

    async function buyRecommendation(index) {
      const rec = currentRecommendations[index];
      if (!rec) return;
      const qty = numberValue('buyQty');
      if (!qty || qty <= 0) {
        setActionStatus('买入数量必须大于 0', true);
        return;
      }
      const buyPrice = numberValue('buyPrice') || Number(rec.entry_zone_high);
      try {
        await postJson('/portfolio/buys', {
          ticker: rec.ticker,
          qty,
          buy_price: buyPrice,
          source_recommendation_id: rec.id,
          note: reasonValue('dashboard buy'),
          stop_loss: rec.stop_loss,
          take_profit1: rec.tp1,
          take_profit2: rec.tp2,
        });
        setActionStatus(`已记录买入 ${rec.ticker} x ${qty} @ ${fmtNum(buyPrice, 2)}`);
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
      };
      if (qty) payload.qty = qty;
      try {
        const result = await postJson(`/portfolio/holdings/${encodeURIComponent(holding.ticker)}/sell`, payload);
        setActionStatus(result.message_cn || `已卖出 ${holding.ticker}`);
        await refreshNow(false);
      } catch (err) {
        setActionStatus(String(err), true);
      }
    }

    function renderAlerts(items) {
      const body = document.getElementById("alertBody");
      if (!items || items.length === 0) {
        body.innerHTML = '<tr><td colspan="6" class="small">当前无卖出提醒。</td></tr>';
        return;
      }
      body.innerHTML = items.map((a) => `
        <tr>
          <td>${badge(a.level)}</td>
          <td>${esc(a.ticker)}</td>
          <td class="mono">${esc(a.reason_code)}</td>
          <td>${esc(a.message_cn)}</td>
          <td>${esc(a.suggested_action_cn)}</td>
          <td class="small">${fmtTime(a.generated_at)}</td>
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
      document.getElementById('holdingCount').textContent = String(data.summary?.open_holding_count ?? 0);
      document.getElementById('alertCount').textContent = String(data.summary?.sell_alert_count ?? 0);
      document.getElementById('killSwitch').innerHTML = data.kill_switch?.enabled
        ? '<span class="badge b-danger">ON</span>'
        : '<span class="badge b-ok">OFF</span>';
      const updateTime = data.timestamp ? fmtTime(data.timestamp) : '-';
      document.getElementById('meta').textContent =
        `最后刷新: ${updateTime} | snapshot: ${data.source_snapshot_id || 'N/A'} | pending events: ${data.summary?.pending_event_count ?? 0}`;
      document.getElementById('refreshPlan').textContent = refreshLabel();
      document.getElementById('providerNote').textContent = `数据源可靠性: ${providerReliability(provider)}`;

      renderRecommendations(data.recommendations || []);
      renderHoldings(data.holdings || []);
      renderAlerts(data.alerts || []);
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
