# Quant Recommendation Agent (PRD v2 Aligned)

This directory contains a PRD-aligned quant decision-support agent for US equities:
- `agents/us_equity_quant_agent.yaml`
- `skills/us_equity_market_research_skill.yaml`
- `skills/us_equity_quant_strategy_skill.yaml`

Production-oriented implementation (API + services + tests) lives in:
- `quant-agent/`

Source specification: `quant_agent_prd_tech_spec_v2.pdf`

## What This Agent Produces

- Ranked stock ideas from blended market/event/signal evidence
- Deterministic trade plans (`entry_zone`, `stop_loss`, `take_profit_zone`)
- Invalidation conditions and confidence scores
- Rejection reason codes from risk/policy guardrails
- Audit-ready recommendation objects tied to `source_snapshot_id`

## MVP Guardrails

- US equities scope
- Decision support only (not unmanaged auto-trading)
- No live order routing without explicit human approval
- Deterministic price levels required
- Point-in-time reproducibility required

## Composite Score (Default)

`0.30 Technical + 0.25 Event/News + 0.20 Relative Strength + 0.15 Fundamental + 0.10 Execution Quality`

## Example Input

```yaml
run_type: "research_batch"
objective: "Generate a daily shortlist of high-quality swing-trade ideas"
as_of: "2026-04-10T09:30:00Z"
snapshot_mode: "point_in_time"
universe: "SP500"
universe_rules:
  min_price: 10
  min_avg_dollar_volume: 20000000
  max_spread_bps: 40
  min_market_cap_usd: 5000000000
signal_config:
  technical_weight: 0.30
  event_news_weight: 0.25
  relative_strength_weight: 0.20
  fundamental_weight: 0.15
  execution_quality_weight: 0.10
price_plan_config:
  strategy_pattern: "breakout"
  atr_window: 14
  breakout_entry_atr_buffer: 0.3
  stop_atr_range: [1.2, 1.8]
  first_target_r_multiple: 2.0
  holding_period: "3-10 trading days"
risk_policy:
  min_confidence: 0.65
  earnings_blackout_minutes: 60
  max_name_weight: 0.10
  max_sector_weight: 0.30
  max_gross_exposure: 1.0
  max_correlated_cluster_weight: 0.35
  reject_on_material_evidence_conflict: true
  event_trading_enabled: false
publication:
  top_n: 10
  output_channels: ["api", "daily_report"]
execution_mode: "research_only"
```

## Core Recommendation Object Fields

- `ticker`
- `direction`
- `entry_zone`
- `stop_loss`
- `take_profit_zone`
- `holding_period`
- `confidence`
- `risk_level`
- `thesis`
- `invalid_if`
- `source_snapshot_id`
- `analysis.summary`
- `analysis.report_title`
- `analysis.report_cn`
- `analysis.why_to_buy_cn`
- `analysis.why_to_sell_cn`
- `analysis.action_guidance_cn`
- `analysis.technical_view`
- `analysis.event_view`
- `analysis.fundamental_view`
- `analysis.execution_view`
- `analysis.risk_notes`

## Publish Targets

- `/universe`
- `/research/run`
- `/recommendations`
- `/recommendations/{id}`
- `/recommendations/{id}/evidence`
- `/source-snapshots/{source_snapshot_id}/export`
- `/paper-orders`
- `/paper-orders/broker-sync`
- `/paper-orders/{order_id}/fill`
- `/paper-orders/{order_id}/cancel`
- `/portfolio/sell-executions/broker-sync`
- `/metrics`

新增持仓监控与卖出提醒能力：
- `POST /portfolio/buys` 记录你实际买入的股票
- `GET /portfolio/holdings` 查看当前监控持仓
- `GET /portfolio/alerts` 获取“是否需要卖出”的中文提醒
- `POST /portfolio/holdings/{ticker}/close` 关闭已卖出持仓
