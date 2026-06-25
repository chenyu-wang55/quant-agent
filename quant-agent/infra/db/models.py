from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RecommendationRecord(Base):
    __tablename__ = "recommendations"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    entry_zone_low: Mapped[float] = mapped_column(Float, nullable=False)
    entry_zone_high: Mapped[float] = mapped_column(Float, nullable=False)
    stop_loss: Mapped[float] = mapped_column(Float, nullable=False)
    tp1: Mapped[float] = mapped_column(Float, nullable=False)
    tp2: Mapped[float] = mapped_column(Float, nullable=False)
    holding_period: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    risk_grade: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    thesis: Mapped[list] = mapped_column(JSON, nullable=False)
    invalid_if: Mapped[list] = mapped_column(JSON, nullable=False)
    explanation: Mapped[str] = mapped_column(Text, nullable=False)
    analysis_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    score_vector: Mapped[dict] = mapped_column(JSON, nullable=False)
    source_snapshot_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    strategy_config_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    feature_snapshot_id: Mapped[str] = mapped_column(String(64), nullable=False)
    signal_snapshot_id: Mapped[str] = mapped_column(String(64), nullable=False)
    pattern_template: Mapped[str] = mapped_column(String(32), nullable=False)
    model_version: Mapped[str] = mapped_column(String(32), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(32), nullable=False)


class StrategyConfigRecord(Base):
    __tablename__ = "strategy_configs"

    strategy_config_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    run_type: Mapped[str] = mapped_column(String(64), nullable=False)
    snapshot_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    universe: Mapped[str] = mapped_column(String(64), nullable=False)
    universe_rules_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    signal_config_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    price_plan_config_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    risk_policy_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    publication_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    execution_mode: Mapped[str] = mapped_column(String(32), nullable=False)


class SignalSnapshotRecord(Base):
    __tablename__ = "signal_snapshots"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    trend_score: Mapped[float] = mapped_column(Float, nullable=False)
    momentum_score: Mapped[float] = mapped_column(Float, nullable=False)
    volatility_score: Mapped[float] = mapped_column(Float, nullable=False)
    liquidity_score: Mapped[float] = mapped_column(Float, nullable=False)
    relative_strength_score: Mapped[float] = mapped_column(Float, nullable=False)
    event_score: Mapped[float] = mapped_column(Float, nullable=False)
    regime_label: Mapped[str] = mapped_column(String(32), nullable=False)


class FeatureSnapshotRecord(Base):
    __tablename__ = "feature_snapshots"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    atr: Mapped[float] = mapped_column(Float, nullable=False)
    ma_20: Mapped[float] = mapped_column(Float, nullable=False)
    ma_50: Mapped[float] = mapped_column(Float, nullable=False)
    ma_200: Mapped[float] = mapped_column(Float, nullable=False)
    volatility_20d: Mapped[float] = mapped_column(Float, nullable=False)
    momentum_20d: Mapped[float] = mapped_column(Float, nullable=False)
    relative_strength_63d: Mapped[float] = mapped_column(Float, nullable=False)
    avg_dollar_volume_20d: Mapped[float] = mapped_column(Float, nullable=False)
    breakout_level_20d: Mapped[float] = mapped_column(Float, nullable=False)
    support_level_20d: Mapped[float] = mapped_column(Float, nullable=False)


class EventRecord(Base):
    __tablename__ = "event_records"

    source_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    headline: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_text: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    sentiment: Mapped[float] = mapped_column(Float, nullable=False)
    relevance: Mapped[float] = mapped_column(Float, nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)


class SystemEventRecord(Base):
    __tablename__ = "system_event_records"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)


class PaperOrderRecord(Base):
    __tablename__ = "paper_orders"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    recommendation_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    execution_mode: Mapped[str] = mapped_column(String(16), nullable=False, default="paper")
    dry_run: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    broker_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    adapter_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    simulated_fill_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class PositionStateRecord(Base):
    __tablename__ = "positions"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    open_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    avg_price: Mapped[float] = mapped_column(Float, nullable=False)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    unrealized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    stop_state: Mapped[str] = mapped_column(String(16), nullable=False)
    target_state: Mapped[str] = mapped_column(String(16), nullable=False)
    last_mark: Mapped[float] = mapped_column(Float, nullable=False)


class HoldingWatchRecord(Base):
    __tablename__ = "holding_watches"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    avg_buy_price: Mapped[float] = mapped_column(Float, nullable=False)
    bought_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_recommendation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    stop_loss: Mapped[float] = mapped_column(Float, nullable=False)
    take_profit1: Mapped[float] = mapped_column(Float, nullable=False)
    take_profit2: Mapped[float] = mapped_column(Float, nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sell_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_sell_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class HoldingControlAuditRecord(Base):
    __tablename__ = "holding_control_audits"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    source_recommendation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    old_stop_loss: Mapped[float] = mapped_column(Float, nullable=False)
    new_stop_loss: Mapped[float] = mapped_column(Float, nullable=False)
    old_take_profit1: Mapped[float] = mapped_column(Float, nullable=False)
    new_take_profit1: Mapped[float] = mapped_column(Float, nullable=False)
    old_take_profit2: Mapped[float] = mapped_column(Float, nullable=False)
    new_take_profit2: Mapped[float] = mapped_column(Float, nullable=False)
    old_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str] = mapped_column(String(128), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)


class TradeLedgerRecord(Base):
    __tablename__ = "trade_ledger"

    trade_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False, index=True)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    source_recommendation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    realized_pnl_delta: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    holding_status_after: Mapped[str | None] = mapped_column(String(16), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SellExecutionAuditRecord(Base):
    __tablename__ = "sell_execution_audits"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    sell_price: Mapped[float] = mapped_column(Float, nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    execution_mode: Mapped[str] = mapped_column(String(16), nullable=False, default="paper")
    dry_run: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    broker_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    adapter_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    applied_to_ledger: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="recorded")
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_recommendation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    realized_pnl_delta: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    estimated_realized_pnl_delta: Mapped[float | None] = mapped_column(Float, nullable=True)
    remaining_qty: Mapped[float] = mapped_column(Float, nullable=False)
    holding_status_after: Mapped[str | None] = mapped_column(String(16), nullable=True)


class SellAlertAuditRecord(Base):
    __tablename__ = "sell_alert_audits"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    current_price: Mapped[float] = mapped_column(Float, nullable=False)
    stop_loss: Mapped[float] = mapped_column(Float, nullable=False)
    take_profit1: Mapped[float] = mapped_column(Float, nullable=False)
    take_profit2: Mapped[float] = mapped_column(Float, nullable=False)
    source_recommendation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    message_cn: Mapped[str] = mapped_column(Text, nullable=False)
    suggested_action_cn: Mapped[str] = mapped_column(Text, nullable=False)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    monitor_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)


class SystemCycleRunRecord(Base):
    __tablename__ = "system_cycle_runs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    job: Mapped[str] = mapped_column(String(64), nullable=False, default="system_cycle")
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="success")
    source_snapshot_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    strategy_config_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    recommendation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sell_alert_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    consumed_event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pending_event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    auto_execution_enabled: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    top_recommendations_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    sell_alerts_json: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    consumed_event_type_counts_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    metrics_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class ApprovalDecisionRecord(Base):
    __tablename__ = "approval_decisions"

    decision_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    recommendation_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    approver: Mapped[str] = mapped_column(String(128), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ExecutionControlRecord(Base):
    __tablename__ = "execution_controls"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    enabled: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_by: Mapped[str] = mapped_column(String(128), nullable=False)


class MarketBarRecord(Base):
    __tablename__ = "market_bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    vendor_id: Mapped[str] = mapped_column(String(64), nullable=False, default="mock-provider")


class SourceSnapshotRecord(Base):
    __tablename__ = "source_snapshots"

    source_snapshot_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    universe: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_name: Mapped[str] = mapped_column(String(128), nullable=False)
    tickers: Mapped[list] = mapped_column(JSON, nullable=False)
    metadata_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class SnapshotSecurityRecord(Base):
    __tablename__ = "snapshot_securities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_snapshot_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    sector: Mapped[str] = mapped_column(String(64), nullable=False)
    market_cap_usd: Mapped[float] = mapped_column(Float, nullable=False)
    avg_dollar_volume: Mapped[float] = mapped_column(Float, nullable=False)
    last_price: Mapped[float] = mapped_column(Float, nullable=False)
    spread_bps: Mapped[float] = mapped_column(Float, nullable=False)


class SnapshotMarketBarRecord(Base):
    __tablename__ = "snapshot_market_bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_snapshot_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    vendor_id: Mapped[str] = mapped_column(String(64), nullable=False)


class SnapshotFundamentalRecord(Base):
    __tablename__ = "snapshot_fundamentals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_snapshot_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    pe_ttm: Mapped[float] = mapped_column(Float, nullable=False)
    roe: Mapped[float] = mapped_column(Float, nullable=False)
    revenue_growth_yoy: Mapped[float] = mapped_column(Float, nullable=False)
    eps_revision_30d: Mapped[float] = mapped_column(Float, nullable=False)


class SnapshotEventRecord(Base):
    __tablename__ = "snapshot_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_snapshot_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    vendor_source_id: Mapped[str] = mapped_column(String(128), nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    headline: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_text: Mapped[str] = mapped_column(Text, nullable=False)
    tickers: Mapped[list] = mapped_column(JSON, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    sentiment: Mapped[float] = mapped_column(Float, nullable=False)
    relevance: Mapped[float] = mapped_column(Float, nullable=False)
    horizon: Mapped[str] = mapped_column(String(32), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
