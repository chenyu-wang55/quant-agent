from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RunType(str, Enum):
    RESEARCH_BATCH = "research_batch"
    RECOMMENDATION_REVIEW = "recommendation_review"
    PAPER_TRADE_PREP = "paper_trade_prep"
    BACKTEST_EVALUATION = "backtest_evaluation"


class SnapshotMode(str, Enum):
    POINT_IN_TIME = "point_in_time"
    LATEST = "latest"


class ExecutionMode(str, Enum):
    RESEARCH_ONLY = "research_only"
    PAPER_TRADING = "paper_trading"
    LIVE_REVIEW_ONLY = "live_review_only"


class Direction(str, Enum):
    BUY = "BUY"
    SHORT = "SHORT"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class PatternType(str, Enum):
    BREAKOUT = "breakout"
    PULLBACK = "pullback"
    MEAN_REVERSION = "mean_reversion"
    SHORT_SETUP = "short_setup"


class RecommendationStatus(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class PaperOrderStatus(str, Enum):
    SUBMITTED = "submitted"
    FILLED = "filled"
    CANCELED = "canceled"


class OrderExecutionMode(str, Enum):
    PAPER = "paper"
    LIVE = "live"


class AutoExecutionMode(str, Enum):
    PAPER = "paper"
    LIVE_DRY_RUN = "live_dry_run"


class ApprovalDecision(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"


class UniverseRules(BaseModel):
    min_price: float = 10.0
    min_avg_dollar_volume: float = 20_000_000.0
    max_spread_bps: float = 40.0
    min_market_cap_usd: float = 5_000_000_000.0
    allowed_sectors: list[str] = Field(default_factory=list)
    max_candidates_after_filter: int = 150


class SignalConfig(BaseModel):
    technical_weight: float = 0.30
    event_news_weight: float = 0.25
    relative_strength_weight: float = 0.20
    fundamental_weight: float = 0.15
    execution_quality_weight: float = 0.10
    required_subsignals: list[str] = Field(
        default_factory=lambda: [
            "trend_breakout_strength",
            "pullback_quality",
            "volatility_compression",
            "earnings_revision_trend",
            "sector_relative_momentum",
            "benchmark_relative_performance",
            "event_freshness",
        ]
    )

    @model_validator(mode="after")
    def validate_weight_sum(self) -> "SignalConfig":
        total = (
            self.technical_weight
            + self.event_news_weight
            + self.relative_strength_weight
            + self.fundamental_weight
            + self.execution_quality_weight
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"Signal weights must sum to 1.0, got {total}")
        return self


class PricePlanConfig(BaseModel):
    strategy_pattern: PatternType = PatternType.BREAKOUT
    atr_window: int = 14
    breakout_entry_atr_buffer: float = 0.3
    stop_atr_range: list[float] = Field(default_factory=lambda: [1.2, 1.8])
    first_target_r_multiple: float = 2.0
    holding_period: str = "3-10 trading days"


class RiskPolicy(BaseModel):
    min_confidence: float = 0.72
    earnings_blackout_minutes: int = 60
    max_name_weight: float = 0.10
    max_sector_weight: float = 0.30
    max_gross_exposure: float = 1.0
    max_correlated_cluster_weight: float = 0.35
    max_entry_gap_pct: float = 0.30
    reject_on_material_evidence_conflict: bool = True
    event_trading_enabled: bool = False


class PublicationConfig(BaseModel):
    top_n: int = 8
    output_channels: list[str] = Field(default_factory=lambda: ["api", "daily_report"])


class ResearchRunRequest(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    run_type: RunType
    objective: str
    as_of: datetime = Field(default_factory=utc_now)
    snapshot_mode: SnapshotMode = SnapshotMode.POINT_IN_TIME
    source_snapshot_id: str | None = None
    universe: str = "SP500"
    universe_rules: UniverseRules = Field(default_factory=UniverseRules)
    signal_config: SignalConfig = Field(default_factory=SignalConfig)
    price_plan_config: PricePlanConfig = Field(default_factory=PricePlanConfig)
    risk_policy: RiskPolicy = Field(default_factory=RiskPolicy)
    publication: PublicationConfig = Field(default_factory=PublicationConfig)
    execution_mode: ExecutionMode = ExecutionMode.RESEARCH_ONLY


class StrategyConfigSnapshot(BaseModel):
    strategy_config_id: str
    created_at: datetime = Field(default_factory=utc_now)
    config_hash: str
    run_type: RunType
    snapshot_mode: SnapshotMode
    universe: str
    universe_rules: dict[str, Any]
    signal_config: dict[str, Any]
    price_plan_config: dict[str, Any]
    risk_policy: dict[str, Any]
    publication: dict[str, Any]
    execution_mode: ExecutionMode


def build_strategy_config_snapshot(request: ResearchRunRequest) -> StrategyConfigSnapshot:
    payload = {
        "run_type": request.run_type.value if isinstance(request.run_type, Enum) else str(request.run_type),
        "snapshot_mode": (
            request.snapshot_mode.value
            if isinstance(request.snapshot_mode, Enum)
            else str(request.snapshot_mode)
        ),
        "universe": request.universe,
        "universe_rules": request.universe_rules.model_dump(mode="json"),
        "signal_config": request.signal_config.model_dump(mode="json"),
        "price_plan_config": request.price_plan_config.model_dump(mode="json"),
        "risk_policy": request.risk_policy.model_dump(mode="json"),
        "publication": request.publication.model_dump(mode="json"),
        "execution_mode": (
            request.execution_mode.value
            if isinstance(request.execution_mode, Enum)
            else str(request.execution_mode)
        ),
    }
    config_blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    config_hash = hashlib.sha256(config_blob).hexdigest()
    return StrategyConfigSnapshot(
        strategy_config_id=f"strat_{config_hash[:16]}",
        config_hash=config_hash,
        run_type=RunType(payload["run_type"]),
        snapshot_mode=SnapshotMode(payload["snapshot_mode"]),
        universe=payload["universe"],
        universe_rules=payload["universe_rules"],
        signal_config=payload["signal_config"],
        price_plan_config=payload["price_plan_config"],
        risk_policy=payload["risk_policy"],
        publication=payload["publication"],
        execution_mode=ExecutionMode(payload["execution_mode"]),
    )


class SourceSnapshotReplayRequest(BaseModel):
    run_type: RunType = RunType.RESEARCH_BATCH
    objective: str = "Replay source snapshot"
    universe_rules: UniverseRules = Field(default_factory=UniverseRules)
    signal_config: SignalConfig = Field(default_factory=SignalConfig)
    price_plan_config: PricePlanConfig = Field(default_factory=PricePlanConfig)
    risk_policy: RiskPolicy = Field(default_factory=RiskPolicy)
    publication: PublicationConfig = Field(default_factory=PublicationConfig)
    execution_mode: ExecutionMode = ExecutionMode.RESEARCH_ONLY


class SourceSnapshotReplayCompareRequest(SourceSnapshotReplayRequest):
    baseline_strategy_config_id: str | None = None
    include_unchanged: bool = True


class SourceSnapshotReplayDiff(BaseModel):
    ticker: str
    status: str
    baseline_recommendation_id: str | None = None
    replay_recommendation_id: str | None = None
    changed_fields: list[str] = Field(default_factory=list)
    baseline_values: dict[str, Any] = Field(default_factory=dict)
    replay_values: dict[str, Any] = Field(default_factory=dict)


class SourceSnapshotReplayComparison(BaseModel):
    source_snapshot_id: str
    compared_at: datetime
    baseline_strategy_config_id: str | None = None
    replay_strategy_config_id: str | None = None
    replay_operation: str
    baseline_count: int
    replay_count: int
    matched_count: int
    changed_count: int
    missing_in_replay_count: int
    new_in_replay_count: int
    deterministic: bool
    diffs: list[SourceSnapshotReplayDiff] = Field(default_factory=list)


class SecurityMetadata(BaseModel):
    ticker: str
    sector: str
    market_cap_usd: float
    avg_dollar_volume: float
    last_price: float
    spread_bps: float


class MarketBar(BaseModel):
    ticker: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class FundamentalSnapshot(BaseModel):
    ticker: str
    timestamp: datetime
    pe_ttm: float
    roe: float
    revenue_growth_yoy: float
    eps_revision_30d: float


class NewsEvent(BaseModel):
    source_id: str
    published_at: datetime
    ingested_at: datetime
    headline: str
    normalized_text: str
    tickers: list[str]
    event_type: str
    sentiment: float
    relevance: float
    horizon: str
    source_url: str


class SourceSnapshotSummary(BaseModel):
    source_snapshot_id: str
    created_at: datetime
    as_of: datetime
    universe: str
    provider_name: str
    tickers: list[str]
    ticker_count: int
    bar_count: int
    fundamental_count: int
    event_count: int
    recommendation_count: int
    data_quality: dict[str, Any] = Field(default_factory=dict)


class SourceSnapshotDetail(SourceSnapshotSummary):
    securities: list[SecurityMetadata] = Field(default_factory=list)
    recent_events: list[NewsEvent] = Field(default_factory=list)


class FeatureSnapshot(BaseModel):
    id: str
    ticker: str
    timestamp: datetime
    atr: float
    ma_20: float
    ma_50: float
    ma_200: float
    volatility_20d: float
    momentum_20d: float
    relative_strength_63d: float
    avg_dollar_volume_20d: float
    breakout_level_20d: float
    support_level_20d: float


class SignalSnapshot(BaseModel):
    id: str
    ticker: str
    timestamp: datetime
    trend_score: float
    momentum_score: float
    volatility_score: float
    liquidity_score: float
    relative_strength_score: float
    event_score: float
    fundamental_score: float
    execution_quality_score: float
    technical_score: float
    composite_score: float
    regime_label: str
    evidence_conflict: bool = False


class TradePlan(BaseModel):
    ticker: str
    pattern: PatternType
    direction: Direction
    entry_zone_low: float
    entry_zone_high: float
    stop_loss: float
    tp1: float
    tp2: float
    holding_period: str
    risk_reward: float
    position_size_pct: float = 0.0


class RecommendationAnalysis(BaseModel):
    summary: str = ""
    report_title: str = ""
    report_cn: str = ""
    why_to_buy_cn: list[str] = Field(default_factory=list)
    why_to_sell_cn: list[str] = Field(default_factory=list)
    action_guidance_cn: str = ""
    technical_view: list[str] = Field(default_factory=list)
    event_view: list[str] = Field(default_factory=list)
    fundamental_view: list[str] = Field(default_factory=list)
    execution_view: list[str] = Field(default_factory=list)
    risk_notes: list[str] = Field(default_factory=list)


class Recommendation(BaseModel):
    id: str
    generated_at: datetime
    ticker: str
    direction: Direction
    entry_zone_low: float
    entry_zone_high: float
    stop_loss: float
    tp1: float
    tp2: float
    holding_period: str
    confidence: float
    risk_grade: RiskLevel
    thesis: list[str]
    invalid_if: list[str]
    explanation: str
    status: RecommendationStatus = RecommendationStatus.APPROVED
    score_vector: dict[str, float]
    source_snapshot_id: str
    strategy_config_id: str | None = None
    feature_snapshot_id: str
    signal_snapshot_id: str
    model_version: str = "v1"
    prompt_version: str = "v1"
    pattern_template: PatternType
    analysis: RecommendationAnalysis = Field(default_factory=RecommendationAnalysis)

    @property
    def entry_zone(self) -> list[float]:
        return [self.entry_zone_low, self.entry_zone_high]

    @property
    def take_profit_zone(self) -> list[float]:
        return [self.tp1, self.tp2]


class RejectedRecommendation(BaseModel):
    ticker: str
    rejection_reason_codes: list[str]
    failed_checks: list[str]


class RunMetrics(BaseModel):
    recommendation_count: int
    rejection_rate: float
    missing_data_rate: float
    explanation_latency_ms: float


class ResearchRunResult(BaseModel):
    run_type: RunType
    generated_at: datetime
    source_snapshot_id: str
    strategy_config_id: str | None = None
    universe_summary: dict[str, Any]
    signal_model: dict[str, Any]
    recommendations: list[Recommendation]
    rejected_recommendations: list[RejectedRecommendation]
    run_metrics: RunMetrics
    publication_payload: dict[str, Any]


class PaperOrderRequest(BaseModel):
    recommendation_id: str
    side: Direction = Direction.BUY
    qty: float = Field(gt=0)
    limit_price: float | None = Field(default=None, gt=0)
    execution_mode: OrderExecutionMode = OrderExecutionMode.PAPER
    dry_run: bool = False
    confirm_live: bool = False
    account_equity: float = Field(default=100_000.0, gt=0)
    risk_per_trade_pct: float = Field(default=0.01, gt=0, le=1.0)
    max_position_pct: float = Field(default=0.10, gt=0, le=1.0)
    max_gross_exposure_pct: float = Field(default=1.0, gt=0, le=5.0)
    max_sector_exposure_pct: float = Field(default=0.30, gt=0, le=5.0)
    enforce_risk_limits: bool = True


class PaperOrderRiskPlan(BaseModel):
    recommendation_id: str
    ticker: str
    side: Direction
    entry_price: float
    stop_loss: float
    risk_per_share: float
    account_equity: float
    risk_budget: float
    max_position_value: float
    current_position_value: float
    remaining_position_value: float
    max_gross_exposure_value: float
    current_gross_exposure_value: float
    remaining_gross_exposure_value: float
    sector: str
    max_sector_exposure_value: float
    current_sector_exposure_value: float
    remaining_sector_exposure_value: float
    max_risk_qty: float
    max_position_qty: float
    max_gross_qty: float
    max_sector_qty: float
    recommended_qty: float
    requested_qty: float | None = None
    requested_notional: float | None = None
    requested_risk_amount: float | None = None
    requested_position_pct: float | None = None
    requested_gross_exposure_pct: float | None = None
    requested_sector_exposure_pct: float | None = None
    requested_risk_pct: float | None = None
    is_within_limits: bool
    violations: list[str] = Field(default_factory=list)
    message_cn: str


class PaperOrder(BaseModel):
    id: str
    recommendation_id: str
    side: Direction
    qty: float
    limit_price: float | None
    execution_mode: OrderExecutionMode = OrderExecutionMode.PAPER
    dry_run: bool = False
    broker_order_id: str | None = None
    adapter_message: str | None = None
    submitted_at: datetime
    status: PaperOrderStatus
    simulated_fill_price: float | None = None
    filled_at: datetime | None = None
    cancel_reason: str | None = None


class PositionState(BaseModel):
    ticker: str
    open_time: datetime
    avg_price: float
    qty: float
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    stop_state: str = "active"
    target_state: str = "active"
    last_mark: float = 0.0


class BacktestRunRequest(BaseModel):
    run_name: str = "baseline_backtest"
    start_date: datetime
    end_date: datetime
    benchmark: str = "SPY"
    top_n: int = 10
    rebalance_frequency: str = "monthly"
    transaction_cost_bps: float = 10.0


class BacktestRunResult(BaseModel):
    run_id: str
    created_at: datetime
    config_hash: str
    metrics: dict[str, float]
    notes: str


class RecommendationApproval(BaseModel):
    decision_id: str
    recommendation_id: str
    decision: ApprovalDecision
    approver: str
    notes: str | None = None
    decided_at: datetime


class KillSwitchState(BaseModel):
    enabled: bool
    reason: str | None = None
    updated_at: datetime = Field(default_factory=utc_now)
    updated_by: str = "system"


class AutopilotPolicy(BaseModel):
    policy_id: int | None = None
    enabled: bool = False
    auto_approve_recommendations: bool = False
    auto_execute_approved: bool = False
    restrict_auto_execution_to_regular_hours: bool = False
    auto_execution_mode: AutoExecutionMode = AutoExecutionMode.PAPER
    auto_approve_min_confidence: float = Field(default=0.72, ge=0.0, le=1.0)
    auto_approve_min_composite: float = Field(default=0.0, ge=0.0)
    max_auto_approvals: int = Field(default=1, ge=0)
    max_auto_buys: int = Field(default=1, ge=0)
    max_auto_sells: int = Field(default=10, ge=0)
    max_daily_auto_approvals: int = Field(default=3, ge=0)
    max_daily_auto_buys: int = Field(default=3, ge=0)
    max_daily_auto_sells: int = Field(default=10, ge=0)
    order_dedupe_minutes: int = Field(default=1440, ge=0)
    rebuy_cooldown_minutes: int = Field(default=240, ge=0)
    min_snapshot_bar_coverage: float = Field(default=1.0, ge=0.0, le=1.0)
    min_snapshot_fundamental_coverage: float = Field(default=1.0, ge=0.0, le=1.0)
    max_snapshot_bar_age_minutes: int = Field(default=4320, ge=0)
    max_open_risk_pct: float = Field(default=0.06, ge=0.0, le=1.0)
    max_daily_realized_loss_pct: float = Field(default=0.03, ge=0.0, le=1.0)
    account_equity: float = Field(default=100_000.0, gt=0)
    risk_per_trade_pct: float = Field(default=0.01, gt=0, le=1.0)
    max_position_pct: float = Field(default=0.10, gt=0, le=1.0)
    max_gross_exposure_pct: float = Field(default=1.0, gt=0, le=5.0)
    max_sector_exposure_pct: float = Field(default=0.30, gt=0, le=5.0)
    reason: str | None = None
    updated_at: datetime = Field(default_factory=utc_now)
    updated_by: str = "system"


class MarketSessionStatus(BaseModel):
    as_of: datetime
    generated_at: datetime = Field(default_factory=utc_now)
    timezone: str = "America/New_York"
    local_time: str
    regular_open_time: str = "09:30"
    regular_close_time: str = "16:00"
    is_weekday: bool
    is_regular_session: bool
    status: str


class AutopilotPreflightCheck(BaseModel):
    name: str
    status: str
    message_cn: str
    details: dict[str, Any] = Field(default_factory=dict)


class AutopilotPreflight(BaseModel):
    status: str = "off"
    can_auto_approve: bool = False
    can_auto_execute: bool = False
    reasons: list[str] = Field(default_factory=list)
    daily_usage: dict[str, Any] = Field(default_factory=dict)
    checks: list[AutopilotPreflightCheck] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=utc_now)


class HoldingStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


class TradeSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class ManualBuyRequest(BaseModel):
    ticker: str
    qty: float
    buy_price: float
    bought_at: datetime | None = None
    source_recommendation_id: str | None = None
    note: str | None = None
    stop_loss: float | None = None
    take_profit1: float | None = None
    take_profit2: float | None = None


class ManualSellRequest(BaseModel):
    qty: float | None = Field(default=None, gt=0)
    sell_price: float = Field(gt=0)
    sold_at: datetime | None = None
    reason: str | None = None
    execution_mode: OrderExecutionMode = OrderExecutionMode.PAPER
    dry_run: bool = False
    confirm_live: bool = False


class HoldingControlUpdateRequest(BaseModel):
    stop_loss: float | None = Field(default=None, gt=0)
    take_profit1: float | None = Field(default=None, gt=0)
    take_profit2: float | None = Field(default=None, gt=0)
    note: str | None = None
    reason: str | None = None
    updated_by: str = "ops"


class HoldingWatch(BaseModel):
    ticker: str
    qty: float
    avg_buy_price: float
    bought_at: datetime
    source_recommendation_id: str | None = None
    stop_loss: float
    take_profit1: float
    take_profit2: float
    note: str | None = None
    status: HoldingStatus = HoldingStatus.OPEN
    updated_at: datetime = Field(default_factory=utc_now)
    realized_pnl: float = 0.0
    closed_at: datetime | None = None
    last_sell_price: float | None = None
    last_sell_reason: str | None = None


class HoldingControlAudit(BaseModel):
    id: str
    ticker: str
    source_recommendation_id: str | None = None
    old_stop_loss: float
    new_stop_loss: float
    old_take_profit1: float
    new_take_profit1: float
    old_take_profit2: float
    new_take_profit2: float
    old_note: str | None = None
    new_note: str | None = None
    reason: str | None = None
    updated_by: str
    updated_at: datetime


class HoldingControlUpdateResult(BaseModel):
    holding: HoldingWatch
    audit: HoldingControlAudit
    message_cn: str


class SellExecutionResult(BaseModel):
    sell_execution_id: str | None = None
    holding: HoldingWatch
    sold_qty: float
    sell_price: float
    realized_pnl_delta: float
    estimated_realized_pnl_delta: float | None = None
    total_realized_pnl: float
    remaining_qty: float
    execution_mode: OrderExecutionMode = OrderExecutionMode.PAPER
    dry_run: bool = False
    broker_order_id: str | None = None
    adapter_message: str | None = None
    applied_to_ledger: bool = True
    message_cn: str


class SellExecutionAudit(BaseModel):
    id: str
    ticker: str
    qty: float
    sell_price: float
    submitted_at: datetime
    execution_mode: OrderExecutionMode = OrderExecutionMode.PAPER
    dry_run: bool = False
    broker_order_id: str | None = None
    adapter_message: str | None = None
    applied_to_ledger: bool = True
    status: str = "recorded"
    reason: str | None = None
    source_recommendation_id: str | None = None
    realized_pnl_delta: float = 0.0
    estimated_realized_pnl_delta: float | None = None
    remaining_qty: float
    holding_status_after: HoldingStatus | None = None


class AlertSellRequest(BaseModel):
    reason_code: str | None = None
    qty: float | None = Field(default=None, gt=0)
    sell_price: float | None = Field(default=None, gt=0)
    sell_all: bool | None = None
    note: str | None = None
    execution_mode: OrderExecutionMode = OrderExecutionMode.PAPER
    dry_run: bool = False
    confirm_live: bool = False


class TradeLedgerEntry(BaseModel):
    trade_id: str
    ticker: str
    side: TradeSide
    qty: float
    price: float
    executed_at: datetime
    source_recommendation_id: str | None = None
    reason: str | None = None
    realized_pnl_delta: float = 0.0
    holding_status_after: HoldingStatus | None = None
    created_at: datetime = Field(default_factory=utc_now)


class PortfolioSummary(BaseModel):
    open_holding_count: int
    closed_holding_count: int
    trade_count: int
    buy_trade_count: int
    sell_trade_count: int
    open_cost_basis: float
    open_market_value: float
    open_unrealized_pnl: float
    open_risk_to_stop: float
    total_realized_pnl: float
    last_trade_at: datetime | None = None
    last_closed_at: datetime | None = None


class TickerPerformance(BaseModel):
    ticker: str
    trade_count: int
    sell_trade_count: int
    total_realized_pnl: float
    win_count: int
    loss_count: int
    flat_count: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float | None = None
    best_trade_pnl: float
    worst_trade_pnl: float


class PortfolioPerformance(BaseModel):
    generated_at: datetime = Field(default_factory=utc_now)
    trade_count: int
    sell_trade_count: int
    closed_trade_count: int
    total_realized_pnl: float
    win_count: int
    loss_count: int
    flat_count: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float | None = None
    expectancy_per_sell: float
    best_trade_pnl: float
    worst_trade_pnl: float
    by_ticker: list[TickerPerformance] = Field(default_factory=list)


class RecommendationAttribution(BaseModel):
    recommendation_id: str
    ticker: str
    source_snapshot_id: str | None = None
    strategy_config_id: str | None = None
    generated_at: datetime | None = None
    confidence: float | None = None
    composite: float | None = None
    sell_trade_count: int
    closed_trade_count: int
    total_realized_pnl: float
    win_count: int
    loss_count: int
    flat_count: int
    win_rate: float
    profit_factor: float | None = None
    expectancy_per_sell: float
    first_sell_at: datetime | None = None
    last_sell_at: datetime | None = None


class SnapshotAttribution(BaseModel):
    source_snapshot_id: str
    recommendation_count: int
    sell_trade_count: int
    closed_trade_count: int
    total_realized_pnl: float
    win_count: int
    loss_count: int
    flat_count: int
    win_rate: float
    profit_factor: float | None = None
    expectancy_per_sell: float
    avg_confidence: float | None = None
    avg_composite: float | None = None
    performance_score: float
    quality_grade: str
    first_sell_at: datetime | None = None
    last_sell_at: datetime | None = None


class StrategyConfigAttribution(BaseModel):
    strategy_config_id: str
    recommendation_count: int
    sell_trade_count: int
    closed_trade_count: int
    total_realized_pnl: float
    win_count: int
    loss_count: int
    flat_count: int
    win_rate: float
    profit_factor: float | None = None
    expectancy_per_sell: float
    avg_confidence: float | None = None
    avg_composite: float | None = None
    performance_score: float
    quality_grade: str
    first_sell_at: datetime | None = None
    last_sell_at: datetime | None = None


class StrategyTuningAction(str, Enum):
    COLLECT_MORE_DATA = "collect_more_data"
    KEEP = "keep"
    TIGHTEN = "tighten"
    RELAX = "relax"
    REVIEW = "review"


class StrategyTuningRecommendation(BaseModel):
    strategy_config_id: str
    action: StrategyTuningAction
    priority: int = Field(ge=0, le=100)
    rationale_cn: str
    metric_snapshot: dict[str, Any] = Field(default_factory=dict)
    current_parameters: dict[str, Any] = Field(default_factory=dict)
    recommended_changes: dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime = Field(default_factory=utc_now)


class StrategyTuningReport(BaseModel):
    generated_at: datetime = Field(default_factory=utc_now)
    recommendation_count: int
    items: list[StrategyTuningRecommendation] = Field(default_factory=list)


class RecommendationAttributionReport(BaseModel):
    generated_at: datetime = Field(default_factory=utc_now)
    recommendation_count: int
    attributed_sell_trade_count: int
    unattributed_sell_trade_count: int
    total_realized_pnl: float
    by_recommendation: list[RecommendationAttribution] = Field(default_factory=list)
    by_snapshot: list[SnapshotAttribution] = Field(default_factory=list)
    by_strategy_config: list[StrategyConfigAttribution] = Field(default_factory=list)


class SellAlertLevel(str, Enum):
    INFO = "info"
    WARN = "warn"
    URGENT = "urgent"


class SellAlert(BaseModel):
    ticker: str
    level: SellAlertLevel
    reason_code: str
    current_price: float
    stop_loss: float
    take_profit1: float
    take_profit2: float
    source_recommendation_id: str | None = None
    message_cn: str
    suggested_action_cn: str
    generated_at: datetime = Field(default_factory=utc_now)


class SellAlertAudit(BaseModel):
    id: str
    ticker: str
    level: SellAlertLevel
    reason_code: str
    current_price: float
    stop_loss: float
    take_profit1: float
    take_profit2: float
    source_recommendation_id: str | None = None
    message_cn: str
    suggested_action_cn: str
    generated_at: datetime
    monitor_run_id: str | None = None


class AlertExecutionResult(BaseModel):
    alert: SellAlert
    execution: SellExecutionResult
    default_action_cn: str


class OperationAction(BaseModel):
    action_type: str
    priority: str
    message_cn: str
    endpoint: str | None = None
    method: str | None = None
    ticker: str | None = None
    recommendation_id: str | None = None
    source_snapshot_id: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class OperationRecommendationCandidate(BaseModel):
    recommendation_id: str
    ticker: str
    approval_status: str
    confidence: float
    composite_score: float
    entry_zone_low: float
    entry_zone_high: float
    stop_loss: float
    tp1: float
    tp2: float
    source_snapshot_id: str
    strategy_config_id: str | None = None


class OperationControlCenter(BaseModel):
    generated_at: datetime = Field(default_factory=utc_now)
    kill_switch: KillSwitchState
    autopilot_policy: AutopilotPolicy = Field(default_factory=AutopilotPolicy)
    autopilot_preflight: AutopilotPreflight = Field(default_factory=AutopilotPreflight)
    latest_source_snapshot_id: str | None = None
    latest_strategy_config_id: str | None = None
    latest_recommendation_count: int = 0
    pending_approval_count: int = 0
    approved_ready_to_buy_count: int = 0
    open_holding_count: int = 0
    sell_alert_count: int = 0
    urgent_sell_alert_count: int = 0
    pending_event_count: int = 0
    recent_order_count: int = 0
    recent_sell_execution_count: int = 0
    pending_approvals: list[OperationRecommendationCandidate] = Field(default_factory=list)
    ready_to_buy: list[OperationRecommendationCandidate] = Field(default_factory=list)
    sell_alerts: list[SellAlert] = Field(default_factory=list)
    actions: list[OperationAction] = Field(default_factory=list)


class SystemCycleRun(BaseModel):
    id: str
    job: str = "system_cycle"
    started_at: datetime
    finished_at: datetime
    status: str = "success"
    source_snapshot_id: str | None = None
    strategy_config_id: str | None = None
    recommendation_count: int = 0
    sell_alert_count: int = 0
    consumed_event_count: int = 0
    pending_event_count: int = 0
    auto_execution_enabled: bool = False
    top_recommendations: list[dict[str, Any]] = Field(default_factory=list)
    sell_alerts: list[dict[str, Any]] = Field(default_factory=list)
    consumed_event_type_counts: dict[str, int] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None
