from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
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


class PaperOrder(BaseModel):
    id: str
    recommendation_id: str
    side: Direction
    qty: float
    limit_price: float | None
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


class SellExecutionResult(BaseModel):
    holding: HoldingWatch
    sold_qty: float
    sell_price: float
    realized_pnl_delta: float
    total_realized_pnl: float
    remaining_qty: float
    message_cn: str


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
    total_realized_pnl: float
    win_count: int
    loss_count: int
    win_rate: float
    profit_factor: float | None = None


class RecommendationAttributionReport(BaseModel):
    generated_at: datetime = Field(default_factory=utc_now)
    recommendation_count: int
    attributed_sell_trade_count: int
    unattributed_sell_trade_count: int
    total_realized_pnl: float
    by_recommendation: list[RecommendationAttribution] = Field(default_factory=list)
    by_snapshot: list[SnapshotAttribution] = Field(default_factory=list)


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
