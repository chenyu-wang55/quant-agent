from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import delete, func, select

from domain.entities.models import (
    ApprovalDecision,
    Direction,
    FeatureSnapshot,
    FundamentalSnapshot,
    HoldingStatus,
    HoldingWatch,
    KillSwitchState,
    MarketBar,
    NewsEvent,
    PaperOrder,
    PaperOrderStatus,
    PatternType,
    PositionState,
    Recommendation,
    RecommendationAnalysis,
    RecommendationApproval,
    RecommendationStatus,
    RiskLevel,
    SecurityMetadata,
    SignalSnapshot,
    SourceSnapshotDetail,
    SourceSnapshotSummary,
    TradeLedgerEntry,
    TradeSide,
)
from infra.db.models import (
    ApprovalDecisionRecord,
    ExecutionControlRecord,
    FeatureSnapshotRecord,
    HoldingWatchRecord,
    PaperOrderRecord,
    PositionStateRecord,
    RecommendationRecord,
    SignalSnapshotRecord,
    SnapshotEventRecord,
    SnapshotFundamentalRecord,
    SnapshotMarketBarRecord,
    SnapshotSecurityRecord,
    SourceSnapshotRecord,
    TradeLedgerRecord,
)
from infra.db.session import SessionLocal


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class RecommendationRepository:
    def upsert_many(self, recommendations: Iterable[Recommendation]) -> None:
        with SessionLocal() as session:
            for rec in recommendations:
                record = RecommendationRecord(
                    id=rec.id,
                    generated_at=rec.generated_at,
                    ticker=rec.ticker,
                    direction=rec.direction.value,
                    entry_zone_low=rec.entry_zone_low,
                    entry_zone_high=rec.entry_zone_high,
                    stop_loss=rec.stop_loss,
                    tp1=rec.tp1,
                    tp2=rec.tp2,
                    holding_period=rec.holding_period,
                    confidence=rec.confidence,
                    risk_grade=rec.risk_grade.value,
                    status=rec.status.value,
                    thesis=rec.thesis,
                    invalid_if=rec.invalid_if,
                    explanation=rec.explanation,
                    analysis_json=rec.analysis.model_dump(),
                    score_vector=rec.score_vector,
                    source_snapshot_id=rec.source_snapshot_id,
                    feature_snapshot_id=rec.feature_snapshot_id,
                    signal_snapshot_id=rec.signal_snapshot_id,
                    pattern_template=rec.pattern_template.value,
                    model_version=rec.model_version,
                    prompt_version=rec.prompt_version,
                )
                session.merge(record)
            session.commit()

    def list_latest(self, limit: int = 100) -> list[Recommendation]:
        with SessionLocal() as session:
            stmt = select(RecommendationRecord).order_by(RecommendationRecord.generated_at.desc()).limit(limit)
            records = list(session.execute(stmt).scalars())
        return [self._to_domain(record) for record in records]

    def get(self, recommendation_id: str) -> Recommendation | None:
        with SessionLocal() as session:
            stmt = select(RecommendationRecord).where(RecommendationRecord.id == recommendation_id).limit(1)
            record = session.execute(stmt).scalars().first()
        if record is None:
            return None
        return self._to_domain(record)

    @staticmethod
    def _to_domain(record: RecommendationRecord) -> Recommendation:
        return Recommendation(
            id=record.id,
            generated_at=record.generated_at,
            ticker=record.ticker,
            direction=Direction(record.direction),
            entry_zone_low=record.entry_zone_low,
            entry_zone_high=record.entry_zone_high,
            stop_loss=record.stop_loss,
            tp1=record.tp1,
            tp2=record.tp2,
            holding_period=record.holding_period,
            confidence=record.confidence,
            risk_grade=RiskLevel(record.risk_grade),
            thesis=list(record.thesis or []),
            invalid_if=list(record.invalid_if or []),
            explanation=record.explanation,
            status=RecommendationStatus(record.status),
            score_vector=dict(record.score_vector or {}),
            source_snapshot_id=record.source_snapshot_id,
            feature_snapshot_id=record.feature_snapshot_id,
            signal_snapshot_id=record.signal_snapshot_id,
            pattern_template=PatternType(record.pattern_template),
            model_version=record.model_version,
            prompt_version=record.prompt_version,
            analysis=RecommendationAnalysis.model_validate(record.analysis_json or {}),
        )


class SignalRepository:
    def upsert_many(self, signals: Iterable[SignalSnapshot]) -> None:
        with SessionLocal() as session:
            for signal in signals:
                record = SignalSnapshotRecord(
                    id=signal.id,
                    ticker=signal.ticker,
                    timestamp=signal.timestamp,
                    trend_score=signal.trend_score,
                    momentum_score=signal.momentum_score,
                    volatility_score=signal.volatility_score,
                    liquidity_score=signal.liquidity_score,
                    relative_strength_score=signal.relative_strength_score,
                    event_score=signal.event_score,
                    regime_label=signal.regime_label,
                )
                session.merge(record)
            session.commit()

    def get_latest_by_ticker(self, ticker: str) -> SignalSnapshot | None:
        with SessionLocal() as session:
            stmt = (
                select(SignalSnapshotRecord)
                .where(SignalSnapshotRecord.ticker == ticker)
                .order_by(SignalSnapshotRecord.timestamp.desc())
                .limit(1)
            )
            record = session.execute(stmt).scalars().first()
        if record is None:
            return None
        return SignalSnapshot(
            id=record.id,
            ticker=record.ticker,
            timestamp=record.timestamp,
            trend_score=record.trend_score,
            momentum_score=record.momentum_score,
            volatility_score=record.volatility_score,
            liquidity_score=record.liquidity_score,
            relative_strength_score=record.relative_strength_score,
            event_score=record.event_score,
            regime_label=record.regime_label,
            fundamental_score=0.0,
            execution_quality_score=0.0,
            technical_score=0.0,
            composite_score=0.0,
            evidence_conflict=False,
        )


class FeatureRepository:
    def upsert_many(self, features: Iterable[FeatureSnapshot]) -> None:
        with SessionLocal() as session:
            for feature in features:
                session.merge(
                    FeatureSnapshotRecord(
                        id=feature.id,
                        ticker=feature.ticker,
                        timestamp=feature.timestamp,
                        atr=feature.atr,
                        ma_20=feature.ma_20,
                        ma_50=feature.ma_50,
                        ma_200=feature.ma_200,
                        volatility_20d=feature.volatility_20d,
                        momentum_20d=feature.momentum_20d,
                        relative_strength_63d=feature.relative_strength_63d,
                        avg_dollar_volume_20d=feature.avg_dollar_volume_20d,
                        breakout_level_20d=feature.breakout_level_20d,
                        support_level_20d=feature.support_level_20d,
                    )
                )
            session.commit()

    def get(self, feature_snapshot_id: str) -> FeatureSnapshot | None:
        with SessionLocal() as session:
            stmt = select(FeatureSnapshotRecord).where(FeatureSnapshotRecord.id == feature_snapshot_id).limit(1)
            record = session.execute(stmt).scalars().first()
        if record is None:
            return None
        return FeatureSnapshot(
            id=record.id,
            ticker=record.ticker,
            timestamp=record.timestamp,
            atr=record.atr,
            ma_20=record.ma_20,
            ma_50=record.ma_50,
            ma_200=record.ma_200,
            volatility_20d=record.volatility_20d,
            momentum_20d=record.momentum_20d,
            relative_strength_63d=record.relative_strength_63d,
            avg_dollar_volume_20d=record.avg_dollar_volume_20d,
            breakout_level_20d=record.breakout_level_20d,
            support_level_20d=record.support_level_20d,
        )


class PaperOrderRepository:
    def add(self, order: PaperOrder) -> None:
        with SessionLocal() as session:
            record = PaperOrderRecord(
                id=order.id,
                recommendation_id=order.recommendation_id,
                side=order.side.value,
                qty=order.qty,
                limit_price=order.limit_price,
                submitted_at=order.submitted_at,
                status=order.status.value,
                simulated_fill_price=order.simulated_fill_price,
                filled_at=order.filled_at,
                cancel_reason=order.cancel_reason,
            )
            session.merge(record)
            session.commit()

    def list_recent(
        self,
        limit: int = 100,
        recommendation_id: str | None = None,
        side: Direction | None = None,
        status: PaperOrderStatus | None = None,
    ) -> list[PaperOrder]:
        with SessionLocal() as session:
            stmt = select(PaperOrderRecord)
            if recommendation_id:
                stmt = stmt.where(PaperOrderRecord.recommendation_id == recommendation_id)
            if side is not None:
                stmt = stmt.where(PaperOrderRecord.side == side.value)
            if status is not None:
                stmt = stmt.where(PaperOrderRecord.status == status.value)
            stmt = stmt.order_by(PaperOrderRecord.submitted_at.desc()).limit(limit)
            records = list(session.execute(stmt).scalars())
        return [
            PaperOrder(
                id=record.id,
                recommendation_id=record.recommendation_id,
                side=Direction(record.side),
                qty=record.qty,
                limit_price=record.limit_price,
                submitted_at=_ensure_utc(record.submitted_at),
                status=PaperOrderStatus(record.status),
                simulated_fill_price=record.simulated_fill_price,
                filled_at=_ensure_utc(record.filled_at) if record.filled_at else None,
                cancel_reason=record.cancel_reason,
            )
            for record in records
        ]


class PositionRepository:
    def replace_all(self, positions: Iterable[PositionState]) -> None:
        with SessionLocal() as session:
            session.execute(delete(PositionStateRecord))
            for position in positions:
                session.add(
                    PositionStateRecord(
                        ticker=position.ticker,
                        open_time=position.open_time,
                        avg_price=position.avg_price,
                        qty=position.qty,
                        realized_pnl=position.realized_pnl,
                        unrealized_pnl=position.unrealized_pnl,
                        stop_state=position.stop_state,
                        target_state=position.target_state,
                        last_mark=position.last_mark,
                    )
                )
            session.commit()

    def list_open(self) -> list[PositionState]:
        with SessionLocal() as session:
            stmt = select(PositionStateRecord)
            records = list(session.execute(stmt).scalars())
        return [
            PositionState(
                ticker=record.ticker,
                open_time=record.open_time,
                avg_price=record.avg_price,
                qty=record.qty,
                realized_pnl=record.realized_pnl,
                unrealized_pnl=record.unrealized_pnl,
                stop_state=record.stop_state,
                target_state=record.target_state,
                last_mark=record.last_mark,
            )
            for record in records
            if record.qty > 0
        ]


class HoldingWatchRepository:
    def upsert(self, holding: HoldingWatch) -> None:
        with SessionLocal() as session:
            session.merge(
                HoldingWatchRecord(
                    ticker=holding.ticker,
                    qty=holding.qty,
                    avg_buy_price=holding.avg_buy_price,
                    bought_at=holding.bought_at,
                    source_recommendation_id=holding.source_recommendation_id,
                    stop_loss=holding.stop_loss,
                    take_profit1=holding.take_profit1,
                    take_profit2=holding.take_profit2,
                    note=holding.note,
                    status=holding.status.value,
                    updated_at=holding.updated_at,
                    realized_pnl=holding.realized_pnl,
                    closed_at=holding.closed_at,
                    last_sell_price=holding.last_sell_price,
                    last_sell_reason=holding.last_sell_reason,
                )
            )
            session.commit()

    def get(self, ticker: str) -> HoldingWatch | None:
        with SessionLocal() as session:
            stmt = select(HoldingWatchRecord).where(HoldingWatchRecord.ticker == ticker).limit(1)
            record = session.execute(stmt).scalars().first()
        return self._to_domain(record) if record else None

    def list_open(self) -> list[HoldingWatch]:
        return self.list_by_status(HoldingStatus.OPEN)

    def list_by_status(self, status: HoldingStatus | None = None, limit: int = 100) -> list[HoldingWatch]:
        with SessionLocal() as session:
            stmt = select(HoldingWatchRecord).order_by(HoldingWatchRecord.updated_at.desc())
            if status is not None:
                stmt = stmt.where(HoldingWatchRecord.status == status.value)
            stmt = stmt.limit(limit)
            records = list(session.execute(stmt).scalars())
        return [self._to_domain(record) for record in records]

    def close(self, ticker: str) -> HoldingWatch | None:
        with SessionLocal() as session:
            stmt = select(HoldingWatchRecord).where(HoldingWatchRecord.ticker == ticker).limit(1)
            record = session.execute(stmt).scalars().first()
            if record is None:
                return None
            record.status = HoldingStatus.CLOSED.value
            record.qty = 0.0
            record.closed_at = datetime.now(timezone.utc)
            session.merge(record)
            session.commit()
            session.refresh(record)
            return self._to_domain(record)

    @staticmethod
    def _to_domain(record: HoldingWatchRecord) -> HoldingWatch:
        return HoldingWatch(
            ticker=record.ticker,
            qty=record.qty,
            avg_buy_price=record.avg_buy_price,
            bought_at=record.bought_at,
            source_recommendation_id=record.source_recommendation_id,
            stop_loss=record.stop_loss,
            take_profit1=record.take_profit1,
            take_profit2=record.take_profit2,
            note=record.note,
            status=HoldingStatus(record.status),
            updated_at=record.updated_at,
            realized_pnl=float(record.realized_pnl or 0.0),
            closed_at=record.closed_at,
            last_sell_price=record.last_sell_price,
            last_sell_reason=record.last_sell_reason,
        )


class TradeLedgerRepository:
    def add(self, entry: TradeLedgerEntry) -> None:
        with SessionLocal() as session:
            session.merge(
                TradeLedgerRecord(
                    trade_id=entry.trade_id,
                    ticker=entry.ticker,
                    side=entry.side.value,
                    qty=entry.qty,
                    price=entry.price,
                    executed_at=entry.executed_at,
                    source_recommendation_id=entry.source_recommendation_id,
                    reason=entry.reason,
                    realized_pnl_delta=entry.realized_pnl_delta,
                    holding_status_after=entry.holding_status_after.value if entry.holding_status_after else None,
                    created_at=entry.created_at,
                )
            )
            session.commit()

    def list_recent(
        self,
        limit: int = 100,
        ticker: str | None = None,
        side: TradeSide | None = None,
    ) -> list[TradeLedgerEntry]:
        with SessionLocal() as session:
            stmt = select(TradeLedgerRecord).order_by(TradeLedgerRecord.executed_at.desc()).limit(limit)
            if ticker is not None:
                stmt = stmt.where(TradeLedgerRecord.ticker == ticker.upper())
            if side is not None:
                stmt = stmt.where(TradeLedgerRecord.side == side.value)
            records = list(session.execute(stmt).scalars())
        return [self._to_domain(record) for record in records]

    @staticmethod
    def _to_domain(record: TradeLedgerRecord) -> TradeLedgerEntry:
        return TradeLedgerEntry(
            trade_id=record.trade_id,
            ticker=record.ticker,
            side=TradeSide(record.side),
            qty=record.qty,
            price=record.price,
            executed_at=record.executed_at,
            source_recommendation_id=record.source_recommendation_id,
            reason=record.reason,
            realized_pnl_delta=float(record.realized_pnl_delta or 0.0),
            holding_status_after=HoldingStatus(record.holding_status_after) if record.holding_status_after else None,
            created_at=record.created_at,
        )


class ApprovalRepository:
    def add(self, item: RecommendationApproval) -> None:
        with SessionLocal() as session:
            session.merge(
                ApprovalDecisionRecord(
                    decision_id=item.decision_id,
                    recommendation_id=item.recommendation_id,
                    decision=item.decision.value,
                    approver=item.approver,
                    notes=item.notes,
                    decided_at=item.decided_at,
                )
            )
            session.commit()

    def latest_for_recommendation(self, recommendation_id: str) -> RecommendationApproval | None:
        with SessionLocal() as session:
            stmt = (
                select(ApprovalDecisionRecord)
                .where(ApprovalDecisionRecord.recommendation_id == recommendation_id)
                .order_by(ApprovalDecisionRecord.decided_at.desc())
                .limit(1)
            )
            record = session.execute(stmt).scalars().first()
            if record is None:
                return None
            return RecommendationApproval(
                decision_id=record.decision_id,
                recommendation_id=record.recommendation_id,
                decision=ApprovalDecision(record.decision),
                approver=record.approver,
                notes=record.notes,
                decided_at=record.decided_at,
            )


class ExecutionControlRepository:
    def get_kill_switch(self) -> KillSwitchState:
        with SessionLocal() as session:
            stmt = select(ExecutionControlRecord).order_by(ExecutionControlRecord.id.desc()).limit(1)
            record = session.execute(stmt).scalars().first()
            if record is None:
                return KillSwitchState(enabled=False, reason=None, updated_by="system")
            return KillSwitchState(
                enabled=bool(record.enabled),
                reason=record.reason,
                updated_at=record.updated_at,
                updated_by=record.updated_by,
            )

    def set_kill_switch(self, enabled: bool, reason: str | None, updated_by: str) -> KillSwitchState:
        state = KillSwitchState(
            enabled=enabled,
            reason=reason,
            updated_at=datetime.now(timezone.utc),
            updated_by=updated_by,
        )
        with SessionLocal() as session:
            session.add(
                ExecutionControlRecord(
                    enabled=1 if enabled else 0,
                    reason=reason,
                    updated_at=state.updated_at,
                    updated_by=updated_by,
                )
            )
            session.commit()
        return state


class SourceSnapshotRepository:
    def snapshot_exists(self, source_snapshot_id: str) -> bool:
        with SessionLocal() as session:
            stmt = (
                select(SourceSnapshotRecord.source_snapshot_id)
                .where(SourceSnapshotRecord.source_snapshot_id == source_snapshot_id)
                .limit(1)
            )
            return session.execute(stmt).first() is not None

    def list_summaries(self, limit: int = 50) -> list[SourceSnapshotSummary]:
        with SessionLocal() as session:
            stmt = (
                select(SourceSnapshotRecord)
                .order_by(SourceSnapshotRecord.created_at.desc())
                .limit(limit)
            )
            records = list(session.execute(stmt).scalars())
            return [self._to_summary(session, record) for record in records]

    def get_summary(self, source_snapshot_id: str) -> SourceSnapshotSummary | None:
        with SessionLocal() as session:
            stmt = (
                select(SourceSnapshotRecord)
                .where(SourceSnapshotRecord.source_snapshot_id == source_snapshot_id)
                .limit(1)
            )
            record = session.execute(stmt).scalars().first()
            if record is None:
                return None
            return self._to_summary(session, record)

    def get_detail(self, source_snapshot_id: str, event_limit: int = 20) -> SourceSnapshotDetail | None:
        summary = self.get_summary(source_snapshot_id)
        if summary is None:
            return None
        return SourceSnapshotDetail(
            **summary.model_dump(),
            securities=self.get_securities(source_snapshot_id),
            recent_events=self.get_events(source_snapshot_id, summary.tickers)[:event_limit],
        )

    def replace_snapshot(
        self,
        source_snapshot_id: str,
        as_of: datetime,
        universe: str,
        provider_name: str,
        securities: Iterable[SecurityMetadata],
        bars_by_ticker: dict[str, list[MarketBar]],
        fundamentals_by_ticker: dict[str, FundamentalSnapshot],
        events: Iterable[NewsEvent],
        earnings_minutes_by_ticker: dict[str, int | None],
    ) -> None:
        securities_list = list(securities)
        events_list = list(events)
        tickers = sorted({security.ticker.upper() for security in securities_list})
        metadata = {
            "earnings_minutes_by_ticker": {
                ticker.upper(): minutes
                for ticker, minutes in earnings_minutes_by_ticker.items()
                if minutes is not None
            }
        }

        with SessionLocal() as session:
            for model in (
                SnapshotEventRecord,
                SnapshotFundamentalRecord,
                SnapshotMarketBarRecord,
                SnapshotSecurityRecord,
            ):
                session.execute(delete(model).where(model.source_snapshot_id == source_snapshot_id))

            session.merge(
                SourceSnapshotRecord(
                    source_snapshot_id=source_snapshot_id,
                    created_at=datetime.now(timezone.utc),
                    as_of=as_of,
                    universe=universe,
                    provider_name=provider_name,
                    tickers=tickers,
                    metadata_json=metadata,
                )
            )

            for security in securities_list:
                session.add(
                    SnapshotSecurityRecord(
                        source_snapshot_id=source_snapshot_id,
                        ticker=security.ticker.upper(),
                        sector=security.sector,
                        market_cap_usd=security.market_cap_usd,
                        avg_dollar_volume=security.avg_dollar_volume,
                        last_price=security.last_price,
                        spread_bps=security.spread_bps,
                    )
                )

            for ticker, bars in bars_by_ticker.items():
                for bar in bars:
                    session.add(
                        SnapshotMarketBarRecord(
                            source_snapshot_id=source_snapshot_id,
                            ticker=ticker.upper(),
                            timestamp=bar.timestamp,
                            open=bar.open,
                            high=bar.high,
                            low=bar.low,
                            close=bar.close,
                            volume=bar.volume,
                            vendor_id=provider_name,
                        )
                    )

            for ticker, fundamentals in fundamentals_by_ticker.items():
                session.add(
                    SnapshotFundamentalRecord(
                        source_snapshot_id=source_snapshot_id,
                        ticker=ticker.upper(),
                        timestamp=fundamentals.timestamp,
                        pe_ttm=fundamentals.pe_ttm,
                        roe=fundamentals.roe,
                        revenue_growth_yoy=fundamentals.revenue_growth_yoy,
                        eps_revision_30d=fundamentals.eps_revision_30d,
                    )
                )

            for event in events_list:
                session.add(
                    SnapshotEventRecord(
                        source_snapshot_id=source_snapshot_id,
                        vendor_source_id=event.source_id,
                        published_at=event.published_at,
                        ingested_at=event.ingested_at,
                        headline=event.headline,
                        normalized_text=event.normalized_text,
                        tickers=[ticker.upper() for ticker in event.tickers],
                        event_type=event.event_type,
                        sentiment=event.sentiment,
                        relevance=event.relevance,
                        horizon=event.horizon,
                        source_url=event.source_url,
                    )
                )

            session.commit()

    def _to_summary(self, session, record: SourceSnapshotRecord) -> SourceSnapshotSummary:
        source_snapshot_id = record.source_snapshot_id
        bar_count = session.scalar(
            select(func.count())
            .select_from(SnapshotMarketBarRecord)
            .where(SnapshotMarketBarRecord.source_snapshot_id == source_snapshot_id)
        )
        fundamental_count = session.scalar(
            select(func.count())
            .select_from(SnapshotFundamentalRecord)
            .where(SnapshotFundamentalRecord.source_snapshot_id == source_snapshot_id)
        )
        event_count = session.scalar(
            select(func.count())
            .select_from(SnapshotEventRecord)
            .where(SnapshotEventRecord.source_snapshot_id == source_snapshot_id)
        )
        recommendation_count = session.scalar(
            select(func.count())
            .select_from(RecommendationRecord)
            .where(RecommendationRecord.source_snapshot_id == source_snapshot_id)
        )
        tickers = [str(ticker).upper() for ticker in record.tickers or []]
        return SourceSnapshotSummary(
            source_snapshot_id=source_snapshot_id,
            created_at=_ensure_utc(record.created_at),
            as_of=_ensure_utc(record.as_of),
            universe=record.universe,
            provider_name=record.provider_name,
            tickers=tickers,
            ticker_count=len(tickers),
            bar_count=int(bar_count or 0),
            fundamental_count=int(fundamental_count or 0),
            event_count=int(event_count or 0),
            recommendation_count=int(recommendation_count or 0),
        )

    def get_metadata(self, source_snapshot_id: str) -> dict:
        with SessionLocal() as session:
            stmt = select(SourceSnapshotRecord).where(
                SourceSnapshotRecord.source_snapshot_id == source_snapshot_id
            )
            record = session.execute(stmt).scalars().first()
        if record is None:
            return {}
        return dict(record.metadata_json or {})

    def get_securities(self, source_snapshot_id: str) -> list[SecurityMetadata]:
        with SessionLocal() as session:
            stmt = (
                select(SnapshotSecurityRecord)
                .where(SnapshotSecurityRecord.source_snapshot_id == source_snapshot_id)
                .order_by(SnapshotSecurityRecord.id)
            )
            records = list(session.execute(stmt).scalars())
        return [
            SecurityMetadata(
                ticker=record.ticker,
                sector=record.sector,
                market_cap_usd=record.market_cap_usd,
                avg_dollar_volume=record.avg_dollar_volume,
                last_price=record.last_price,
                spread_bps=record.spread_bps,
            )
            for record in records
        ]

    def get_bars(self, source_snapshot_id: str, ticker: str, limit: int) -> list[MarketBar]:
        ticker_upper = ticker.upper()
        with SessionLocal() as session:
            stmt = (
                select(SnapshotMarketBarRecord)
                .where(
                    SnapshotMarketBarRecord.source_snapshot_id == source_snapshot_id,
                    SnapshotMarketBarRecord.ticker == ticker_upper,
                )
                .order_by(SnapshotMarketBarRecord.timestamp.desc())
                .limit(limit)
            )
            records = list(session.execute(stmt).scalars())
        records.reverse()
        return [
            MarketBar(
                ticker=record.ticker,
                timestamp=_ensure_utc(record.timestamp),
                open=record.open,
                high=record.high,
                low=record.low,
                close=record.close,
                volume=record.volume,
            )
            for record in records
        ]

    def get_fundamental(self, source_snapshot_id: str, ticker: str) -> FundamentalSnapshot | None:
        with SessionLocal() as session:
            stmt = (
                select(SnapshotFundamentalRecord)
                .where(
                    SnapshotFundamentalRecord.source_snapshot_id == source_snapshot_id,
                    SnapshotFundamentalRecord.ticker == ticker.upper(),
                )
                .order_by(SnapshotFundamentalRecord.timestamp.desc())
                .limit(1)
            )
            record = session.execute(stmt).scalars().first()
        if record is None:
            return None
        return FundamentalSnapshot(
            ticker=record.ticker,
            timestamp=_ensure_utc(record.timestamp),
            pe_ttm=record.pe_ttm,
            roe=record.roe,
            revenue_growth_yoy=record.revenue_growth_yoy,
            eps_revision_30d=record.eps_revision_30d,
        )

    def get_events(self, source_snapshot_id: str, tickers: list[str]) -> list[NewsEvent]:
        ticker_set = {ticker.upper() for ticker in tickers}
        with SessionLocal() as session:
            stmt = (
                select(SnapshotEventRecord)
                .where(SnapshotEventRecord.source_snapshot_id == source_snapshot_id)
                .order_by(SnapshotEventRecord.published_at.desc())
            )
            records = list(session.execute(stmt).scalars())

        events: list[NewsEvent] = []
        for record in records:
            record_tickers = [str(ticker).upper() for ticker in record.tickers or []]
            if ticker_set and not ticker_set.intersection(record_tickers):
                continue
            events.append(
                NewsEvent(
                    source_id=record.vendor_source_id,
                    published_at=_ensure_utc(record.published_at),
                    ingested_at=_ensure_utc(record.ingested_at),
                    headline=record.headline,
                    normalized_text=record.normalized_text,
                    tickers=record_tickers,
                    event_type=record.event_type,
                    sentiment=record.sentiment,
                    relevance=record.relevance,
                    horizon=record.horizon,
                    source_url=record.source_url,
                )
            )
        return events
