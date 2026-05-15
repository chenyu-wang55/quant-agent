from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import delete, select

from domain.entities.models import (
    ApprovalDecision,
    Direction,
    FeatureSnapshot,
    HoldingStatus,
    HoldingWatch,
    KillSwitchState,
    PaperOrder,
    PatternType,
    PositionState,
    Recommendation,
    RecommendationAnalysis,
    RecommendationApproval,
    RecommendationStatus,
    RiskLevel,
    SignalSnapshot,
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
)
from infra.db.session import SessionLocal


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
                )
            )
            session.commit()

    def get(self, ticker: str) -> HoldingWatch | None:
        with SessionLocal() as session:
            stmt = select(HoldingWatchRecord).where(HoldingWatchRecord.ticker == ticker).limit(1)
            record = session.execute(stmt).scalars().first()
        return self._to_domain(record) if record else None

    def list_open(self) -> list[HoldingWatch]:
        with SessionLocal() as session:
            stmt = select(HoldingWatchRecord).where(HoldingWatchRecord.status == HoldingStatus.OPEN.value)
            records = list(session.execute(stmt).scalars())
        return [self._to_domain(record) for record in records]

    def close(self, ticker: str) -> HoldingWatch | None:
        with SessionLocal() as session:
            stmt = select(HoldingWatchRecord).where(HoldingWatchRecord.ticker == ticker).limit(1)
            record = session.execute(stmt).scalars().first()
            if record is None:
                return None
            record.status = HoldingStatus.CLOSED.value
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
