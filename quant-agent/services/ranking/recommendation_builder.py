from __future__ import annotations

import hashlib
from datetime import timezone

from domain.entities.models import (
    Direction,
    Recommendation,
    RecommendationAnalysis,
    RecommendationStatus,
    RiskLevel,
    SecurityMetadata,
    SignalSnapshot,
    TradePlan,
)
from services.llm.explainer import ExplanationService


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class RecommendationBuilder:
    def __init__(self, explanation_service: ExplanationService | None = None) -> None:
        self.explanation_service = explanation_service or ExplanationService()

    def build(
        self,
        security: SecurityMetadata,
        signal: SignalSnapshot,
        trade_plan: TradePlan,
        source_snapshot_id: str,
        feature_snapshot_id: str,
        strategy_config_id: str | None = None,
    ) -> Recommendation:
        generated_at = signal.timestamp.astimezone(timezone.utc)

        confidence = _clamp(signal.composite_score, 0.0, 1.0)
        risk_grade = self._risk_grade(signal, trade_plan)

        score_vector = {
            "technical": signal.technical_score,
            "event_news": signal.event_score,
            "relative_strength": signal.relative_strength_score,
            "fundamental": signal.fundamental_score,
            "execution_quality": signal.execution_quality_score,
            "composite": signal.composite_score,
        }

        thesis = self._build_thesis(signal)
        invalid_if = [
            f"Close below stop-loss {trade_plan.stop_loss:.2f}"
            if trade_plan.direction == Direction.BUY
            else f"Close above stop-loss {trade_plan.stop_loss:.2f}",
            f"Regime changes to risk-off from {signal.regime_label}",
            "Material evidence conflict appears in event and price streams",
        ]

        rec_id = hashlib.sha1(
            (
                f"{security.ticker}|{source_snapshot_id}|{signal.id}|{feature_snapshot_id}|"
                f"{trade_plan.direction.value}|{trade_plan.entry_zone_low}|"
                f"{trade_plan.entry_zone_high}|{trade_plan.stop_loss}|"
                f"{trade_plan.tp1}|{trade_plan.tp2}|{round(confidence, 4)}"
            ).encode("utf-8")
        ).hexdigest()[:16]

        recommendation = Recommendation(
            id=rec_id,
            generated_at=generated_at,
            ticker=security.ticker,
            direction=trade_plan.direction,
            entry_zone_low=trade_plan.entry_zone_low,
            entry_zone_high=trade_plan.entry_zone_high,
            stop_loss=trade_plan.stop_loss,
            tp1=trade_plan.tp1,
            tp2=trade_plan.tp2,
            holding_period=trade_plan.holding_period,
            confidence=round(confidence, 6),
            risk_grade=risk_grade,
            thesis=thesis,
            invalid_if=invalid_if,
            explanation="",
            status=RecommendationStatus.APPROVED,
            score_vector=score_vector,
            source_snapshot_id=source_snapshot_id,
            strategy_config_id=strategy_config_id,
            feature_snapshot_id=feature_snapshot_id,
            signal_snapshot_id=signal.id,
            pattern_template=trade_plan.pattern,
            analysis=self._build_analysis(signal=signal, trade_plan=trade_plan),
        )

        recommendation.explanation = self.explanation_service.build(recommendation)
        return recommendation

    @staticmethod
    def _risk_grade(signal: SignalSnapshot, trade_plan: TradePlan) -> RiskLevel:
        if signal.volatility_score > 0.7 and trade_plan.risk_reward >= 2.0:
            return RiskLevel.LOW
        if signal.volatility_score > 0.4 and trade_plan.risk_reward >= 1.5:
            return RiskLevel.MEDIUM
        return RiskLevel.HIGH

    @staticmethod
    def _build_thesis(signal: SignalSnapshot) -> list[str]:
        components = {
            "technical": signal.technical_score,
            "event/news": signal.event_score,
            "relative strength": signal.relative_strength_score,
            "fundamental": signal.fundamental_score,
            "execution quality": signal.execution_quality_score,
        }
        ranked = sorted(components.items(), key=lambda kv: kv[1], reverse=True)
        return [f"{name} rule score {score:.3f} ({'supportive' if score >= 0.65 else 'weak' if score < 0.45 else 'mixed'})"
                for name, score in ranked[:3]]

    @staticmethod
    def _build_analysis(signal: SignalSnapshot, trade_plan: TradePlan) -> RecommendationAnalysis:
        def describe(label: str, score: float) -> str:
            judgement = "规则证据较强" if score >= 0.65 else "规则证据不足" if score < 0.45 else "规则证据混合"
            return f"{label}评分 {score:.3f}，{judgement}；评分不等于原始事实或预测胜率。"

        technical = [describe("趋势", signal.trend_score), describe("动量", signal.momentum_score),
                     describe("技术综合", signal.technical_score), f"市场环境：{signal.regime_label}。"]
        event_view = [describe("事件", signal.event_score),
                      "本报告未附原始新闻证据，不能据评分断言具体利好、利空或没有新闻。"]
        fundamental_view = [describe("基本面", signal.fundamental_score),
                            "具体盈利修正与财务数值需核对源数据及其可用时间。"]
        execution_view = [describe("流动性", signal.liquidity_score),
                          describe("执行质量", signal.execution_quality_score),
                          f"程序计划入场区间 {trade_plan.entry_zone_low:.2f}-{trade_plan.entry_zone_high:.2f}，"
                          f"止损 {trade_plan.stop_loss:.2f}，目标 {trade_plan.tp1:.2f}/{trade_plan.tp2:.2f}。"]
        risk_notes = ["规则信号强度不是经校准的胜率；执行仍需通过审批、数据与风控门槛。",
                      "新闻、基本面或价格证据缺失时不能推断其对交易形成支持。"]
        if signal.regime_label == "risk_off":
            risk_notes.append("当前处于 risk_off，应暂停新开仓或按既定风控降低敞口。")
        if signal.evidence_conflict:
            risk_notes.append("事件与价格证据存在冲突，需要复核后才能形成交易判断。")
        supporting = [("技术", signal.technical_score), ("相对强弱", signal.relative_strength_score),
                      ("基本面", signal.fundamental_score), ("执行质量", signal.execution_quality_score)]
        why_to_buy_cn = [describe(name, score) for name, score in supporting if score >= 0.65]
        if not why_to_buy_cn:
            why_to_buy_cn = ["尚无充分的规则支持项，不建议仅依据该信号新开仓。"]
        stop_action = "跌破" if trade_plan.direction == Direction.BUY else "升破"
        why_to_sell_cn = [f"价格{stop_action}止损 {trade_plan.stop_loss:.2f} 时按已批准计划处理。",
                          f"目标价 {trade_plan.tp1:.2f}/{trade_plan.tp2:.2f}，执行前确认方向与持仓。"]
        blocked_signal = (signal.composite_score < 0.45 or signal.regime_label == "risk_off"
                          or signal.evidence_conflict)
        action_guidance_cn = ("当前信号证据不足或存在风险阻断，先观察并复核，不建议新开仓。"
                              if blocked_signal else "仅在数据、审批和风控通过后按程序价格计划执行，禁止追价或扩大仓位。")
        summary = (f"综合规则信号强度 {signal.composite_score:.3f}，市场环境 {signal.regime_label}；"
                   "此数值不是预测胜率。")
        return RecommendationAnalysis(
            summary=summary, report_title=f"{trade_plan.ticker} 量化证据分析",
            report_cn=" ".join([summary, *technical, *event_view, *fundamental_view,
                                *execution_view, *risk_notes, action_guidance_cn]),
            why_to_buy_cn=why_to_buy_cn, why_to_sell_cn=why_to_sell_cn,
            action_guidance_cn=action_guidance_cn, technical_view=technical,
            event_view=event_view, fundamental_view=fundamental_view,
            execution_view=execution_view, risk_notes=risk_notes,
        )
