from __future__ import annotations

import hashlib
from datetime import datetime, timezone

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
            "technical trend and structure are supportive": signal.technical_score,
            "event/news flow is favorable": signal.event_score,
            "relative strength vs benchmark is positive": signal.relative_strength_score,
            "fundamental quality and revisions are supportive": signal.fundamental_score,
            "execution quality is acceptable": signal.execution_quality_score,
        }
        ranked = sorted(components.items(), key=lambda kv: kv[1], reverse=True)
        return [text for text, _ in ranked[:3]]

    @staticmethod
    def _build_analysis(signal: SignalSnapshot, trade_plan: TradePlan) -> RecommendationAnalysis:
        technical = [
            "价格结构维持上行节奏，趋势并未被破坏。",
            "短期动能仍有延续性，回撤后承接相对稳定。",
            f"当前市场环境为 {signal.regime_label}，顺势交易胜率相对更高。",
        ]
        event_view = [
            "事件流与价格流分开评估，避免单条新闻误导交易决策。",
            "近期事件没有出现明显反向冲击，交易逻辑可继续观察执行。",
        ]
        fundamental_view = [
            "盈利修正与经营质量维度对中期持有形成支撑。",
            "流动性和成交质量较好，执行时的冲击成本可控。",
        ]
        execution_view = [
            f"建议在 {trade_plan.entry_zone_low:.2f}-{trade_plan.entry_zone_high:.2f} 区间内分批进入，避免追高。",
            f"防守位放在 {trade_plan.stop_loss:.2f}，一旦收盘失守应优先止损。",
            f"第一目标 {trade_plan.tp1:.2f} 先锁定部分利润，第二目标 {trade_plan.tp2:.2f} 作为趋势延伸目标。",
        ]
        risk_notes = [
            "若价格收盘跌破止损位，说明交易假设被破坏，应及时退出。",
            "若市场风格转向 risk-off，应主动降低仓位或暂停新开仓。",
            "若后续事件与价格方向出现明显冲突，需重新评估持仓理由。",
        ]

        why_to_buy_cn: list[str] = []
        if signal.technical_score >= 0.65:
            why_to_buy_cn.append("技术结构健康，趋势与动能同向，具备继续上行的基础。")
        if signal.relative_strength_score >= 0.55:
            why_to_buy_cn.append("相对大盘更强，说明资金偏好仍在该标的。")
        if signal.fundamental_score >= 0.50:
            why_to_buy_cn.append("基本面与盈利修正维度提供了中周期支撑。")
        if signal.execution_quality_score >= 0.55:
            why_to_buy_cn.append("流动性较好，实际执行更容易贴近计划价位。")
        if not why_to_buy_cn:
            why_to_buy_cn.append("当前信号偏中性，可小仓位试错并严格执行止损。")

        why_to_sell_cn = [
            f"价格收盘跌破 {trade_plan.stop_loss:.2f}，说明买入逻辑失效，优先止损。",
            f"价格触达 {trade_plan.tp1:.2f} 可先减仓，触达 {trade_plan.tp2:.2f} 可进一步止盈。",
            "若市场切换为 risk-off，或出现明显利空事件，应提前收缩风险敞口。",
        ]

        action_guidance_cn = (
            "先按计划价位分批进场，再按止损/止盈规则机械执行；"
            "不要在情绪波动时随意扩大仓位或取消风控。"
        )

        report_title = f"{trade_plan.ticker} 量化推荐分析"
        report_cn = (
            f"{trade_plan.ticker} 当前属于‘有条件可做’的多头机会。"
            f"核心原因是趋势仍向上、相对强弱不弱于市场、且执行流动性可接受。"
            f"策略上建议在入场区间 {trade_plan.entry_zone_low:.2f}-{trade_plan.entry_zone_high:.2f} 分批建仓，"
            f"以 {trade_plan.stop_loss:.2f} 作为失效边界。"
            f"若价格上行到 {trade_plan.tp1:.2f}/{trade_plan.tp2:.2f}，按计划分批兑现利润。"
            "这不是对收益的承诺，而是基于当前证据的风险收益决策。"
        )

        return RecommendationAnalysis(
            summary=(
                f"当前推荐基于多维证据的一致性，而不是单一指标。综合信号强度为 {signal.composite_score:.3f}。"
            ),
            report_title=report_title,
            report_cn=report_cn,
            why_to_buy_cn=why_to_buy_cn,
            why_to_sell_cn=why_to_sell_cn,
            action_guidance_cn=action_guidance_cn,
            technical_view=technical,
            event_view=event_view,
            fundamental_view=fundamental_view,
            execution_view=execution_view,
            risk_notes=risk_notes,
        )
