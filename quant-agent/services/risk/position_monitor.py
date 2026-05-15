from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

from domain.entities.models import HoldingWatch, SellAlert, SellAlertLevel, SignalSnapshot
from services.ingestion.interfaces import DataProvider


class PositionMonitor:
    def evaluate(
        self,
        holdings: list[HoldingWatch],
        provider: DataProvider,
        as_of: datetime | None = None,
        signal_lookup: Callable[[str], SignalSnapshot | None] | None = None,
    ) -> list[SellAlert]:
        check_time = as_of.astimezone(timezone.utc) if as_of else datetime.now(timezone.utc)
        alerts: list[SellAlert] = []

        for holding in holdings:
            if holding.status.value != "open":
                continue

            try:
                latest_price = provider.get_latest_price(holding.ticker, check_time)
                if latest_price is None:
                    bars = provider.get_bars(holding.ticker, check_time, lookback_days=3)
                    latest_price = bars[-1].close if bars else None
            except Exception:
                continue

            if latest_price is None:
                continue
            current_price = float(latest_price)

            if current_price <= holding.stop_loss:
                alerts.append(
                    SellAlert(
                        ticker=holding.ticker,
                        level=SellAlertLevel.URGENT,
                        reason_code="stop_loss_breach",
                        current_price=current_price,
                        stop_loss=holding.stop_loss,
                        take_profit1=holding.take_profit1,
                        take_profit2=holding.take_profit2,
                        source_recommendation_id=holding.source_recommendation_id,
                        message_cn=(
                            f"{holding.ticker} 当前价格 {current_price:.2f} 已跌破止损位 {holding.stop_loss:.2f}，"
                            "原始交易假设失效。"
                        ),
                        suggested_action_cn="建议尽快执行止损退出，避免亏损扩大。",
                    )
                )
            elif current_price >= holding.take_profit2:
                alerts.append(
                    SellAlert(
                        ticker=holding.ticker,
                        level=SellAlertLevel.WARN,
                        reason_code="take_profit2_hit",
                        current_price=current_price,
                        stop_loss=holding.stop_loss,
                        take_profit1=holding.take_profit1,
                        take_profit2=holding.take_profit2,
                        source_recommendation_id=holding.source_recommendation_id,
                        message_cn=(
                            f"{holding.ticker} 当前价格 {current_price:.2f} 已触达第二目标位 {holding.take_profit2:.2f}。"
                        ),
                        suggested_action_cn="建议以落袋为主，剩余仓位可结合趋势保留小仓跟踪。",
                    )
                )
            elif current_price >= holding.take_profit1:
                alerts.append(
                    SellAlert(
                        ticker=holding.ticker,
                        level=SellAlertLevel.INFO,
                        reason_code="take_profit1_hit",
                        current_price=current_price,
                        stop_loss=holding.stop_loss,
                        take_profit1=holding.take_profit1,
                        take_profit2=holding.take_profit2,
                        source_recommendation_id=holding.source_recommendation_id,
                        message_cn=(
                            f"{holding.ticker} 当前价格 {current_price:.2f} 已触达第一目标位 {holding.take_profit1:.2f}。"
                        ),
                        suggested_action_cn="建议先减仓锁定部分利润，并将剩余仓位保护到成本附近。",
                    )
                )

            if signal_lookup is not None:
                signal = signal_lookup(holding.ticker)
                if signal is not None and signal.regime_label == "risk_off":
                    alerts.append(
                        SellAlert(
                            ticker=holding.ticker,
                            level=SellAlertLevel.WARN,
                            reason_code="regime_risk_off",
                            current_price=current_price,
                            stop_loss=holding.stop_loss,
                            take_profit1=holding.take_profit1,
                            take_profit2=holding.take_profit2,
                            source_recommendation_id=holding.source_recommendation_id,
                            message_cn=f"{holding.ticker} 所处市场环境转为 risk-off，系统建议降低风险暴露。",
                            suggested_action_cn="建议主动降仓或收紧止损，不建议继续加仓。",
                        )
                    )

        return alerts
