from __future__ import annotations

from domain.entities.models import (
    Direction,
    FeatureSnapshot,
    MarketBar,
    PatternType,
    PricePlanConfig,
    SecurityMetadata,
    SignalSnapshot,
    TradePlan,
)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _entry_mid(entry_low: float, entry_high: float) -> float:
    return (entry_low + entry_high) / 2.0


def _is_plan_too_far_from_current(current: float, entry_low: float, entry_high: float) -> bool:
    if current <= 0:
        return False
    deviation = abs(_entry_mid(entry_low, entry_high) - current) / current
    return deviation > 0.40


class PriceSizingEngine:
    """Deterministic entry/stop/target generation rules."""

    def build(
        self,
        security: SecurityMetadata,
        bars: list[MarketBar],
        feature: FeatureSnapshot,
        signal: SignalSnapshot,
        cfg: PricePlanConfig,
        direction: Direction = Direction.BUY,
    ) -> TradePlan:
        current = bars[-1].close
        atr = max(feature.atr, 0.01)
        stop_mult = max(cfg.stop_atr_range[0], 1.2)
        pattern = cfg.strategy_pattern

        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        local_swing_high = max(highs[-10:])
        local_swing_low = min(lows[-10:])

        if direction == Direction.BUY:
            if pattern == PatternType.BREAKOUT:
                breakout_level = max(feature.breakout_level_20d, current)
                entry_low = breakout_level - cfg.breakout_entry_atr_buffer * atr
                entry_high = breakout_level + cfg.breakout_entry_atr_buffer * atr
                stop = min(local_swing_low, entry_low - stop_mult * atr)
            elif pattern == PatternType.PULLBACK:
                support = max(feature.support_level_20d, feature.ma_20)
                entry_low = support - 0.2 * atr
                entry_high = support + 0.2 * atr
                stop = min(local_swing_low, support - stop_mult * atr)
            elif pattern == PatternType.MEAN_REVERSION:
                entry_low = feature.ma_20 - 1.5 * atr
                entry_high = feature.ma_20 - 0.7 * atr
                stop = entry_low - stop_mult * atr
            else:
                entry_low = current - 0.2 * atr
                entry_high = current + 0.2 * atr
                stop = entry_low - stop_mult * atr

            mid = _entry_mid(entry_low, entry_high)
            risk_per_share = max(0.01, mid - stop)
            tp1 = mid + cfg.first_target_r_multiple * risk_per_share
            tp2 = mid + (cfg.first_target_r_multiple + 1.0) * risk_per_share
            rr = (tp1 - mid) / risk_per_share
        else:
            if pattern == PatternType.SHORT_SETUP:
                resistance = max(feature.breakout_level_20d, local_swing_high)
                entry_low = resistance - 0.2 * atr
                entry_high = resistance + 0.2 * atr
                stop = resistance + stop_mult * atr
            else:
                entry_low = current - 0.2 * atr
                entry_high = current + 0.2 * atr
                stop = entry_high + stop_mult * atr

            mid = _entry_mid(entry_low, entry_high)
            risk_per_share = max(0.01, stop - mid)
            tp1 = mid - cfg.first_target_r_multiple * risk_per_share
            tp2 = mid - (cfg.first_target_r_multiple + 1.0) * risk_per_share
            rr = (mid - tp1) / risk_per_share

        if _is_plan_too_far_from_current(current, entry_low, entry_high):
            # Fallback to ATR-anchored plan around current mark when source bars contain outliers.
            entry_low = current - 0.2 * atr
            entry_high = current + 0.2 * atr
            mid = _entry_mid(entry_low, entry_high)
            if direction == Direction.BUY:
                stop = entry_low - stop_mult * atr
                risk_per_share = max(0.01, mid - stop)
                tp1 = mid + cfg.first_target_r_multiple * risk_per_share
                tp2 = mid + (cfg.first_target_r_multiple + 1.0) * risk_per_share
                rr = (tp1 - mid) / risk_per_share
            else:
                stop = entry_high + stop_mult * atr
                risk_per_share = max(0.01, stop - mid)
                tp1 = mid - cfg.first_target_r_multiple * risk_per_share
                tp2 = mid - (cfg.first_target_r_multiple + 1.0) * risk_per_share
                rr = (mid - tp1) / risk_per_share

        # Basic size envelope: 1% risk budget clipped by concentration constraints.
        size_pct = _clamp(0.01 * mid / max(risk_per_share, 0.01), 0.01, 0.10)

        return TradePlan(
            ticker=security.ticker,
            pattern=pattern,
            direction=direction,
            entry_zone_low=round(entry_low, 4),
            entry_zone_high=round(entry_high, 4),
            stop_loss=round(stop, 4),
            tp1=round(tp1, 4),
            tp2=round(tp2, 4),
            holding_period=cfg.holding_period,
            risk_reward=round(rr, 4),
            position_size_pct=round(size_pct, 4),
        )
