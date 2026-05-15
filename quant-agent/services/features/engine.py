from __future__ import annotations

import hashlib
import math

from domain.entities.models import FeatureSnapshot, MarketBar


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mu = _mean(values)
    var = sum((v - mu) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def _sma(values: list[float], window: int) -> float:
    if not values:
        return 0.0
    if len(values) < window:
        return _mean(values)
    return _mean(values[-window:])


class FeatureEngine:
    def compute(
        self,
        ticker: str,
        bars: list[MarketBar],
        benchmark_bars: list[MarketBar],
        atr_window: int = 14,
    ) -> FeatureSnapshot:
        if len(bars) < max(atr_window + 1, 30):
            raise ValueError(f"Not enough bars for {ticker}")

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars]

        # ATR
        trs: list[float] = []
        for idx in range(1, len(bars)):
            tr = max(
                highs[idx] - lows[idx],
                abs(highs[idx] - closes[idx - 1]),
                abs(lows[idx] - closes[idx - 1]),
            )
            trs.append(tr)
        atr = _mean(trs[-atr_window:])

        # Trend and moving averages
        ma_20 = _sma(closes, 20)
        ma_50 = _sma(closes, 50)
        ma_200 = _sma(closes, 200)

        # Volatility and momentum
        returns = []
        for idx in range(1, len(closes)):
            prev = closes[idx - 1]
            if prev <= 0:
                continue
            returns.append(closes[idx] / prev - 1.0)
        volatility_20d = _std(returns[-20:]) * math.sqrt(252)
        momentum_20d = closes[-1] / closes[-21] - 1.0 if len(closes) >= 21 else 0.0

        # Relative strength
        bench_closes = [b.close for b in benchmark_bars]
        if len(closes) >= 64 and len(bench_closes) >= 64:
            stock_63d = closes[-1] / closes[-64] - 1.0
            bench_63d = bench_closes[-1] / bench_closes[-64] - 1.0
            relative_strength_63d = stock_63d - bench_63d
        else:
            relative_strength_63d = 0.0

        avg_dollar_volume_20d = _mean(
            [closes[idx] * volumes[idx] for idx in range(max(0, len(closes) - 20), len(closes))]
        )

        lookback_highs = highs[-21:-1] if len(highs) >= 21 else highs
        lookback_lows = lows[-21:-1] if len(lows) >= 21 else lows
        breakout_level_20d = max(lookback_highs) if lookback_highs else closes[-1]
        support_level_20d = min(lookback_lows) if lookback_lows else closes[-1]

        feature_id = hashlib.sha1(
            f"{ticker}|{bars[-1].timestamp.isoformat()}|{round(atr, 4)}".encode("utf-8")
        ).hexdigest()[:16]

        return FeatureSnapshot(
            id=feature_id,
            ticker=ticker,
            timestamp=bars[-1].timestamp,
            atr=round(atr, 6),
            ma_20=round(ma_20, 6),
            ma_50=round(ma_50, 6),
            ma_200=round(ma_200, 6),
            volatility_20d=round(volatility_20d, 6),
            momentum_20d=round(momentum_20d, 6),
            relative_strength_63d=round(relative_strength_63d, 6),
            avg_dollar_volume_20d=round(avg_dollar_volume_20d, 2),
            breakout_level_20d=round(breakout_level_20d, 6),
            support_level_20d=round(support_level_20d, 6),
        )
