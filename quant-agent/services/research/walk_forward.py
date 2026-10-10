from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta

from services.research.calibration import LabelObservation


def add_months(value: datetime, months: int) -> datetime:
    index = value.year * 12 + value.month - 1 + months
    year, month = divmod(index, 12)
    return value.replace(year=year, month=month+1, day=min(value.day, calendar.monthrange(year, month+1)[1]))


@dataclass(frozen=True)
class WalkForwardWindow:
    train_start: datetime
    validation_start: datetime
    oos_start: datetime
    oos_end: datetime


@dataclass(frozen=True)
class WalkForwardPlan:
    start: datetime
    end: datetime
    train_months: int = 36
    validation_months: int = 6
    oos_months: int = 6
    step_months: int = 6
    holdout_months: int = 12
    minimum_windows: int = 3
    embargo_days: int = 14

    def windows(self) -> list[WalkForwardWindow]:
        if (self.start.tzinfo is None or self.end.tzinfo is None or self.end <= self.start
                or min(self.train_months, self.validation_months, self.oos_months,
                       self.step_months, self.holdout_months, self.minimum_windows) <= 0
                or self.step_months < self.oos_months or self.embargo_days < 0):
            raise ValueError("invalid or overlapping walk-forward windows")
        holdout_start = add_months(self.end, -self.holdout_months)
        result = []
        cursor = self.start
        while True:
            validation = add_months(cursor, self.train_months)
            oos = add_months(validation, self.validation_months)
            end = add_months(oos, self.oos_months)
            if end > holdout_start:
                break
            result.append(WalkForwardWindow(cursor, validation, oos, end))
            cursor = add_months(cursor, self.step_months)
        if len(result) < self.minimum_windows:
            raise ValueError("insufficient_history_for_three_oos_windows")
        return result

    def partition(self, observations: list[LabelObservation], window: WalkForwardWindow) -> dict[str, list[LabelObservation]]:
        """Half-open boundaries, purge crossing labels and embargo following samples."""
        gap = timedelta(days=self.embargo_days)
        bounds = {
            "train": (window.train_start, window.validation_start),
            "validation": (window.validation_start+gap, window.oos_start),
            "oos": (window.oos_start+gap, window.oos_end),
        }
        return {name: [item for item in observations if start <= item.decision_at < end and item.label_end < end]
                for name, (start, end) in bounds.items()}
