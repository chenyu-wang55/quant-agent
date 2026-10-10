from datetime import datetime, timedelta, timezone

import pytest

from services.research.calibration import BinnedCalibrator, LabelObservation
from services.research.walk_forward import WalkForwardPlan


def at(year, month=1, day=1):
    return datetime(year, month, day, tzinfo=timezone.utc)


def label(name, decision, end, score=0.8, outcome=False):
    return LabelObservation(name, decision, end, score, outcome)


def test_fit_rejects_future_labels_and_uses_training_probability_baseline():
    samples = [label("loss", at(2020), at(2020, 2)), label("win", at(2020, 3), at(2020, 4), 0.2, True)]
    model = BinnedCalibrator.fit(samples, training_end=at(2021), target="net_profit_at_exit_after_frozen_costs",
                                dataset_fingerprint="a"*64, strategy_config_id="rules-v1")
    assert model.baseline_probability == 0.5
    assert model.predict(0.8) < 0.5 and model.predict(0.2) > 0.5
    with pytest.raises(ValueError, match="cross"):
        BinnedCalibrator.fit(samples, training_end=at(2020, 2), target="net", dataset_fingerprint="a"*64,
                             strategy_config_id="rules-v1")
    with pytest.raises(ValueError, match="unseen"):
        model.evaluate(samples)
    metrics = model.evaluate([label("oos", at(2021), at(2021, 2))])
    assert metrics["brier"] < metrics["baseline_brier"]


def test_walk_forward_holds_out_final_year_and_purges_crossing_labels():
    plan = WalkForwardPlan(at(2018), at(2025), embargo_days=14)
    windows = plan.windows()
    assert len(windows) >= 3
    assert windows[-1].oos_end <= at(2024)
    first = windows[0]
    samples = [label("cross", first.validation_start-timedelta(days=1), first.validation_start+timedelta(days=1)),
               label("embargo", first.oos_start+timedelta(days=1), first.oos_start+timedelta(days=2)),
               label("oos", first.oos_start+timedelta(days=15), first.oos_start+timedelta(days=25))]
    groups = plan.partition(samples, first)
    assert not groups["train"]
    assert [item.observation_id for item in groups["oos"]] == ["oos"]
    with pytest.raises(ValueError, match="insufficient_history"):
        WalkForwardPlan(at(2020), at(2023)).windows()
