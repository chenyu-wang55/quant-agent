from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from services.llm.evidence import EvidenceDocument, EvidenceExtraction, validate_extraction


def _wilson(successes: int, total: int) -> list[float] | None:
    if not total:
        return None
    z = 1.96
    p = successes/total
    denominator = 1+z*z/total
    center = (p+z*z/(2*total))/denominator
    radius = z*math.sqrt(p*(1-p)/total+z*z/(4*total*total))/denominator
    return [max(0, center-radius), min(1, center+radius)]


def evaluate_extractions(gold: list[dict[str, Any]], predictions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Score archived, frozen test predictions; never makes a model request."""
    if not gold or len({item["case_id"] for item in gold}) != len(gold):
        raise ValueError("gold cases must be nonempty and unique")
    groups: dict[str, set[str]] = {}
    for case in gold:
        groups.setdefault(case["group_id"], set()).add(case["split"])
    if any(len(splits) != 1 for splits in groups.values()):
        raise ValueError("issuer/event/time groups overlap across sample splits")
    test = [case for case in gold if case["split"] == "test"]
    if not test:
        raise ValueError("frozen test split missing")
    correct = predicted = expected = valid = events_found = events_expected = 0
    failures, latencies, costs = [], [], []
    versions = set()
    fields: dict[str, dict[str, int]] = {}
    for case in test:
        documents = [EvidenceDocument.model_validate(item) for item in case["documents"]]
        at = datetime.fromisoformat(case["decision_at"])
        expected_result = EvidenceExtraction.model_validate(case["gold"])
        validate_extraction(expected_result, documents, at)
        def key(fact):
            return fact.source_id, fact.field, fact.value
        truth = {key(fact) for fact in expected_result.facts}
        expected += len(truth)
        events_expected += sum(item[1] == "event" for item in truth)
        for _, field, _ in truth:
            fields.setdefault(field, {"expected": 0, "correct": 0, "predicted": 0})["expected"] += 1
        prediction = predictions.get(case["case_id"])
        try:
            if not prediction or prediction.get("validation") != "passed":
                raise ValueError("missing_or_failed_extraction")
            if not all(prediction.get(name) for name in ("actual_model", "prompt_version", "schema_version")):
                raise ValueError("missing_model_provenance")
            versions.add(tuple(prediction[name] for name in ("actual_model", "prompt_version", "schema_version")))
            result = EvidenceExtraction.model_validate(prediction["extraction"])
            validate_extraction(result, documents, at)
            actual = {key(fact) for fact in result.facts}
            valid += 1
            predicted += len(actual)
            matched = truth & actual
            for _, field, _ in actual:
                fields.setdefault(field, {"expected": 0, "correct": 0, "predicted": 0})["predicted"] += 1
            for _, field, _ in matched:
                fields[field]["correct"] += 1
            correct += len(matched)
            events_found += sum(item[1] == "event" for item in matched)
            if actual != truth:
                failures.append({"case_id": case["case_id"], "reason": "field_mismatch",
                                 "missing": sorted(truth-actual), "unexpected": sorted(actual-truth)})
        except (ValueError, KeyError) as exc:
            failures.append({"case_id": case["case_id"], "reason": str(exc)[:120]})
        if prediction:
            if prediction.get("latency_ms") is not None:
                latencies.append(float(prediction["latency_ms"]))
            costs.append(prediction.get("actual_usd"))
    accuracy = correct/predicted if predicted else 0
    recall = correct/expected if expected else 0
    event_recall = events_found/events_expected if events_expected else None
    coverage = valid/len(test)
    if len(versions) > 1:
        raise ValueError("compare each model/prompt/schema version in a separate evaluation")
    latencies.sort()
    def quantile(p):
        return latencies[min(len(latencies)-1, math.ceil(len(latencies)*p)-1)] if latencies else None
    return {"gold_case_count": len(gold), "test_count": len(test), "valid_count": valid,
            "field_precision": accuracy, "field_recall": recall,
            "precision_ci95": _wilson(correct, predicted), "recall_ci95": _wilson(correct, expected),
            "event_recall": event_recall, "validated_coverage": coverage,
            "versions": sorted(versions), "fields": fields,
            "latency_p50_ms": quantile(0.5), "latency_p95_ms": quantile(0.95),
            "known_actual_usd": sum(cost for cost in costs if cost is not None),
            "unknown_cost_count": sum(cost is None for cost in costs), "failures": failures,
            "quality_target_passed": (len(gold) >= 200 and len(test) >= 60 and accuracy >= 0.95
                                       and event_recall is not None and event_recall >= 0.9 and coverage >= 0.9),
            "scope": "extraction_only_not_strategy_acceptance"}
