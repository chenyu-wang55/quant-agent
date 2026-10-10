from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator


class EvidenceDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_id: str
    source_url: str
    published_at: datetime
    available_at: datetime
    text: str

    @model_validator(mode="after")
    def validate_times(self):
        if self.published_at.tzinfo is None or self.available_at.tzinfo is None:
            raise ValueError("evidence timestamps require explicit timezones")
        if self.published_at > self.available_at or not self.source_id or not self.text or not self.source_url:
            raise ValueError("evidence provenance is incomplete or out of order")
        return self


class ExtractedFact(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_id: str
    field: Literal["event", "metric", "date", "ticker", "direction"]
    value: str
    quote: str


class EvidenceExtraction(BaseModel):
    model_config = ConfigDict(extra="forbid")
    facts: list[ExtractedFact]


def validate_evidence(documents: list[EvidenceDocument], decision_at: datetime) -> None:
    if decision_at.tzinfo is None:
        raise ValueError("decision timestamp requires explicit timezone")
    if not documents or len({doc.source_id for doc in documents}) != len(documents):
        raise ValueError("evidence must be nonempty and source IDs unique")
    if any(doc.available_at > decision_at for doc in documents):
        raise ValueError("future evidence is not permitted")


def validate_extraction(result: EvidenceExtraction, documents: list[EvidenceDocument], decision_at: datetime) -> None:
    validate_evidence(documents, decision_at)
    by_id = {doc.source_id: doc for doc in documents}
    if len(result.facts) > 50:
        raise ValueError("too many extracted facts")
    for fact in result.facts:
        doc = by_id.get(fact.source_id)
        if doc is None or not fact.quote or fact.quote not in doc.text or not fact.value or fact.value not in fact.quote:
            raise ValueError("fact must reference a verbatim source quote and value")


def render_extraction(result: EvidenceExtraction, documents: list[EvidenceDocument], decision_at: datetime) -> str:
    validate_extraction(result, documents, decision_at)
    by_id = {doc.source_id: doc for doc in documents}
    return "\n".join(f"{fact.field}：{fact.value}；原文：{fact.quote}；来源：{by_id[fact.source_id].source_url}"
                     for fact in result.facts)
