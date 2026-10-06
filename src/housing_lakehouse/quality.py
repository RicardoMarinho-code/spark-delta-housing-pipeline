"""Declarative data quality rules, quarantine and circuit breaker.

Each rule is a boolean condition that must hold for a record to be valid (null counts as a failure:
the rule must handle missing values explicitly). Blocking rules send the record to quarantine;
warnings are only counted. All counts come from a single aggregation.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

BLOCKING = "blocking"
WARNING = "warning"


class QualityError(Exception):
    """The rejection rate crossed the threshold: the load stops before publishing bad data."""


@dataclass(frozen=True)
class Rule:
    name: str
    condition: Column
    severity: str = BLOCKING
    description: str = ""


@dataclass
class QualityResult:
    valid: DataFrame
    quarantine: DataFrame
    metrics: list[dict]
    total: int
    quarantined: int
    _cache: DataFrame | None = field(default=None, repr=False)

    @property
    def rejection_rate(self) -> float:
        return self.quarantined / self.total if self.total else 0.0

    def release(self) -> None:
        if self._cache is not None:
            self._cache.unpersist()


def _failures(rules: list[Rule]) -> Column:
    exprs = [F.when(~F.coalesce(r.condition, F.lit(False)), F.lit(r.name)) for r in rules]
    if not exprs:
        return F.array().cast("array<string>")
    return F.filter(F.array(*exprs), lambda x: x.isNotNull())


def evaluate(df: DataFrame, rules: list[Rule]) -> QualityResult:
    blocking = [r for r in rules if r.severity == BLOCKING]
    warnings = [r for r in rules if r.severity == WARNING]
    flagged = (
        df.withColumn("_failures", _failures(blocking))
        .withColumn("_warnings", _failures(warnings))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    counts = flagged.agg(
        F.count(F.lit(1)).alias("_total"),
        F.count(F.when(F.size("_failures") > 0, True)).alias("_quarantined"),
        *[
            F.count(
                F.when(F.array_contains("_failures" if r.severity == BLOCKING else "_warnings", r.name), True)
            ).alias(r.name)
            for r in rules
        ],
    ).first()
    total = int(counts["_total"])
    metrics = [
        {
            "rule": r.name,
            "severity": r.severity,
            "description": r.description,
            "failures": int(counts[r.name]),
            "total": total,
            "rate": int(counts[r.name]) / total if total else 0.0,
        }
        for r in rules
    ]
    return QualityResult(
        valid=flagged.where(F.size("_failures") == 0).drop("_failures"),
        quarantine=flagged.where(F.size("_failures") > 0),
        metrics=metrics,
        total=total,
        quarantined=int(counts["_quarantined"]),
        _cache=flagged,
    )


def enforce_threshold(result: QualityResult, max_rate: float) -> None:
    if result.rejection_rate > max_rate:
        worst = sorted(
            (m for m in result.metrics if m["severity"] == BLOCKING and m["failures"]),
            key=lambda m: -m["failures"],
        )[:3]
        summary = ", ".join(f"{m['rule']}={m['failures']}" for m in worst)
        raise QualityError(
            f"Rejection rate {result.rejection_rate:.1%} above the {max_rate:.1%} threshold "
            f"({result.quarantined} of {result.total}). Top rules: {summary}"
        )
