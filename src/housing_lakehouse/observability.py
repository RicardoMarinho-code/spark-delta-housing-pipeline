"""Observability: structured logs, run audit, quality metrics and alerts.

Every step runs inside `track(...)`, which writes one row to `audit/runs` with status, duration and
volumes (even when the step fails) and raises an alert on failure. The audit table is also the
incremental-load control: silver only processes reference dates without a recorded success.
"""

from __future__ import annotations

import json
import logging
import os
import statistics
import sys
import urllib.request
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from housing_lakehouse.config import Config
from housing_lakehouse.storage import append, exists, read

log = logging.getLogger("housing_lakehouse")

RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("stage", StringType()),
        StructField("table_name", StringType()),
        StructField("reference_date", DateType()),
        StructField("status", StringType()),
        StructField("started_at", TimestampType()),
        StructField("ended_at", TimestampType()),
        StructField("duration_s", DoubleType()),
        StructField("rows_in", LongType()),
        StructField("rows_out", LongType()),
        StructField("rows_quarantined", LongType()),
        StructField("error", StringType()),
        StructField("details", StringType()),
    ]
)

QUALITY_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("table_name", StringType()),
        StructField("reference_date", DateType()),
        StructField("rule", StringType()),
        StructField("severity", StringType()),
        StructField("description", StringType()),
        StructField("failures", LongType()),
        StructField("total", LongType()),
        StructField("rate", DoubleType()),
        StructField("recorded_at", TimestampType()),
    ]
)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        base.update(getattr(record, "context", {}))
        return json.dumps(base, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO") -> None:
    if log.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    log.addHandler(handler)
    log.setLevel(level)
    log.propagate = False


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


def alert(message: str, context: dict | None = None) -> None:
    """Logs the alert and, when ALERT_WEBHOOK_URL is set (Teams/Slack), posts it there."""
    log.warning(message, extra={"context": {"alert": True, **(context or {})}})
    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        return
    body = json.dumps({"text": message}).encode()
    request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as error:  # noqa: BLE001 - an alert must never break the pipeline
        log.warning(f"Could not send alert: {error}")


@dataclass
class Run:
    run_id: str
    stage: str
    table_name: str
    started_at: datetime
    reference_date: date | None = None
    status: str = "running"
    ended_at: datetime | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    rows_quarantined: int | None = None
    error: str | None = None
    details: dict = field(default_factory=dict)

    @property
    def duration_s(self) -> float | None:
        return (self.ended_at - self.started_at).total_seconds() if self.ended_at else None

    def as_row(self) -> tuple:
        return (
            self.run_id,
            self.stage,
            self.table_name,
            self.reference_date,
            self.status,
            self.started_at,
            self.ended_at,
            self.duration_s,
            self.rows_in,
            self.rows_out,
            self.rows_quarantined,
            self.error,
            json.dumps(self.details, ensure_ascii=False, default=str),
        )


@contextmanager
def track(spark: SparkSession, cfg: Config, stage: str, table_name: str, run_id: str) -> Iterator[Run]:
    run = Run(run_id=run_id, stage=stage, table_name=table_name, started_at=datetime.now(timezone.utc))
    log.info(f"{stage}/{table_name}: started", extra={"context": {"run_id": run_id}})
    try:
        yield run
        if run.status == "running":
            run.status = "success"
    except Exception as error:
        run.status = "failed"
        run.error = f"{type(error).__name__}: {error}"[:2000]
        alert(
            f"[housing-lakehouse] {stage}/{table_name} failed ({run.reference_date}): {run.error}",
            {"run_id": run_id},
        )
        raise
    finally:
        run.ended_at = datetime.now(timezone.utc)
        try:
            append(spark.createDataFrame([run.as_row()], RUNS_SCHEMA), cfg.path("audit", "runs"))
        except Exception as error:  # noqa: BLE001 - an audit failure must not hide the original error
            log.error(f"Could not write the audit row: {error}")
        log.info(
            f"{stage}/{table_name}: {run.status}",
            extra={
                "context": {
                    "run_id": run_id,
                    "reference_date": run.reference_date,
                    "duration_s": round(run.duration_s or 0, 2),
                    "rows_in": run.rows_in,
                    "rows_out": run.rows_out,
                    "rows_quarantined": run.rows_quarantined,
                }
            },
        )


def record_quality(spark: SparkSession, cfg: Config, run: Run, metrics: list[dict]) -> None:
    now = datetime.now(timezone.utc)
    rows = [
        (
            run.run_id,
            run.table_name,
            run.reference_date,
            m["rule"],
            m["severity"],
            m["description"],
            m["failures"],
            m["total"],
            m["rate"],
            now,
        )
        for m in metrics
    ]
    if rows:
        append(spark.createDataFrame(rows, QUALITY_SCHEMA), cfg.path("audit", "quality"))


def check_volume(spark: SparkSession, cfg: Config, run: Run, history: int = 3) -> None:
    """Alerts when a load's volume strays from the median of the last successful loads."""
    path = cfg.path("audit", "runs")
    if run.rows_in is None or not exists(spark, path):
        return
    previous = [
        r.rows_in
        for r in read(spark, path)
        .where(
            (F.col("stage") == run.stage)
            & (F.col("table_name") == run.table_name)
            & (F.col("status") == "success")
            & F.col("rows_in").isNotNull()
        )
        .orderBy(F.col("started_at").desc())
        .limit(history)
        .collect()
    ]
    if not previous:
        return
    baseline = statistics.median(previous)
    change = (run.rows_in - baseline) / baseline if baseline else 0.0
    run.details["volume_change"] = round(change, 4)
    if abs(change) > cfg.max_volume_change:
        alert(
            f"[housing-lakehouse] Unusual volume in {run.stage}/{run.table_name}: {run.rows_in} rows, "
            f"{change:+.1%} vs. the median of {baseline:.0f} over recent loads",
            {"run_id": run.run_id},
        )


def check_freshness(spark: SparkSession, cfg: Config, tables: list[str], today: date | None = None) -> list[str]:
    """Lists the tables without a successful load for more than `max_days_without_load` days."""
    today = today or date.today()
    path = cfg.path("audit", "runs")
    latest: dict[str, date] = {}
    if exists(spark, path):
        for r in (
            read(spark, path)
            .where((F.col("stage") == "silver") & (F.col("status") == "success"))
            .groupBy("table_name")
            .agg(F.max("reference_date").alias("latest"))
            .collect()
        ):
            latest[r.table_name] = r.latest
    stale = []
    for table in tables:
        last = latest.get(table)
        if last is None or (today - last).days > cfg.max_days_without_load:
            stale.append(table)
            alert(f"[housing-lakehouse] {table} has had no new load since {last or 'ever'}")
    return stale
