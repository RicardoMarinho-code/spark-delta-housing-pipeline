"""Bronze: raw file → Delta, content untouched.

Replaces the typical legacy load (TRUNCATE + full reload, everything as text, no validation, no log):
- the header is checked against the layout before reading (a layout change stops the load with a
  clear message);
- each monthly file becomes a partition (`_reference_date`) rewritten with replaceWhere, so reruns
  are idempotent and earlier months are preserved;
- rows with the wrong number of fields do not break the load: they are flagged in `_corrupt_record`;
- an already ingested file is skipped (unless `reprocess=True`);
- volume is recorded and an alert fires when it strays from recent loads.

Bronze holds identifiers in clear text: access is restricted to the pipeline identity and retention
is limited (`apply_retention`).
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from housing_lakehouse.config import Config, Source
from housing_lakehouse.layouts import LAYOUTS, Layout
from housing_lakehouse.observability import Run, check_volume, track
from housing_lakehouse.storage import exists, overwrite, overwrite_partition, read

log = logging.getLogger("housing_lakehouse")

CORRUPT_COLUMN = "_corrupt_record"
# CadÚnico files use no text qualifier. A control character that never appears in the data turns
# quoting off without clashing with the \x00 found in some ZIP codes.
NO_QUOTE = "\u0001"


class LayoutError(Exception):
    """The file does not follow the expected layout."""


def reference_date_from_filename(name: str) -> date | None:
    """Extracts the DDMMYYYY date from the file name (ARQ_FAMILIA_11092026_XYZ.TXT → 2026-09-11)."""
    for chunk in re.findall(r"(?<!\d)(\d{8})(?!\d)", name):
        try:
            return datetime.strptime(chunk, "%d%m%Y").date()
        except ValueError:
            continue
    return None


def validate_header(header: str, layout: Layout, separator: str) -> None:
    received = [c.strip().upper() for c in header.lstrip("﻿").strip().split(separator)]
    expected = layout.names
    if received == expected:
        return
    missing = [c for c in expected if c not in received]
    unexpected = [c for c in received if c not in expected]
    if not missing and not unexpected:
        raise LayoutError(f"{layout.name}: columns in the wrong order")
    raise LayoutError(f"{layout.name}: layout changed. Missing: {missing or '-'}; unexpected: {unexpected or '-'}")


def list_files(spark: SparkSession, directory: str, pattern: str) -> list[str]:
    """Glob through the Hadoop FileSystem: works the same on local disk, ADLS (abfss://) and DBFS."""
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{directory.rstrip('/')}/{pattern}")
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    statuses = fs.globStatus(path) or []
    return sorted(str(s.getPath().toString()) for s in statuses if s.isFile())


def _read_csv(spark: SparkSession, file: str, source: Source, layout: Layout) -> DataFrame:
    schema = StructType(
        [StructField(name, StringType()) for name in layout.names] + [StructField(CORRUPT_COLUMN, StringType())]
    )
    return (
        spark.read.schema(schema)
        .option("header", True)
        .option("sep", source.separator)
        .option("encoding", source.encoding)
        .option("quote", NO_QUOTE)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .csv(file)
    )


def _already_ingested(spark: SparkSession, target: str, file_name: str) -> bool:
    if not exists(spark, target):
        return False
    return read(spark, target).where(F.col("_source_file") == file_name).limit(1).count() > 0


def _ingest_file(
    spark: SparkSession, cfg: Config, source: Source, layout: Layout, file: str, run: Run, reprocess: bool
) -> None:
    name = file.rsplit("/", 1)[-1]
    target = cfg.path("bronze", source.name)
    run.details["file"] = name
    if source.kind == "snapshot":
        run.reference_date = reference_date_from_filename(name)
        if run.reference_date is None:
            raise LayoutError(f"{name}: file name has no DDMMYYYY date")
        if not reprocess and _already_ingested(spark, target, name):
            run.status = "skipped"
            log.info(f"{name} already ingested; skipping")
            return
    else:
        run.reference_date = date.today()

    first = spark.read.text(file).first()
    validate_header(first[0] if first else "", layout, source.separator)

    df = _read_csv(spark, file, source, layout).select(
        "*",
        F.lit(name).alias("_source_file"),
        F.lit(run.reference_date).alias("_reference_date"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(run.run_id).alias("_run_id"),
        F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in layout.names]), 256).alias("_record_hash"),
    )
    if source.kind == "snapshot":
        overwrite_partition(df, target, "_reference_date", run.reference_date)
        written = read(spark, target).where(F.col("_reference_date") == F.lit(run.reference_date))
    else:
        overwrite(df, target)
        written = read(spark, target)

    counts = written.agg(F.count(F.lit(1)).alias("rows"), F.count(CORRUPT_COLUMN).alias("corrupt")).first()
    run.rows_in = run.rows_out = int(counts["rows"])
    run.details["corrupt_rows"] = int(counts["corrupt"])
    check_volume(spark, cfg, run)


def ingest(spark: SparkSession, cfg: Config, source_name: str, run_id: str, reprocess: bool = False) -> int:
    """Ingests every file of the source found in the input folder. Returns how many files were read."""
    source = cfg.sources[source_name]
    layout = LAYOUTS[source_name]
    files = list_files(spark, cfg.input_path, source.pattern)
    if not files:
        log.warning(f"No {source.pattern} file in {cfg.input_path}")
        return 0
    read_count = 0
    for file in files:
        with track(spark, cfg, "bronze", source_name, run_id) as run:
            _ingest_file(spark, cfg, source, layout, file, run, reprocess)
            read_count += run.status != "skipped"
    return read_count


def ingest_all(spark: SparkSession, cfg: Config, run_id: str, reprocess: bool = False) -> None:
    for source_name in cfg.sources:
        ingest(spark, cfg, source_name, run_id, reprocess)


def _months_before(reference: date, months: int) -> date:
    year, month = reference.year, reference.month - months
    while month <= 0:
        month += 12
        year -= 1
    return date(year, month, 1)


def apply_retention(spark: SparkSession, cfg: Config, run_id: str, today: date | None = None) -> None:
    """Deletes bronze partitions older than the retention period (LGPD art. 15 and 16).

    DELETE marks the files as removed; VACUUM physically deletes what is past the Delta retention
    window (7 days by default, which keeps short-term time travel).
    """
    cutoff = _months_before(today or date.today(), cfg.bronze_retention_months)
    for name, source in cfg.sources.items():
        target = cfg.path("bronze", name)
        if source.kind != "snapshot" or not exists(spark, target):
            continue
        with track(spark, cfg, "retention", name, run_id) as run:
            table = DeltaTable.forPath(spark, target)
            table.delete(F.col("_reference_date") < F.lit(cutoff))
            metrics = table.history(1).select("operationMetrics").first()[0] or {}
            run.rows_out = int(metrics.get("numDeletedRows", 0))
            run.details["cutoff"] = cutoff.isoformat()
            table.vacuum()
