"""Reads and writes of the lakehouse Delta tables (by path: local, ADLS or DBFS)."""

from __future__ import annotations

from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def overwrite(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def overwrite_partition(df: DataFrame, path: str, column: str, value: date) -> None:
    """Rewrites only the `column = value` partition: reloading the same file never duplicates data."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{column} = '{value.isoformat()}'")
        .partitionBy(column)
        .save(path)
    )


def append(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def upsert(spark: SparkSession, df: DataFrame, path: str, key: str, version_column: str) -> dict[str, int]:
    """MERGE on the key: inserts new rows and updates only rows with a newer version than the stored one
    (unchanged records are not rewritten). Returns the MERGE metrics."""
    if not exists(spark, path):
        df.write.format("delta").save(path)
        return {"rows_inserted": df.count(), "rows_updated": 0}
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition=f"t.{version_column} IS NULL OR s.{version_column} > t.{version_column}")
        .whenNotMatchedInsertAll()
        .execute()
    )
    metrics = target.history(1).select("operationMetrics").first()[0] or {}
    return {
        "rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
    }
