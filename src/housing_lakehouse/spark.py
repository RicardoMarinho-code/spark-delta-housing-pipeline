"""SparkSession factory: reuses the Databricks session or starts a local one with Delta Lake."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def configure(spark: SparkSession) -> SparkSession:
    # Invalid dates become null (and go to quarantine) instead of raising from the legacy parser.
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")
    return spark


def get_spark(app_name: str = "housing-lakehouse") -> SparkSession:
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return configure(SparkSession.builder.getOrCreate())
    active = SparkSession.getActiveSession()
    if active is not None:
        return configure(active)

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.enabled", os.getenv("SPARK_UI", "false"))
    )
    return configure(configure_spark_with_delta_pip(builder).getOrCreate())
