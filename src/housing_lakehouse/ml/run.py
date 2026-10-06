"""ML step of the pipeline: reads gold, trains, logs to MLflow and writes forecast and allocation to gold."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from housing_lakehouse.config import Config
from housing_lakehouse.ml.allocation import allocate
from housing_lakehouse.ml.forecast import build_dataset, log_to_mlflow, train
from housing_lakehouse.observability import track
from housing_lakehouse.storage import overwrite, read


def process(spark: SparkSession, cfg: Config, run_id: str, use_mlflow: bool = True) -> None:
    precariousness = read(spark, cfg.path("gold", "municipality_precariousness")).toPandas()
    municipalities = read(spark, cfg.path("silver", "dim_municipality")).toPandas()
    fjp_deficit = read(spark, cfg.path("silver", "fjp_deficit")).toPandas()

    with track(spark, cfg, "ml", "deficit_forecast", run_id) as run:
        result = train(build_dataset(precariousness, municipalities, fjp_deficit))
        run.details["metrics"] = result.metrics
        if use_mlflow:
            local_dir = str(Path(cfg.lakehouse_path).parent / "mlruns") if "://" not in cfg.lakehouse_path else ""
            run.details["mlflow_run_id"] = log_to_mlflow(
                result, local_dir, {"run_id": run_id, "target": "relative_deficit_fjp"}
            )
        predictions = result.predictions.assign(run_id=run_id)
        overwrite(spark.createDataFrame(predictions), cfg.path("gold", "deficit_forecast"))
        run.rows_out = len(predictions)

    with track(spark, cfg, "ml", "unit_allocation", run_id) as run:
        inputs = predictions.merge(
            precariousness[["municipality_code", "precariousness_index"]], on="municipality_code", how="left"
        )
        inputs["precariousness_index"] = inputs["precariousness_index"].astype(float)
        allocated = allocate(
            inputs,
            budget=cfg.allocation_budget,
            unit_cost_by_region=cfg.unit_cost_by_region,
            execution_cap=cfg.execution_cap,
            min_region_share=cfg.min_region_share,
        )
        columns = [
            "municipality_code",
            "municipality_name",
            "state",
            "region",
            "deficit_forecast_units",
            "precariousness_index",
            "unit_cost",
            "capacity",
            "allocated_units",
            "investment",
        ]
        overwrite(spark.createDataFrame(allocated[columns]), cfg.path("gold", "unit_allocation"))
        run.rows_out = int((allocated["allocated_units"] > 0).sum())
        run.details["total_units"] = int(allocated["allocated_units"].sum())
        run.details["total_investment"] = float(allocated["investment"].sum())
