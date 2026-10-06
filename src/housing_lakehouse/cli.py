"""Command line: `housing <step>`. Entry point for Airflow, ADF and Databricks."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Any

from housing_lakehouse.config import load_config
from housing_lakehouse.observability import new_run_id, setup_logging
from housing_lakehouse.quality import QualityError

log = logging.getLogger("housing_lakehouse")

MONITORED_TABLES = ["cadunico_family", "cadunico_person", "mcmv_beneficiaries"]


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="Configuration YAML (default: the packaged one)")
    common.add_argument("--run-id", help="Correlates records with the orchestrator run")

    parser = argparse.ArgumentParser(prog="housing", description="CadÚnico × social housing lakehouse")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate-data", parents=[common], help="Writes synthetic files to the input folder")
    g.add_argument("--municipalities", type=int, default=200)
    g.add_argument("--families", type=int, default=20_000)
    g.add_argument("--months", type=int, default=2)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--defect-rate", type=float, default=0.01)

    sub.add_parser("bronze", parents=[common], help="Ingests new files").add_argument(
        "--reprocess", action="store_true", help="Rewrites partitions of files already ingested"
    )
    sub.add_parser("silver", parents=[common], help="Cleans, validates and pseudonymizes").add_argument(
        "--table", choices=["all", "references", "families", "persons", "beneficiaries"], default="all"
    )
    sub.add_parser("gold", parents=[common], help="Computes the indicators")
    sub.add_parser("ml", parents=[common], help="Trains the forecast and optimizes the allocation").add_argument(
        "--no-mlflow", action="store_true"
    )
    sub.add_parser("publish", parents=[common], help="Publishes municipality profiles to MongoDB").add_argument(
        "--mongo-uri", default=os.getenv("MONGO_URI")
    )
    sub.add_parser("pipeline", parents=[common], help="bronze → silver → gold → ml (→ publish if MONGO_URI)")
    sub.add_parser("monitor", parents=[common], help="Table freshness and latest runs")
    sub.add_parser("retention", parents=[common], help="Deletes raw data past the retention period")
    sub.add_parser("catalog", parents=[common], help="Generates docs/lgpd_catalog.md").add_argument(
        "--output", default="docs/lgpd_catalog.md"
    )
    return parser


def _run_pipeline(spark: Any, cfg: Any, run_id: str) -> None:
    from housing_lakehouse.layers import bronze, gold, silver
    from housing_lakehouse.ml import run as ml

    bronze.ingest_all(spark, cfg, run_id)
    silver.process(spark, cfg, run_id)
    gold.process(spark, cfg, run_id)
    ml.process(spark, cfg, run_id)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    setup_logging()
    cfg = load_config(args.config)
    run_id = args.run_id or new_run_id()

    if args.command == "generate-data":
        from housing_lakehouse.synthetic.generator import generate

        summary = generate(
            cfg.input_path,
            n_municipalities=args.municipalities,
            n_families=args.families,
            months=args.months,
            seed=args.seed,
            defect_rate=args.defect_rate,
            bands=cfg.income_bands,
        )
        log.info(f"Synthetic data written to {cfg.input_path}", extra={"context": summary})
        return 0

    if args.command == "catalog":
        from housing_lakehouse.governance.catalog import render_markdown

        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(render_markdown(), encoding="utf-8")
        log.info(f"Catalog written to {args.output}")
        return 0

    from housing_lakehouse.spark import get_spark

    spark = get_spark()
    try:
        if args.command == "bronze":
            from housing_lakehouse.layers import bronze

            bronze.ingest_all(spark, cfg, run_id, reprocess=args.reprocess)
        elif args.command == "silver":
            from housing_lakehouse.layers import silver

            silver.process(spark, cfg, run_id, args.table)
        elif args.command == "gold":
            from housing_lakehouse.layers import gold

            gold.process(spark, cfg, run_id)
        elif args.command == "ml":
            from housing_lakehouse.ml import run as ml

            ml.process(spark, cfg, run_id, use_mlflow=not args.no_mlflow)
        elif args.command == "publish":
            if not args.mongo_uri:
                log.error("Pass --mongo-uri or set MONGO_URI")
                return 1
            from housing_lakehouse.publishing import mongo

            mongo.process(spark, cfg, run_id, args.mongo_uri)
        elif args.command == "pipeline":
            _run_pipeline(spark, cfg, run_id)
            if os.getenv("MONGO_URI"):
                from housing_lakehouse.publishing import mongo

                mongo.process(spark, cfg, run_id, os.environ["MONGO_URI"])
        elif args.command == "monitor":
            from housing_lakehouse.observability import check_freshness
            from housing_lakehouse.storage import exists, read

            check_freshness(spark, cfg, MONITORED_TABLES)
            path = cfg.path("audit", "runs")
            if exists(spark, path):
                read(spark, path).orderBy("started_at", ascending=False).select(
                    "started_at", "stage", "table_name", "reference_date", "status", "duration_s",
                    "rows_in", "rows_out", "rows_quarantined",
                ).show(30, truncate=False)  # fmt: skip
        elif args.command == "retention":
            from housing_lakehouse.layers import bronze

            bronze.apply_retention(spark, cfg, run_id)
    except QualityError as error:
        log.error(str(error))
        return 2
    finally:
        if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
            spark.stop()
    return 0
