"""Monthly DAG of the housing lakehouse.

    wait_for_files → bronze → silver_references → silver_families → silver_persons ┐
                                                └→ silver_beneficiaries ──────────┴→ gold → ml → publish
    monitor always runs at the end (trigger_rule=all_done): freshness, volume and run report.

Executor chosen by HOUSING_EXECUTOR:
- local (default): each task runs `housing <step>` on the Airflow worker;
- databricks: each task becomes a Databricks run with the project wheel.

The Airflow `run_id` is passed as --run-id, so the lakehouse audit and the Airflow history share
the same key.
"""

from __future__ import annotations

import os
import shlex
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.sensors.python import PythonSensor
    from airflow.sdk import DAG
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.sensors.python import PythonSensor

EXECUTOR = os.getenv("HOUSING_EXECUTOR", "local")
WHEEL = os.getenv("HOUSING_WHEEL", "/Volumes/housing/artifacts/wheels/housing_lakehouse-0.1.0-py3-none-any.whl")
CLUSTER_ID = os.getenv("HOUSING_CLUSTER_ID", "")
RUN_ID = "{{ run_id }}"


def _on_failure(context: dict) -> None:
    from housing_lakehouse.observability import alert

    task = context["task_instance"]
    alert(f"[Airflow] {task.dag_id}.{task.task_id} failed (run {context['run_id']})")


def _cadunico_file_arrived() -> bool:
    """Releases the flow once this month's family file is in the input folder (local mode)."""
    from pathlib import Path

    from housing_lakehouse.config import load_config

    cfg = load_config()
    return any(Path(cfg.input_path).glob(cfg.sources["cadunico_family"].pattern))


def step(task_id: str, *arguments: str, **kwargs):
    parameters = [*arguments, "--run-id", RUN_ID]
    if EXECUTOR == "databricks":
        from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

        return DatabricksSubmitRunOperator(
            task_id=task_id,
            databricks_conn_id="databricks_default",
            json={
                "run_name": f"housing-{task_id}",
                "existing_cluster_id": CLUSTER_ID,
                "python_wheel_task": {
                    "package_name": "housing_lakehouse",
                    "entry_point": "housing",
                    "parameters": parameters,
                },
                "libraries": [{"whl": WHEEL}],
            },
            **kwargs,
        )
    return BashOperator(
        task_id=task_id, bash_command="housing " + " ".join(shlex.quote(p) for p in parameters), **kwargs
    )


with DAG(
    dag_id="housing_lakehouse",
    description="CadÚnico × social housing: bronze → silver (LGPD) → gold → ML → MongoDB",
    schedule="0 6 12 * *",  # 12th of each month, after the CadÚnico extract arrives
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(hours=4),
        "on_failure_callback": _on_failure,
    },
    tags=["cadunico", "housing", "lgpd"],
) as dag:
    if EXECUTOR == "local":
        wait_for_files = PythonSensor(
            task_id="wait_for_files",
            python_callable=_cadunico_file_arrived,
            mode="reschedule",
            poke_interval=60 * 60,
            timeout=3 * 24 * 60 * 60,
        )
    bronze = step("bronze", "bronze")
    references = step("silver_references", "silver", "--table", "references")
    families = step("silver_families", "silver", "--table", "families")
    persons = step("silver_persons", "silver", "--table", "persons")
    beneficiaries = step("silver_beneficiaries", "silver", "--table", "beneficiaries")
    gold = step("gold", "gold")
    ml = step("ml", "ml")
    publish = step("publish", "publish")
    monitor = step("monitor", "monitor", trigger_rule="all_done", retries=0)

    if EXECUTOR == "local":
        wait_for_files >> bronze
    bronze >> references >> [families, beneficiaries]
    families >> persons
    [persons, beneficiaries] >> gold >> ml >> publish >> monitor
