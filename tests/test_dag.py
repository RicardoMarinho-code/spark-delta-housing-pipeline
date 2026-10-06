"""DAG integrity: runs only where Airflow is installed (the `dag` CI job)."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")


def test_dag_loads_without_errors_and_has_the_right_order():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(Path(__file__).parents[1] / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("housing_lakehouse")
    assert dag is not None
    tasks = {t.task_id: t for t in dag.tasks}
    assert tasks["silver_persons"].upstream_task_ids == {"silver_families"}
    assert tasks["gold"].upstream_task_ids == {"silver_persons", "silver_beneficiaries"}
    assert tasks["monitor"].trigger_rule == "all_done"
    assert "--run-id" in tasks["bronze"].bash_command
