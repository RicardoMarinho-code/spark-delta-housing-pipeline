"""End-to-end pipeline on synthetic data: runs once and several tests inspect the result."""

from __future__ import annotations

import json
import re

import pytest

pytestmark = pytest.mark.spark

CPF_PATTERN = re.compile(r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$")


@pytest.fixture(scope="module")
def lakehouse(spark, tmp_path_factory):
    from housing_lakehouse.config import load_config
    from housing_lakehouse.layers import bronze, gold, silver
    from housing_lakehouse.ml import run as ml
    from housing_lakehouse.synthetic.generator import generate

    root = tmp_path_factory.mktemp("e2e")
    cfg = load_config(input_path=str(root / "input"), lakehouse_path=str(root / "lakehouse"))
    generate(cfg.input_path, n_municipalities=40, n_families=2_000, months=2, seed=11, defect_rate=0.02)
    bronze.ingest_all(spark, cfg, "run-1")
    silver.process(spark, cfg, "run-1")
    gold.process(spark, cfg, "run-1")
    ml.process(spark, cfg, "run-1", use_mlflow=False)
    return cfg


def _table(spark, cfg, layer, table):
    from housing_lakehouse.storage import read

    return read(spark, cfg.path(layer, table))


def _audit(spark, cfg):
    return _table(spark, cfg, "audit", "runs")


def test_every_step_is_audited_as_success(spark, lakehouse):
    rows = _audit(spark, lakehouse).collect()
    assert all(r.status == "success" for r in rows), [(r.table_name, r.error) for r in rows if r.status != "success"]
    steps = {(r.stage, r.table_name) for r in rows}
    for expected in [
        ("bronze", "cadunico_family"),
        ("silver", "cadunico_family"),
        ("silver", "cadunico_person"),
        ("silver", "mcmv_beneficiaries"),
        ("gold", "municipality_precariousness"),
        ("gold", "mcmv_targeting"),
        ("ml", "deficit_forecast"),
        ("ml", "unit_allocation"),
    ]:
        assert expected in steps


def test_bronze_is_partitioned_and_idempotent(spark, lakehouse):
    from housing_lakehouse.layers import bronze

    before = _table(spark, lakehouse, "bronze", "cadunico_family").count()
    months = _table(spark, lakehouse, "bronze", "cadunico_family").select("_reference_date").distinct().count()
    assert months == 2
    assert bronze.ingest(spark, lakehouse, "cadunico_family", "run-2") == 0  # already ingested: skipped
    assert _table(spark, lakehouse, "bronze", "cadunico_family").count() == before
    skipped = _audit(spark, lakehouse).where("run_id = 'run-2' AND status = 'skipped'").count()
    assert skipped == 2


def test_silver_is_incremental_with_merge(spark, lakehouse):
    runs = (
        _audit(spark, lakehouse)
        .where("stage = 'silver' AND table_name = 'cadunico_family'")
        .orderBy("reference_date")
        .collect()
    )
    assert len(runs) == 2
    second = json.loads(runs[1].details)["merge"]
    assert second["rows_inserted"] > 0  # families registered during the month
    assert second["rows_updated"] > 0  # families that updated their registry
    families = _table(spark, lakehouse, "silver", "cadunico_family")
    assert families.count() == families.select("family_id").distinct().count()


def test_quarantine_gets_defects_without_raw_data(spark, lakehouse):
    quarantine = _table(spark, lakehouse, "quarantine", "cadunico_family")
    reasons = {m for r in quarantine.select("_failures").collect() for m in r._failures}
    assert {"layout_valid", "municipality_valid", "income_non_negative", "unique_record"} <= reasons
    assert "_corrupt_record" not in quarantine.columns
    assert "no_logradouro_fam" not in quarantine.columns


def test_no_personal_data_in_clear_text(spark, lakehouse):
    """Scans every text column of silver, quarantine and gold for names and CPFs taken from bronze."""
    from pyspark.sql import functions as F

    bronze_persons = _table(spark, lakehouse, "bronze", "cadunico_person")
    names = {r[0] for r in bronze_persons.select("NO_PESSOA").where("NO_PESSOA IS NOT NULL").limit(500).collect()}
    cpfs = {r[0] for r in bronze_persons.select("NU_CPF_PESSOA").where("NU_CPF_PESSOA <> ''").limit(500).collect()}
    forbidden = names | cpfs
    for layer, table in [
        ("silver", "cadunico_family"),
        ("silver", "cadunico_person"),
        ("silver", "mcmv_beneficiaries"),
        ("quarantine", "cadunico_person"),
        ("gold", "review_flags"),
    ]:
        df = _table(spark, lakehouse, layer, table)
        text_columns = [c for c, t in df.dtypes if t == "string"]
        for row in df.select(*[F.col(c) for c in text_columns]).collect():
            for value in row:
                assert value not in forbidden, f"{layer}.{table} leaked {value!r}"
                assert not (value and CPF_PATTERN.match(value)), f"{layer}.{table} has a CPF {value!r}"


def test_join_through_the_pseudonym(spark, lakehouse):
    targeting = _table(spark, lakehouse, "gold", "mcmv_targeting").groupBy().sum("n_contracts", "n_in_cadunico")
    contracts, in_cadunico = targeting.first()
    assert 0 < in_cadunico < contracts  # contracts both from people in and outside CadÚnico
    flags = _table(spark, lakehouse, "gold", "review_flags")
    assert flags.count() > 0
    assert set(flags.columns) >= {"cpf_pseudo", "reasons"}


def test_gold_and_ml(spark, lakehouse):
    precariousness = _table(spark, lakehouse, "gold", "municipality_precariousness")
    assert precariousness.where("deficit_rate < 0 OR deficit_rate > 1").count() == 0
    assert precariousness.where("precariousness_index IS NULL").count() == 0
    forecast = _table(spark, lakehouse, "gold", "deficit_forecast")
    assert forecast.where("relative_deficit_forecast < 0").count() == 0
    allocation = _table(spark, lakehouse, "gold", "unit_allocation").toPandas()
    assert allocation["investment"].sum() <= lakehouse.allocation_budget
    assert (allocation["allocated_units"] <= allocation["capacity"]).all()
