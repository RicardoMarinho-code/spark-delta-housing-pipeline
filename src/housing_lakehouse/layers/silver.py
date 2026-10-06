"""Silver: clean, typed, validated and pseudonymized data.

Flow for each table:
    bronze (pending partition) → cleaning and typing → LGPD policy → enrichment
    → quality rules → quarantine + metrics → circuit breaker → MERGE/overwrite into silver

The load is incremental: it only processes bronze reference dates without a recorded success in the
audit table, in chronological order. Families and persons are MERGEd (the latest update wins);
beneficiaries and reference tables are snapshots and overwrite the table.
"""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from housing_lakehouse.config import Config
from housing_lakehouse.governance.documents import ibge_is_valid
from housing_lakehouse.governance.policy import apply_privacy_policy
from housing_lakehouse.governance.pseudonymization import get_key
from housing_lakehouse.housing_rules import band_ceiling
from housing_lakehouse.layouts import BENEFICIARIES, FAMILY, FJP_DEFICIT, MUNICIPALITIES, PERSON, Layout
from housing_lakehouse.layouts import Column as LayoutColumn
from housing_lakehouse.observability import Run, record_quality, track
from housing_lakehouse.quality import WARNING, QualityResult, Rule, enforce_threshold, evaluate
from housing_lakehouse.storage import exists, overwrite, overwrite_partition, read, upsert

log = logging.getLogger("housing_lakehouse")

DATE_FORMATS = ("dd/MM/yyyy", "yyyy-MM-dd", "ddMMyyyy")
KEPT_TECHNICAL = ("_reference_date", "_source_file", "_warnings")
PROGRAMS = ("FAR", "FDS", "PNHR", "FGTS")


# ---------------------------------------------------------------- cleaning and typing
def _type_expr(c: LayoutColumn) -> str:
    n = f"`{c.name}`"
    if c.dtype == "int":
        return f"try_cast({n} AS INT)"
    if c.dtype == "dec":
        # accepts 1234.56 and 1.234,56
        return (
            f"try_cast(CASE WHEN {n} LIKE '%,%' THEN replace(replace({n}, '.', ''), ',', '.') "
            f"ELSE {n} END AS DECIMAL(14,2))"
        )
    if c.dtype == "date":
        attempts = ", ".join(f"try_to_timestamp({n}, '{f}')" for f in DATE_FORMATS)
        return f"CAST(coalesce({attempts}) AS DATE)"
    return n


def prepare(df: DataFrame, layout: Layout) -> DataFrame:
    """Strips \\x00 and whitespace, turns empty strings into null and casts without raising (ANSI-safe).

    `_invalid_types` lists the columns whose value could not be converted. The raw line of corrupt
    records stays in bronze only: here there is just the `_layout_invalid` flag.
    """
    cleaned = []
    for name in layout.names:
        value = F.trim(F.regexp_replace(F.col(name), r"\x00", ""))
        cleaned.append(F.when(value != "", value).alias(name))
    discarded = {"_corrupt_record", "_ingested_at", "_run_id"}
    technical = [c for c in df.columns if c.startswith("_") and c not in discarded]
    clean = df.select(*cleaned, F.col("_corrupt_record").isNotNull().alias("_layout_invalid"), *technical)

    typed = [f"{_type_expr(c)} AS `{c.name}`" for c in layout.columns]
    invalid = [
        f"CASE WHEN `{c.name}` IS NOT NULL AND ({_type_expr(c)}) IS NULL THEN '{c.name}' END"
        for c in layout.columns
        if c.dtype != "str"
    ]
    invalid_list = f"filter(array({', '.join(invalid)}), x -> x IS NOT NULL)" if invalid else "array()"
    others = [f"`{c}`" for c in clean.columns if c.startswith("_")]
    return clean.selectExpr(*typed, f"CAST({invalid_list} AS ARRAY<STRING>) AS _invalid_types", *others)


# ---------------------------------------------------------------- enrichment
def _flag_exists(df: DataFrame, reference: DataFrame, key: str, flag: str, broadcast: bool) -> DataFrame:
    ref = reference.select(key).distinct().withColumn(flag, F.lit(True))
    ref = F.broadcast(ref) if broadcast else ref
    return df.join(ref, key, "left").withColumn(flag, F.coalesce(F.col(flag), F.lit(False)))


def _flag_duplicates(df: DataFrame, keys: list[str], order: list[Column]) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(*order, F.col("_record_hash"))
    return df.withColumn("_duplicate", (F.row_number().over(window) > 1) & F.col(keys[0]).isNotNull())


def _to_silver(df: DataFrame, run_id: str) -> DataFrame:
    discard = [c for c in df.columns if c.startswith("_") and c not in KEPT_TECHNICAL]
    return df.drop(*discard).withColumn("_run_id", F.lit(run_id))


# ---------------------------------------------------------------- rules
def _common(key: str) -> list[Rule]:
    return [
        Rule("layout_valid", ~F.col("_layout_invalid"), description="Row field count differs from the layout"),
        Rule("types_valid", F.size("_invalid_types") == 0, WARNING, "Some value could not be converted"),
        Rule(f"{key}_present", F.col(key).isNotNull()),
        Rule("unique_record", ~F.col("_duplicate"), description="Repeated in the file; the latest is kept"),
        Rule(
            "municipality_valid", F.col("_municipality_exists"), description="IBGE code not in the municipality table"
        ),
    ]


def family_rules(reference_date: date) -> list[Rule]:
    return _common("family_id") + [
        Rule("income_non_negative", F.col("total_income") >= 0),
        Rule(
            "update_date_valid",
            F.col("last_update_date").isNotNull() & (F.col("last_update_date") <= F.lit(reference_date)),
            description="Missing, invalid or later than the file date",
        ),
        Rule("plausible_people_in_dwelling", F.col("people_in_dwelling").between(1, 30), WARNING),
        Rule("consistent_bedrooms", F.col("bedrooms") <= F.col("rooms"), WARNING),
        Rule("zip_valid", F.col("zip_prefix").isNotNull(), WARNING, "ZIP code without 8 digits"),
    ]


def person_rules() -> list[Rule]:
    return _common("person_id") + [
        Rule("family_id_present", F.col("family_id").isNotNull()),
        Rule("age_valid", F.col("age").between(0, 120), description="Birth date missing, invalid or in the future"),
        Rule("cpf_valid_when_present", F.col("cpf_status") != "invalid", WARNING),
        Rule("nis_valid_when_present", F.col("nis_status") != "invalid", WARNING),
        Rule("sex_valid", F.col("sex").isin(1, 2), WARNING),
        Rule("family_exists", F.col("_family_exists"), WARNING, "Family not in silver (or quarantined)"),
    ]


def beneficiary_rules(reference_date: date) -> list[Rule]:
    return [
        Rule("layout_valid", ~F.col("_layout_invalid")),
        Rule("types_valid", F.size("_invalid_types") == 0, WARNING),
        Rule("cpf_valid", F.col("cpf_status") == "valid", description="A contract needs a valid CPF"),
        Rule("unique_record", ~F.col("_duplicate")),
        Rule("program_valid", F.col("program").isin(*PROGRAMS)),
        Rule("municipality_valid", F.col("_municipality_exists")),
        Rule("income_non_negative", F.col("declared_income") >= 0),
        Rule("band_valid", F.col("band_ceiling").isNotNull()),
        Rule(
            "contract_date_valid",
            F.col("contract_date").isNotNull() & (F.col("contract_date") <= F.lit(reference_date)),
        ),
        Rule("plausible_age", F.col("age").between(18, 110), WARNING),
    ]


# ---------------------------------------------------------------- execution
def _validate(spark: SparkSession, cfg: Config, run: Run, df: DataFrame, rules: list[Rule]) -> QualityResult:
    result = evaluate(df, rules)
    run.rows_in = result.total
    run.rows_quarantined = result.quarantined
    run.rows_out = result.total - result.quarantined
    run.details["failures_by_rule"] = {m["rule"]: m["failures"] for m in result.metrics if m["failures"]}
    record_quality(spark, cfg, run, result.metrics)
    # Quarantine is already pseudonymized: it can be analyzed without access to bronze.
    overwrite_partition(
        result.quarantine.drop("_record_hash"),
        cfg.path("quarantine", run.table_name),
        "_reference_date",
        run.reference_date,
    )
    try:
        enforce_threshold(result, cfg.max_rejection_rate)
    except Exception:
        result.release()
        raise
    return result


def pending_references(spark: SparkSession, cfg: Config, table: str) -> list[date]:
    bronze = cfg.path("bronze", table)
    if not exists(spark, bronze):
        return []
    available = sorted(r[0] for r in read(spark, bronze).select("_reference_date").distinct().collect())
    audit = cfg.path("audit", "runs")
    done: set[date] = set()
    if exists(spark, audit):
        done = {
            r[0]
            for r in read(spark, audit)
            .where((F.col("stage") == "silver") & (F.col("table_name") == table) & (F.col("status") == "success"))
            .select("reference_date")
            .distinct()
            .collect()
        }
    return [d for d in available if d not in done]


def _bronze(spark: SparkSession, cfg: Config, table: str, reference_date: date | None = None) -> DataFrame:
    df = read(spark, cfg.path("bronze", table))
    return df if reference_date is None else df.where(F.col("_reference_date") == F.lit(reference_date))


def process_references(spark: SparkSession, cfg: Config, run_id: str) -> None:
    today = date.today()
    with track(spark, cfg, "silver", "dim_municipality", run_id) as run:
        run.reference_date = today
        df = apply_privacy_policy(
            prepare(_bronze(spark, cfg, "ref_municipalities"), MUNICIPALITIES), MUNICIPALITIES, b"", today
        )
        df = df.withColumn("_reference_date", F.lit(today))
        rules = [
            Rule("layout_valid", ~F.col("_layout_invalid")),
            Rule("ibge_code_valid", ibge_is_valid(F.col("municipality_code")), description="IBGE check digit"),
            Rule("state_valid", F.length("state") == 2),
            Rule("population_positive", F.col("population") > 0),
        ]
        result = _validate(spark, cfg, run, df, rules)
        try:
            overwrite(_to_silver(result.valid, run_id), cfg.path("silver", "dim_municipality"))
        finally:
            result.release()

    municipalities = read(spark, cfg.path("silver", "dim_municipality"))
    with track(spark, cfg, "silver", "fjp_deficit", run_id) as run:
        run.reference_date = today
        df = apply_privacy_policy(prepare(_bronze(spark, cfg, "ref_fjp_deficit"), FJP_DEFICIT), FJP_DEFICIT, b"", today)
        df = _flag_exists(df, municipalities, "municipality_code", "_municipality_exists", broadcast=True)
        df = df.withColumn("_reference_date", F.lit(today))
        rules = [
            Rule("layout_valid", ~F.col("_layout_invalid")),
            Rule("municipality_valid", F.col("_municipality_exists")),
            Rule("deficit_non_negative", F.col("deficit_total") >= 0),
        ]
        result = _validate(spark, cfg, run, df, rules)
        try:
            overwrite(_to_silver(result.valid, run_id), cfg.path("silver", "fjp_deficit"))
        finally:
            result.release()


def process_families(spark: SparkSession, cfg: Config, run_id: str) -> None:
    municipalities = read(spark, cfg.path("silver", "dim_municipality"))
    key = get_key()
    for ref in pending_references(spark, cfg, "cadunico_family"):
        with track(spark, cfg, "silver", "cadunico_family", run_id) as run:
            run.reference_date = ref
            df = apply_privacy_policy(prepare(_bronze(spark, cfg, "cadunico_family", ref), FAMILY), FAMILY, key, ref)
            df = df.withColumn(
                "per_capita_income",
                F.round(F.col("total_income") / F.greatest(F.col("family_members"), F.lit(1)), 2),
            )
            df = _flag_exists(df, municipalities, "municipality_code", "_municipality_exists", broadcast=True)
            df = _flag_duplicates(df, ["family_id"], [F.col("last_update_date").desc_nulls_last()])
            result = _validate(spark, cfg, run, df, family_rules(ref))
            try:
                run.details["merge"] = upsert(
                    spark,
                    _to_silver(result.valid, run_id),
                    cfg.path("silver", "cadunico_family"),
                    key="family_id",
                    version_column="last_update_date",
                )
            finally:
                result.release()


def process_persons(spark: SparkSession, cfg: Config, run_id: str) -> None:
    municipalities = read(spark, cfg.path("silver", "dim_municipality"))
    families = read(spark, cfg.path("silver", "cadunico_family"))
    key = get_key()
    for ref in pending_references(spark, cfg, "cadunico_person"):
        with track(spark, cfg, "silver", "cadunico_person", run_id) as run:
            run.reference_date = ref
            df = apply_privacy_policy(prepare(_bronze(spark, cfg, "cadunico_person", ref), PERSON), PERSON, key, ref)
            df = df.withColumn(
                "age_group",
                F.when(F.col("age") < 18, "0-17").when(F.col("age") < 60, "18-59").when(F.col("age") >= 60, "60+"),
            )
            df = _flag_exists(df, municipalities, "municipality_code", "_municipality_exists", broadcast=True)
            df = _flag_exists(df, families, "family_id", "_family_exists", broadcast=False)
            df = _flag_duplicates(df, ["person_id"], [F.col("registration_date").desc_nulls_last()])
            result = _validate(spark, cfg, run, df, person_rules())
            try:
                run.details["merge"] = upsert(
                    spark,
                    _to_silver(result.valid, run_id),
                    cfg.path("silver", "cadunico_person"),
                    key="person_id",
                    version_column="_reference_date",
                )
            finally:
                result.release()


def process_beneficiaries(spark: SparkSession, cfg: Config, run_id: str) -> None:
    pending = pending_references(spark, cfg, "mcmv_beneficiaries")
    if not pending:
        return
    ref = pending[-1]  # full snapshot: the latest one is enough
    municipalities = read(spark, cfg.path("silver", "dim_municipality"))
    with track(spark, cfg, "silver", "mcmv_beneficiaries", run_id) as run:
        run.reference_date = ref
        df = apply_privacy_policy(
            prepare(_bronze(spark, cfg, "mcmv_beneficiaries", ref), BENEFICIARIES), BENEFICIARIES, get_key(), ref
        )
        df = df.withColumn("program", F.upper("program"))
        df = df.withColumn("band_ceiling", band_ceiling(F.col("income_band"), cfg.income_bands))
        df = df.withColumn("income_within_band", F.col("declared_income") <= F.col("band_ceiling"))
        df = _flag_exists(df, municipalities, "municipality_code", "_municipality_exists", broadcast=True)
        df = _flag_duplicates(df, ["cpf_pseudo", "program", "contract_date"], [])
        result = _validate(spark, cfg, run, df, beneficiary_rules(ref))
        try:
            overwrite(_to_silver(result.valid, run_id), cfg.path("silver", "mcmv_beneficiaries"))
        finally:
            result.release()


STEPS = {
    "references": process_references,
    "families": process_families,
    "persons": process_persons,
    "beneficiaries": process_beneficiaries,
}


def process(spark: SparkSession, cfg: Config, run_id: str, table: str = "all") -> None:
    steps = STEPS if table == "all" else {table: STEPS[table]}
    for step in steps.values():
        step(spark, cfg, run_id)
