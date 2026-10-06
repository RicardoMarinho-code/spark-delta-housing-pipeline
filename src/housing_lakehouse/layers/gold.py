"""Gold: municipality-level indicators ready for analysis, ML and publishing.

- municipality_precariousness: housing deficit and inadequacy estimated from CadÚnico + a composite index;
- mcmv_targeting: how well contracts reach the CadÚnico population, by program and municipality;
- review_flags: contracts with income above the band or a repeated CPF (pseudonymized, restricted
  access; a flag for review, not a conclusion);
- service_gap: families in deficit × contracted units × FJP official deficit.

Gold tables are internal (exact counts). Disclosure control is applied when publishing.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from housing_lakehouse.config import Config
from housing_lakehouse.housing_rules import (
    DEFICIT_COMPONENTS,
    INADEQUACIES,
    REGISTERED_STATUS,
    band_ceiling,
    classify_households,
)
from housing_lakehouse.observability import track
from housing_lakehouse.storage import overwrite, read


def _rate(numerator: str, denominator: str) -> Column:
    return F.when(F.col(denominator) > 0, F.round(F.col(numerator) / F.col(denominator), 4))


def municipality_precariousness(families: DataFrame, municipalities: DataFrame) -> DataFrame:
    base = classify_households(families).where(F.col("registry_status") == REGISTERED_STATUS)
    agg = base.groupBy("municipality_code").agg(
        F.count(F.lit(1)).alias("n_families"),
        F.count("deficit_component").alias("n_families_deficit"),
        *[F.count(F.when(F.col("deficit_component") == c, True)).alias(f"n_{c}") for c in DEFICIT_COMPONENTS],
        *[F.count(F.when(F.col(i), True)).alias(f"n_{i}") for i in INADEQUACIES],
        F.percentile_approx("per_capita_income", 0.5).cast("double").alias("median_per_capita_income"),
        F.max("_reference_date").alias("reference_date"),
    )
    agg = agg.withColumn("deficit_rate", _rate("n_families_deficit", "n_families"))
    for name in (*DEFICIT_COMPONENTS, *INADEQUACIES):
        agg = agg.withColumn(f"{name}_rate", _rate(f"n_{name}", "n_families"))
    mean_inadequacy = sum(F.col(f"{i}_rate") for i in INADEQUACIES) / len(INADEQUACIES)
    agg = agg.withColumn(
        "precariousness_index", F.round(100 * (0.5 * F.col("deficit_rate") + 0.5 * mean_inadequacy), 2)
    )
    return agg.join(
        municipalities.select("municipality_code", "municipality_name", "state", "region"), "municipality_code", "left"
    )


def _cross_beneficiaries(
    beneficiaries: DataFrame, persons: DataFrame, families: DataFrame, bands: dict[int, float]
) -> DataFrame:
    """Links each contract to its CadÚnico family through the CPF pseudonym (the clear CPF is never used)."""
    cpf_family = (
        persons.where(F.col("cpf_pseudo").isNotNull()).select("cpf_pseudo", "family_id").dropDuplicates(["cpf_pseudo"])
    )
    family = classify_households(families).select(
        "family_id", F.col("total_income").alias("cadunico_income"), "deficit_component"
    )
    contracts_per_cpf = beneficiaries.groupBy("cpf_pseudo").agg(
        F.count(F.lit(1)).alias("contracts_per_cpf"), F.countDistinct("program").alias("programs_per_cpf")
    )
    ceiling = band_ceiling(F.col("income_band"), bands)
    return (
        beneficiaries.join(cpf_family, "cpf_pseudo", "left")
        .join(family, "family_id", "left")
        .join(contracts_per_cpf, "cpf_pseudo", "left")
        .withColumn("in_cadunico", F.col("family_id").isNotNull())
        .withColumn("declared_income_above_band", F.coalesce(F.col("declared_income") > ceiling, F.lit(False)))
        .withColumn("cadunico_income_above_band", F.coalesce(F.col("cadunico_income") > ceiling, F.lit(False)))
        .withColumn("multiple_contracts", F.col("contracts_per_cpf") > 1)
        .withColumn("family_in_deficit", F.col("deficit_component").isNotNull())
    )


def mcmv_targeting(crossed: DataFrame) -> DataFrame:
    agg = crossed.groupBy("program", "municipality_code").agg(
        F.count(F.lit(1)).alias("n_contracts"),
        F.count(F.when(F.col("in_cadunico"), True)).alias("n_in_cadunico"),
        F.count(F.when(F.col("family_in_deficit"), True)).alias("n_family_in_deficit"),
        F.count(F.when(F.col("declared_income_above_band"), True)).alias("n_declared_income_above_band"),
        F.count(F.when(F.col("cadunico_income_above_band"), True)).alias("n_cadunico_income_above_band"),
        F.count(F.when(F.col("multiple_contracts"), True)).alias("n_multiple_contracts"),
    )
    for name in ("in_cadunico", "family_in_deficit", "declared_income_above_band", "multiple_contracts"):
        agg = agg.withColumn(f"{name}_rate", _rate(f"n_{name}", "n_contracts"))
    return agg


def review_flags(crossed: DataFrame) -> DataFrame:
    reasons = F.filter(
        F.array(
            F.when(F.col("declared_income_above_band"), F.lit("declared_income_above_band")),
            F.when(F.col("cadunico_income_above_band"), F.lit("cadunico_income_above_band")),
            F.when(F.col("multiple_contracts"), F.lit("multiple_contracts")),
        ),
        lambda x: x.isNotNull(),
    )
    return (
        crossed.withColumn("reasons", reasons)
        .where(F.size("reasons") > 0)
        .select(
            "cpf_pseudo",
            "program",
            "municipality_code",
            "contract_date",
            "income_band",
            "declared_income",
            "cadunico_income",
            "contracts_per_cpf",
            "reasons",
        )
    )


def service_gap(
    precariousness: DataFrame, beneficiaries: DataFrame, fjp_deficit: DataFrame, municipalities: DataFrame
) -> DataFrame:
    latest_year = fjp_deficit.agg(F.max("year")).first()[0]
    fjp = fjp_deficit.where(F.col("year") == latest_year).select(
        "municipality_code", F.col("deficit_total").alias("fjp_deficit"), F.col("year").alias("fjp_year")
    )
    units = beneficiaries.groupBy("municipality_code").agg(F.count(F.lit(1)).alias("n_units_contracted"))
    return (
        municipalities.select("municipality_code", "municipality_name", "state", "region", "households")
        .join(
            precariousness.select("municipality_code", "n_families", "n_families_deficit"), "municipality_code", "left"
        )
        .join(units, "municipality_code", "left")
        .join(fjp, "municipality_code", "left")
        .fillna(0, subset=["n_families", "n_families_deficit", "n_units_contracted"])
        .withColumn("gap_cadunico", F.greatest(F.col("n_families_deficit") - F.col("n_units_contracted"), F.lit(0)))
        .withColumn("gap_fjp", F.greatest(F.col("fjp_deficit") - F.col("n_units_contracted"), F.lit(0)))
        .withColumn("coverage_cadunico", _rate("n_units_contracted", "n_families_deficit"))
        .withColumn("coverage_fjp", _rate("n_units_contracted", "fjp_deficit"))
    )


def process(spark: SparkSession, cfg: Config, run_id: str) -> None:
    silver = {
        t: read(spark, cfg.path("silver", t))
        for t in ("cadunico_family", "cadunico_person", "mcmv_beneficiaries", "dim_municipality", "fjp_deficit")
    }

    with track(spark, cfg, "gold", "municipality_precariousness", run_id) as run:
        precariousness = municipality_precariousness(silver["cadunico_family"], silver["dim_municipality"])
        overwrite(precariousness, cfg.path("gold", "municipality_precariousness"))
        precariousness = read(spark, cfg.path("gold", "municipality_precariousness"))
        run.rows_out = precariousness.count()
        run.reference_date = precariousness.agg(F.max("reference_date")).first()[0]

    crossed = _cross_beneficiaries(
        silver["mcmv_beneficiaries"], silver["cadunico_person"], silver["cadunico_family"], cfg.income_bands
    ).cache()
    try:
        for table, build in (("mcmv_targeting", mcmv_targeting), ("review_flags", review_flags)):
            with track(spark, cfg, "gold", table, run_id) as run:
                overwrite(build(crossed), cfg.path("gold", table))
                run.rows_out = read(spark, cfg.path("gold", table)).count()
    finally:
        crossed.unpersist()

    with track(spark, cfg, "gold", "service_gap", run_id) as run:
        gap = service_gap(
            precariousness, silver["mcmv_beneficiaries"], silver["fjp_deficit"], silver["dim_municipality"]
        )
        overwrite(gap, cfg.path("gold", "service_gap"))
        run.rows_out = read(spark, cfg.path("gold", "service_gap")).count()
