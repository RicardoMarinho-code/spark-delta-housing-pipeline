"""Housing business rules applied to CadÚnico data.

The housing deficit follows the methodology of Fundação João Pinheiro (FJP), Brazil's official
source, adapted to what CadÚnico records. Components are hierarchical (each family falls into
one, in this order):

1. precarious housing: improvised dwelling or makeshift walls (bare wattle and daub, scrap wood,
   straw, other material);
2. cohabitation: more than one family in the dwelling;
3. rent burden: rent above 30% of household income;
4. overcrowding in a rented dwelling: more than 3 people per bedroom.

Inadequacy (problems that do not require a new home) is counted separately: water, bathroom,
sewage, garbage, electricity and dirt floor.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

DEFICIT_COMPONENTS = ("precarious_housing", "cohabitation", "rent_burden", "rented_overcrowding")
INADEQUACIES = (
    "no_piped_water",
    "no_bathroom",
    "inadequate_sewage",
    "inadequate_garbage",
    "no_electricity",
    "dirt_floor",
)
REGISTERED_STATUS = 3
RENT_BURDEN_LIMIT = 0.30
MAX_PEOPLE_PER_BEDROOM = 3


def classify_households(families: DataFrame) -> DataFrame:
    rent = F.coalesce(F.col("expense_rent"), F.lit(0))
    income = F.coalesce(F.col("total_income"), F.lit(0))
    people_per_bedroom = F.col("people_in_dwelling") / F.greatest(F.col("bedrooms"), F.lit(1))
    precarious = F.col("wall_material").isin(5, 6, 7, 8) | (F.col("dwelling_kind") == 2)
    component = (
        F.when(precarious, "precarious_housing")
        .when(F.col("families_in_dwelling") > 1, "cohabitation")
        .when((rent > 0) & (rent > income * RENT_BURDEN_LIMIT), "rent_burden")
        .when((rent > 0) & (people_per_bedroom > MAX_PEOPLE_PER_BEDROOM), "rented_overcrowding")
    )
    return families.select(
        "*",
        component.alias("deficit_component"),
        F.coalesce(F.col("piped_water") == 2, F.lit(False)).alias("no_piped_water"),
        F.coalesce(F.col("has_bathroom") == 2, F.lit(False)).alias("no_bathroom"),
        F.coalesce(F.col("sewage_disposal").isin(3, 4, 5, 6), F.lit(False)).alias("inadequate_sewage"),
        F.coalesce(F.col("garbage_disposal").isin(3, 4, 5, 6), F.lit(False)).alias("inadequate_garbage"),
        F.coalesce(F.col("lighting").isin(4, 5, 6), F.lit(False)).alias("no_electricity"),
        F.coalesce(F.col("floor_material") == 1, F.lit(False)).alias("dirt_floor"),
    )


def band_ceiling(band: Column, bands: dict[int, float]) -> Column:
    """Income ceiling of the MCMV band (null for unknown bands). CASE instead of a map: ANSI-safe."""
    return F.coalesce(*[F.when(band == k, F.lit(float(v))) for k, v in sorted(bands.items())])


def band_for_income(income: float, bands: dict[int, float]) -> int:
    for band, ceiling in sorted(bands.items()):
        if income <= ceiling:
            return band
    return max(bands)
