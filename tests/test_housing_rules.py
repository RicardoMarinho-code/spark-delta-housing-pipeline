from decimal import Decimal

import pytest

from housing_lakehouse.housing_rules import band_for_income

BANDS = {1: 2850.0, 2: 4700.0, 3: 8600.0, 4: 12000.0}


def test_band_for_income():
    assert band_for_income(0, BANDS) == 1
    assert band_for_income(2850, BANDS) == 1
    assert band_for_income(2850.01, BANDS) == 2
    assert band_for_income(50_000, BANDS) == 4


@pytest.mark.spark
def test_deficit_components_are_hierarchical(spark):
    from housing_lakehouse.housing_rules import classify_households

    schema = (
        "id string, wall_material int, dwelling_kind int, families_in_dwelling int, "
        "expense_rent decimal(14,2), total_income decimal(14,2), people_in_dwelling int, bedrooms int, "
        "piped_water int, has_bathroom int, sewage_disposal int, garbage_disposal int, lighting int, "
        "floor_material int"
    )
    d = Decimal
    rows = [
        # id, walls, kind, families, rent, income, people, bedrooms, water, bathroom, sewage, garbage, light, floor
        ("precarious_and_cohabitation", 7, 1, 2, d(0), d(900), 4, 2, 1, 1, 1, 1, 1, 2),
        ("improvised", 1, 2, 1, d(0), d(900), 3, 1, 2, 2, 4, 3, 5, 1),
        ("cohabitation", 1, 1, 3, d(0), d(900), 6, 2, 1, 1, 1, 1, 1, 2),
        ("burden", 1, 1, 1, d(500), d(1000), 2, 1, 1, 1, 1, 1, 1, 2),
        ("crowded", 1, 1, 1, d(200), d(2000), 7, 2, 1, 1, 1, 1, 1, 2),
        ("adequate", 1, 1, 1, d(200), d(2000), 3, 2, 1, 1, 1, 1, 1, 2),
        ("rent_without_income", 2, 1, 1, d(300), d(0), 2, 1, 1, 1, 1, 1, 1, 2),
    ]
    result = {r.id: r for r in classify_households(spark.createDataFrame(rows, schema)).collect()}
    assert result["precarious_and_cohabitation"].deficit_component == "precarious_housing"
    assert result["improvised"].deficit_component == "precarious_housing"
    assert result["cohabitation"].deficit_component == "cohabitation"
    assert result["burden"].deficit_component == "rent_burden"
    assert result["crowded"].deficit_component == "rented_overcrowding"
    assert result["adequate"].deficit_component is None
    assert result["rent_without_income"].deficit_component == "rent_burden"
    improvised = result["improvised"]
    assert all(
        improvised[i]
        for i in ("no_piped_water", "no_bathroom", "inadequate_sewage", "inadequate_garbage",
                  "no_electricity", "dirt_floor")
    )  # fmt: skip
    assert not result["adequate"].no_piped_water
