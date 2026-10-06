import pandas as pd
import pytest

from housing_lakehouse.ml.allocation import allocate

COSTS = {"North": 100.0, "South": 200.0}


def _municipalities() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "municipality_code": [1, 2, 3, 4],
            "region": ["North", "North", "South", "South"],
            "deficit_forecast_units": [100, 50, 100, 10],
            "precariousness_index": [80.0, 20.0, 10.0, 5.0],
        }
    )


def test_respects_budget_capacity_and_priority():
    d = allocate(_municipalities(), budget=5_000, unit_cost_by_region=COSTS, execution_cap=0.3, min_region_share=0)
    assert d["investment"].sum() <= 5_000
    assert (d["allocated_units"] <= d["capacity"]).all()
    assert (d["allocated_units"] >= 0).all()
    # without a regional floor, the budget goes first to the most precarious municipality
    assert d.loc[0, "allocated_units"] == d.loc[0, "capacity"] == 30


def test_guarantees_minimum_region_share():
    without_floor = allocate(_municipalities(), 5_000, COSTS, 0.3, min_region_share=0)
    with_floor = allocate(_municipalities(), 5_000, COSTS, 0.3, min_region_share=0.8)
    south = lambda d: d.loc[d["region"] == "South", "investment"].sum()  # noqa: E731
    assert south(without_floor) < south(with_floor)
    assert with_floor["investment"].sum() <= 5_000


def test_region_without_cost_is_an_error():
    with pytest.raises(ValueError, match="South"):
        allocate(_municipalities(), 1_000, {"North": 100.0})
