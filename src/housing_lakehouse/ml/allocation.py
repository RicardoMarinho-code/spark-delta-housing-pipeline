"""Prescriptive allocation of housing units across municipalities (mixed-integer linear programming).

Maximize   Σ priority_m · x_m          (priority = CadÚnico precariousness index)
subject to Σ cost_m · x_m ≤ budget
           0 ≤ x_m ≤ execution_cap · forecast_deficit_m              (delivery capacity)
           Σ_{m ∈ region} cost_m · x_m ≥ the region's minimum share   (regional equity)
           x_m integer

Each region's minimum share is proportional to its forecast deficit, capped at what the region can
absorb, so the problem is always feasible.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.optimize import Bounds, LinearConstraint, milp


def allocate(
    municipalities: pd.DataFrame,
    budget: float,
    unit_cost_by_region: dict[str, float],
    execution_cap: float = 0.3,
    min_region_share: float = 0.8,
) -> pd.DataFrame:
    """Expects columns municipality_code, region, deficit_forecast_units, precariousness_index."""
    d = municipalities.copy().reset_index(drop=True)
    d["unit_cost"] = d["region"].map(unit_cost_by_region).astype(float)
    if d["unit_cost"].isna().any():
        missing = sorted(d.loc[d["unit_cost"].isna(), "region"].astype(str).unique())
        raise ValueError(f"No unit cost for regions: {missing}")
    d["capacity"] = np.floor(d["deficit_forecast_units"].fillna(0).clip(lower=0) * execution_cap)
    priority = d["precariousness_index"].fillna(d["precariousness_index"].median()).fillna(0).clip(lower=0)
    cost = d["unit_cost"].to_numpy()

    rows, lower, upper = [cost], [0.0], [float(budget)]
    total_deficit = d["deficit_forecast_units"].fillna(0).clip(lower=0).sum()
    if total_deficit > 0:
        for _, idx in d.groupby("region").groups.items():
            idx = np.asarray(idx)
            share = d.loc[idx, "deficit_forecast_units"].fillna(0).clip(lower=0).sum() / total_deficit
            absorbable = float((cost[idx] * d.loc[idx, "capacity"].to_numpy()).sum())
            # subtract one unit cost so integrality never makes the minimum unreachable
            minimum = max(0.0, min(min_region_share * budget * share, absorbable) - cost[idx].max())
            row = np.zeros(len(d))
            row[idx] = cost[idx]
            rows.append(row)
            lower.append(minimum)
            upper.append(np.inf)

    result = milp(
        c=-(priority.to_numpy() + 1e-6),
        constraints=LinearConstraint(np.vstack(rows), lower, upper),
        integrality=np.ones(len(d)),
        bounds=Bounds(np.zeros(len(d)), d["capacity"].to_numpy()),
    )
    if not result.success:
        raise RuntimeError(f"Optimization found no solution: {result.message}")
    d["allocated_units"] = np.round(result.x).astype(int)
    d["investment"] = d["allocated_units"] * d["unit_cost"]
    return d
