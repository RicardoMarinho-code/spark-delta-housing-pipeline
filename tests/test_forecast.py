import numpy as np
import pandas as pd
import pytest

from housing_lakehouse.ml.forecast import FEATURES, TARGET, build_dataset, train


def _inputs(n: int = 120, seed: int = 0):
    rng = np.random.default_rng(seed)
    states = [f"S{i}" for i in range(8)]
    latent = rng.uniform(0, 1, n)
    households = rng.integers(2_000, 50_000, n)
    municipalities = pd.DataFrame(
        {
            "municipality_code": np.arange(n),
            "municipality_name": [f"M{i}" for i in range(n)],
            "state": rng.choice(states, n),
            "region": "R",
            "households": households,
            "population": households * 3,
        }
    )
    precariousness = pd.DataFrame({"municipality_code": np.arange(n), "n_families": (households * 0.3).astype(int)})
    for f in FEATURES:
        if f.endswith("_rate"):
            precariousness[f] = np.clip(0.3 * latent + rng.normal(0, 0.03, n), 0, 1)
    precariousness["median_per_capita_income"] = 600 - 300 * latent
    deficit = pd.DataFrame(
        {
            "municipality_code": np.arange(n),
            "year": 2022,
            "deficit_total": ((0.03 + 0.1 * latent) * households).round().astype(int),
        }
    )
    return precariousness, municipalities, deficit


def test_dataset_has_target_and_features():
    data = build_dataset(*_inputs())
    assert set(FEATURES) <= set(data.columns)
    assert data[TARGET].between(0, 1).all()


def test_model_learns_and_predictions_are_valid():
    result = train(build_dataset(*_inputs()))
    assert result.metrics["r2_model"] > 0.5
    assert result.metrics["n_folds"] == 5
    assert (result.predictions["relative_deficit_forecast"] >= 0).all()
    assert result.predictions["relative_deficit_cv"].notna().all()


def test_dataset_too_small():
    precariousness, municipalities, deficit = _inputs(n=6)
    with pytest.raises(ValueError):
        train(build_dataset(precariousness, municipalities, deficit))
