"""Municipal housing deficit forecast from CadÚnico indicators.

The official FJP deficit is published with a lag of years; CadÚnico is updated monthly. The model
learns the relation between CadÚnico precariousness indicators and the FJP relative deficit
(deficit / households) and then estimates the deficit on every new monthly load (nowcasting).

Validation: GroupKFold by state, to measure generalization to states the model has not seen, and a
comparison with a simple linear regression on the deficit rate (baseline). Metrics and model go to
MLflow when it is available.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import GroupKFold, cross_val_predict
from sklearn.pipeline import make_pipeline

from housing_lakehouse.housing_rules import DEFICIT_COMPONENTS, INADEQUACIES

log = logging.getLogger("housing_lakehouse")

TARGET = "relative_deficit"
FEATURES = [
    "deficit_rate",
    *[f"{c}_rate" for c in DEFICIT_COMPONENTS],
    *[f"{i}_rate" for i in INADEQUACIES],
    "median_per_capita_income",
    "families_per_1000_inhabitants",
    "log_population",
]
IDENTIFIERS = ["municipality_code", "municipality_name", "state", "region", "households"]


@dataclass
class TrainingResult:
    model: HistGradientBoostingRegressor
    metrics: dict[str, float]
    predictions: pd.DataFrame
    params: dict


def build_dataset(
    precariousness: pd.DataFrame, municipalities: pd.DataFrame, fjp_deficit: pd.DataFrame
) -> pd.DataFrame:
    """One row per municipality: CadÚnico features + FJP target (latest year available)."""
    fjp = fjp_deficit[fjp_deficit["year"] == fjp_deficit["year"].max()][["municipality_code", "deficit_total"]]
    data = municipalities[IDENTIFIERS + ["population"]].merge(
        precariousness.drop(columns=["municipality_name", "state", "region"], errors="ignore"),
        on="municipality_code",
        how="left",
    )
    data = data.merge(fjp, on="municipality_code", how="left")
    data["families_per_1000_inhabitants"] = 1000 * data["n_families"] / data["population"]
    data["log_population"] = np.log1p(data["population"])
    data[TARGET] = data["deficit_total"] / data["households"]
    for column in FEATURES:
        data[column] = pd.to_numeric(data[column], errors="coerce").astype(float)
    return data


def train(data: pd.DataFrame, seed: int = 42) -> TrainingResult:
    train_set = data.dropna(subset=[TARGET]).reset_index(drop=True)
    groups = train_set["state"]
    n_folds = min(5, groups.nunique())
    if n_folds < 2 or len(train_set) < 10:
        raise ValueError(f"Dataset too small to validate: {len(train_set)} municipalities in {groups.nunique()} states")
    params = {
        "max_iter": 300,
        "learning_rate": 0.05,
        "max_leaf_nodes": 15,
        "min_samples_leaf": 10,
        "l2_regularization": 0.1,
        "random_state": seed,
    }
    model = HistGradientBoostingRegressor(**params)
    baseline = make_pipeline(SimpleImputer(strategy="median"), LinearRegression())
    cv = GroupKFold(n_splits=n_folds)
    x, y = train_set[FEATURES], train_set[TARGET]

    oof_model = cross_val_predict(model, x, y, groups=groups, cv=cv)
    oof_baseline = cross_val_predict(baseline, x[["deficit_rate"]], y, groups=groups, cv=cv)
    metrics = {
        "r2_model": float(r2_score(y, oof_model)),
        "mae_model": float(mean_absolute_error(y, oof_model)),
        "r2_baseline": float(r2_score(y, oof_baseline)),
        "mae_baseline": float(mean_absolute_error(y, oof_baseline)),
        "n_municipalities": float(len(train_set)),
        "n_folds": float(n_folds),
    }
    model.fit(x, y)

    predictions = data[IDENTIFIERS].copy()
    predictions["relative_deficit_fjp"] = data[TARGET]
    predictions["relative_deficit_forecast"] = np.clip(model.predict(data[FEATURES]), 0, None)
    predictions["deficit_forecast_units"] = (
        predictions["relative_deficit_forecast"] * predictions["households"]
    ).round()
    oof = pd.Series(oof_model, index=train_set["municipality_code"])
    predictions["relative_deficit_cv"] = predictions["municipality_code"].map(oof)
    predictions["cv_residual"] = predictions["relative_deficit_fjp"] - predictions["relative_deficit_cv"]
    return TrainingResult(model=model, metrics=metrics, predictions=predictions, params=params)


def log_to_mlflow(result: TrainingResult, local_dir: str, tags: dict[str, str]) -> str | None:
    try:
        import mlflow
        import mlflow.sklearn
    except ImportError:
        log.info("MLflow not installed; metrics are kept in the audit table only")
        return None
    if not os.getenv("MLFLOW_TRACKING_URI") and not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        mlflow.set_tracking_uri(Path(local_dir).resolve().as_uri())
    mlflow.set_experiment(os.getenv("MLFLOW_EXPERIMENT", "housing-lakehouse-deficit"))
    with mlflow.start_run(run_name=f"deficit-{datetime.now(timezone.utc):%Y%m%d-%H%M}") as run:
        mlflow.set_tags(tags)
        mlflow.log_params(result.params | {"features": ",".join(FEATURES)})
        mlflow.log_metrics(result.metrics)
        try:
            mlflow.sklearn.log_model(result.model, name="model")
        except TypeError:  # MLflow 2.x
            mlflow.sklearn.log_model(result.model, artifact_path="model")
        return run.info.run_id
