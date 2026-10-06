"""Pipeline configuration: packaged YAML (or the file in HOUSING_CONFIG) + environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields, replace
from importlib import resources
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Source:
    name: str
    pattern: str
    separator: str
    encoding: str
    kind: str  # snapshot | reference


@dataclass(frozen=True)
class Config:
    input_path: str
    lakehouse_path: str
    sources: dict[str, Source]
    max_rejection_rate: float
    max_volume_change: float
    max_days_without_load: int
    bronze_retention_months: int
    min_cell_size: int
    income_bands: dict[int, float]
    allocation_budget: float
    execution_cap: float
    min_region_share: float
    unit_cost_by_region: dict[str, float]
    mongo_database: str
    mongo_collection: str

    def path(self, layer: str, table: str) -> str:
        """`<lakehouse>/<layer>/<table>`, or `<lakehouse>/<table>` when the lakehouse path contains
        `{layer}` (one ADLS container per layer, e.g. abfss://{layer}@account.dfs.core.windows.net/x)."""
        if "{layer}" in self.lakehouse_path:
            return f"{self.lakehouse_path.format(layer=layer).rstrip('/')}/{table}"
        return f"{self.lakehouse_path.rstrip('/')}/{layer}/{table}"


def _absolute(path: str) -> str:
    """Relative local paths become absolute; URIs (abfss://, dbfs:/) are left untouched."""
    if "://" in path or path.startswith("dbfs:"):
        return path
    return str(Path(path).resolve())


def _read_yaml(path: str | None) -> dict[str, Any]:
    origin = path or os.getenv("HOUSING_CONFIG")
    if origin:
        return yaml.safe_load(Path(origin).read_text(encoding="utf-8"))
    text = resources.files("housing_lakehouse").joinpath("conf/pipeline.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(text)


def load_config(path: str | None = None, **overrides: Any) -> Config:
    raw = _read_yaml(path)
    cfg = Config(
        input_path=_absolute(os.getenv("HOUSING_INPUT") or raw["input_path"]),
        lakehouse_path=_absolute(os.getenv("HOUSING_LAKEHOUSE") or raw["lakehouse_path"]),
        sources={name: Source(name=name, **data) for name, data in raw["sources"].items()},
        max_rejection_rate=float(raw["quality"]["max_rejection_rate"]),
        max_volume_change=float(raw["quality"]["max_volume_change"]),
        max_days_without_load=int(raw["quality"]["max_days_without_load"]),
        bronze_retention_months=int(raw["privacy"]["bronze_retention_months"]),
        min_cell_size=int(raw["privacy"]["min_cell_size"]),
        income_bands={int(k): float(v) for k, v in raw["mcmv"]["income_bands"].items()},
        allocation_budget=float(raw["allocation"]["budget"]),
        execution_cap=float(raw["allocation"]["execution_cap"]),
        min_region_share=float(raw["allocation"]["min_region_share"]),
        unit_cost_by_region={k: float(v) for k, v in raw["allocation"]["unit_cost_by_region"].items()},
        mongo_database=raw["publishing"]["mongo_database"],
        mongo_collection=raw["publishing"]["mongo_collection"],
    )
    valid = {f.name for f in fields(Config)}
    unknown = set(overrides) - valid
    if unknown:
        raise ValueError(f"Unknown configuration parameters: {sorted(unknown)}")
    changes = {
        k: _absolute(v) if k in ("input_path", "lakehouse_path") else v for k, v in overrides.items() if v is not None
    }
    return replace(cfg, **changes)
