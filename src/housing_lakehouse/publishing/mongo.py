"""One profile per municipality in MongoDB: a single document with everything a dashboard or API needs.

Only aggregated data leaves through here, with disclosure control applied (small counts suppressed).
The load is idempotent: each municipality is an `_id` and the write is an upsert.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from housing_lakehouse.governance.disclosure import suppress
from housing_lakehouse.housing_rules import DEFICIT_COMPONENTS, INADEQUACIES

# count → derived fields that must also disappear when the count is suppressed
DISCLOSURE_DEPENDENCIES: dict[str, list[str]] = {
    "n_families": ["median_per_capita_income"],
    "n_families_deficit": ["deficit_rate"],
    **{f"n_{c}": [f"{c}_rate"] for c in (*DEFICIT_COMPONENTS, *INADEQUACIES)},
}


def _value(v: Any) -> Any:
    """Converts to types BSON accepts (Decimal and date are not)."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return v.isoformat()
    return v


def build_profiles(
    precariousness: list[dict],
    service_gap: list[dict],
    forecast: list[dict],
    allocation: list[dict],
    min_cell_size: int,
    generated_at: datetime | None = None,
) -> list[dict]:
    generated_at = generated_at or datetime.now(timezone.utc)
    by_code = {
        name: {r["municipality_code"]: {k: _value(v) for k, v in r.items()} for r in rows}
        for name, rows in (("gap", service_gap), ("forecast", forecast), ("allocation", allocation))
    }
    profiles = []
    for raw in precariousness:
        p, suppressed = suppress({k: _value(v) for k, v in raw.items()}, DISCLOSURE_DEPENDENCIES, min_cell_size)
        code = p["municipality_code"]
        gap = by_code["gap"].get(code, {})
        fc = by_code["forecast"].get(code, {})
        alloc = by_code["allocation"].get(code, {})
        profiles.append(
            {
                "_id": code,
                "municipality": {
                    "ibge_code": code,
                    "name": p.get("municipality_name"),
                    "state": p.get("state"),
                    "region": p.get("region"),
                },
                "reference_date": p.get("reference_date"),
                "cadunico": {
                    "families": p.get("n_families"),
                    "families_in_deficit": p.get("n_families_deficit"),
                    "deficit_rate": p.get("deficit_rate"),
                    "deficit_components": {c: p.get(f"n_{c}") for c in DEFICIT_COMPONENTS},
                    "inadequacies": {i: p.get(f"{i}_rate") for i in INADEQUACIES},
                    "precariousness_index": p.get("precariousness_index"),
                    "median_per_capita_income": p.get("median_per_capita_income"),
                },
                "service": {
                    "units_contracted": gap.get("n_units_contracted"),
                    "fjp_deficit": gap.get("fjp_deficit"),
                    "fjp_coverage": gap.get("coverage_fjp"),
                },
                "forecast": {
                    "relative_deficit": fc.get("relative_deficit_forecast"),
                    "deficit_units": fc.get("deficit_forecast_units"),
                },
                "allocation": {
                    "units": alloc.get("allocated_units"),
                    "investment": alloc.get("investment"),
                },
                "disclosure_control": {"min_cell_size": min_cell_size, "suppressed_fields": suppressed},
                "updated_at": generated_at,
            }
        )
    return profiles


def publish(profiles: list[dict], collection: Any) -> dict[str, int]:
    from pymongo import ASCENDING, DESCENDING, ReplaceOne

    if not profiles:
        return {"inserted": 0, "updated": 0}
    result = collection.bulk_write([ReplaceOne({"_id": p["_id"]}, p, upsert=True) for p in profiles], ordered=False)
    collection.create_index([("municipality.state", ASCENDING)])
    collection.create_index([("cadunico.precariousness_index", DESCENDING)])
    return {"inserted": result.upserted_count, "updated": result.modified_count}


def process(spark: Any, cfg: Any, run_id: str, uri: str) -> None:
    from pymongo import MongoClient

    from housing_lakehouse.observability import track
    from housing_lakehouse.storage import read

    def rows(table: str) -> list[dict]:
        return [r.asDict() for r in read(spark, cfg.path("gold", table)).collect()]

    with track(spark, cfg, "publishing", cfg.mongo_collection, run_id) as run:
        profiles = build_profiles(
            rows("municipality_precariousness"),
            rows("service_gap"),
            rows("deficit_forecast"),
            rows("unit_allocation"),
            cfg.min_cell_size,
        )
        client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
        try:
            run.details.update(publish(profiles, client[cfg.mongo_database][cfg.mongo_collection]))
        finally:
            client.close()
        run.rows_out = len(profiles)
        run.details["profiles_with_suppression"] = sum(
            bool(p["disclosure_control"]["suppressed_fields"]) for p in profiles
        )
