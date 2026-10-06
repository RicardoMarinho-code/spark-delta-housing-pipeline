from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from housing_lakehouse.governance.disclosure import suppress
from housing_lakehouse.publishing.mongo import build_profiles, publish


def test_suppresses_small_count_and_derived_rate():
    record = {"n_a": 3, "a_rate": 0.03, "n_b": 0, "b_rate": 0.0, "n_c": 40, "c_rate": 0.4}
    out, suppressed = suppress(record, {"n_a": ["a_rate"], "n_b": ["b_rate"], "n_c": ["c_rate"]}, minimum=5)
    assert out["n_a"] is None and out["a_rate"] is None
    assert out["n_b"] == 0  # zero identifies nobody
    assert out["n_c"] == 40
    assert suppressed == ["n_a", "a_rate"]
    assert record["n_a"] == 3  # the original is untouched


def _precariousness(code: int, families: int, deficit: int) -> dict:
    return {
        "municipality_code": code,
        "municipality_name": f"Municipality {code}",
        "state": "GO",
        "region": "Center-West",
        "reference_date": date(2026, 9, 11),
        "n_families": families,
        "n_families_deficit": deficit,
        "deficit_rate": Decimal("0.25"),
        "precariousness_index": 31.5,
        "median_per_capita_income": Decimal("410.50"),
    }


def test_profiles_apply_disclosure_control_and_bson_types():
    profiles = build_profiles(
        [_precariousness(1, 200, 50), _precariousness(2, 40, 3)],
        service_gap=[{"municipality_code": 1, "n_units_contracted": 12, "fjp_deficit": 900, "coverage_fjp": 0.013}],
        forecast=[{"municipality_code": 1, "relative_deficit_forecast": 0.07, "deficit_forecast_units": float("nan")}],
        allocation=[],
        min_cell_size=5,
    )
    p1, p2 = profiles
    assert p1["_id"] == 1
    assert p1["reference_date"] == "2026-09-11"
    assert p1["cadunico"]["deficit_rate"] == 0.25
    assert p1["forecast"]["deficit_units"] is None
    assert p1["service"]["units_contracted"] == 12
    assert p2["cadunico"]["families_in_deficit"] is None
    assert p2["cadunico"]["deficit_rate"] is None
    assert "n_families_deficit" in p2["disclosure_control"]["suppressed_fields"]


class InMemoryCollection:
    """The bare minimum of a MongoDB collection to test the upsert without a server."""

    def __init__(self):
        self.documents: dict = {}
        self.indexes: list = []

    def bulk_write(self, operations, ordered=True):
        inserted = updated = 0
        for op in operations:
            doc = op._doc
            if doc["_id"] in self.documents:
                updated += self.documents[doc["_id"]] != doc
            else:
                inserted += 1
            self.documents[doc["_id"]] = doc
        return SimpleNamespace(upserted_count=inserted, modified_count=updated)

    def create_index(self, keys):
        self.indexes.append(keys)


def test_publishing_is_idempotent():
    collection = InMemoryCollection()
    profiles = build_profiles([_precariousness(1, 200, 50)], [], [], [], 5)
    assert publish(profiles, collection) == {"inserted": 1, "updated": 0}
    assert publish(profiles, collection) == {"inserted": 0, "updated": 0}
    assert len(collection.documents) == 1
    assert collection.documents[1]["cadunico"]["families"] == 200
    assert collection.indexes
