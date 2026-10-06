from pathlib import Path

import pytest

from housing_lakehouse.config import load_config


def test_packaged_config(monkeypatch):
    monkeypatch.delenv("HOUSING_LAKEHOUSE", raising=False)
    cfg = load_config()
    assert set(cfg.sources) == {
        "cadunico_family",
        "cadunico_person",
        "mcmv_beneficiaries",
        "ref_municipalities",
        "ref_fjp_deficit",
    }
    assert cfg.sources["cadunico_family"].encoding == "ISO-8859-1"
    assert cfg.income_bands[1] == 2850.0
    assert Path(cfg.lakehouse_path).is_absolute()


def test_local_path_and_one_container_per_layer(monkeypatch, tmp_path):
    cfg = load_config(lakehouse_path=str(tmp_path / "lake"))
    assert cfg.path("silver", "cadunico_family") == f"{tmp_path / 'lake'}/silver/cadunico_family"

    monkeypatch.setenv("HOUSING_LAKEHOUSE", "abfss://{layer}@account.dfs.core.windows.net/housing")
    cfg = load_config()
    assert (
        cfg.path("bronze", "cadunico_person") == "abfss://bronze@account.dfs.core.windows.net/housing/cadunico_person"
    )


def test_unknown_parameter():
    with pytest.raises(ValueError, match="Unknown"):
        load_config(max_rejection_rate_x=0.1)
