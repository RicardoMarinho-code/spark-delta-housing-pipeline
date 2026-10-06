import hashlib
import hmac

import pytest

from housing_lakehouse.governance.pseudonymization import get_key, pseudonym
from tests.conftest import TEST_KEY


def test_pseudonym_is_domain_separated_hmac_sha256():
    key = TEST_KEY.encode()
    expected = hmac.new(key, b"cpf:52998224725", hashlib.sha256).hexdigest()
    assert pseudonym("52998224725", key, "cpf") == expected
    assert pseudonym("52998224725", key, "nis") != expected
    assert pseudonym("52998224725", b"another-key-with-more-than-32-characters", "cpf") != expected


def test_key_is_required_and_long_enough(monkeypatch):
    monkeypatch.delenv("PSEUDONYMIZATION_KEY", raising=False)
    with pytest.raises(RuntimeError):
        get_key()
    monkeypatch.setenv("PSEUDONYMIZATION_KEY", "short")
    with pytest.raises(ValueError):
        get_key()
    monkeypatch.setenv("PSEUDONYMIZATION_KEY", TEST_KEY)
    assert get_key() == TEST_KEY.encode()


@pytest.mark.spark
def test_spark_udf_matches_python_version(spark):
    from pyspark.sql import functions as F

    from housing_lakehouse.governance.pseudonymization import pseudonymize

    key = TEST_KEY.encode()
    df = spark.createDataFrame([("52998224725",), (None,)], "v string")
    rows = df.select(pseudonymize(F.col("v"), key, "cpf").alias("p")).collect()
    assert rows[0].p == pseudonym("52998224725", key, "cpf")
    assert rows[1].p is None


@pytest.mark.spark
def test_policy_normalizes_cpf_before_pseudonymizing(spark):
    from datetime import date

    from housing_lakehouse.governance.policy import apply_privacy_policy
    from housing_lakehouse.layouts import BENEFICIARIES

    key = TEST_KEY.encode()
    base_row = {n: None for n in BENEFICIARIES.names}
    rows = [
        {**base_row, "NU_CPF_BENEFICIARIO": cpf, "NO_BENEFICIARIO": "JOHN DOE", "CO_CEP_IMOVEL": "70000-123"}
        for cpf in ("529.982.247-25", "52998224725", "529.982.247-24", None)
    ]
    schema = ", ".join(f"{n} string" for n in BENEFICIARIES.names)
    df = spark.createDataFrame([tuple(r[n] for n in BENEFICIARIES.names) for r in rows], schema)
    out = apply_privacy_policy(df, BENEFICIARIES, key, date(2026, 9, 11)).collect()

    assert "no_beneficiario" not in out[0].asDict()
    assert "nu_cpf_beneficiario" not in out[0].asDict()
    assert out[0].cpf_pseudo == out[1].cpf_pseudo == pseudonym("52998224725", key, "cpf")
    assert [r.cpf_status for r in out] == ["valid", "valid", "invalid", "missing"]
    assert out[2].cpf_pseudo is None
    assert out[0].zip_prefix == "70000"
