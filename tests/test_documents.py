import random

import pytest

from housing_lakehouse.governance.documents import (
    cpf_is_valid_py,
    format_cpf,
    generate_cpf,
    generate_nis,
    ibge_check_digit,
    ibge_is_valid_py,
    nis_is_valid_py,
)


def test_known_cpfs():
    assert cpf_is_valid_py("529.982.247-25")
    assert cpf_is_valid_py("52998224725")
    assert not cpf_is_valid_py("529.982.247-24")
    assert not cpf_is_valid_py("111.111.111-11")
    assert not cpf_is_valid_py("")
    assert not cpf_is_valid_py(None)
    assert not cpf_is_valid_py("123456789012")


def test_known_nis():
    assert nis_is_valid_py("120.00000.00-4")
    assert not nis_is_valid_py("12000000005")
    assert not nis_is_valid_py("00000000000")


def test_ibge_state_capitals():
    for code in (5300108, 3550308, 3304557, 2927408):
        assert ibge_is_valid_py(code)
    assert not ibge_is_valid_py(5300107)
    assert not ibge_is_valid_py(999999)
    assert ibge_check_digit("530010") == 8


def test_generators_produce_valid_documents():
    rng = random.Random(7)
    for _ in range(500):
        cpf = generate_cpf(rng)
        assert cpf_is_valid_py(cpf)
        assert cpf_is_valid_py(format_cpf(cpf))
        assert nis_is_valid_py(generate_nis(rng))


@pytest.mark.spark
def test_spark_validation_matches_python(spark):
    from pyspark.sql import functions as F

    from housing_lakehouse.governance.documents import cpf_is_valid, ibge_is_valid, nis_is_valid, normalize_document

    rng = random.Random(3)
    cpfs = [generate_cpf(rng) for _ in range(200)]
    cpfs += [c[:10] + str((int(c[10]) + 1) % 10) for c in cpfs[:100]]
    cpfs += [format_cpf(c) for c in cpfs[:50]] + ["11111111111", "", "abc", "1234"]
    nis = [generate_nis(rng) for _ in range(100)] + ["12000000005", "00000000000"]
    ibge = [5300108, 3550308, 5300107, 123, 9999999]

    df = spark.createDataFrame([(v,) for v in cpfs], "v string")
    got = [r.ok for r in df.select(cpf_is_valid(normalize_document(F.col("v"))).alias("ok")).collect()]
    assert got == [cpf_is_valid_py(v) for v in cpfs]

    df = spark.createDataFrame([(v,) for v in nis], "v string")
    got = [r.ok for r in df.select(nis_is_valid(normalize_document(F.col("v"))).alias("ok")).collect()]
    assert got == [nis_is_valid_py(v) for v in nis]

    df = spark.createDataFrame([(v,) for v in ibge], "v int")
    got = [r.ok for r in df.select(ibge_is_valid(F.col("v")).alias("ok")).collect()]
    assert got == [ibge_is_valid_py(v) for v in ibge]
