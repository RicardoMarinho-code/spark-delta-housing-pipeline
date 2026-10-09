import pytest

pytestmark = pytest.mark.spark


def test_splits_quarantine_and_counts_per_rule(spark):
    from pyspark.sql import functions as F

    from housing_lakehouse.quality import WARNING, Rule, evaluate

    df = spark.createDataFrame([(1, 10), (2, -5), (None, 7), (4, None), (5, 1000)], "id int, income int")
    rules = [
        Rule("id_present", F.col("id").isNotNull()),
        Rule("income_non_negative", F.col("income") >= 0),  # null fails too
        Rule("income_high", F.col("income") < 500, WARNING),
    ]
    result = evaluate(df, rules)
    try:
        assert result.total == 5
        assert result.quarantined == 3
        assert sorted(r.id for r in result.valid.collect()) == [1, 5]
        failures = {m["rule"]: m["failures"] for m in result.metrics}
        # income_high fails for 1000 and for the null income (null counts as a failure)
        assert failures == {"id_present": 1, "income_non_negative": 2, "income_high": 2}
        assert result.valid.where("id = 5").first()["_warnings"] == ["income_high"]
        reasons = {r.id: r._failures for r in result.quarantine.collect()}
        assert reasons[2] == ["income_non_negative"]
    finally:
        result.release()


def test_circuit_breaker(spark):
    from pyspark.sql import functions as F

    from housing_lakehouse.quality import QualityError, Rule, enforce_threshold, evaluate

    df = spark.createDataFrame([(i,) for i in range(10)], "v int")
    result = evaluate(df, [Rule("even", F.col("v") % 2 == 0)])
    try:
        enforce_threshold(result, 0.6)
        with pytest.raises(QualityError, match="even=5"):
            enforce_threshold(result, 0.05)
    finally:
        result.release()
