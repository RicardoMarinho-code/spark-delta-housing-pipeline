from __future__ import annotations

import os

import pytest

TEST_KEY = "test-key-with-at-least-thirty-two-characters"


@pytest.fixture(scope="session", autouse=True)
def _environment():
    os.environ["PSEUDONYMIZATION_KEY"] = TEST_KEY
    os.environ.pop("ALERT_WEBHOOK_URL", None)
    os.environ.setdefault("SPARK_MASTER", "local[2]")
    os.environ["SPARK_SHUFFLE_PARTITIONS"] = "2"


@pytest.fixture(scope="session")
def spark():
    """Local Spark session with Delta. In CI (REQUIRE_SPARK=1) a Spark start-up failure fails the suite;
    on machines without a working Spark (e.g. Windows without winutils) Spark tests are skipped."""
    try:
        from housing_lakehouse.spark import get_spark

        session = get_spark("tests")
    except Exception as error:  # noqa: BLE001
        if os.getenv("REQUIRE_SPARK") == "1":
            raise
        pytest.skip(f"Spark unavailable in this environment: {type(error).__name__}")
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path):
    from housing_lakehouse.config import load_config

    return load_config(input_path=str(tmp_path / "input"), lakehouse_path=str(tmp_path / "lakehouse"))
