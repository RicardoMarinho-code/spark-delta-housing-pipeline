"""Pseudonymization with HMAC-SHA256 (LGPD art. 13, §4).

- Deterministic: the same CPF yields the same pseudonym in every dataset, so the CadÚnico ×
  beneficiaries join happens without the CPF ever leaving bronze.
- Keyed: without the key nobody can recompute pseudonyms from a list of CPFs, which a plain SHA-256
  would allow because the CPF space is small.
- Domain-separated: "cpf:<value>" and "nis:<value>" produce different pseudonyms even when the
  digits match.

The key comes from PSEUDONYMIZATION_KEY, which on Databricks is filled from Key Vault (secret scope).
It never appears in code or in the query plan: the HMAC runs in a pandas UDF that receives the key
through its closure. A version built on native `sha2` would be faster, but the key-derived blocks
would show up as literals in the plan (Spark UI, event logs).
"""

from __future__ import annotations

import hashlib
import hmac
import os

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as F

KEY_ENV = "PSEUDONYMIZATION_KEY"
MIN_KEY_LENGTH = 32


def get_key() -> bytes:
    value = os.getenv(KEY_ENV)
    if not value:
        raise RuntimeError(
            f"Set {KEY_ENV}. On Databricks, point it to the Key Vault secret: "
            "{{secrets/housing-lakehouse/pseudonymization-key}}"
        )
    if len(value) < MIN_KEY_LENGTH:
        raise ValueError(f"The pseudonymization key must have at least {MIN_KEY_LENGTH} characters.")
    return value.encode("utf-8")


def pseudonym(value: str, key: bytes, domain: str) -> str:
    """Python version: used by tests and for controlled re-identification (whoever holds data + key)."""
    return hmac.new(key, f"{domain}:{value}".encode(), hashlib.sha256).hexdigest()


def pseudonymize(column: Column, key: bytes, domain: str) -> Column:
    prefix = f"{domain}:".encode()

    @F.pandas_udf("string")
    def _hmac(values: pd.Series) -> pd.Series:
        return values.map(
            lambda v: None if pd.isna(v) else hmac.new(key, prefix + str(v).encode("utf-8"), hashlib.sha256).hexdigest()
        )

    return _hmac(column)
