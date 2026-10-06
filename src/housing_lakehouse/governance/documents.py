"""Validation of CPF (taxpayer ID), NIS (social ID) and IBGE municipality codes.

Each check exists in plain Python and as native Spark expressions; the tests make sure both agree.
The Spark version uses no UDF, so it runs inside the JVM without shipping rows to Python.
"""

from __future__ import annotations

import random
import re

from pyspark.sql import Column
from pyspark.sql import functions as F

NIS_WEIGHTS = (3, 2, 9, 8, 7, 6, 5, 4, 3, 2)


# ---------------------------------------------------------------- Python
def _cpf_check_digit(digits: list[int]) -> int:
    first_weight = len(digits) + 1
    remainder = sum(d * (first_weight - i) for i, d in enumerate(digits)) % 11
    return 0 if remainder < 2 else 11 - remainder


def _nis_check_digit(digits: list[int]) -> int:
    dv = 11 - sum(d * w for d, w in zip(digits, NIS_WEIGHTS, strict=True)) % 11
    return 0 if dv >= 10 else dv


def _only_digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def cpf_is_valid_py(cpf: str | None) -> bool:
    d = _only_digits(cpf)
    if not d or len(d) > 11:
        return False
    d = d.zfill(11)
    if len(set(d)) == 1:
        return False
    n = [int(x) for x in d]
    return _cpf_check_digit(n[:9]) == n[9] and _cpf_check_digit(n[:10]) == n[10]


def nis_is_valid_py(nis: str | None) -> bool:
    d = _only_digits(nis)
    if not d or len(d) > 11:
        return False
    d = d.zfill(11)
    if len(set(d)) == 1:
        return False
    n = [int(x) for x in d]
    return _nis_check_digit(n[:10]) == n[10]


def ibge_check_digit(base6: str) -> int:
    total = 0
    for i, ch in enumerate(base6):
        product = int(ch) * (1 if i % 2 == 0 else 2)
        total += product // 10 + product % 10
    return (10 - total % 10) % 10


def ibge_is_valid_py(code: int | str | None) -> bool:
    text = str(code or "")
    return len(text) == 7 and text.isdigit() and ibge_check_digit(text[:6]) == int(text[6])


def generate_cpf(rng: random.Random) -> str:
    while True:
        base = [rng.randrange(10) for _ in range(9)]
        if len(set(base)) > 1:
            break
    base.append(_cpf_check_digit(base))
    base.append(_cpf_check_digit(base))
    return "".join(map(str, base))


def generate_nis(rng: random.Random) -> str:
    base = [1] + [rng.randrange(10) for _ in range(9)]  # CadÚnico NIS numbers usually start with 1 or 2
    base.append(_nis_check_digit(base))
    return "".join(map(str, base))


def format_cpf(cpf: str) -> str:
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"


# ---------------------------------------------------------------- Spark
def only_digits(column: Column) -> Column:
    return F.regexp_replace(column, r"\D", "")


def normalize_document(column: Column) -> Column:
    """Digits only, left-padded with zeros to 11 positions; null when empty or longer than 11 digits."""
    d = only_digits(column)
    return F.when((F.length(d) >= 1) & (F.length(d) <= 11), F.lpad(d, 11, "0"))


def _digits(column: Column, n: int) -> list[Column]:
    # ascii(c) - 48 instead of a cast: with ANSI mode on (the Spark 4 default), CAST('' AS INT) raises
    return [F.ascii(F.substring(column, i + 1, 1)) - 48 for i in range(n)]


def _mod11_check_digit(digits: list[Column]) -> Column:
    first_weight = len(digits) + 1
    remainder = sum(d * (first_weight - i) for i, d in enumerate(digits)) % 11
    return F.when(remainder < 2, 0).otherwise(11 - remainder)


def _repeated(normalized: Column) -> Column:
    return normalized.rlike(r"^(\d)\1{10}$")


def cpf_is_valid(normalized: Column) -> Column:
    d = _digits(normalized, 11)
    ok = (_mod11_check_digit(d[:9]) == d[9]) & (_mod11_check_digit(d[:10]) == d[10]) & ~_repeated(normalized)
    return F.coalesce(ok, F.lit(False))


def nis_is_valid(normalized: Column) -> Column:
    d = _digits(normalized, 11)
    dv = 11 - sum(d[i] * NIS_WEIGHTS[i] for i in range(10)) % 11
    dv = F.when(dv >= 10, 0).otherwise(dv)
    return F.coalesce((dv == d[10]) & ~_repeated(normalized), F.lit(False))


def ibge_is_valid(code: Column) -> Column:
    text = code.cast("string")
    d = _digits(text, 7)
    total = sum(
        F.floor(d[i] * (1 if i % 2 == 0 else 2) / 10) + (d[i] * (1 if i % 2 == 0 else 2)) % 10 for i in range(6)
    )
    dv = (10 - total % 10) % 10
    return F.coalesce(text.rlike(r"^\d{7}$") & (dv == d[6]), F.lit(False))


VALIDATORS = {"cpf": cpf_is_valid, "nis": nis_is_valid}
