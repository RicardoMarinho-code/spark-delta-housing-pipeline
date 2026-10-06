"""Applies the LGPD policy declared in the column catalog (layouts.py) to a typed DataFrame."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from housing_lakehouse.governance.documents import VALIDATORS, normalize_document, only_digits
from housing_lakehouse.governance.pseudonymization import pseudonymize
from housing_lakehouse.layouts import DROP, GENERALIZE, PSEUDONYMIZE, Layout


def normalize_code(column: Column) -> Column:
    """Internal codes (family, person): digits only, no leading zeros; null when empty."""
    d = F.regexp_replace(only_digits(F.trim(column)), r"^0+", "")
    return F.when(d != "", d)


def _zip5(column: Column, _: date) -> Column:
    d = only_digits(column)
    return F.when(F.length(d) == 8, F.substring(d, 1, 5))


def _age(column: Column, reference_date: date) -> Column:
    return F.floor(F.months_between(F.lit(reference_date), column) / 12).cast("int")


TREATMENTS: dict[str, Callable[[Column, date], Column]] = {"zip5": _zip5, "age": _age}


def apply_privacy_policy(df: DataFrame, layout: Layout, key: bytes, reference_date: date) -> DataFrame:
    """Drops, pseudonymizes or generalizes each column as the catalog says, renaming it to its target.

    Technical columns (prefix "_") pass through. Documents (CPF/NIS) get a `<document>_status` column
    (valid | invalid | missing); an invalid document gets no pseudonym, so it cannot create false joins.
    """
    selection: list[Column] = []
    for col in layout.columns:
        if col.action == DROP:
            continue
        source = F.col(col.name)
        if col.action == PSEUDONYMIZE and col.document:
            normalized = normalize_document(source)
            valid = VALIDATORS[col.document](normalized)
            selection.append(F.when(valid, pseudonymize(normalized, key, col.document)).alias(col.target_name))
            status = (
                F.when(source.isNull() | (only_digits(source) == ""), "missing")
                .when(valid, "valid")
                .otherwise("invalid")
            )
            selection.append(status.alias(f"{col.document}_status"))
            continue
        if col.action == PSEUDONYMIZE:
            expr = pseudonymize(normalize_code(source), key, col.target_name)
        elif col.action == GENERALIZE:
            expr = TREATMENTS[col.treatment](source, reference_date)
        else:
            expr = source
        selection.append(expr.alias(col.target_name))
    technical = [c for c in df.columns if c.startswith("_")]
    return df.select(*selection, *technical)
