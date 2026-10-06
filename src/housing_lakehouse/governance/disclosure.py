"""Statistical disclosure control for published data: small-cell suppression.

A small count (e.g. 2 indigenous families in precarious housing in one municipality) can identify
people. Before publishing, counts from 1 to `minimum - 1` become null, together with the rates
derived from them (otherwise the count could be recomputed from the rate and the total).

This is primary suppression only. Complementary suppression (blocking recomputation by
differencing totals) is listed as future work in docs/lgpd.md.
"""

from __future__ import annotations

from typing import Any


def suppress(record: dict[str, Any], dependencies: dict[str, list[str]], minimum: int) -> tuple[dict, list[str]]:
    """Returns a copy with small cells suppressed, plus the list of suppressed fields."""
    out = dict(record)
    suppressed: list[str] = []
    for count, derived in dependencies.items():
        value = out.get(count)
        if value is not None and 0 < value < minimum:
            for field in (count, *derived):
                if field in out:
                    out[field] = None
                    suppressed.append(field)
    return out, suppressed
