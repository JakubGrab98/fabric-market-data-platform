# ADR 0003: Long/EAV format for fact_fundamenty and fact_macro

## Status
Accepted

## Context
FMP's financial-statement fields differ per statement type (balance sheet /
income statement / cash flow) and are treated as dynamic all the way
through Bronze — `notebooks/bronze/fmp/transforms.py` infers each table's
schema from the union of fields actually present in the API response,
rather than a fixed `StructType`, because the field set can change between
API revisions. GUS BDL's Bronze shape
(`notebooks/bronze/gus/transforms.py`) is already one row per
`(indicator_name, year)` — effectively long-format from the source.

A wide `fact_fundamenty` (one column per financial line item) or a wide
`fact_macro` (one column per macro indicator) would need a Gold-schema
change — table/column update in `docs/data-model.md` plus the Power BI
semantic model, per `CLAUDE.md`'s boundary rule — every time FMP adds a
field or a new macro indicator is configured.

## Decision
`fact_fundamenty` and `fact_macro` are both long/EAV-format facts:
`metric_name`/`metric_value` rows rather than one column per line item or
indicator. See `docs/data-model.md` for the exact column list and grain of
each.

## Consequences
- New FMP fields or new macro indicators show up as new `metric_name`
  values — no Gold schema change, no `docs/data-model.md` update, no Power
  BI semantic model change needed to absorb them.
- Trade-off, stated plainly: Power BI consumption needs a pivot (matrix
  visual or DAX/Power Query unpivot-reversal) to get one row per period
  with columns per metric — not a plain table of measures out of the box.
  This is deliberate, not an oversight.
- `notebooks/silver/fundamentals/transforms.py`'s `unpivot_fundamentals_metrics`
  melts Bronze's wide, dynamic columns into this shape generically (every
  non-envelope column becomes a metric row) rather than hardcoding a fixed
  list of line items — the mechanism this ADR requires.
