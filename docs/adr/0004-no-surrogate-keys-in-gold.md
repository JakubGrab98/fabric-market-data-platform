# ADR 0004: No surrogate keys in Gold

## Status
Accepted

## Context
Kimball-style star schemas conventionally use generated surrogate integer
keys (e.g. an auto-incrementing `spolka_id`) rather than natural business
keys, mainly to support slowly-changing-dimension (SCD) history tracking
and to decouple fact tables from source-system key changes. This project's
Bronze and Silver layers already key everything by natural identifiers —
`ticker`, `date`, `currency_code` — with no surrogate-key machinery
anywhere in the pipeline.

## Decision
Gold tables use natural keys throughout: `dim_spolka` is keyed by `ticker`,
`dim_data` by `date`, and each fact table's grain is expressed directly in
terms of those natural keys (e.g. `fact_ceny` on `(ticker, date)`). No
surrogate integer keys are introduced.

## Consequences
- Fact-table builds don't need a dimension lookup/join to resolve a
  surrogate key before writing — `notebooks/gold/fact_*/transforms.py` are
  simple `select()`s straight from Silver.
- No SCD history tracking is possible without revisiting this (e.g. if a
  ticker's listing currency actually changed, the old value wouldn't be
  recoverable from `dim_spolka` alone). No such requirement exists today.
- Revisit only if a real slowly-changing-dimension need shows up — don't
  add surrogate keys speculatively ahead of that.
