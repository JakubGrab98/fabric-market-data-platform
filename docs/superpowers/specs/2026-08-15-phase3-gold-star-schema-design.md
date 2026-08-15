# Phase 3: Gold star schema

## Context

`docs/data-model.md` already specifies columns, keys, and grain for all five
Gold tables. This spec covers *how* each notebook builds its table — the
mechanism, not the schema (that's `docs/data-model.md`'s job, and this spec
doesn't repeat it).

Phase 2 Silver is complete for all four Bronze sources (fx_rates, prices,
fundamentals, macro) as of
`docs/superpowers/specs/2026-08-15-phase2-silver-fmp-gus-design.md`. Gold
reads from Silver (`silver_prices`, `silver_fundamentals`, `silver_macro`)
except `dim_company`, which reads directly from
`notebooks/config/tickers.yaml` (static reference config, never ingested
through Bronze/Silver — see `docs/data-model.md`).

## Keys

No surrogate keys anywhere — natural keys throughout (`ticker`, `date`),
matching the decision already recorded in `docs/data-model.md`. This also
means fact tables don't need a dimension lookup/join to resolve a surrogate
key before merging; they select/rename directly from Silver.

## dim_company

Built from `notebooks/config/tickers.yaml`, not from a Bronze/Silver read.
`load_ticker_config` is duplicated here rather than imported from
`notebooks/silver/prices/transforms.py` — consistent with the existing,
deliberate duplication across Bronze/Silver modules (`docs/next-steps.md`:
the shared-module extraction is blocked on confirming Fabric's cross-folder
notebook import behavior, not an oversight here).

Idempotent upsert on `ticker`.

## dim_date

Generated, not ingested — a standard calendar utility table. Built with
`sequence()` over a `(start_date, end_date)` bound, not a hardcoded date
range (the range itself is a notebook parameter).

`is_trading_day_gpw` is a **weekday-only approximation** in this first cut
(`Mon-Fri`, via Spark's `dayofweek()`) — it does **not** exclude Polish
public holidays yet. `docs/data-model.md` recommends a maintained holiday
library (e.g. `holidays`, `country="PL"`) for the real formula; that's a new
dependency this spec deliberately doesn't add speculatively. Tracked as a
follow-up in `docs/next-steps.md` rather than silently shipped as "done."

Idempotent upsert on `date`.

## fact_prices / fact_fundamentals / fact_macro

All three are near-identical in mechanism: Silver already produces (or, for
`fact_macro`, nearly produces) the exact Gold-shape columns specified in
`docs/data-model.md`, so the Gold transform is a `select()` into the
canonical column order — no new business logic, just making the contract
explicit and dropping any Silver-only column that shouldn't cross into Gold
(`fact_macro` drops Silver's `variable_id`, kept there only for lineage).

Idempotent upsert keys: `fact_prices` on `(ticker, date)`; `fact_fundamentals`
on `(ticker, period_end_date, statement_type, metric_name)`; `fact_macro` on
`(country, indicator_name, reference_date)` (`reference_date` already
uniquely encodes the year, since it's always December 31 of that year).

## Testing split

Same as every prior spec: the `select()`/generation logic is pure DataFrame
code, tested with the local `spark` fixture. `MERGE INTO` stays untested in
`notebook.py`.

## Out of scope

- Power BI semantic model / report build (Phase 3's roadmap item, tracked
  separately — this spec is the Fabric notebook layer only).
- Real GPW holiday calendar for `is_trading_day_gpw` (see above).
- Backfilling `dim_date`'s range choice against real data volume — start
  with a documented, generous parameter default and adjust if needed.
