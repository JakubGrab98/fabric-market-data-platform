# Phase 2 continued: Silver `fundamentals` and `macro`

## Context

`docs/superpowers/specs/2026-08-10-phase2-silver-fx-prices-design.md` built
`silver_fx_rates`/`silver_prices` first and deliberately deferred FMP/GUS
Silver as "a separate, later round" because the dedup pattern might not
transfer cleanly. It doesn't transfer cleanly — this spec is that round.

`docs/data-model.md` (written just before this spec) already commits to the
Gold contract these two Silver modules need to produce: `fact_fundamentals`
and `fact_macro` are both long/EAV-format facts (`metric_name`/
`metric_value` rows, not one column per line item), chosen specifically
because FMP's field set is statement-type-dependent and dynamically
inferred in Bronze, and GUS's Bronze shape is already long. Silver's job
here is to get each Bronze table into that exact Gold-ready shape — this
spec's transforms are more of a pass-through-into-target-shape than the
prior spec's cast-and-join.

## `fundamentals` (from `bronze_fmp_balance_sheet` / `bronze_fmp_income_statement` / `bronze_fmp_cash_flow`)

**Why one Silver module reads three Bronze tables**: they share a ticker/
date/provenance envelope and only differ in which line-item fields are
present — treating them as three independent Silver modules would
triplicate the dedup/unpivot logic for no benefit. The notebook reads all
three, adds a `statement_type` literal per source table, and unions before
processing.

**FMP envelope columns** (present on every statement type, not unpivoted as
metrics): `date`, `symbol`, `reportedCurrency`, `cik`, `filingDate`,
`acceptedDate`, `fiscalYear`, `period`, plus the `ticker`/`source`/
`retrieved_at` columns this pipeline stamps in Bronze. These are FMP's
documented envelope fields, not verified against a live response (same
caveat as the rest of the FMP pipeline — see `docs/next-steps.md`); if a
live call ever shows a different envelope, this list is the one place to
fix it.

**Mechanism**:
1. Union the three Bronze tables (`unionByName(allowMissingColumns=True)`,
   same as the Bronze notebook already does across tickers), stamping
   `statement_type` (`balance_sheet` / `income_statement` / `cash_flow`)
   per source table before the union.
2. **Deduplicate**: partition by `(ticker, date, period, statement_type)`,
   order by `retrieved_at` desc, keep `row_number() == 1` — same
   "latest retrieved_at wins" rule as fx_rates/prices.
3. **Standardize**: cast `date` (period end) to `DateType`, dropping
   unparseable rows (same reasoning as the prior spec: a null natural-key
   column would silently re-duplicate on every `MERGE INTO`). Derive
   `period_type` from FMP's `period` field (`"FY"` -> `annual`, anything
   else -> `quarter`) — a formula, not a hardcoded value, so it still
   reflects reality if `period_limit`/`period` params ever change.
4. **Unpivot generically**: every Bronze column *not* in the envelope set
   is a metric. Column names are converted `camelCase` -> `snake_case`
   (`totalAssets` -> `total_assets`) per `CLAUDE.md`. Values are cast to
   `double`; rows where that cast is null (a non-numeric field slipping
   through, or a genuinely missing value) are dropped rather than carried
   as meaningless zero/null metric rows. Implemented with a dynamically
   built `stack()` expression since the metric column list isn't known
   until the DataFrame's actual schema is inspected at runtime — this is
   the one genuinely new mechanism relative to fx_rates/prices, forced by
   FMP's dynamic schema (see rationale in `docs/data-model.md`).
5. **Provenance and idempotent upsert**: same as fx_rates/prices —
   `source`/`retrieved_at` carried through, `MERGE INTO` on the natural key
   `(ticker, period_end_date, statement_type, metric_name)`.

**Schema — `silver_fundamentals`**: `ticker` (string), `period_end_date`
(date), `statement_type` (string), `period_type` (string), `fiscal_year`
(int), `reported_currency` (string), `metric_name` (string), `metric_value`
(double), `source` (string), `retrieved_at` (timestamp).

## `macro` (from `bronze_gus_macro`)

Much closer to the fx_rates/prices mechanism — Bronze is already one row per
`(indicator_name, year)` with a clean fixed schema
(`BRONZE_MACRO_SCHEMA`).

**Mechanism**:
1. **Deduplicate**: partition by `(indicator_name, year)`, order by
   `retrieved_at` desc, keep latest.
2. **Standardize**: add `country` (literal `"PL"` — GUS BDL is Poland-only;
   see `docs/data-model.md` for why this column exists now even with only
   one value today) and derive `reference_date` = December 31 of `year`
   (`to_date` on a constructed `"{year}-12-31"` string), matching
   `fact_macro`'s grain decision in `docs/data-model.md`.
3. **Provenance and idempotent upsert**: `source`/`retrieved_at`/
   `variable_id` carried through (kept for lineage even though Gold doesn't
   project it); `MERGE INTO` on `(indicator_name, year)`.

**Schema — `silver_macro`**: `indicator_name` (string), `variable_id`
(int), `year` (int), `country` (string), `reference_date` (date), `value`
(double), `unit` (string), `source` (string), `retrieved_at` (timestamp).

## Testing split

Same split as the prior Silver spec: dedup/standardize/unpivot are pure
DataFrame logic, tested with the existing local `spark` fixture in
`tests/conftest.py`. The `MERGE INTO`/`DeltaTable` call stays untested in
`notebook.py`, consistent with every other notebook in this repo.

## Out of scope

- Verifying the FMP envelope-column assumption against a live API response
  (blocked on an API key — tracked in `docs/next-steps.md`).
- A shared `notebooks/silver/common.py` (still blocked on the Fabric
  cross-folder-import question, same as the prior spec).
- Gold-layer modeling (already done, in `docs/data-model.md`).
