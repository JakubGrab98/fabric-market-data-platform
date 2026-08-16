# Phase 6: cross-layer data quality reconciliation

## Context

`README_EN.md`'s Data Quality section lists "Cross-layer consistency checks
(row counts, date ranges) before publishing to Gold" — nothing implements
this yet. Bronze/Silver/Gold are all built (Phases 1-3), each individually
unit-tested, but nothing verifies the *transitions between* layers actually
behave the way each layer's design assumes (e.g. that Silver's dedup only
ever removes rows, never rows that shouldn't have been removed; that Gold's
`select()`-only transforms are truly lossless).

## Location

A new `notebooks/quality/reconciliation/` module — a new top-level notebook
category alongside `bronze`/`silver`/`gold`/`streaming`, since data quality
is its own roadmap phase (Phase 6) and README section, not an extension of
any single existing layer. Same internal shape as every other module
(`transforms.py` for testable logic, thin `notebook.py`).

## Scope

Row counts and date/period ranges only, matching the README bullet exactly
— not schema-drift or null-rate checks (a larger, separately-scoped
follow-up if it turns out to be needed).

Two layer transitions, with different pass criteria:

- **Bronze → Silver**: Silver deduplicates by natural key
  (`docs/superpowers/specs/2026-08-10-phase2-silver-fx-prices-design.md`,
  `.../2026-08-15-phase2-silver-fmp-gus-design.md`), so the target's row
  count is allowed to be *less than or equal to* the source's — never
  greater. The date/period range, however, is expected to be **identical**:
  dedup drops duplicate retrievals of the same date, not distinct dates.
- **Silver → Gold**: every Gold fact transform in this repo today
  (`notebooks/gold/fact_*/transforms.py`) is a `select()` with no filtering
  — a lossless passthrough (ADR-worthy pattern, not new to this spec). Row
  count and date/period range must match **exactly**.

Checked pairs (only where a Gold fact table actually exists downstream of a
Silver table — `dim_company`/`dim_date` aren't checked, since neither is
sourced from Bronze/Silver):

| Bronze | Silver | Gold | Range column (source → target) |
|---|---|---|---|
| `bronze_stooq_prices` | `silver_prices` | `fact_prices` | `date` → `date` |
| `bronze_nbp_fx_rates` | `silver_fx_rates` | *(none)* | `effective_date` → `effective_date` |
| `bronze_fmp_{balance_sheet,income_statement,cash_flow}` (unioned) | `silver_fundamentals` | `fact_fundamentals` | `date` → `period_end_date` |
| `bronze_gus_macro` | `silver_macro` | `fact_macro` | `year` → `year` (Bronze→Silver); `reference_date` → `reference_date` (Silver→Gold) |

`silver_fx_rates` has no downstream Gold fact (no `fact_fx_rates` in
`docs/data-model.md` — it's an input to future FX conversion, not a
standalone Gold table), so only its Bronze→Silver leg is checked.

## Mechanism

`compare_row_counts(source_df, target_df, *, exact)` and
`compare_range(source_df, target_df, source_column, target_column)` are
pure DataFrame aggregations (`count()`, `min()`/`max()`) — testable with the
local `spark` fixture, same as every other transform in this repo.
`summarize_check_results(results)` takes a `{name: result}` mapping and
returns overall pass/fail plus which named checks failed — plain-dict logic,
no Spark needed.

The notebook runs every pair, collects named results, and **raises** if any
check failed, listing which ones. This repo has no orchestration layer yet
(Phase 4/Data Factory isn't built) to consume a structured failure signal,
so a raised exception — the same mechanism every Bronze fetch error already
uses (`FmpFetchError`, `GusFetchError`, ...) — is the right-sized behavior
today: it fails the notebook run loudly. A dedicated results/audit Delta
table is not built now — speculative ahead of Phase 4 actually existing to
consume it.

## Testing split

Same split as every other module: `compare_row_counts`/`compare_range`/
`summarize_check_results` are fully unit-tested. The notebook cell's
`spark.read.table(...)` calls against real Bronze/Silver/Gold tables stay
untested, consistent with every other notebook in this repo (none has run
against a real Fabric workspace yet — `docs/source-log.md`).

## Out of scope

- Schema-drift and null-rate checks (README's other Data Quality items —
  larger scope, separate follow-up).
- A results/audit Delta table (see above).
- Referential-integrity checks (e.g. every `fact_prices.ticker` exists in
  `dim_company`) — a different kind of check than row-count/date-range
  reconciliation; tracked as a follow-up in `docs/next-steps.md`, not built
  here.
