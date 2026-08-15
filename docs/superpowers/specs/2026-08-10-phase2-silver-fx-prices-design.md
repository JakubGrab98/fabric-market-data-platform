# Phase 2 start: Silver `fx_rates` and `prices`

## Context

Phase 1 (batch ingestion into Bronze) is complete for NBP (FX rates), Stooq (GPW prices), FMP
(fundamentals), and GUS BDL (macro indicators). Phase 2 (`README_EN.md`) is "Transformation:
cleaning and standardization in Silver." This spec covers the first two Silver modules —
**`fx_rates`** (from `bronze_nbp_fx_rates`) and **`prices`** (from `bronze_stooq_prices`) — the two
oldest, smallest-surface Bronze sources, chosen to validate the Bronze→Silver pipeline shape
before extending it to FMP/GUS (tracked as a follow-up in `docs/next-steps.md`).

`notebooks/silver/README.md` already states the contract this spec implements: "Cleaning,
deduplication, and standardization (currencies, dates, units) across both batch and streaming
Bronze inputs. Upsert/merge into Delta — must be idempotent for a given date/parameter set."

## Why two modules, one spec

`fx_rates` and `prices` are independent Silver tables with different schemas, different natural
keys, and different source Bronze tables — same relationship as FMP/GUS were in Phase 1. They
share one spec because the *mechanism* (dedup-then-merge) is identical between them, unlike
FMP/GUS where the designs genuinely diverged. They get separate implementation plans.

## Naming: source-agnostic, not source-prefixed

Folders and tables are named `notebooks/silver/fx_rates/` / `silver_fx_rates` and
`notebooks/silver/prices/` / `silver_prices` — not `nbp`/`stooq` — because Silver's own README
describes merging **both batch and streaming** Bronze inputs into one table (e.g., Phase 5's
Finnhub stream will eventually feed into `silver_prices` alongside Stooq). Naming the table after
today's only source would mean renaming it later; naming it after the concept it represents does
not.

## Mechanism (shared by both modules)

Each Silver notebook, every run:

1. `spark.read.table(<bronze_table>)` — the **whole** Bronze table, no date-range filter. Bronze
   tables are tiny today (single-digit thousands of rows at most); a date-range parameter on the
   Silver side would add complexity (keeping Bronze/Silver run ranges in sync) with no present
   benefit. Revisit if/when volume makes a full-table read genuinely expensive.
2. **Deduplicate** using a window function: partition by the natural key, order by `retrieved_at`
   descending, keep only `row_number() == 1`. This is the "latest retrieved_at wins" rule —
   handles the case where Bronze was re-ingested for an overlapping range and the source
   corrected a value between pulls (rare, but real for "today's" price/rate).
   - `silver_fx_rates` natural key: `(currency_code, effective_date)`
   - `silver_prices` natural key: `(ticker, date)`
3. **Standardize types**: the Bronze `effective_date`/`date` string column becomes a proper
   `DateType` column.
4. **`prices` only** — join `currency` in from `notebooks/config/tickers.yaml` (by `ticker`). No
   FX conversion: every ticker in the config today trades in PLN, so there is nothing to convert
   yet, and building conversion logic (which would need `silver_fx_rates` as an input, plus a
   rule for missing rates on non-trading days) ahead of an actual non-PLN ticker is speculative.
   The column exists so a future non-PLN ticker is visible as a currency mismatch immediately,
   rather than silently mixed into PLN-denominated aggregates.
5. **Provenance carries through**: the winning row's `source` and `retrieved_at` are kept on the
   Silver row (not dropped), so a Silver value can still be traced to which Bronze ingestion run
   produced it.
6. **Idempotent upsert**: `MERGE INTO` the Silver Delta table on the natural key —
   `whenMatchedUpdateAll().whenNotMatchedInsertAll()`. On first run (table doesn't exist yet), a
   plain `write` creates it.

## Testing split: what's testable without Delta

Steps 1-5 above are pure Spark DataFrame logic (dedup, casts, a config join) — testable today with
the same local `SparkSession.builder.master("local[1]")` fixture every existing test file already
uses, no new dependency required. This logic lives in each module's `transforms.py`, per the
existing repo convention ("notebook cells stay thin; logic lives in `transforms.py`").

Step 6 (`MERGE INTO` / `DeltaTable`) needs a real Delta-enabled Spark session — something no test
in this repo has needed so far, because every existing Bronze notebook's `.write.format("delta")`
call was left in the untested `notebook.py`, not `transforms.py`. This spec follows the same
split: the merge call stays in `notebook.py`, stays untested, and this project does **not** add
`delta-spark` as a test dependency. `notebook.py` already can't run standalone locally anyway
(Fabric injects the `spark` global) — this isn't a new gap, just an existing one extended to
Silver.

## Duplication between the two modules (deliberate, not an oversight)

The dedup-by-natural-key window function will be near-identical between `fx_rates/transforms.py`
and `prices/transforms.py`. This spec does **not** extract a shared `notebooks/silver/common.py`
helper. Reason: `docs/next-steps.md` already flags an open, unresolved question — whether Fabric's
notebook runtime can import a sibling-of-siblings module at all, given every existing
`notebook.py` uses flat `from transforms import ...` (not a package-relative import). Silver
duplicating the same pattern Bronze already established (e.g. `load_ticker_config` copied across
all four Bronze sources) is consistent, not a new problem — and adds a third/fourth data point to
the case for spiking that shared-module question, already tracked as a follow-up.

## Schemas

**`silver_fx_rates`**: `currency_code` (string), `effective_date` (date), `mid_rate` (double),
`source` (string), `retrieved_at` (timestamp).

**`silver_prices`**: `ticker` (string), `date` (date), `open`/`high`/`low`/`close` (double),
`volume` (long), `currency` (string), `source` (string), `retrieved_at` (timestamp).

## Out of scope for this spec

- FMP/GUS Silver modules (separate, later round — Bronze tables are wider/newer and the dedup
  pattern may not transfer as cleanly, e.g. FMP's per-statement-type tables).
- FX conversion logic in `silver_prices` (no non-PLN ticker exists yet to need it).
- A shared `notebooks/silver/common.py` (blocked on the Fabric cross-folder-import question).
- Gold-layer modeling of either table.