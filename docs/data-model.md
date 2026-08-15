# Data model — Gold layer

Authoritative column-level reference for the star schema, per `docs/README.md`.
`README_EN.md` and `architecture.mermaid` describe the same five tables at a
narrative level; this file is the source of truth for columns, types, keys,
and grain. Update this file in the same PR as any Gold table/column change
(`CLAUDE.md` boundary rule) — including the Power BI semantic model
references, once one exists.

## Naming

Physical table names are **Polish**, matching `architecture.mermaid` (already
committed) and `CLAUDE.md`'s "Table naming" convention: `dim_spolka`,
`dim_data`, `fact_ceny`, `fact_fundamenty`, `fact_makro`. `README_EN.md` uses
English glosses (`dim_company`, `fact_prices`, ...) for the same tables in
prose — that's a documentation-language choice, not a second physical naming
scheme. `CLAUDE.md` defers to `README_PL.md` for naming, which doesn't exist
yet (tracked in `docs/next-steps.md`); this doc follows `architecture.mermaid`
as the tie-breaker since it's the one already-committed source using these
names concretely.

All columns `snake_case` per `CLAUDE.md`.

## Keys: natural, not surrogate

Every table below uses a natural key (`ticker`, `date`, ...) rather than a
generated surrogate int key. This matches the convention already established
in Bronze/Silver (`ticker` and `date` are the natural keys throughout
`notebooks/silver/prices`, `notebooks/silver/fx_rates`) and avoids surrogate-
key machinery (identity generation, SCD handling) this project has no present
need for — no dimension attribute here is expected to change in a way that
needs history tracked. Revisit only if a real slowly-changing-dimension
requirement shows up (e.g. a ticker's listing currency actually changes).

## dim_spolka

**Grain**: one row per ticker.

Built directly from `notebooks/config/tickers.yaml` — this is static
reference config, not data ingested from an external API through Bronze/
Silver, so it has no `source`/`retrieved_at` columns (nothing to trace back
to a retrieval run).

| Column | Type | Notes |
|---|---|---|
| `ticker` | string | **PK.** Matches Silver `ticker`. |
| `company_name` | string | From config `company_name`. |
| `listing_currency` | string | From config `currency`. |
| `fmp_symbol` | string, nullable | From config `fmp_symbol`. **Unverified** — see `notebooks/config/tickers.yaml` header comment and `docs/next-steps.md`; carried through so a future consumer can see which FMP symbol a fundamentals row actually resolved from. |

## dim_data

**Grain**: one row per calendar date.

Generated (a standard Kimball calendar utility table), not ingested — no
`source`/`retrieved_at`. Date range should come from a notebook parameter
(e.g. min date seen across Silver tables to today + 1 year buffer), not a
hardcoded literal range, per `CLAUDE.md`'s "formulas over magic numbers."

| Column | Type | Notes |
|---|---|---|
| `date` | date | **PK.** |
| `year` | int | |
| `quarter` | int | 1-4 |
| `month` | int | 1-12 |
| `month_name` | string | |
| `week_of_year` | int | |
| `day_of_week` | int | 1-7 |
| `day_name` | string | |
| `is_trading_day_gpw` | boolean | `is_weekday AND NOT is_polish_public_holiday`. Compute the holiday side with a maintained library (e.g. the `holidays` PyPI package, `country="PL"`) rather than a hand-maintained date list — that's the "formula" here, not a magic number. **Known limitation**: GPW has exchange-specific closures beyond public holidays (e.g. Christmas Eve half-day) this flag won't catch; refine only if an analysis actually needs that precision. |

## fact_ceny

**Grain**: one row per `(ticker, date)`.

Straight carry-through from `silver_prices` — Gold adds the star-schema FK
shape but no new transformation; Silver is already flat at this grain.

| Column | Type | Notes |
|---|---|---|
| `ticker` | string | FK -> `dim_spolka.ticker` |
| `date` | date | FK -> `dim_data.date` |
| `open` | double | |
| `high` | double | |
| `low` | double | |
| `close` | double | |
| `volume` | long | |
| `currency` | string | From Silver's currency join. Not FX-converted (see below). |
| `source` | string | Provenance carried through. |
| `retrieved_at` | timestamp | Provenance carried through. |

**Known limitation**: no FX-converted column yet, because `silver_prices`
deliberately doesn't do FX conversion (every current ticker trades in PLN —
see `docs/superpowers/specs/2026-08-10-phase2-silver-fx-prices-design.md`,
"out of scope"). If/when a non-PLN ticker is added and conversion lands in
Silver, it would surface here as an additional column (e.g. `close_pln`),
not a replacement of `currency`/`close`.

## fact_fundamenty

**Grain**: one row per `(ticker, period_end_date, statement_type,
metric_name)` — a long/narrow ("EAV") fact, not one column per financial
line item.

**Why long format, not one wide fact**: FMP's field set differs per
statement type (balance sheet / income statement / cash flow) and is treated
as dynamic all the way through Bronze (`notebooks/bronze/fmp/transforms.py`
infers schema from the field union across records rather than a fixed
`StructType`, precisely because the field set varies and can change between
API revisions). A single wide `fact_fundamenty` with one column per possible
line item would be extremely sparse and would need a Gold-schema change
(`docs/data-model.md` + Power BI update, per `CLAUDE.md`) every time FMP
adds or drops a field. The long format absorbs that variability without a
schema change — new metrics just appear as new `metric_name` values.

**Trade-off, stated plainly**: this is not directly a normal Power BI table
of measures — consumption needs a pivot (matrix visual or a DAX/Power Query
unpivot-reversal) to get one row per period with columns per metric. This is
a deliberate trade-off for schema stability over consumption convenience,
not an oversight.

| Column | Type | Notes |
|---|---|---|
| `ticker` | string | FK -> `dim_spolka.ticker` |
| `period_end_date` | date | FK -> `dim_data.date`. The statement's reporting period end date. |
| `statement_type` | string | `balance_sheet` \| `income_statement` \| `cash_flow` |
| `period_type` | string | `quarter` \| `annual`. Bronze currently only fetches `quarter` (`period_limit`/`period=quarter` in `build_*_url`); column exists because FMP's API supports both. |
| `fiscal_year` | int | From FMP's `fiscalYear`/`calendarYear` field. |
| `metric_name` | string | FMP's field name, standardized to `snake_case` in Silver (FMP returns camelCase, e.g. `totalAssets` -> `total_assets`). |
| `metric_value` | double | |
| `reported_currency` | string | From FMP's `reportedCurrency` field, where present. |
| `source` | string | Provenance carried through. |
| `retrieved_at` | timestamp | Provenance carried through. |

## fact_makro

**Grain**: one row per `(country, indicator_name, year)`, represented via
`reference_date` = December 31 of the reporting year (standard convention
for annual snapshot data joining to a daily date dimension).

Bronze/GUS is already effectively long-format
(`BRONZE_MACRO_SCHEMA` in `notebooks/bronze/gus/transforms.py`:
`indicator_name`, `variable_id`, `year`, `value`, `unit`, `source`,
`retrieved_at`) — Gold continues that shape rather than pivoting to one
column per indicator, for the same schema-stability reason as
`fact_fundamenty`: new indicators (e.g. a future GDP-per-capita or CPI
sub-index) shouldn't require a Gold-schema change.

| Column | Type | Notes |
|---|---|---|
| `country` | string | Currently always `"PL"` — GUS BDL is Poland-only; Eurostat (other countries) was explicitly scoped out of Phase 1 (`docs/next-steps.md`). Included now so the grain is unambiguous and adding a second country later is a new row shape, not a schema change. |
| `reference_date` | date | FK -> `dim_data.date`. December 31 of `year`. |
| `indicator_name` | string | `cpi` \| `unemployment_rate` \| `gdp` — matches `notebooks/config/macro_indicators.yaml` `name`. |
| `value` | double | |
| `unit` | string | `-` (index) / `%` / `mln zł` depending on indicator — carried through, not normalized to one unit, since the three indicators aren't comparable on a common scale anyway. |
| `source` | string | Provenance carried through. |
| `retrieved_at` | timestamp | Provenance carried through. |

## Open dependencies

- `fact_fundamenty` and `fact_makro` assume Silver modules for FMP and GUS
  that don't exist yet (tracked as the next implementation step after this
  doc). Their Silver output is being designed to match this Gold contract
  directly — long format with `metric_name`/`metric_value` for fundamentals,
  a pass-through of the Bronze macro shape for macro — so no schema rework
  should be needed once that Silver work lands.
- `is_trading_day_gpw`'s holiday-library choice is a recommendation, not yet
  implemented — first real decision point when `notebooks/gold/dim_data` is
  built.
