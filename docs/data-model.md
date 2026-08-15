# Data model — Gold layer

Authoritative column-level reference for the star schema, per `docs/README.md`.
`README_EN.md` and `architecture.mermaid` describe the same five tables at a
narrative level; this file is the source of truth for columns, types, keys,
and grain. Update this file in the same PR as any Gold table/column change
(`CLAUDE.md` boundary rule) — including the Power BI semantic model
references, once one exists.

## Naming

Physical table names are **English**: `dim_company`, `dim_date`,
`fact_prices`, `fact_fundamentals`, `fact_macro`, matching
`architecture.mermaid` and `CLAUDE.md`'s "Table naming" convention. This
project briefly used Polish names for the same tables (`dim_spolka`,
`dim_data`, `fact_ceny`, `fact_fundamenty`, `fact_makro`) — see ADR 0001
(`docs/adr/0001-polish-gold-table-names.md`) for that original decision and
ADR 0006 (`docs/adr/0006-english-gold-table-names.md`) for why it was
reversed to keep the codebase in one language throughout.

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

## dim_company

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

## dim_date

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
| `is_trading_day_gpw` | boolean | `is_weekday AND NOT is_polish_public_holiday`. Compute the holiday side with a maintained library (e.g. the `holidays` PyPI package, `country="PL"`) rather than a hand-maintained date list — that's the "formula" here, not a magic number. **Known limitation**: GPW has exchange-specific closures beyond public holidays (e.g. Christmas Eve half-day) this flag won't catch; refine only if an analysis actually needs that precision. (`gpw` here names the real Warsaw Stock Exchange, Giełda Papierów Wartościowych — a proper noun, not a language-consistency exception.) |

## fact_prices

**Grain**: one row per `(ticker, date)`.

Straight carry-through from `silver_prices` — Gold adds the star-schema FK
shape but no new transformation; Silver is already flat at this grain.

| Column | Type | Notes |
|---|---|---|
| `ticker` | string | FK -> `dim_company.ticker` |
| `date` | date | FK -> `dim_date.date` |
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

## fact_fundamentals

**Grain**: one row per `(ticker, period_end_date, statement_type,
metric_name)` — a long/narrow ("EAV") fact, not one column per financial
line item.

**Why long format, not one wide fact**: FMP's field set differs per
statement type (balance sheet / income statement / cash flow) and is treated
as dynamic all the way through Bronze (`notebooks/bronze/fmp/transforms.py`
infers schema from the field union across records rather than a fixed
`StructType`, precisely because the field set varies and can change between
API revisions). A single wide `fact_fundamentals` with one column per
possible line item would be extremely sparse and would need a Gold-schema
change (`docs/data-model.md` + Power BI update, per `CLAUDE.md`) every time
FMP adds or drops a field. The long format absorbs that variability without
a schema change — new metrics just appear as new `metric_name` values.

**Trade-off, stated plainly**: this is not directly a normal Power BI table
of measures — consumption needs a pivot (matrix visual or a DAX/Power Query
unpivot-reversal) to get one row per period with columns per metric. This is
a deliberate trade-off for schema stability over consumption convenience,
not an oversight.

| Column | Type | Notes |
|---|---|---|
| `ticker` | string | FK -> `dim_company.ticker` |
| `period_end_date` | date | FK -> `dim_date.date`. The statement's reporting period end date. |
| `statement_type` | string | `balance_sheet` \| `income_statement` \| `cash_flow` |
| `period_type` | string | `quarter` \| `annual`. Bronze currently only fetches `quarter` (`period_limit`/`period=quarter` in `build_*_url`); column exists because FMP's API supports both. |
| `fiscal_year` | int | From FMP's `fiscalYear`/`calendarYear` field. |
| `metric_name` | string | FMP's field name, standardized to `snake_case` in Silver (FMP returns camelCase, e.g. `totalAssets` -> `total_assets`). |
| `metric_value` | double | |
| `reported_currency` | string | From FMP's `reportedCurrency` field, where present. |
| `source` | string | Provenance carried through. |
| `retrieved_at` | timestamp | Provenance carried through. |

## fact_macro

**Grain**: one row per `(country, indicator_name, year)`, represented via
`reference_date` = December 31 of the reporting year (standard convention
for annual snapshot data joining to a daily date dimension).

Bronze/GUS is already effectively long-format
(`BRONZE_MACRO_SCHEMA` in `notebooks/bronze/gus/transforms.py`:
`indicator_name`, `variable_id`, `year`, `value`, `unit`, `source`,
`retrieved_at`) — Gold continues that shape rather than pivoting to one
column per indicator, for the same schema-stability reason as
`fact_fundamentals`: new indicators (e.g. a future GDP-per-capita or CPI
sub-index) shouldn't require a Gold-schema change.

| Column | Type | Notes |
|---|---|---|
| `country` | string | Currently always `"PL"` — GUS BDL is Poland-only; Eurostat (other countries) was explicitly scoped out of Phase 1 (`docs/next-steps.md`). Included now so the grain is unambiguous and adding a second country later is a new row shape, not a schema change. |
| `reference_date` | date | FK -> `dim_date.date`. December 31 of `year`. |
| `indicator_name` | string | `cpi` \| `unemployment_rate` \| `gdp` — matches `notebooks/config/macro_indicators.yaml` `name`. |
| `value` | double | |
| `unit` | string | `-` (index) / `%` / `mln zł` depending on indicator — carried through, not normalized to one unit, since the three indicators aren't comparable on a common scale anyway. |
| `source` | string | Provenance carried through. |
| `retrieved_at` | timestamp | Provenance carried through. |

## Open dependencies

- `is_trading_day_gpw`'s holiday-library choice is a recommendation, not yet
  implemented — first real decision point when `notebooks/gold/dim_date` is
  revisited.
