# Phase 1 completion: FMP fundamentals + GUS BDL macro Bronze ingestion

## Context

Phase 1 (batch ingestion) of the roadmap ([README_EN.md](../../../README_EN.md)) is partially
done: NBP (FX) and Stooq (GPW prices) Bronze notebooks exist, each following the same shape —
thin `notebook.py` + `transforms.py` with pure functions + config-driven entity list + tests
mirroring `transforms.py`. This spec covers the two remaining Phase 1 sources: **FMP**
(company fundamentals) and **GUS BDL** (macroeconomic indicators, Poland only for now).

Eurostat is explicitly out of scope for this round — GUS and Eurostat are different APIs with
different data shapes despite being grouped together in the roadmap; Eurostat becomes its own
decision later if EU-wide comparison data is actually needed.

## 1. FMP fundamentals (`notebooks/bronze/fmp/`)

### Scope

All three FMP `stable` statement endpoints, quarterly period, all source fields kept (Bronze
stays 1:1 with source per `CLAUDE.md`):

- `https://financialmodelingprep.com/stable/balance-sheet-statement?symbol=X&period=quarter&apikey=...`
- `https://financialmodelingprep.com/stable/income-statement?symbol=X&period=quarter&apikey=...`
- `https://financialmodelingprep.com/stable/cashflow-statement?symbol=X&period=quarter&apikey=...`

Exact response field lists are not pinned in this spec — they get confirmed against a live FMP
response during implementation (same posture as the existing caveat on Stooq's CSV header: verify
before trusting silently).

### Tables (three, not one)

Each statement type has a genuinely different field set, so — unlike GUS below — this stays as
three separate Bronze tables rather than one generic table:

- `bronze_fmp_balance_sheet`
- `bronze_fmp_income_statement`
- `bronze_fmp_cash_flow`

Each: all fields from that endpoint's JSON response + `ticker` (internal ticker, not the FMP
symbol) + `source` + `retrieved_at`.

### Config

Add `fmp_symbol` to `notebooks/config/tickers.yaml`, alongside the existing `stooq_symbol` — FMP's
symbol format isn't guaranteed to match Stooq's, especially for non-US listings, so it needs its
own explicit, verified field rather than reusing `ticker`.

### Parameters — deviation from the date-range pattern

NBP and Stooq take `start_date`/`end_date`. FMP's statement endpoints instead return the most
recent N periods via a `limit` parameter — there's no date-range query. This notebook takes
`period_limit: int = 8` (last 8 quarters) instead of a date range. This is a deliberate departure
from the existing convention, not an oversight.

### API key

First source in this repo that requires authentication. `fmp_api_key: str` is a notebook
parameter (substituted from Fabric Variable Library / Key Vault at pipeline run time), per
`CLAUDE.md`'s "no hardcoded IDs/secrets" rule. Tests never call the live API — they exercise
`parse_*` against fixture payloads, so no real key is needed to run the test suite.

### Rate limiting

3 tickers × 3 statements = 9 requests per run, against a 250 requests/day free-tier limit. No
throttling/backoff logic is added now (YAGNI) — revisit if the ticker universe grows enough to
approach the limit.

### Shared helper

One `fetch_fmp_json(url, timeout) -> dict` used by all three `parse_*` functions, mirroring the
`fetch_nbp_rates`/`fetch_stooq_csv` shape in the existing notebooks.

## 2. GUS BDL macro (`notebooks/bronze/gus/`)

### Scope

GUS BDL API only (`https://bdl.stat.gov.pl/api/v1`), Poland-level (national) data, three
indicators: CPI/inflation, unemployment rate, GDP. Anonymous access is sufficient (no API key) —
rate limits (100 req/15min anonymous) comfortably cover 3 requests/run.

### Table (one, not three)

Unlike FMP, every GUS BDL variable returns the same generic time-series shape
(`{id, year, val, unit, ...}` per `/data/by-variable/{id}`) regardless of which statistic it is.
One generic table is the right fit here — three separate tables would just duplicate an identical
schema three times:

`bronze_gus_macro`: `indicator_name`, `variable_id`, `year`, `value`, `unit`, `source`,
`retrieved_at`.

### Config

`notebooks/config/macro_indicators.yaml`: list of `{name, gus_variable_id, unit}`. The actual
numeric `gus_variable_id` values for CPI/unemployment/GDP are **not guessed in this spec** — they
get looked up via `GET /variables/search?name=...` during implementation and written into the
config as a concrete, verified implementation step, not a placeholder left in code.

### Parameters

- `start_year: int`, `end_year: int` — the year-range equivalent of `start_date`/`end_date`.
- `unit_level: str = "0"` — `0` means national/Poland-wide in the BDL API; kept as a parameter
  (not hardcoded) in case a future notebook run needs a different territorial level.
- No `api_key` parameter for now — anonymous access is sufficient at this request volume.

## 3. Tests

`tests/bronze/test_fmp_transforms.py` and `tests/bronze/test_gus_transforms.py`, mirroring the
existing `test_nbp_transforms.py`/`test_stooq_transforms.py` shape: URL-builder assertions, one
`parse_*` test per parse function against a fixture payload asserting provenance columns
(`source`, `retrieved_at`) are stamped correctly, and a config-loader test using `tmp_path`.

## Out of scope for this spec

- Eurostat ingestion (separate decision later).
- `docs/source-log.md` / `docs/data-model.md` (documentation catch-up — separate task, not blocking
  Phase 1 ingestion work).
- Any Silver/Gold modeling of FMP or GUS data.