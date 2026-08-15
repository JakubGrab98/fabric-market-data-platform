# Source log

Per-source reference for every external data source ingested into Bronze:
endpoint, format, licensing/rate-limit constraints, and verification status.
Every Bronze row also carries its own `source`/`retrieved_at` columns
(`CLAUDE.md` — non-optional) for per-row provenance; this file is the
per-source summary those columns roll up to, not a replacement for them.

**No source below has been run against a real Fabric workspace yet** — all
four notebooks are implemented and unit-tested locally
(`tests/bronze/test_*_transforms.py`), but none has a confirmed production
run date. Fill in "First production run" once one actually happens instead
of leaving a placeholder date that looks real.

## NBP (Narodowy Bank Polski) — FX rates

- **Endpoint**: `https://api.nbp.pl/api/exchangerates/rates/A/{code}/{start}/{end}/?format=json` (Table A, mid rates).
- **Bronze notebook**: `notebooks/bronze/nbp/`
- **Licensing**: Public API, no key required, no documented rate limit.
- **Constraints**: A single request can't span more than 367 days — the
  notebook chunks wider ranges (`chunk_date_range` in
  `notebooks/bronze/nbp/transforms.py`). A 404 means "no rate for this
  range" (e.g. weekend/holiday-only range), not an error — handled as an
  empty result, not a failure.
- **Verification**: Confirmed against a live response during Phase 1
  development (Table A JSON shape: `code`, `rates[].effectiveDate`,
  `rates[].mid`).
- **First production run**: not yet run.

## Stooq — historical prices (incl. GPW)

- **Endpoint**: `https://stooq.com/q/d/l/?s={symbol}&d1={start}&d2={end}&i=d` (daily CSV export).
- **Bronze notebook**: `notebooks/bronze/stooq/`
- **Licensing**: Public, no key required. Stooq's terms restrict automated/
  bulk use in some contexts — this project's request volume (a handful of
  tickers, daily cadence) is far below anything Stooq has publicly flagged
  as problematic, but this hasn't been formally reviewed against their ToS.
- **Constraints**: **Not verified live from this environment** — Stooq's
  anti-bot JS challenge has blocked every attempt so far during development.
  `EXPECTED_STOOQ_COLUMNS` in `notebooks/bronze/stooq/transforms.py` is
  Stooq's documented CSV header, not one confirmed by a real response here.
  Confirm against a real Fabric run (a different network path/IP than local
  dev) before trusting this silently.
- **First production run**: not yet run.

## Financial Modeling Prep (FMP) — fundamentals

- **Endpoint**: `https://financialmodelingprep.com/stable/{balance-sheet-statement,income-statement,cashflow-statement}?symbol={symbol}&period=quarter&limit={n}&apikey={key}`
- **Bronze notebook**: `notebooks/bronze/fmp/`
- **Licensing**: Requires an API key. Free tier: 250 requests/day (per
  README_EN.md's tech stack table) — at 3 tickers × 3 statement types = 9
  requests per run, this is far under the daily cap even with frequent
  re-runs, but hasn't been load-tested.
- **Constraints**: **No API key has been available in any session so far**
  (`docs/next-steps.md`). This means: (a) `notebooks/config/tickers.yaml`'s
  `fmp_symbol` values are unverified against FMP's actual symbol resolution,
  and (b) the envelope-field assumptions in
  `notebooks/silver/fundamentals/transforms.py`
  (`FMP_ENVELOPE_COLUMNS` — `date`, `symbol`, `reportedCurrency`, `cik`,
  `filingDate`, `acceptedDate`, `fiscalYear`, `period`) are built from FMP's
  public documentation, not a live response.
- **Verification**: Not verified live. See `docs/next-steps.md` for the
  exact verification steps once a key is available.
- **First production run**: not yet run.

## GUS BDL (Bank Danych Lokalnych) — macro indicators

- **Endpoint**: `https://bdl.stat.gov.pl/api/v1/data/by-variable/{variable_id}?unit-level=0&year={year}&format=json`
- **Bronze notebook**: `notebooks/bronze/gus/`
- **Licensing**: Public API, no key required. Anonymous rate limit ~100
  requests/15 min (per `docs/next-steps.md`); today's volume (3 indicators ×
  ~10 years ≈ 3-4 dozen requests/run) is well under that, but there's no
  backoff/retry logic if `start_year` is ever widened significantly.
- **Constraints**: National-level (`unit-level=0`, "POLSKA") only — no
  voivodeship breakdown ingested. Eurostat (EU-wide data) is explicitly
  scoped out (`docs/next-steps.md`) — different API, different shape, not an
  extension of this notebook.
- **Verification**: Confirmed against live responses during Phase 1
  development for all three configured indicators (cpi, unemployment_rate,
  gdp) — see `notebooks/config/macro_indicators.yaml`'s header comment for
  the specific verified figures and the variable-id lookup method.
- **First production run**: not yet run.
