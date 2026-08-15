# Next steps

Living checklist of what's open after each work session — written so a new session (human or
Claude Code) can pick up without re-deriving context from git log. Update this file as part of
the same commit/session that changes the state it describes; delete a line once it's resolved
rather than leaving it stale.

## Immediate — before the Bronze/Silver FMP path is trusted in production

- **`fmp_symbol` in `notebooks/config/tickers.yaml` is still unverified.** No FMP API key was
  available in any session so far. Before the FMP notebook runs for real, verify each of
  PKN/PKO/CDR via `https://financialmodelingprep.com/stable/search-symbol?query=<ticker>&apikey=<key>`
  and update the yaml (currently flagged "UNVERIFIED" in its own comment — see there for the
  exact risk: a wrong exchange suffix could silently pull a different company's data under the
  right ticker label).
- **FMP's inferred-schema design is unproven against a real response** — both in Bronze
  (`spark.read.json` field-union approach in `notebooks/bronze/fmp/transforms.py`) and now in
  Silver (`FMP_ENVELOPE_COLUMNS` in `notebooks/silver/fundamentals/transforms.py`, which assumes
  `date`/`symbol`/`reportedCurrency`/`cik`/`filingDate`/`acceptedDate`/`fiscalYear`/`period` as
  FMP's envelope fields — documented, not verified). One `curl` against a real ticker with a real
  key would validate or invalidate both at once. See `docs/source-log.md`'s FMP entry.

## Phase 3 follow-ups (deliberately deferred, not blocking)

- **`dim_date.is_trading_day_gpw` is a weekday-only approximation** (Mon-Fri via Spark's
  `dayofweek()`) — it does not exclude Polish public holidays yet. `docs/data-model.md` and
  `docs/superpowers/specs/2026-08-15-phase3-gold-star-schema-design.md` both recommend a
  maintained holiday library (e.g. the `holidays` PyPI package, `country="PL"`) for the real
  formula rather than a hand-maintained date list, but that's a new dependency this project
  hasn't added — evaluate and add it deliberately, not as a side effect of an unrelated change.
- **Power BI semantic model / first report** (Phase 3's other roadmap item) hasn't been started —
  the Gold notebooks exist, but nothing has been connected to Power BI Direct Lake yet.
- **No Gold notebook has run against real Bronze/Silver data** (nothing has run against a real
  Fabric workspace at all yet — see `docs/source-log.md`). Unit tests cover the transform logic;
  an actual end-to-end run through Fabric is still open.

## Standing, cross-phase follow-ups

- **Fabric cross-folder notebook import behavior is still unconfirmed.** Every `notebook.py` uses
  a flat `from transforms import ...` (sibling-local import); whether a *shared* module (e.g.
  `notebooks/silver/common.py`) would resolve the same way inside Fabric's actual runtime has
  never been tested. This blocks extracting the now five-way-duplicated `load_*_config` helpers
  and the near-identical Silver dedup-by-natural-key pattern. See ADR 0005
  (`docs/adr/0005-deliberate-duplication-pending-fabric-import-spike.md`) — spike this before
  adding a sixth copy of either pattern.
- **GUS BDL rate limiting isn't handled.** Fine today (3 indicators × ~10 years ≈ 3-4 dozen
  requests/run, anonymous limit is 100/15min), but `start_year` is a run parameter — widening it
  significantly could approach the limit. No backoff/retry logic exists in `fetch_gus_data`.
- **The API-key redaction pattern (`_redact_api_key_from_url` in `notebooks/bronze/fmp/transforms.py`)
  is FMP-local and hardcoded to the `apikey=` query param name.** `CLAUDE.md` lists Finnhub
  (streaming, token-authenticated) as an upcoming source — it'll need the same kind of redaction
  with a different param name (`token=`). Worth promoting to a shared, parameterized helper when
  that second consumer actually shows up; not worth doing speculatively now.
- **Stooq's CSV header assumption is still unverified live** (`docs/source-log.md` — blocked by
  Stooq's anti-bot JS challenge in every dev environment tried so far). Confirm from an actual
  Fabric run, which may have a different network path.
- **No CI** (`.github/workflows` or equivalent) runs `pytest`/`ruff`/`black` automatically —
  currently a manual step before every PR.

## Bigger picture

- **Phase 4 (Automation) is the natural next phase** per `README_EN.md`'s roadmap — scheduling and
  monitoring the now-complete Bronze→Silver→Gold pipeline via Data Factory. `pipelines/` is
  currently just a placeholder `README.md`.
- **Phase 5 (Streaming path)** — Finnhub WS Bridge → Eventstream → Eventhouse → Activator — is
  fully unstarted (`notebooks/streaming/` is a placeholder `README.md` only). No Finnhub API key
  has been used in this project yet either.
- **Phase 6 (Data quality)**: the source log (`docs/source-log.md`) and transform test coverage
  now exist; what's still open is the cross-layer consistency checks `README_EN.md`'s "Data
  Quality" section describes (row counts, date ranges between layers) — nothing implements those
  yet, only the Silver-level dedup/standardize logic.
