# Next steps

Living checklist of what's open after each work session — written so a new session (human or
Claude Code) can pick up without re-deriving context from git log. Update this file as part of
the same commit/session that changes the state it describes; delete a line once it's resolved
rather than leaving it stale.

## Immediate — before the Phase 1 sources are trusted in production

- **`fmp_symbol` in `notebooks/config/tickers.yaml` is still unverified.** No FMP API key was
  available in any session so far. Before the FMP notebook runs for real, verify each of
  PKN/PKO/CDR via `https://financialmodelingprep.com/stable/search-symbol?query=<ticker>&apikey=<key>`
  and update the yaml (currently flagged "UNVERIFIED" in its own comment — see there for the
  exact risk: a wrong exchange suffix could silently pull a different company's data under the
  right ticker label).
- **FMP's inferred-schema design is unproven against a real response.** The `stable/` endpoint
  paths, the `limit` parameter's semantics, and the whole field-union/`spark.read.json` approach
  in `notebooks/bronze/fmp/transforms.py` were built against documentation, not a live call (same
  reason as the point above — no key). One `curl` against a real ticker with a real key would
  validate or invalidate all of it at once.

## Phase 1 follow-ups (deliberately deferred, not blocking)

- **Eurostat** was explicitly scoped out of the GUS work — "GUS/Eurostat" in the roadmap is two
  different APIs with different data shapes. Revisit only if EU-wide comparison data is actually
  needed for an analysis; if so it's a new Bronze notebook, not an extension of `notebooks/bronze/gus/`.
- **`docs/source-log.md`, `docs/data-model.md`, `docs/adr/`** are referenced by `docs/README.md`
  but don't exist yet. Every Bronze source added so far (NBP, Stooq, FMP, GUS) has been ad hoc
  about this — worth doing once, covering all four sources, rather than piecemeal per source.
- **GUS BDL rate limiting isn't handled.** Fine today (3 indicators × ~10 years ≈ 3-4 dozen
  requests/run, anonymous limit is 100/15min), but `start_year` is a run parameter — widening it
  significantly could approach the limit. No backoff/retry logic exists in `fetch_gus_data`.
- **Four-way duplication across `notebooks/bronze/{nbp,stooq,fmp,gus}/transforms.py`:**
  - Each module hand-rolls its own `load_*_config` (same three-line YAML read). Low risk, but a
    fourth copy is where it stopped being obviously fine.
  - Each module's test file has a byte-identical module-scoped `spark` fixture. This one's a
    clean, no-downside extraction — a shared `tests/conftest.py` should replace all four.
  - The UTC-normalization line (`retrieved_at.astimezone(timezone.utc).replace(tzinfo=None)`) is
    duplicated four times; only two of the four copies (nbp, stooq) still carry the comment
    explaining *why* (a Spark timezone quirk) — fmp/gus lost it, so the line now reads as
    unexplained boilerplate in half the codebase. Either copy the comment over or hoist the whole
    normalization into a shared helper.
  - Before extracting a shared `notebooks/common.py` for the transforms-level duplication (as
    opposed to the test-level one, which is safe): spike whether Fabric's notebook runtime can
    actually import a sibling-of-siblings module, since each `notebook.py` currently does
    `from transforms import ...` (flat, not packaged) — this may not resolve the same way across
    notebook folders inside Fabric.
- **The API-key redaction pattern (`_redact_api_key_from_url` in `notebooks/bronze/fmp/transforms.py`)
  is FMP-local and hardcoded to the `apikey=` query param name.** `CLAUDE.md` lists Finnhub
  (streaming, token-authenticated) as an upcoming source — it'll need the same kind of redaction
  with a different param name (`token=`). Worth promoting to a shared, parameterized helper when
  that second consumer actually shows up; not worth doing speculatively now.
- **Cosmetic:** `notebooks/config/macro_indicators.yaml`'s provenance comment says "Verified 5.5%
  for 2023" for `unemployment_rate` — the actual verified value (and the one in the committed
  `unit`/id) is 5.1%. Doesn't affect the real config value, but a future reader cross-checking the
  comment will hit a discrepancy. One-line fix, take it opportunistically.

## Bigger picture

- **Phase 2 (Silver transformation) is the natural next phase** per `README_EN.md`'s roadmap —
  cleaning/standardizing the now-four Bronze sources (NBP, Stooq, FMP, GUS). Consider starting
  with just NBP + Stooq (the two sources that existed before this session, with the smallest
  surface) to validate the Bronze→Silver pipeline shape before extending it to FMP/GUS.
- **All four Bronze sources are append-only and defer deduplication to Silver, which doesn't
  exist yet.** That's four sources' worth of unbounded duplicate accumulation riding on one
  unwritten layer — worth being deliberate about Silver's dedup strategy early in Phase 2 rather
  than letting it become an afterthought.
- **`feature/repo-skeleton` (this repo's current branch) has never been pushed or PR'd to `main`.**
  All work so far — including this session's FMP/GUS additions — lives only on this local branch.
  Decide when/whether to open that PR.
- **`README_PL.md`, referenced by `CLAUDE.md`, doesn't exist** (only a one-line `README.md` stub
  and the full `README_EN.md`). Either write it or update `CLAUDE.md`'s reference — noted here
  since it's the kind of small inconsistency that's easy to forget once you're heads-down in a
  specific source's implementation.
