# CLAUDE.md

Context for Claude Code working in this repository. Keep this file short and
universally applicable — detailed specs belong in `/docs`, not here.

## Project

Investment data analytics platform on Microsoft Fabric. Ingests market,
fundamental, and macroeconomic data via public APIs, models it using a
hybrid medallion + event-driven architecture, and serves it through Power BI
and a real-time dashboard. Educational / portfolio project — not a
production trading system, and outputs are never investment advice.

Full architecture and rationale: `README_EN.md` and `architecture.mermaid`.
Don't duplicate that content here — read it when you need the "why."

## Tech stack

- **Microsoft Fabric**: OneLake, Lakehouse, Notebooks (PySpark/Python),
  Data Factory Pipelines, Power BI (Direct Lake)
- **Real-Time Intelligence**: Eventstream, Eventhouse (KQL), Activator
- **Python**: `requests`/`httpx`, `pandas`, `pyspark`
- **Delta Lake** table format across all layers
- **Data sources**: Finnhub (streaming quotes, WebSocket), Financial
  Modeling Prep (fundamentals), Stooq (historical prices, incl. GPW), NBP
  (FX), GUS/Eurostat (macro)

## Repository layout

```
/notebooks
  /bronze          ingestion notebooks, one per source (batch)
  /silver          cleaning / standardization transforms
  /gold            star-schema build notebooks
  /streaming       Finnhub WS bridge, Eventstream/Eventhouse setup
/pipelines         exported Data Factory pipeline definitions (JSON)
/docs              data model, source log, ADRs
/tests             unit tests for transform logic (local PySpark)
architecture.mermaid
README_EN.md
```

If a task touches a directory not listed here, ask before assuming its
purpose.

## Commands

- Run unit tests: `pytest tests/ -v`
- Lint: `ruff check .`
- Format: `black .`
- Local Spark session for notebook logic: notebooks import from
  `/notebooks/**/transforms.py` so transform functions are testable outside
  Fabric — write logic there, keep the notebook cells thin (call the
  function, don't embed the logic inline).

## Conventions

- **Idempotent notebooks.** Every ingestion/transform notebook must be safe
  to re-run for the same date/parameters without creating duplicates
  (upsert/merge into Delta, not blind append) except explicitly
  append-only raw Bronze landing tables.
- **No hardcoded IDs.** Use Fabric Variable Library items (connection
  references, item references) or notebook parameters — never hardcode a
  workspace ID, Lakehouse ID, or ticker list inline. Ticker/company lists
  live in a config file under `/notebooks/config/`.
- **Table naming**: `dim_*` / `fact_*` in Gold, matching the names in
  `architecture.mermaid` (`dim_company`, `dim_date`, `fact_prices`,
  `fact_fundamentals`, `fact_macro`). Columns in `snake_case`. English
  throughout the codebase — no non-English identifiers, including table/
  column names, except real-world proper nouns (e.g. `gpw` for the Warsaw
  Stock Exchange).
- **Every raw record keeps its source and retrieval timestamp** as columns
  (`source`, `retrieved_at`) — this feeds the source log described in
  `README_EN.md` and is not optional.
- **Formulas over magic numbers.** No hardcoded thresholds/exchange rates in
  transform code — pull from a config table or parameter, with a comment
  explaining where the value came from.

## Git workflow

- **Atomic commits.** One logical change per commit — don't bundle, e.g., a
  Bronze notebook change with a Gold schema change.
- **Conventional Commits** prefix + short imperative summary: `feat:`,
  `fix:`, `data:`, `docs:`, `chore:`.
- **No direct commits to `main`.** Branch per phase/feature
  (`feature/...`, `fix/...`), PR before merge — even solo, this keeps a
  reviewable history.

## Code style

- Formatting/linting is enforced by `ruff` + `black` (see Commands) — fix
  what the linter flags, don't restate style rules here.
- Type hints required on every function in `/notebooks/**/transforms.py`.
- One-line docstring (+ Args/Returns if non-trivial) on public transform
  functions only — not on every notebook cell.

## Boundaries — do not

- Commit API keys, tokens, or connection strings. They go in Fabric
  Key Vault / Variable Library, referenced by name. `.env*` is gitignored —
  verify before committing.
- Commit real pulled datasets. `/data` (if present) is for small,
  anonymized fixtures used by tests only.
- Change the Gold schema (table or column names) without updating
  `docs/data-model.md` and the Power BI semantic model references in the
  same PR — they will silently break otherwise.
- Treat this repo's outputs as investment recommendations in any generated
  text, comments, or docs — keep language descriptive/analytical.

## When something is ambiguous

Prefer asking over guessing for: which API endpoint/field maps to a given
Gold column, how to handle a data source's rate limit in a notebook loop,
and anything affecting the Gold schema. Silent assumptions here tend to be
expensive to unwind across notebooks + pipeline + semantic model.
