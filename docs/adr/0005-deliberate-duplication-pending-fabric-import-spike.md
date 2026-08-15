# ADR 0005: Deliberate duplication across Bronze/Silver/Gold modules

## Status
Accepted (temporary — expected to be revisited)

## Context
Small pieces of logic are copied across multiple `transforms.py` modules
rather than shared:
- `load_ticker_config`/`load_*_config` (a three-line YAML read) is
  duplicated in `notebooks/bronze/{stooq,fmp}`, `notebooks/silver/prices`,
  and `notebooks/gold/dim_spolka`.
- The dedup-by-natural-key window-function pattern is near-identical across
  `notebooks/silver/{fx_rates,prices,fundamentals,macro}`.
- The UTC-normalization line
  (`retrieved_at.astimezone(timezone.utc).replace(tzinfo=None)`) is
  duplicated across every Bronze source.

Every Fabric notebook (`notebook.py`) imports its sibling `transforms.py`
with a flat, non-package-relative `from transforms import ...` — Fabric
injects each notebook's own folder onto the path, not the repo root. It is
**not confirmed** whether Fabric's notebook runtime can resolve an import
of a *different* notebook folder's module (a genuine
`notebooks/silver/common.py` shared by `notebooks/silver/prices/notebook.py`
and `notebooks/silver/fundamentals/notebook.py`, for example) the same way
it resolves the sibling-local import.

## Decision
Keep the duplication until that Fabric import behavior is spiked and
confirmed, rather than extracting a shared module speculatively and
discovering at deploy time that it doesn't resolve.

## Consequences
- Each new Bronze/Silver/Gold module currently means one more copy of these
  small helpers. Low risk today (the copies are small and mechanical), but
  worth resolving before a sixth or seventh copy accumulates.
- The one exception already made: `tests/conftest.py`'s shared `spark`
  fixture *was* extracted, because it's test-only code that never runs
  inside the Fabric notebook runtime — the import-resolution question this
  ADR is about doesn't apply to it.
- Next step (tracked in `docs/next-steps.md`): spike whether
  `notebooks/<layer>/common.py` imports resolve inside an actual Fabric
  notebook run. Once confirmed either way, either extract the shared
  helpers or close this ADR as "duplication is the permanent answer here."
