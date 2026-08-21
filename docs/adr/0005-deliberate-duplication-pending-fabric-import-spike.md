# ADR 0005: Deliberate duplication across Bronze/Silver/Gold modules

## Status
Accepted — confirmed permanent, not pending. Originally accepted as
temporary pending a Fabric import spike; that spike happened while wiring
up `scripts/build_fabric_sync.py` and the answer is negative (see below).
Keeping the original filename/number so the history stays intact.

## Context
Small pieces of logic are copied across multiple `transforms.py` modules
rather than shared:
- `load_ticker_config`/`load_*_config` (a three-line YAML read) is
  duplicated in `notebooks/bronze/{stooq,fmp}`, `notebooks/silver/prices`,
  and `notebooks/gold/dim_company`.
- The dedup-by-natural-key window-function pattern is near-identical across
  `notebooks/silver/{fx_rates,prices,fundamentals,macro}`.
- The UTC-normalization line
  (`retrieved_at.astimezone(timezone.utc).replace(tzinfo=None)`) is
  duplicated across every Bronze source.

Every Fabric notebook (`notebook.py`) imports its sibling `transforms.py`
with a flat, non-package-relative `from transforms import ...`. This
originally read as an open question ("does Fabric resolve a *shared*
module the same way it resolves the sibling-local one?"). It's now
answered: per Microsoft's own docs (Notebook source control & deployment),
a notebook's importable Python files live in its own `Resources/builtin/`
folder — see `scripts/build_fabric_sync.py`, which generates exactly this
structure per item under `/fabric`. That folder is **item-scoped**. There
is no mechanism for one notebook's resources to be imported by a different
notebook; `from transforms import ...` works because each item gets its
own private copy of `transforms.py` in its own resources folder, not
because Fabric resolves cross-folder imports generally.

## Decision
Keep the duplication — permanently, not pending further investigation. A
genuinely shared `notebooks/silver/common.py` isn't reachable via the
per-notebook Resources mechanism at all; the only way to share code across
notebooks in Fabric is a custom Environment library (a heavier, different
mechanism: a separate item type, attached to notebooks via environment
binding, with its own deployment/versioning story). Not worth adopting for
a handful of three-line helpers.

## Consequences
- Each new Bronze/Silver/Gold module means one more copy of these small
  helpers, by design now, not as a stopgap.
- The one exception already made: `tests/conftest.py`'s shared `spark`
  fixture *was* extracted, because it's test-only code that never runs
  inside the Fabric notebook runtime — the Resources-folder scoping this
  ADR is about doesn't apply to it.
- If the duplication cost ever genuinely outgrows this (many more sources,
  or logic complex enough that copies drift out of sync), the fix is a
  Fabric custom Environment library, not a shared `transforms.py` — a
  bigger, separate decision to make when/if it's actually needed.
