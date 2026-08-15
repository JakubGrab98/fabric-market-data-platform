# ADR 0002: No FX conversion in Silver prices

## Status
Accepted

## Context
`silver_prices` joins in each ticker's listing currency from
`notebooks/config/tickers.yaml`, but every ticker configured so far (PKN,
PKO, CDR) trades in PLN. Converting to a common currency would require
`silver_fx_rates` as an input plus a rule for missing rates on non-trading
days (weekends/holidays a price exists for but an FX rate might not) —
real design work with nothing to validate it against yet.

## Decision
`silver_prices` carries a `currency` column (a passthrough label) but does
not convert values. See
`docs/superpowers/specs/2026-08-10-phase2-silver-fx-prices-design.md`.

## Consequences
- The `currency` column exists specifically so a future non-PLN ticker
  becomes visible as a currency mismatch immediately (a `PLN`-only
  aggregate accidentally including a `USD` row would be obviously wrong),
  rather than silently blending into PLN-denominated numbers.
- `fact_prices` in Gold inherits this — no converted-currency column today.
  `docs/data-model.md` documents where a future converted column (e.g.
  `close_pln`) would land: an addition, not a replacement of `currency`/
  `close`.
- Revisit this decision the day a non-PLN ticker is actually added to
  `tickers.yaml` — not before.
