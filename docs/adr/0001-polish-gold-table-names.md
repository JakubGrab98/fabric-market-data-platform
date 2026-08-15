# ADR 0001: Gold physical table names are Polish

## Status
Accepted

## Context
`architecture.mermaid` (committed early, describing the target architecture)
names the five Gold tables in Polish: `dim_spolka`, `dim_data`, `fact_ceny`,
`fact_fundamenty`, `fact_makro`. `CLAUDE.md`'s "Table naming" convention
repeats these same Polish names and says they should match `README_PL.md`.
`README_EN.md`, written for an English-reading audience, describes the same
five tables using English glosses (`dim_company`, `fact_prices`, ...).
`README_PL.md` didn't exist until `docs/data-model.md` was written, leaving
a real, if narrow, ambiguity: are the *physical* table names Polish or
English?

## Decision
Physical table names are Polish, matching `architecture.mermaid` and
`CLAUDE.md`. `README_EN.md`'s English names are prose glosses for its
English-reading audience, not a second physical naming scheme — the doc
should (and does, as of this ADR) say so explicitly.

## Consequences
- `docs/data-model.md`, every `notebooks/gold/*` module, and every Power BI
  reference use the Polish names as ground truth.
- `README_EN.md` keeps English glosses in parentheses next to the Polish
  name for readability, not as an alternative to it.
- `README_PL.md` (added alongside this decision) uses the Polish names
  natively, closing the gap `CLAUDE.md` originally pointed at.
