# ADR 0006: Gold physical table names are English (supersedes ADR 0001)

## Status
Accepted

## Context
ADR 0001 chose Polish physical Gold table names (`dim_spolka`, `dim_data`,
`fact_ceny`, `fact_fundamenty`, `fact_makro`) to match `architecture.mermaid`
and `CLAUDE.md`'s naming convention at the time. Everything else in the
codebase — every notebook, `transforms.py` function and variable name,
column name (`company_name`, `metric_name`, `reported_currency`, ...),
docstring, commit message, and code comment — is English. `README_PL.md`
existed alongside `README_EN.md` as a second, parallel document.

That split meant the codebase read in two languages depending which file
you opened: Polish table/folder names, English everything else. Explicit
project direction is to standardize on English throughout — mixed-language
naming isn't acceptable here, independent of which specific words were
Polish.

## Decision
Gold physical table names are English: `dim_company`, `dim_date`,
`fact_prices`, `fact_fundamentals`, `fact_macro`. `README_PL.md` is removed
— `README_EN.md` is the only README. `CLAUDE.md`'s "Table naming" convention
is updated to match.

The exception: `is_trading_day_gpw` keeps `gpw` — that's the actual name of
the Warsaw Stock Exchange (Giełda Papierów Wartościowych), a proper noun
this platform's data genuinely refers to, not a translated common word. The
same reasoning would apply to any other real-world proper noun (a company
name, an index name) — this ADR is about not mixing languages for the
project's own vocabulary, not about scrubbing every Polish word regardless
of what it names.

## Consequences
- `notebooks/gold/{dim_spolka,dim_data,fact_ceny,fact_fundamenty,fact_makro}`
  are renamed to `notebooks/gold/{dim_company,dim_date,fact_prices,
  fact_fundamentals,fact_macro}`, along with their `transforms.py` function
  names, `FACT_*_COLUMNS` constants, and matching `tests/gold/` files.
- `docs/data-model.md`, `docs/adr/0003-*`, `docs/adr/0004-*`,
  `docs/adr/0005-*`, `README_EN.md`, `architecture.mermaid`, and
  `docs/next-steps.md` are updated to the new names.
- Anyone who already ran a Gold notebook against a real Fabric workspace
  under the old names would need to rename or migrate those Delta tables —
  moot today since `docs/source-log.md` confirms no Gold notebook has run
  in production yet.
