# Bronze notebooks

One ingestion notebook per batch source (FMP, NBP, GUS/Eurostat, Stooq).
Raw data, 1:1 with the source API response — append-only, `source` and
`retrieved_at` columns on every record. Notebook cells stay thin; logic
lives in `transforms.py` next to each notebook.
