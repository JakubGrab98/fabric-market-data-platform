"""Transform functions for the Gold fact_ceny build notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame

FACT_CENY_COLUMNS = [
    "ticker",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "currency",
    "source",
    "retrieved_at",
]


def build_fact_ceny(silver_prices_df: DataFrame) -> DataFrame:
    """Select fact_ceny's canonical columns, in the order defined in docs/data-model.md.

    silver_prices is already at fact_ceny's grain (one row per ticker/date)
    and shape — this makes the Gold contract explicit and drops any
    Silver-only column that shouldn't cross into Gold, rather than
    passing the Silver DataFrame through unchanged.

    Args:
        silver_prices_df: DataFrame matching silver_prices' schema.
    Returns:
        DataFrame with exactly FACT_CENY_COLUMNS.
    """
    return silver_prices_df.select(*FACT_CENY_COLUMNS)
