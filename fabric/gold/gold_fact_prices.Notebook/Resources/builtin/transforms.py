"""Transform functions for the Gold fact_prices build notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame

FACT_PRICES_COLUMNS = [
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


def build_fact_prices(silver_prices_df: DataFrame) -> DataFrame:
    """Select fact_prices' canonical columns, in the order defined in docs/data-model.md.

    silver_prices is already at fact_prices' grain (one row per ticker/date)
    and shape — this makes the Gold contract explicit and drops any
    Silver-only column that shouldn't cross into Gold, rather than
    passing the Silver DataFrame through unchanged.

    Args:
        silver_prices_df: DataFrame matching silver_prices' schema.
    Returns:
        DataFrame with exactly FACT_PRICES_COLUMNS.
    """
    return silver_prices_df.select(*FACT_PRICES_COLUMNS)
