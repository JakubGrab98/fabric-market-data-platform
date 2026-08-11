"""Transform functions for the Silver prices deduplication/standardization notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, to_date


def deduplicate_prices(bronze_df: DataFrame) -> DataFrame:
    """Keep one row per (ticker, date), latest retrieved_at wins.

    Args:
        bronze_df: DataFrame matching bronze_stooq_prices' schema (ticker, date, open,
            high, low, close, volume, source, retrieved_at).
    Returns:
        DataFrame with the same columns, one row per (ticker, date).
    """
    window = Window.partitionBy("ticker", "date").orderBy(col("retrieved_at").desc())
    return (
        bronze_df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    )


def standardize_prices(deduped_df: DataFrame) -> DataFrame:
    """Cast date from a yyyy-MM-dd string to a proper date type.

    Args:
        deduped_df: Deduplicated DataFrame matching bronze_stooq_prices' schema.
    Returns:
        DataFrame with date as DateType; all other columns unchanged.
    """
    return deduped_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
