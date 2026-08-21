"""Transform functions for the Silver macro deduplication/standardization notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, concat, lit, row_number, to_date


def deduplicate_macro(bronze_df: DataFrame) -> DataFrame:
    """Keep one row per (indicator_name, year), latest retrieved_at wins.

    Args:
        bronze_df: DataFrame matching bronze_gus_macro's schema (indicator_name,
            variable_id, year, value, unit, source, retrieved_at).
    Returns:
        DataFrame with the same columns, one row per (indicator_name, year).
    """
    window = Window.partitionBy("indicator_name", "year").orderBy(col("retrieved_at").desc())
    return (
        bronze_df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    )


def standardize_macro(deduped_df: DataFrame) -> DataFrame:
    """Add a country label and derive a year-end reference_date.

    GUS BDL is Poland-only today (Eurostat/other countries explicitly out of
    scope — see docs/next-steps.md), so country is a constant "PL" rather
    than a config/API-driven value; the column exists so the grain stays
    unambiguous if a second country is ever added. reference_date is
    December 31 of `year`, joining this annual data to a daily date
    dimension at a single, predictable point per docs/data-model.md.

    Args:
        deduped_df: Deduplicated DataFrame matching bronze_gus_macro's schema.
    Returns:
        DataFrame with country and reference_date columns added.
    """
    return deduped_df.withColumn("country", lit("PL")).withColumn(
        "reference_date", to_date(concat(col("year").cast("string"), lit("-12-31")))
    )
