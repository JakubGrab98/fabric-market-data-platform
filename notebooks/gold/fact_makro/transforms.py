"""Transform functions for the Gold fact_makro build notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame

FACT_MAKRO_COLUMNS = [
    "country",
    "reference_date",
    "indicator_name",
    "value",
    "unit",
    "source",
    "retrieved_at",
]


def build_fact_makro(silver_macro_df: DataFrame) -> DataFrame:
    """Select fact_makro's canonical columns, in the order defined in docs/data-model.md.

    Drops silver_macro's variable_id — kept in Silver for lineage, but not
    part of the Gold contract (docs/data-model.md).

    Args:
        silver_macro_df: DataFrame matching silver_macro's schema.
    Returns:
        DataFrame with exactly FACT_MAKRO_COLUMNS.
    """
    return silver_macro_df.select(*FACT_MAKRO_COLUMNS)
