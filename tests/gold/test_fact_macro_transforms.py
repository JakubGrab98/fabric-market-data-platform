from datetime import date, datetime

from pyspark.sql import Row

from notebooks.gold.fact_macro.transforms import FACT_MACRO_COLUMNS, build_fact_macro


def test_build_fact_macro_selects_canonical_columns_and_drops_variable_id(spark):
    rows = [
        Row(
            indicator_name="cpi",
            variable_id=217230,
            year=2023,
            country="PL",
            reference_date=date(2023, 12, 31),
            value=114.4,
            unit="-",
            source="gus",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    silver_df = spark.createDataFrame(rows)

    fact_df = build_fact_macro(silver_df)

    assert fact_df.columns == FACT_MACRO_COLUMNS
    assert "variable_id" not in fact_df.columns
    row = fact_df.collect()[0]
    assert row.indicator_name == "cpi"
    assert row.reference_date == date(2023, 12, 31)
