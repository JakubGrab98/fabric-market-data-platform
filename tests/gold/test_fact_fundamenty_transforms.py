from datetime import date, datetime

from pyspark.sql import Row

from notebooks.gold.fact_fundamenty.transforms import (
    FACT_FUNDAMENTY_COLUMNS,
    build_fact_fundamenty,
)


def test_build_fact_fundamenty_selects_canonical_columns(spark):
    rows = [
        Row(
            ticker="PKN",
            period_end_date=date(2023, 3, 31),
            statement_type="balance_sheet",
            period_type="quarter",
            fiscal_year=2023,
            metric_name="total_assets",
            metric_value=100.5,
            reported_currency="PLN",
            source="fmp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    silver_df = spark.createDataFrame(rows)

    fact_df = build_fact_fundamenty(silver_df)

    assert fact_df.columns == FACT_FUNDAMENTY_COLUMNS
    row = fact_df.collect()[0]
    assert row.metric_name == "total_assets"
    assert row.metric_value == 100.5
