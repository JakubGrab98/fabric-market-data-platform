from datetime import datetime

from pyspark.sql import Row

from notebooks.gold.fact_prices.transforms import FACT_PRICES_COLUMNS, build_fact_prices


def test_build_fact_prices_selects_canonical_columns(spark):
    rows = [
        Row(
            ticker="PKN",
            date="2024-01-02",
            open=60.0,
            high=61.5,
            low=59.5,
            close=61.0,
            volume=100000,
            currency="PLN",
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    silver_df = spark.createDataFrame(rows)

    fact_df = build_fact_prices(silver_df)

    assert fact_df.columns == FACT_PRICES_COLUMNS
    row = fact_df.collect()[0]
    assert row.ticker == "PKN"
    assert row.close == 61.0
