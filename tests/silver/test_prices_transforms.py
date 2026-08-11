from datetime import datetime

import pytest
from pyspark.sql import Row, SparkSession

from notebooks.silver.prices.transforms import deduplicate_prices


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def test_deduplicate_prices_keeps_latest_retrieved_at(spark):
    rows = [
        Row(
            ticker="PKN",
            date="2024-01-02",
            open=60.0,
            high=61.0,
            low=59.5,
            close=60.5,
            volume=100000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
        Row(
            ticker="PKN",
            date="2024-01-02",
            open=60.0,
            high=61.5,
            low=59.5,
            close=61.0,
            volume=105000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 5, 8, 0, 0),  # noqa: DTZ001
        ),
        Row(
            ticker="PKO",
            date="2024-01-02",
            open=40.0,
            high=40.5,
            low=39.5,
            close=40.2,
            volume=50000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_prices(bronze_df)
    result = {(r.ticker, r.date): r.close for r in deduped.collect()}

    assert len(result) == 2
    assert result[("PKN", "2024-01-02")] == 61.0
    assert result[("PKO", "2024-01-02")] == 40.2


def test_deduplicate_prices_single_row_per_key_unaffected(spark):
    rows = [
        Row(
            ticker="PKN",
            date="2024-01-02",
            open=60.0,
            high=61.0,
            low=59.5,
            close=60.5,
            volume=100000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_prices(bronze_df)

    assert deduped.count() == 1
