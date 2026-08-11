from datetime import date, datetime

import pytest
from pyspark.sql import Row, SparkSession

from notebooks.silver.fx_rates.transforms import deduplicate_fx_rates, standardize_fx_rates


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def test_deduplicate_fx_rates_keeps_latest_retrieved_at(spark):
    rows = [
        Row(
            currency_code="USD",
            effective_date="2024-01-02",
            mid_rate=3.9432,
            source="nbp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
        Row(
            currency_code="USD",
            effective_date="2024-01-02",
            mid_rate=3.9500,
            source="nbp",
            retrieved_at=datetime(2024, 1, 5, 8, 0, 0),  # noqa: DTZ001
        ),
        Row(
            currency_code="EUR",
            effective_date="2024-01-02",
            mid_rate=4.3,
            source="nbp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_fx_rates(bronze_df)
    result = {(r.currency_code, r.effective_date): r.mid_rate for r in deduped.collect()}

    assert len(result) == 2
    assert result[("USD", "2024-01-02")] == 3.9500
    assert result[("EUR", "2024-01-02")] == 4.3


def test_deduplicate_fx_rates_single_row_per_key_unaffected(spark):
    rows = [
        Row(
            currency_code="USD",
            effective_date="2024-01-02",
            mid_rate=3.9432,
            source="nbp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_fx_rates(bronze_df)

    assert deduped.count() == 1


def test_standardize_fx_rates_casts_effective_date(spark):
    rows = [
        Row(
            currency_code="USD",
            effective_date="2024-01-02",
            mid_rate=3.9432,
            source="nbp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    df = spark.createDataFrame(rows)

    standardized = standardize_fx_rates(df)
    row = standardized.collect()[0]

    assert row.effective_date == date(2024, 1, 2)
    assert standardized.schema["effective_date"].dataType.typeName() == "date"
