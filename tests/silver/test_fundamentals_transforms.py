from datetime import date, datetime

import pytest
from pyspark.sql import Row

from notebooks.silver.fundamentals.transforms import (
    deduplicate_fundamentals,
    standardize_fundamentals,
    unpivot_fundamentals_metrics,
)


def test_deduplicate_fundamentals_keeps_latest_retrieved_at(spark):
    rows = [
        Row(
            ticker="PKN",
            date="2023-03-31",
            period="Q1",
            statement_type="balance_sheet",
            totalAssets=100.0,
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
        Row(
            ticker="PKN",
            date="2023-03-31",
            period="Q1",
            statement_type="balance_sheet",
            totalAssets=110.0,
            retrieved_at=datetime(2024, 1, 5, 8, 0, 0),  # noqa: DTZ001
        ),
        Row(
            ticker="PKN",
            date="2023-03-31",
            period="Q1",
            statement_type="income_statement",
            totalAssets=None,
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_fundamentals(bronze_df)
    result = {
        (r.ticker, r.date, r.period, r.statement_type): r.totalAssets for r in deduped.collect()
    }

    assert len(result) == 2
    assert result[("PKN", "2023-03-31", "Q1", "balance_sheet")] == 110.0


def test_standardize_fundamentals_derives_period_type_and_casts_date(spark):
    rows = [
        Row(ticker="PKN", date="2023-03-31", period="Q1"),
        Row(ticker="PKN", date="2023-12-31", period="FY"),
    ]
    df = spark.createDataFrame(rows)

    standardized = standardize_fundamentals(df)
    by_period = {r.period: r for r in standardized.collect()}

    assert by_period["Q1"].period_type == "quarter"
    assert by_period["Q1"].date == date(2023, 3, 31)
    assert by_period["FY"].period_type == "annual"
    assert standardized.schema["date"].dataType.typeName() == "date"


def test_standardize_fundamentals_drops_rows_with_unparseable_date(spark):
    rows = [
        Row(ticker="PKN", date="not-a-date", period="Q1"),
        Row(ticker="PKN", date="2023-03-31", period="Q1"),
    ]
    df = spark.createDataFrame(rows)

    standardized = standardize_fundamentals(df)

    assert standardized.count() == 1


def test_unpivot_fundamentals_metrics_melts_and_snake_cases(spark):
    rows = [
        Row(
            ticker="PKN",
            date=date(2023, 3, 31),
            symbol="PKN",
            reportedCurrency="PLN",
            cik="123",
            filingDate="2023-05-01",
            acceptedDate="2023-05-01",
            fiscalYear=2023,
            period="Q1",
            statement_type="balance_sheet",
            period_type="quarter",
            source="fmp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
            totalAssets=100.5,
            totalLiabilities=40.0,
        ),
    ]
    df = spark.createDataFrame(rows)

    melted = unpivot_fundamentals_metrics(df)
    by_metric = {r.metric_name: r.metric_value for r in melted.collect()}

    assert by_metric == {"total_assets": 100.5, "total_liabilities": 40.0}
    row = melted.collect()[0]
    assert row.period_end_date == date(2023, 3, 31)
    assert row.reported_currency == "PLN"
    assert row.fiscal_year == 2023


def test_unpivot_fundamentals_metrics_drops_non_numeric_metric_values(spark):
    rows = [
        Row(
            ticker="PKN",
            date=date(2023, 3, 31),
            symbol="PKN",
            reportedCurrency="PLN",
            cik="123",
            filingDate="2023-05-01",
            acceptedDate="2023-05-01",
            fiscalYear=2023,
            period="Q1",
            statement_type="balance_sheet",
            period_type="quarter",
            source="fmp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
            totalAssets=100.5,
            reportingCurrencyNote="non-numeric text",
        ),
    ]
    df = spark.createDataFrame(rows)

    melted = unpivot_fundamentals_metrics(df)
    metric_names = {r.metric_name for r in melted.collect()}

    assert metric_names == {"total_assets"}


def test_unpivot_fundamentals_metrics_raises_when_no_metric_columns(spark):
    rows = [
        Row(
            ticker="PKN",
            date=date(2023, 3, 31),
            symbol="PKN",
            reportedCurrency="PLN",
            cik="123",
            filingDate="2023-05-01",
            acceptedDate="2023-05-01",
            fiscalYear=2023,
            period="Q1",
            statement_type="balance_sheet",
            period_type="quarter",
            source="fmp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    df = spark.createDataFrame(rows)

    with pytest.raises(ValueError, match="No metric columns"):
        unpivot_fundamentals_metrics(df)
