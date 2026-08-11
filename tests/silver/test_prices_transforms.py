from datetime import date, datetime

import pytest
from pyspark.sql import Row, SparkSession

from notebooks.silver.prices.transforms import (
    add_currency_column,
    deduplicate_prices,
    load_ticker_config,
    standardize_prices,
)


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


def test_standardize_prices_casts_date(spark):
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
    df = spark.createDataFrame(rows)

    standardized = standardize_prices(df)
    row = standardized.collect()[0]

    assert row.date == date(2024, 1, 2)
    assert standardized.schema["date"].dataType.typeName() == "date"


def test_standardize_prices_drops_rows_with_unparseable_date(spark):
    rows = [
        Row(
            ticker="PKN",
            date="not-a-date",
            open=60.0,
            high=61.0,
            low=59.5,
            close=60.5,
            volume=100000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
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
    df = spark.createDataFrame(rows)

    standardized = standardize_prices(df)
    rows_out = standardized.collect()

    assert len(rows_out) == 1
    assert rows_out[0].ticker == "PKO"
    assert rows_out[0].date == date(2024, 1, 2)


def test_load_ticker_config(tmp_path):
    config_file = tmp_path / "tickers.yaml"
    config_file.write_text(
        "tickers:\n"
        "  - ticker: PKN\n"
        "    stooq_symbol: pkn.wa\n"
        "    fmp_symbol: PKN\n"
        "    company_name: PKN Orlen\n"
        "    currency: PLN\n",
        encoding="utf-8",
    )

    tickers = load_ticker_config(config_file)

    assert tickers == [
        {
            "ticker": "PKN",
            "stooq_symbol": "pkn.wa",
            "fmp_symbol": "PKN",
            "company_name": "PKN Orlen",
            "currency": "PLN",
        }
    ]


def test_add_currency_column_joins_by_ticker(spark):
    prices_rows = [
        Row(ticker="PKN", date=date(2024, 1, 2), close=60.5),
        Row(ticker="PKO", date=date(2024, 1, 2), close=40.2),
    ]
    prices_df = spark.createDataFrame(prices_rows)
    tickers = [
        {"ticker": "PKN", "currency": "PLN"},
        {"ticker": "PKO", "currency": "PLN"},
    ]

    result = add_currency_column(prices_df, tickers, spark)
    rows = {r.ticker: r.currency for r in result.collect()}

    assert rows == {"PKN": "PLN", "PKO": "PLN"}


def test_add_currency_column_leaves_unknown_ticker_null(spark):
    prices_rows = [Row(ticker="XYZ", date=date(2024, 1, 2), close=1.0)]
    prices_df = spark.createDataFrame(prices_rows)
    tickers = [{"ticker": "PKN", "currency": "PLN"}]

    result = add_currency_column(prices_df, tickers, spark)
    row = result.collect()[0]

    assert row.ticker == "XYZ"
    assert row.currency is None
