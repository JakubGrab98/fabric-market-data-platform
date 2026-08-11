from datetime import date, datetime, timezone

import pytest

from notebooks.bronze.stooq.transforms import (
    StooqFetchError,
    build_stooq_csv_url,
    load_ticker_config,
    parse_stooq_prices_csv,
)

SAMPLE_CSV = "Date,Open,High,Low,Close,Volume\n2024-01-02,60.0,61.5,59.8,61.0,120000\n2024-01-03,61.0,61.2,60.1,60.5,98000\n"


def test_build_stooq_csv_url():
    url = build_stooq_csv_url("pkn.wa", date(2024, 1, 1), date(2024, 1, 31))
    assert url == ("https://stooq.com/q/d/l/?s=pkn.wa&d1=20240101&d2=20240131&i=d")


def test_parse_stooq_prices_csv_maps_rows_and_stamps_provenance(spark):
    retrieved_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

    df = parse_stooq_prices_csv(SAMPLE_CSV, "PKN", "stooq", retrieved_at, spark)
    rows = df.orderBy("date").collect()

    assert len(rows) == 2
    first = rows[0]
    assert first.ticker == "PKN"
    assert first.date == "2024-01-02"
    assert first.open == 60.0
    assert first.close == 61.0
    assert first.volume == 120000
    assert first.source == "stooq"
    # Spark's TimestampType has no tzinfo on read-back; session tz is pinned
    # to UTC in the fixture above so the naive value is directly comparable.
    assert first.retrieved_at == retrieved_at.replace(tzinfo=None)


def test_parse_stooq_prices_csv_rejects_unexpected_header(spark):
    bad_csv = "Data,Otwarcie,Najwyzszy,Najnizszy,Zamkniecie,Wolumen\n"

    with pytest.raises(StooqFetchError):
        parse_stooq_prices_csv(bad_csv, "PKN", "stooq", datetime.now(timezone.utc), spark)


def test_load_ticker_config(tmp_path):
    config_file = tmp_path / "tickers.yaml"
    config_file.write_text(
        "tickers:\n  - ticker: PKN\n    stooq_symbol: pkn.wa\n"
        "    company_name: PKN Orlen\n    currency: PLN\n"
    )

    tickers = load_ticker_config(config_file)

    assert tickers == [
        {
            "ticker": "PKN",
            "stooq_symbol": "pkn.wa",
            "company_name": "PKN Orlen",
            "currency": "PLN",
        }
    ]
