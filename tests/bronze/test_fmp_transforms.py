import pytest
from pyspark.sql import SparkSession

from notebooks.bronze.fmp.transforms import load_ticker_config


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


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
