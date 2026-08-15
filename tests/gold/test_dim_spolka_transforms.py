from notebooks.gold.dim_spolka.transforms import build_dim_spolka, load_ticker_config


def test_load_ticker_config(tmp_path):
    config_path = tmp_path / "tickers.yaml"
    config_path.write_text(
        "tickers:\n"
        "  - ticker: PKN\n"
        "    stooq_symbol: pkn.wa\n"
        "    fmp_symbol: PKN\n"
        '    company_name: "PKN Orlen"\n'
        "    currency: PLN\n",
        encoding="utf-8",
    )

    tickers = load_ticker_config(config_path)

    assert tickers == [
        {
            "ticker": "PKN",
            "stooq_symbol": "pkn.wa",
            "fmp_symbol": "PKN",
            "company_name": "PKN Orlen",
            "currency": "PLN",
        }
    ]


def test_build_dim_spolka_maps_config_to_columns(spark):
    tickers = [
        {
            "ticker": "PKN",
            "stooq_symbol": "pkn.wa",
            "fmp_symbol": "PKN",
            "company_name": "PKN Orlen",
            "currency": "PLN",
        },
        {
            "ticker": "PKO",
            "stooq_symbol": "pko.wa",
            "fmp_symbol": "PKO",
            "company_name": "PKO Bank Polski",
            "currency": "PLN",
        },
    ]

    dim_df = build_dim_spolka(tickers, spark)
    rows = {r.ticker: r for r in dim_df.collect()}

    assert set(dim_df.columns) == {"ticker", "company_name", "listing_currency", "fmp_symbol"}
    assert rows["PKN"].company_name == "PKN Orlen"
    assert rows["PKN"].listing_currency == "PLN"
    assert rows["PKN"].fmp_symbol == "PKN"
    assert dim_df.count() == 2
