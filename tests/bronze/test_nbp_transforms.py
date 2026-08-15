from datetime import date, datetime, timezone

from notebooks.bronze.nbp.transforms import (
    build_nbp_rates_url,
    chunk_date_range,
    load_currency_config,
    parse_nbp_rates,
)

SAMPLE_PAYLOAD = {
    "table": "A",
    "currency": "dolar amerykański",
    "code": "USD",
    "rates": [
        {"no": "001/A/NBP/2024", "effectiveDate": "2024-01-02", "mid": 3.9432},
        {"no": "002/A/NBP/2024", "effectiveDate": "2024-01-03", "mid": 3.9909},
    ],
}


def test_build_nbp_rates_url():
    url = build_nbp_rates_url("USD", date(2024, 1, 1), date(2024, 1, 31))
    assert (
        url == "https://api.nbp.pl/api/exchangerates/rates/A/USD/2024-01-01/2024-01-31/?format=json"
    )


def test_chunk_date_range_splits_on_max_days():
    chunks = chunk_date_range(date(2024, 1, 1), date(2025, 6, 1), max_days=367)

    assert chunks[0] == (date(2024, 1, 1), date(2025, 1, 1))
    assert chunks[1] == (date(2025, 1, 2), date(2025, 6, 1))
    assert len(chunks) == 2


def test_chunk_date_range_single_chunk_when_within_limit():
    chunks = chunk_date_range(date(2024, 1, 1), date(2024, 1, 31))

    assert chunks == [(date(2024, 1, 1), date(2024, 1, 31))]


def test_parse_nbp_rates_maps_rows_and_stamps_provenance(spark):
    retrieved_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

    df = parse_nbp_rates(SAMPLE_PAYLOAD, "nbp", retrieved_at, spark)
    rows = df.orderBy("effective_date").collect()

    assert len(rows) == 2
    first = rows[0]
    assert first.currency_code == "USD"
    assert first.effective_date == "2024-01-02"
    assert first.mid_rate == 3.9432
    assert first.source == "nbp"
    assert first.retrieved_at == retrieved_at.replace(tzinfo=None)


def test_load_currency_config(tmp_path):
    config_file = tmp_path / "currencies.yaml"
    config_file.write_text(
        "currencies:\n  - code: USD\n    name: dolar amerykański\n", encoding="utf-8"
    )

    currencies = load_currency_config(config_file)

    assert currencies == [{"code": "USD", "name": "dolar amerykański"}]
