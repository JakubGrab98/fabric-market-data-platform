from datetime import datetime, timezone

from notebooks.bronze.gus.transforms import (
    build_gus_data_url,
    load_macro_indicator_config,
    parse_gus_data,
)

SAMPLE_PAYLOAD = {
    "totalRecords": 1,
    "variableId": 12345,
    "measureUnitId": 1,
    "aggregateId": 1,
    "lastUpdate": None,
    "results": [
        {
            "id": "000000000000",
            "name": "POLSKA",
            "values": [
                {"year": "2023", "val": 3.5, "attrId": 1},
                {"year": "2024", "val": 4.2, "attrId": 1},
            ],
        }
    ],
}


def test_load_macro_indicator_config(tmp_path):
    config_file = tmp_path / "macro_indicators.yaml"
    config_file.write_text(
        "indicators:\n" "  - name: cpi\n" "    gus_variable_id: 12345\n" "    unit: '%'\n",
        encoding="utf-8",
    )

    indicators = load_macro_indicator_config(config_file)

    assert indicators == [{"name": "cpi", "gus_variable_id": 12345, "unit": "%"}]


def test_build_gus_data_url_defaults_to_national_level():
    url = build_gus_data_url(12345, 2024)
    assert (
        url == "https://bdl.stat.gov.pl/api/v1/data/by-variable/12345"
        "?unit-level=0&year=2024&format=json"
    )


def test_build_gus_data_url_custom_unit_level():
    url = build_gus_data_url(12345, 2024, unit_level="2")
    assert "unit-level=2" in url


def test_parse_gus_data_maps_rows_and_stamps_provenance(spark):
    retrieved_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

    df = parse_gus_data(SAMPLE_PAYLOAD, "cpi", 12345, "%", "gus", retrieved_at, spark)
    rows = df.orderBy("year").collect()

    assert len(rows) == 2
    first = rows[0]
    assert first.indicator_name == "cpi"
    assert first.variable_id == 12345
    assert first.year == 2023
    assert first.value == 3.5
    assert first.unit == "%"
    assert first.source == "gus"
    assert first.retrieved_at == retrieved_at.replace(tzinfo=None)


def test_parse_gus_data_handles_null_value(spark):
    payload = {
        "results": [
            {"id": "x", "name": "POLSKA", "values": [{"year": "2023", "val": None, "attrId": 1}]}
        ]
    }
    retrieved_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

    df = parse_gus_data(payload, "cpi", 12345, "%", "gus", retrieved_at, spark)
    row = df.collect()[0]

    assert row.value is None
