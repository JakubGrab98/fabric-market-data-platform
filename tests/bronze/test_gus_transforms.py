from notebooks.bronze.gus.transforms import build_gus_data_url, load_macro_indicator_config


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
