from notebooks.bronze.gus.transforms import load_macro_indicator_config


def test_load_macro_indicator_config(tmp_path):
    config_file = tmp_path / "macro_indicators.yaml"
    config_file.write_text(
        "indicators:\n" "  - name: cpi\n" "    gus_variable_id: 12345\n" "    unit: '%'\n",
        encoding="utf-8",
    )

    indicators = load_macro_indicator_config(config_file)

    assert indicators == [{"name": "cpi", "gus_variable_id": 12345, "unit": "%"}]
