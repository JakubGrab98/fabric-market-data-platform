from datetime import date, datetime

from pyspark.sql import Row

from notebooks.silver.macro.transforms import deduplicate_macro, standardize_macro


def test_deduplicate_macro_keeps_latest_retrieved_at(spark):
    rows = [
        Row(
            indicator_name="cpi",
            variable_id=217230,
            year=2023,
            value=114.4,
            unit="-",
            source="gus",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
        Row(
            indicator_name="cpi",
            variable_id=217230,
            year=2023,
            value=114.5,
            unit="-",
            source="gus",
            retrieved_at=datetime(2024, 1, 5, 8, 0, 0),  # noqa: DTZ001
        ),
        Row(
            indicator_name="gdp",
            variable_id=458271,
            year=2023,
            value=3_100_850.0,
            unit="mln zł",
            source="gus",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_macro(bronze_df)
    result = {(r.indicator_name, r.year): r.value for r in deduped.collect()}

    assert len(result) == 2
    assert result[("cpi", 2023)] == 114.5


def test_deduplicate_macro_single_row_per_key_unaffected(spark):
    rows = [
        Row(
            indicator_name="cpi",
            variable_id=217230,
            year=2023,
            value=114.4,
            unit="-",
            source="gus",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_macro(bronze_df)

    assert deduped.count() == 1


def test_standardize_macro_adds_country_and_reference_date(spark):
    rows = [
        Row(
            indicator_name="unemployment_rate",
            variable_id=60270,
            year=2023,
            value=5.1,
            unit="%",
            source="gus",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),  # noqa: DTZ001
        ),
    ]
    df = spark.createDataFrame(rows)

    standardized = standardize_macro(df)
    row = standardized.collect()[0]

    assert row.country == "PL"
    assert row.reference_date == date(2023, 12, 31)
    assert standardized.schema["reference_date"].dataType.typeName() == "date"
