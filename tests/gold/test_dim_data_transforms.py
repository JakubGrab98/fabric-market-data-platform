from datetime import date

from notebooks.gold.dim_data.transforms import generate_date_dimension


def test_generate_date_dimension_covers_full_range(spark):
    dim_df = generate_date_dimension(date(2024, 1, 1), date(2024, 1, 7), spark)

    assert dim_df.count() == 7
    dates = {r.date for r in dim_df.collect()}
    assert dates == {date(2024, 1, d) for d in range(1, 8)}


def test_generate_date_dimension_derives_calendar_attributes(spark):
    dim_df = generate_date_dimension(date(2024, 3, 15), date(2024, 3, 15), spark)
    row = dim_df.collect()[0]

    assert row.year == 2024
    assert row.quarter == 1
    assert row.month == 3
    assert row.month_name == "March"
    assert row.day_name == "Friday"


def test_generate_date_dimension_flags_weekdays_as_trading_days(spark):
    dim_df = generate_date_dimension(date(2024, 3, 11), date(2024, 3, 17), spark)
    by_day_name = {r.day_name: r.is_trading_day_gpw for r in dim_df.collect()}

    assert by_day_name["Monday"] is True
    assert by_day_name["Friday"] is True
    assert by_day_name["Saturday"] is False
    assert by_day_name["Sunday"] is False
