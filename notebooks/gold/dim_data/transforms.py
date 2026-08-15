"""Transform functions for the Gold dim_data build notebook."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofweek,
    explode,
    expr,
    month,
    quarter,
    sequence,
    weekofyear,
    year,
)


def generate_date_dimension(start_date: date, end_date: date, spark: SparkSession) -> DataFrame:
    """Generate one row per calendar date in [start_date, end_date] (inclusive).

    A standard Kimball calendar utility table — generated, not ingested, so
    it has no source/retrieved_at columns.

    is_trading_day_gpw is currently a weekday-only approximation (Mon-Fri via
    Spark's dayofweek(), which is 1=Sunday..7=Saturday) — it does not yet
    exclude Polish public holidays. See docs/next-steps.md for the follow-up
    (a maintained PL holiday calendar is the right formula here, not a
    hand-maintained date list — CLAUDE.md "formulas over magic numbers" — but
    that's a new dependency this notebook doesn't add speculatively).

    Args:
        start_date: First date in the generated range (inclusive).
        end_date: Last date in the generated range (inclusive).
        spark: Active SparkSession.
    Returns:
        DataFrame with columns: date, year, quarter, month, month_name,
        week_of_year, day_of_week, day_name, is_trading_day_gpw.
    """
    bounds_df = spark.createDataFrame([(start_date, end_date)], ["start_date", "end_date"])
    dates_df = bounds_df.select(
        explode(
            sequence(col("start_date"), col("end_date"), expr("interval 1 day")),
        ).alias("date")
    )
    return (
        dates_df.withColumn("year", year(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("week_of_year", weekofyear(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("day_name", date_format(col("date"), "EEEE"))
        .withColumn("is_trading_day_gpw", dayofweek(col("date")).between(2, 6))
    )
