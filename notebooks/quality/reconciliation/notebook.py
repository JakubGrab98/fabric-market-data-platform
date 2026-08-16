# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MARKDOWN ********************

# ## Quality — reconciliation
#
# Cross-layer consistency checks: row counts and date/period ranges between Bronze->Silver
# (dedup may reduce row count, but never increase it, and must never narrow the date range)
# and Silver->Gold (every Gold fact transform today is a lossless select() — counts and
# ranges must match exactly). Read-only — writes nothing. Raises if any check fails, listing
# which ones; this repo has no orchestration layer yet to consume a structured result, so a
# raised exception is the pipeline-failure signal for now (see docs/next-steps.md).
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from transforms import compare_range, compare_row_counts, summarize_check_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
bronze_stooq_prices_table: str = "bronze_stooq_prices"
silver_prices_table: str = "silver_prices"
fact_prices_table: str = "fact_prices"

bronze_nbp_fx_rates_table: str = "bronze_nbp_fx_rates"
silver_fx_rates_table: str = "silver_fx_rates"

bronze_fmp_balance_sheet_table: str = "bronze_fmp_balance_sheet"
bronze_fmp_income_statement_table: str = "bronze_fmp_income_statement"
bronze_fmp_cash_flow_table: str = "bronze_fmp_cash_flow"
silver_fundamentals_table: str = "silver_fundamentals"
fact_fundamentals_table: str = "fact_fundamentals"

bronze_gus_macro_table: str = "bronze_gus_macro"
silver_macro_table: str = "silver_macro"
fact_macro_table: str = "fact_macro"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = {}

# --- Bronze -> Silver: row count may shrink (dedup), date/period range must not ---

bronze_prices_df = spark.read.table(bronze_stooq_prices_table)
silver_prices_df = spark.read.table(silver_prices_table)
results["prices_bronze_to_silver_count"] = compare_row_counts(
    bronze_prices_df, silver_prices_df, exact=False
)
results["prices_bronze_to_silver_range"] = compare_range(
    bronze_prices_df, silver_prices_df, "date", "date"
)

bronze_fx_rates_df = spark.read.table(bronze_nbp_fx_rates_table)
silver_fx_rates_df = spark.read.table(silver_fx_rates_table)
results["fx_rates_bronze_to_silver_count"] = compare_row_counts(
    bronze_fx_rates_df, silver_fx_rates_df, exact=False
)
results["fx_rates_bronze_to_silver_range"] = compare_range(
    bronze_fx_rates_df, silver_fx_rates_df, "effective_date", "effective_date"
)

bronze_fundamentals_df = (
    spark.read.table(bronze_fmp_balance_sheet_table)
    .unionByName(spark.read.table(bronze_fmp_income_statement_table), allowMissingColumns=True)
    .unionByName(spark.read.table(bronze_fmp_cash_flow_table), allowMissingColumns=True)
)
silver_fundamentals_df = spark.read.table(silver_fundamentals_table)
results["fundamentals_bronze_to_silver_count"] = compare_row_counts(
    bronze_fundamentals_df, silver_fundamentals_df, exact=False
)
results["fundamentals_bronze_to_silver_range"] = compare_range(
    bronze_fundamentals_df, silver_fundamentals_df, "date", "period_end_date"
)

bronze_macro_df = spark.read.table(bronze_gus_macro_table)
silver_macro_df = spark.read.table(silver_macro_table)
results["macro_bronze_to_silver_count"] = compare_row_counts(
    bronze_macro_df, silver_macro_df, exact=False
)
results["macro_bronze_to_silver_range"] = compare_range(
    bronze_macro_df, silver_macro_df, "year", "year"
)

# --- Silver -> Gold: lossless select(), row count and range must match exactly ---

fact_prices_df = spark.read.table(fact_prices_table)
results["prices_silver_to_gold_count"] = compare_row_counts(
    silver_prices_df, fact_prices_df, exact=True
)
results["prices_silver_to_gold_range"] = compare_range(
    silver_prices_df, fact_prices_df, "date", "date"
)

fact_fundamentals_df = spark.read.table(fact_fundamentals_table)
results["fundamentals_silver_to_gold_count"] = compare_row_counts(
    silver_fundamentals_df, fact_fundamentals_df, exact=True
)
results["fundamentals_silver_to_gold_range"] = compare_range(
    silver_fundamentals_df, fact_fundamentals_df, "period_end_date", "period_end_date"
)

fact_macro_df = spark.read.table(fact_macro_table)
results["macro_silver_to_gold_count"] = compare_row_counts(
    silver_macro_df, fact_macro_df, exact=True
)
results["macro_silver_to_gold_range"] = compare_range(
    silver_macro_df, fact_macro_df, "reference_date", "reference_date"
)

summary = summarize_check_results(results)
if not summary["all_passed"]:
    raise RuntimeError(
        f"Reconciliation checks failed: {summary['failed']}. Full results: {results}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
