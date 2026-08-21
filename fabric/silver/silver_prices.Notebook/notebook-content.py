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

# ## Silver — prices
#
# Deduplicates and standardizes `bronze_stooq_prices` into `silver_prices`: one row per
# (ticker, date), latest retrieved_at wins, date cast to a real date, currency joined in from
# `notebooks/config/tickers.yaml` (passthrough label, no FX conversion). Idempotent — upserts
# via MERGE INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable
from transforms import (
    add_currency_column,
    deduplicate_prices,
    load_ticker_config,
    standardize_prices,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
bronze_table_name: str = "bronze_stooq_prices"
silver_table_name: str = "silver_prices"
ticker_config_path: str = "notebooks/config/tickers.yaml"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.table(bronze_table_name)
tickers = load_ticker_config(ticker_config_path)

deduped_df = deduplicate_prices(bronze_df)
standardized_df = standardize_prices(deduped_df)
silver_df = add_currency_column(standardized_df, tickers, spark)

if spark.catalog.tableExists(silver_table_name):
    delta_table = DeltaTable.forName(spark, silver_table_name)
    (
        delta_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.ticker = source.ticker AND target.date = source.date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").saveAsTable(silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
