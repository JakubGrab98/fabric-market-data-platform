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

# ## Gold — dim_spolka
#
# Builds dim_spolka directly from `notebooks/config/tickers.yaml` — static reference data, not
# something ingested through Bronze/Silver. One row per ticker. Idempotent — upserts via MERGE
# INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable
from transforms import build_dim_spolka, load_ticker_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
ticker_config_path: str = "notebooks/config/tickers.yaml"
gold_table_name: str = "dim_spolka"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tickers = load_ticker_config(ticker_config_path)
gold_df = build_dim_spolka(tickers, spark)

if spark.catalog.tableExists(gold_table_name):
    delta_table = DeltaTable.forName(spark, gold_table_name)
    (
        delta_table.alias("target")
        .merge(gold_df.alias("source"), "target.ticker = source.ticker")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    gold_df.write.format("delta").saveAsTable(gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
