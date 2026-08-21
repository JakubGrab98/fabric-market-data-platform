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

# ## Gold — fact_prices
#
# Selects silver_prices into fact_prices' canonical column order — Silver is already at the
# right grain (ticker, date) and shape, so this makes the Gold contract explicit. Idempotent —
# upserts via MERGE INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable
from transforms import build_fact_prices

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
silver_table_name: str = "silver_prices"
gold_table_name: str = "fact_prices"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spark.read.table(silver_table_name)
gold_df = build_fact_prices(silver_df)

if spark.catalog.tableExists(gold_table_name):
    delta_table = DeltaTable.forName(spark, gold_table_name)
    (
        delta_table.alias("target")
        .merge(
            gold_df.alias("source"),
            "target.ticker = source.ticker AND target.date = source.date",
        )
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
