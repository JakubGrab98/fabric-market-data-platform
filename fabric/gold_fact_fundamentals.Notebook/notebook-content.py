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

# ## Gold — fact_fundamentals
#
# Selects silver_fundamentals into fact_fundamentals' canonical column order — Silver is already
# at the right long/EAV grain (ticker, period_end_date, statement_type, metric_name), see
# docs/data-model.md. Idempotent — upserts via MERGE INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable
from transforms import build_fact_fundamentals

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
silver_table_name: str = "silver_fundamentals"
gold_table_name: str = "fact_fundamentals"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spark.read.table(silver_table_name)
gold_df = build_fact_fundamentals(silver_df)

if spark.catalog.tableExists(gold_table_name):
    delta_table = DeltaTable.forName(spark, gold_table_name)
    (
        delta_table.alias("target")
        .merge(
            gold_df.alias("source"),
            "target.ticker = source.ticker "
            "AND target.period_end_date = source.period_end_date "
            "AND target.statement_type = source.statement_type "
            "AND target.metric_name = source.metric_name",
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
