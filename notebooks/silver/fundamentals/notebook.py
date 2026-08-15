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

# ## Silver — fundamentals
#
# Deduplicates, standardizes, and unpivots the three Bronze FMP statement tables
# (`bronze_fmp_balance_sheet`, `bronze_fmp_income_statement`, `bronze_fmp_cash_flow`) into
# `silver_fundamentals`: one row per (ticker, period_end_date, statement_type, metric_name),
# latest retrieved_at wins. Long/EAV format — see docs/data-model.md for why. Idempotent —
# upserts via MERGE INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import lit
from transforms import (
    deduplicate_fundamentals,
    standardize_fundamentals,
    unpivot_fundamentals_metrics,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
balance_sheet_table_name: str = "bronze_fmp_balance_sheet"
income_statement_table_name: str = "bronze_fmp_income_statement"
cash_flow_table_name: str = "bronze_fmp_cash_flow"
silver_table_name: str = "silver_fundamentals"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

statement_tables = [
    (balance_sheet_table_name, "balance_sheet"),
    (income_statement_table_name, "income_statement"),
    (cash_flow_table_name, "cash_flow"),
]

frames = []
for table_name, statement_type in statement_tables:
    frames.append(spark.read.table(table_name).withColumn("statement_type", lit(statement_type)))

bronze_df = frames[0]
for frame in frames[1:]:
    bronze_df = bronze_df.unionByName(frame, allowMissingColumns=True)

deduped_df = deduplicate_fundamentals(bronze_df)
standardized_df = standardize_fundamentals(deduped_df)
silver_df = unpivot_fundamentals_metrics(standardized_df)

if spark.catalog.tableExists(silver_table_name):
    delta_table = DeltaTable.forName(spark, silver_table_name)
    (
        delta_table.alias("target")
        .merge(
            silver_df.alias("source"),
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
    silver_df.write.format("delta").saveAsTable(silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
