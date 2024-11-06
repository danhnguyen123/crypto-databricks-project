# Databricks notebook source
# MAGIC %run ../Utilities/configuration

# COMMAND ----------

# MAGIC %run ../Utilities/common_functions

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

TABLE_NAME = "transaction"

# COMMAND ----------

df_staging = DeltaTable.forName(spark, f"{CATALOG}.{BRONZE_SCHEMA}.{TABLE_NAME}")

# COMMAND ----------

try:
    df_select = (df_staging.toDF()
        .select(
            F.col("id").alias("transaction_id"), 
            F.col("customer_id").alias("customer_id"), 
            F.col("date"),
            F.col("market_execution_json"),
            F.col("market_execution_json.source_amount").alias("source_amount"),
            F.col("market_execution_json.destination_amount").alias("destination_amount"),
            F.col("network"),
            F.col("txn_date"),
            F.col("load_timestamp"),
        )
    )
except Exception as e:
    raise(e)

# COMMAND ----------

try:
df = (df_select
    .withColumn("transaction_time", F.from_unixtime(F.col("date")).cast("timestamp"))
    .withColumn("execution_plan", F.when(F.col("market_execution_json").getItem("pool").isNotNull(), "pool").otherwise("order_book"))
    .withColumn("pool_id", F.expr("transform(market_execution_json.pool.split_swaps, swap -> swap.swap_route[0].pool_id)"))
    .withColumn("order_book_id", F.expr("transform(market_execution_json.hybrid.SplitSwapResults, swap -> swap.order_book_filled_order_ids[0])"))
    .withColumn("protocol_fee_token",
        F.when(
            F.col("market_execution_json.pool").isNotNull(),
            F.col("market_execution_json.pool.protocol_fees")[0]["token"]
        ).otherwise(F.col("market_execution_json.protocol_fees")[0]["token"])
    )
    .withColumn("protocol_fee_amount",
        F.when(
            F.col("market_execution_json.pool").isNotNull(),
            F.col("market_execution_json.pool.protocol_fees")[0]["amount"]
        ).otherwise(F.col("market_execution_json.protocol_fees")[0]["amount"])
    )
    .withColumn("swap_count",
        F.when(
            F.col("market_execution_json.hybrid").isNotNull(),
            F.size(F.col("market_execution_json.hybrid.SplitSwapResults"))
        ).otherwise(F.size(F.col("market_execution_json.pool.split_swaps")))
    )
)
except Exception as e:
    raise(e)

# COMMAND ----------

df = df.select("transaction_id", 
               "customer_id", 
               "transaction_time", 
               "execution_plan", 
               "pool_id", 
               "order_book_id",
               "protocol_fee_token",
               "protocol_fee_amount",
               "swap_count",
               "network",
               "txn_date",
               "load_timestamp"
               )


# COMMAND ----------

df.cache()

# COMMAND ----------

df.take(1)

# COMMAND ----------

# Detect merge condition
merge_condition = f_get_primary_key(df)
merge_condition

# COMMAND ----------

# Incremental load
f_merge_delta_data(df, CATALOG, SILVER_SCHEMA, TABLE_NAME, merge_condition, 'txn_date')

# COMMAND ----------

df.unpersist()

# COMMAND ----------

dbutils.notebook.exit("Success")
