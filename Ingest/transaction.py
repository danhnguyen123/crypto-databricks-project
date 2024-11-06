# Databricks notebook source
dbutils.widgets.text("p_txn_date", "2023-10-28")
v_txn_date = dbutils.widgets.get("p_txn_date")
v_txn_date

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
import pyspark.sql.utils

# COMMAND ----------

# MAGIC %run ../Utilities/configuration

# COMMAND ----------

# MAGIC %run ../Utilities/common_functions

# COMMAND ----------

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# COMMAND ----------

try:
    path = f"{RAW_FOLDER_PATH}/*/txn_date={v_txn_date}/*/*.json"
    logger.debug(f"Reading transaction raw data from {path}")
    df_input=spark.read.option("multiline", "true").json(path).cache()
except pyspark.sql.utils.AnalysisException as ae:
    logger.error(f"Schema mismatch: {ae}")
    raise ae
except Exception as e:
    logger.error(f"An error occurred: {e}")
    raise e

# COMMAND ----------

df=f_add_input_file_name_loadtime(df_input)

# COMMAND ----------

display(df)

# COMMAND ----------

bronze_table_path = f"{CATALOG}.{BRONZE_SCHEMA}.transaction"
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("replaceWhere", f"txn_date = '{v_txn_date}'") \
  .partitionBy("txn_date") \
  .saveAsTable(bronze_table_path)

# COMMAND ----------

df_input.unpersist()

# COMMAND ----------

dbutils.notebook.exit("Success")
