# Databricks notebook source
import datetime

# COMMAND ----------

# dbutils.widgets.text("p_txn_date", "2023-10-28")
v_txn_date = dbutils.widgets.get("p_txn_date")
print(v_txn_date)

# COMMAND ----------

v_txn_date = v_txn_date.split("T")[0]
print(v_txn_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## transaction

# COMMAND ----------

v_result = dbutils.notebook.run("transaction", 0, {"p_txn_date": v_txn_date})

# COMMAND ----------

v_result
