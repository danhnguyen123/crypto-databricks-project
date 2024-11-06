# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS crypto_lakehouse_dev.silver.transaction
# MAGIC (
# MAGIC     transaction_id LONG,
# MAGIC     customer_id LONG,
# MAGIC     transaction_time TIMESTAMP,
# MAGIC     execution_plan STRING,
# MAGIC     pool_id ARRAY<LONG>,
# MAGIC     order_book_id ARRAY<LONG>,
# MAGIC     protocol_fee_token STRING,
# MAGIC     protocol_fee_amount LONG,
# MAGIC     swap_count INTEGER,
# MAGIC     network STRING,
# MAGIC     txn_date STRING,
# MAGIC     load_timestamp TIMESTAMP
# MAGIC  
# MAGIC )
# MAGIC using DELTA
# MAGIC partitioned by (txn_date)
