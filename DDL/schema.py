# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS crypto_lakehouse_dev.bronze
# MAGIC COMMENT 'Schema for transactional raw data in bronze layer'
# MAGIC -- MANAGED LOCATION 's3://crypto-lakehouse-dev/silver'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS crypto_lakehouse_dev.silver
# MAGIC COMMENT 'Schema for transactional data in silver layer'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS crypto_lakehouse_dev.gold
# MAGIC COMMENT 'Schema for transactional data in gold layer'
