# Databricks notebook source
# MAGIC %run ../Utilities/common_functions

# COMMAND ----------

import os

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


access_key = f_get_secret(key="aws-access-key")
secret_key = f_get_secret(key="aws-secret-key")
encoded_secret_key = secret_key.replace("/", "%2F")
BUCKET_SOURCE = os.getenv("BUCKET_SOURCE", "crypto-transaction-networks")
BUCKET_SINK_DEV = os.getenv("BUCKET_SINK_DEV", "crypto-lakehouse-dev")
BUCKET_SINK = os.getenv("LOG_LEVEL", "crypto-lakehouse-prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Source

# COMMAND ----------

mountPoint = f"/mnt/{BUCKET_SOURCE}/unstructured_data"
dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{BUCKET_SOURCE}/unstructured_data", mountPoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Sink (Dev)

# COMMAND ----------

mountPoint = f"/mnt/{BUCKET_SINK_DEV}/bronze"
dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{BUCKET_SINK_DEV}/bronze", mountPoint)

# COMMAND ----------

mountPoint = f"/mnt/{BUCKET_SINK_DEV}/silver"
dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{BUCKET_SINK_DEV}/silver", mountPoint)

# COMMAND ----------

mountPoint = f"/mnt/{BUCKET_SINK_DEV}/gold"
dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{BUCKET_SINK_DEV}/gold", mountPoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Sink (Prod)

# COMMAND ----------

mountPoint = f"/mnt/{BUCKET_SINK}/bronze"
dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{BUCKET_SINK}/bronze", mountPoint)

# COMMAND ----------

mountPoint = f"/mnt/{BUCKET_SINK}/silver"
dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{BUCKET_SINK}/silver", mountPoint)

# COMMAND ----------

mountPoint = f"/mnt/{BUCKET_SINK}/gold"
dbutils.fs.unmount(mountPoint)
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{BUCKET_SINK}/silver", mountPoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test

# COMMAND ----------

display(dbutils.fs.mounts())
