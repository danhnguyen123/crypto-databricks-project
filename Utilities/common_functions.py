# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def f_get_secret(key):
    try:
        return dbutils.secrets.get(scope="cryptoproject", key=key)
    except Exception as e:
        raise(e)

# COMMAND ----------

def f_add_input_file_name_loadtime(df,date_part=None,file_name=None):
    from pyspark.sql.functions import input_file_name,split,current_timestamp,lit,to_date,regexp_extract
    try:
        if(file_name is None and date_part is None):
            df_final=df.select("*").\
                withColumn("input_file_name",input_file_name()).\
                withColumn("network", regexp_extract(input_file_name(), r"unstructured_data/(.*?)/txn_date", 1)). \
                withColumn("txn_date", regexp_extract(input_file_name(), r"txn_date=([0-9\-]+)", 1)). \
                withColumn("load_timestamp",current_timestamp())
        # elif(file_name is None):
        #     df_final=df.select("*").\
        #         withColumn("input_file_name",split(input_file_name(),'/')[4]).\
        #         withColumn("date_part",lit(date_part)).\
        #         withColumnRenamed("date_part",to_date(date_part)).\
        #         withColumn("load_timestamp",current_timestamp())
        # else:
        #     df_final=df.select("*").\
        #         withColumn("input_file_name",lit(file_name)).\
        #         withColumn("date_part",to_date(lit(date_part))).\
        #         withColumn("load_timestamp",current_timestamp())
        return df_final
    except Exception as e:
        raise(e)

# COMMAND ----------

def f_validate_schema(df,sink_schema):
    no_rows=df.count()
    if(no_rows<=100000):
       no_files=1
    elif(no_rows>100000 and no_rows<=1000000):
       no_files=3
    elif(no_rows>1000000 and no_rows<=10000000):
       no_files=5
    source_schema=df.limit(1).dtypes
    if(source_schema==sink_schema):
        return no_files
    else:
        raise Exception("Schema is not matched")

# COMMAND ----------

def f_get_primary_key(df):
    try:
        import mack as mk
        primary_key=mk.find_composite_key_candidates(df)
        merge_condition=" "
        for i in range(len(primary_key)):
            if(i==len(primary_key)-1):
                merge_condition+="tgt."+primary_key[i]+"=src."+primary_key[i]
            else:
                merge_condition+="tgt."+primary_key[i]+"=src."+primary_key[i]+" AND "
        return (merge_condition)
    except Exception as err:
        print("Error occured ",str(err))
        raise err

# COMMAND ----------

def f_merge_delta_data(input_df, catalog_name, schema_name, table_name, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
  # dynamic: use merge_condition  A.partition_column = B.partition_column 
  # if use merge_condition  A.partition_column = 'value', don'n need set dynamic is true
  try:
    target_table = f"{catalog_name}.{schema_name}.{table_name}"
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(target_table)):
      deltaTable = DeltaTable.forName(spark, target_table)
      deltaTable.alias("tgt").merge(
          input_df.alias("src"),
          merge_condition) \
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
      input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(target_table)
  except Exception as err:
      print("Error occured ",str(err))
      raise err

# COMMAND ----------

def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
  # dynamic: use merge_condition  A.partition_column = B.partition_column 
  # if use merge_condition  A.partition_column = 'value', don'n need set dynamic is true
  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else:
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
