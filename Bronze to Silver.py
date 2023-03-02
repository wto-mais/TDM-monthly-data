# Databricks notebook source
import pandas as pd
import os
import re
from pyspark.sql import functions as sf

# COMMAND ----------

storage_account_name = "wtomais"
container_name = "tdm"
bronze_data_path = "bronze_parquet"
silver_data_path = "silver_parquet"
access_key = "w1y+azqR2lKTTBJdKbzR3R3BMKKjoFL5ktwlMaatv05PgkSM/rmUB4WadojX8uQdAG12HmjE2bmf+AStxLab6Q=="
spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net", access_key)
input_container_path = "wasbs://%s@%s.blob.core.windows.net" % (container_name, storage_account_name)
bronze_path = input_container_path + '/' + bronze_data_path
silver_path = input_container_path + '/' + silver_data_path

# COMMAND ----------

# Get the list of all the files in the bronze layer
output_container_path = "wasbs://%s@%s.blob.core.windows.net" % (container_name, storage_account_name)
bronze_path = output_container_path + '/' + bronze_data_path 
all_files = dbutils.fs.ls(bronze_path)
df = pd.DataFrame(all_files)
df['date'] = df['name'].str.extract(r'^bulk_(?:update|\d\d\d\d)_(\d\d\d\d\d\d\d\d)\.parquet$')
df = df[df.date.notnull()]


for i in df.name:
    print(i)
    temp_path = df.loc[df['name'] == i,'path'].iat[0]
    temp_df = (spark.read
      .format("parquet")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(temp_path)
    )
    temp_rows = temp_df.count()
    print(temp_rows)


# COMMAND ----------

all_files = dbutils.fs.ls(bronze_path)
file_list = pd.DataFrame(all_files)
file_list['date'] = file_list['name'].str.extract(r'^bulk_(?:update|\d\d\d\d)_(\d\d\d\d\d\d\d\d)\.parquet$')
file_list = file_list[file_list.date.notnull()]

files = file_list['path']
parquet_df = spark.read.parquet(*files).withColumn('path', sf.input_file_name())

files_df = spark.createDataFrame(file_list[['path', 'name','date']])
parquet_df = parquet_df.join(files_df,['path'],'left')

key_colmuns = ['name','date','REPORTER','FLOW','YEAR','MONTH']
key_tdm = ['REPORTER','FLOW','YEAR','MONTH']
#parquet_df.groupBy('name').count().show()
df_inv = parquet_df.select(*key_colmuns).dropDuplicates()
df_inv = df_inv.groupBy(*key_tdm).agg(
    sf.max_by('name', 'date').alias('name'),
    sf.max('date').alias('date')
)

parquet_df_clean = parquet_df.join(df_inv, ['name','date','REPORTER','FLOW','YEAR','MONTH'] ,'right')
#parquet_df_clean.groupBy('name').count().show()
parquet_df_clean = parquet_df_clean.drop('name','date','path')
parquet_df_clean.write.mode('overwrite').parquet(silver_path)

# COMMAND ----------

configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net": access_key}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = silver_path,
  mount_point = "/mnt/tdm_silver_data",
  extra_configs = configs)


# COMMAND ----------

output_container_path = "wasbs://%s@%s.blob.core.windows.net" % (container_name, storage_account_name)
temp_path = "%s/misc_temp" % output_container_path
target_path = "%s/misc/silver_inv.csv" % output_container_path

(df_inv
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("com.databricks.spark.csv")
 .save(temp_path))

temporary_csv = os.path.join(temp_path, dbutils.fs.ls(temp_path)[3][1])
dbutils.fs.cp(temporary_csv, target_path)
dbutils.fs.rm(temp_path,True)

# COMMAND ----------

# MAGIC %fs
# MAGIC mounts

# COMMAND ----------

df3 = (spark.read
  .format("parquet")
  .option("header", "true")
  .option("inferSchema", "true")
  .load('/mnt/tdm_silver_data')
)

df3.count()
