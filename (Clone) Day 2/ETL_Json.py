# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls
# MAGIC

# COMMAND ----------

# dbutils.fs.mount(
#   source = "wasbs://raw@saunextadls.blob.core.windows.net",
#   mount_point = "/mnt/saunextadls/raw",
#   extra_configs = {"fs.azure.account.key.saunextadls.blob.core.windows.net":"DsZWJs7JVVHZz1I7GKyclV8ejCdj0V2UkqMlgAp6QyVOw5rvrHvmVTgwcThdHUymWg7MXon65/0z+AStj4Yiug=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/saunextadls/raw/json/

# COMMAND ----------

df= spark.read.json("dbfs:/mnt/saunextadls/raw/json")

# COMMAND ----------

df.display()

# COMMAND ----------



df1 = df.withColumn("ingestionDate", current_timestamp()).withColumn("path", input_file_name())

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists json

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("bronzejson")
##this will create a managed table as no path is defined

# COMMAND ----------

df.write.mode("overwrite").option("path","dbfs:/mnt/saunextadls/raw/output/siddhi/json/").saveAsTable("json.bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use json;
# MAGIC
# MAGIC select * from bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use json;
# MAGIC
# MAGIC select * from bronze;

# COMMAND ----------


