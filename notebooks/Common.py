# Databricks notebook source
# MAGIC %md
# MAGIC ##Defining common variables

# COMMAND ----------

checkpoint = spark.sql("describe external location `checkpoints`").select("url").collect()[0][0]
landing = spark.sql("describe external location `landing`").select("url").collect()[0][0]
bronze = spark.sql("describe external location `bronze`").select("url").collect()[0][0]
silver = spark.sql("describe external location `silver`").select("url").collect()[0][0]
gold = spark.sql("describe external location `gold`").select("url").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handling duplicate records

# COMMAND ----------

def remove_dup(df):
  print("Removing duplicates")
  df_dup = df.dropDuplicates()
  print("Done!\n==================================")
  return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handling Nulls

# COMMAND ----------

def handle_null(df, columns):
  print("Handing nulls")
  df_string = df.fillna("NA", subset=columns)
  df_numeric = df_string.fillna(0, subset=columns)
  print("Done!\n==================================")
  return df_numeric

