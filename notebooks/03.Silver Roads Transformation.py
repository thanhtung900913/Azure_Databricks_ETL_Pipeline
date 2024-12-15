# Databricks notebook source
# MAGIC %run "/Workspace/Users/thanhtung29062004@outlook.com/Common"

# COMMAND ----------

dbutils.widgets.text(name="input", defaultValue="", label="Enter environment name")
env=dbutils.widgets.get("input")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read data from Raw Roads

# COMMAND ----------

def read_RawRoads(environment):
  print("Reading raw roads from environment: " + environment)
  df = (spark.readStream
  .table(f"{environment}.`bronze`.`raw_road`"))
  print("Raw roads read successfully")
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating road_category column

# COMMAND ----------

from pyspark.sql.functions import col, when
def road_categories(df):
  print("Adding road categories")
  df_road_Cat = df.withColumn("Road_Category_Name",
                when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                .when(col('Road_Category') == 'PA','Class A Principal road')
                .when(col('Road_Category') == 'PM','Class A Principal Motorway')
                .when(col('Road_Category') == 'M','Class B road')
                .otherwise('NA')
                )
  print("Road categories added successfully")
  return df_road_Cat

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating road_type column

# COMMAND ----------

from pyspark.sql.functions import col, when
def road_types(df):
    print("Adding road types")
    df_road_Type = df.withColumn("Road_Type",
                  when(col('Road_Category_Name').like('%Class A%'),'Major')
                  .when(col('Road_Category_Name').like('%Class B%'),'Minor')
                    .otherwise('NA')
                  )
    print("Road types added successfully")
    return df_road_Type

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing data to Silver Road table

# COMMAND ----------

def write_Roads_SilverTable(StreamingDF,environment):
    print("writing to silver table") 
    write_StreamSilver = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "/SilverRoadsLoad/Checkpoints/")
                .outputMode('append')
                .queryName("SilverRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}`.`silver`.`silver_roads`"))
    
    write_StreamSilver.awaitTermination()
    print(f'Writing `{environment}_catalog`.`silver`.`silver_roads` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calling function

# COMMAND ----------

df_roads = read_RawRoads(env)


# COMMAND ----------

df_dups = remove_dup(df_roads)
df_clean = handle_null(df_dups, df_dups.columns)

# COMMAND ----------

df_roadCat = road_categories(df_clean)
df_roadType = road_types(df_roadCat)

# COMMAND ----------

write_Roads_SilverTable(df_roadType, env)
