# Databricks notebook source
# MAGIC %run "/Workspace/Users/thanhtung29062004@outlook.com/Common"

# COMMAND ----------

dbutils.widgets.text(name="input",defaultValue="", label="Enter environment name in lower case")
env = dbutils.widgets.get("input")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read silver_traffic table

# COMMAND ----------

def read_SilverTraffic(environment):
    print(f"Reading Silver Traffic from {environment}")
    df = (spark.readStream
          .table(f"{environment}.`silver`.`silver_traffic`"))
    print(f"Reading Silver Traffic from {environment} completed")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read silver_roads table

# COMMAND ----------

def read_SilverRoads(environment):
    print(f"Reading Silver Roads from {environment}")
    df = (spark.readStream
          .table(f"{environment}.`silver`.`silver_roads`"))
    print(f"Reading Silver Roads from {environment} completed")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating Vehicle Intensity column

# COMMAND ----------

from pyspark.sql.functions import col
def create_VehicleIntensity(df):
    print('Creating Vehicle Intensity column : ',end='')
    df_veh = df.withColumn('Vehicle_Intensity',
                col('Count_Motor_Vehicles') / col('Link_length_km')
                )
    print("Creating Vehicle Intensity column successfully!!")
    return df_veh

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating LoadTime column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def create_LoadTime(df):
    print("Creating Load Time column")
    df_timestamp = df.withColumn('Load_Time',
                      current_timestamp()
                      )
    print("Creating Load Time column successfully!!")
    return df_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing data to Gold Traffic

# COMMAND ----------

def write_Traffic_GoldTable(StreamingDF,environment):
    print('Writing the gold_traffic Data : ',end='') 
    write_gold_traffic = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "GoldTrafficLoad/Checkpoints/")
                .outputMode('append')
                .queryName("GoldTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}`.`gold`.`gold_traffic`"))
    
    write_gold_traffic.awaitTermination()
    print(f'Writing `{environment}`.`gold`.`gold_traffic` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writting data to Gold Roads

# COMMAND ----------

def write_Roads_GoldTable(StreamingDF,environment):
    print('Writing the gold_roads Data : ',end='') 

    write_gold_roads = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',checkpoint+ "GoldRoadsLoad/Checkpoints/")
                .outputMode('append')
                .queryName("GoldRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{environment}`.`gold`.`gold_roads`"))
    
    write_gold_roads.awaitTermination()
    print(f'Writing `{environment}`.`gold`.`gold_roads` Success!')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calling function

# COMMAND ----------

## Reading from Silver tables
df_SilverTraffic = read_SilverTraffic(env)
df_SilverRoads = read_SilverRoads(env)

# COMMAND ----------

## Tranformations     
df_vehicle = create_VehicleIntensity(df_SilverTraffic)
df_FinalTraffic = create_LoadTime(df_vehicle)
df_FinalRoads = create_LoadTime(df_SilverRoads)

# COMMAND ----------

## Writing to gold tables    
write_Traffic_GoldTable(df_FinalTraffic,env)
write_Roads_GoldTable(df_FinalRoads,env)
