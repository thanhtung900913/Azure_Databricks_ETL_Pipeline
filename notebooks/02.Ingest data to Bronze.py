# Databricks notebook source
checkpoint = spark.sql(" DESCRIBE EXTERNAL LOCATION `checkpoints` ").select("url").collect()[0][0]
landing = spark.sql(" DESCRIBE EXTERNAL LOCATION `landing` ").select("url").collect()[0][0]

# COMMAND ----------

dbutils.widgets.text(name="input", defaultValue="", label="Enter environtment in lower case")
env = dbutils.widgets.get("input")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Raw Traffic Data Funcion

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp

def readRawTraffic():
  print("Reading Raw Traffic Data")
  schema = StructType([
        StructField("Record_ID",IntegerType()),
        StructField("Count_point_id",IntegerType()),
        StructField("Direction_of_travel",StringType()),
        StructField("Year",IntegerType()),
        StructField("Count_date",StringType()),
        StructField("hour",IntegerType()),
        StructField("Region_id",IntegerType()),
        StructField("Region_name",StringType()),
        StructField("Local_authority_name",StringType()),
        StructField("Road_name",StringType()),
        StructField("Road_Category_ID",IntegerType()),
        StructField("Start_junction_road_name",StringType()),
        StructField("End_junction_road_name",StringType()),
        StructField("Latitude",DoubleType()),
        StructField("Longitude",DoubleType()),
        StructField("Link_length_km",DoubleType()),
        StructField("Pedal_cycles",IntegerType()),
        StructField("Two_wheeled_motor_vehicles",IntegerType()),
        StructField("Cars_and_taxis",IntegerType()),
        StructField("Buses_and_coaches",IntegerType()),
        StructField("LGV_Type",IntegerType()),
        StructField("HGV_Type",IntegerType()),
        StructField("EV_Car",IntegerType()),
        StructField("EV_Bike",IntegerType())
  ])
  rawTrafficStream = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", "csv")
                      .option("cloudFiles.schemaLocation", f"{checkpoint}/rawTrafficLoad/schemaInfer")
                      .option("header", "True")
                      .schema(schema)
                      .load(f"{landing}/raw_traffic/")
                      .withColumn("Extract_Time", current_timestamp())
                      )
  print("Reading Successful!")
  print("======================================")
  return rawTrafficStream

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Raw Road Data Function

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import current_timestamp

def readRawRoad():
    print("Reading Raw Road Data")
    schema = StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType())
    ])
    rawRoadStream = (spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format","csv")
                    .option('cloudFiles.schemaLocation',f'{checkpoint}/rawRoadLoad/schemaInfer')
                    .option('header','True')
                    .schema(schema)
                    .load(landing+'/raw_roads/')
                    )
    print("Reading Successful!")
    print("======================================")
    return rawRoadStream
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Write Traffic Data function

# COMMAND ----------

def writeRawTraffic(StreamingDF, environment):
    print("Writing Raw Traffic Data")
    writeStream = (StreamingDF.writeStream
                   .format("delta")
                   .option("checkpointLocation", f"{checkpoint}/rawTrafficLoad/checkpoint")
                   .outputMode("append")
                   .queryName(f"rawTraffic_{environment}")
                   .trigger(availableNow = True)
                   .toTable(f"{environment}.`bronze`.`raw_traffic`")
    )
    writeStream.awaitTermination()
    print("Writing Successful!")
    print("======================================")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Write Road Data function

# COMMAND ----------

def writeRawRoad(StreamingDF, env):
    print("Writing Road Traffic Data")
    writeStream = (StreamingDF.writeStream
                   .format("delta")
                   .option("checkpointLocation", f"{checkpoint}/roadTrafficLoad/checkpoint")
                   .outputMode("append")
                   .queryName(f"roadTraffic_{env}")
                   .trigger(availableNow=True)
                   .toTable(f"{env}.`bronze`.`raw_road`")
                   )
    writeStream.awaitTermination()
    print("Writing Successful!")
    print("======================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Call function

# COMMAND ----------

trafficStream = readRawTraffic()
writeRawTraffic(trafficStream, env)

# COMMAND ----------

roadStream = readRawRoad()
writeRawRoad(roadStream, env)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `databricks_dev_ws`.`bronze`.`raw_road`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `databricks_dev_ws`.`bronze`.`raw_traffic`
