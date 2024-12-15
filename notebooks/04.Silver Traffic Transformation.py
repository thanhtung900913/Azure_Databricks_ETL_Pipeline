# Databricks notebook source
# MAGIC %run "/Workspace/Users/thanhtung29062004@outlook.com/Common"

# COMMAND ----------

dbutils.widgets.text(name="input", defaultValue="", label="Enter environment in lower case")
env = dbutils.widgets.get("input")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read data from raw_traffic table

# COMMAND ----------

def read_data(environment):
    print("Reading data from raw_traffic table")
    df = (spark.readStream
          .table(f"{environment}.`bronze`.`raw_traffic`")
          )
    print("Reading data from raw_traffic table successfully")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add new colums

# COMMAND ----------

# MAGIC %md
# MAGIC ####Electric vehicle

# COMMAND ----------

from pyspark.sql.functions import col
def get_Count_Electric_Vehicles(df):
    print("Counting Electric Vehicles")
    df = (df.withColumn("Electric_Vehicles_Count", col('EV_Car') + col('EV_Bike')))
    print("Counting Electric Vehicles successfully")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####Motor vehicle

# COMMAND ----------

from pyspark.sql.functions import col
def get_Count_Motor_Vehicles(df):
    print("Counting Motor Vehicles")
    df = (df.withColumn("Count_Motor_Vehicles",
                        col('Electric_Vehicles_Count') + col('Two_wheeled_motor_vehicles') + col('Cars_and_taxis') + col('Buses_and_coaches') + col('LGV_Type') + col('HGV_Type')))
    print("Counting Motor Vehicles successfully")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transform time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_timestamp(df):
    print("Adding timestamp")
    df = df.withColumn("Transformed_Time", current_timestamp())
    print("Adding timestamp successfully")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write data to silver traffic table

# COMMAND ----------

def write_Traffic_SilverTable(StreamingDF, environment):
    print("Writing to Silver Table")
    write_Stream = (StreamingDF.writeStream
                    .format("delta")
                    .option("checkpointLocation", f"{checkpoint}/SilverTraffic_Load/checkpoints/")
                    .queryName("silver_traffic")
                    .trigger(availableNow = True)
                    .outputMode("append")
                    .table(f"{environment}.`silver`.`silver_traffic`")
    )
    write_Stream.awaitTermination()
    print("Writing to Silver Table successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calling function

# COMMAND ----------

# Read data
df = read_data(env)

# Remove duplicates and handling nulls
df_dup = remove_dup(df)
df_null = handle_null(df, df.columns)

# Add new columns
df_ev = get_Count_Electric_Vehicles(df_null)
df_motor = get_Count_Motor_Vehicles(df_ev)
df_final = add_timestamp(df_motor)

#Write data to silver traffic table
write_Traffic_SilverTable(df_final, env)

# COMMAND ----------


df = (spark.sql(f"select * from `{env}`.`silver`.`silver_traffic`"))
display(df)
