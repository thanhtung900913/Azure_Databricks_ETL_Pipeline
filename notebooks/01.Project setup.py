# Databricks notebook source
# MAGIC %md
# MAGIC ##Get external path

# COMMAND ----------

BRONZE_PATH = spark.sql(" DESCRIBE EXTERNAL LOCATION `bronze` ").select("url").collect()[0][0]
SILVER_PATH = spark.sql(" DESCRIBE EXTERNAL LOCATION `silver` ").select("url").collect()[0][0]
GOLD_PATH = spark.sql(" DESCRIBE EXTERNAL LOCATION `gold` ").select("url").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ##Get environment's name

# COMMAND ----------

dbutils.widgets.text(name = "input", defaultValue="", label="Enter environment in lowercase")
env = dbutils.widgets.get("input")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Bronze Schema

# COMMAND ----------

def create_bronze_schema(environment, path):
  print(f"Using {environment}_catalog")
  spark.sql(f" USE CATALOG '{environment}' ")
  print(f"Creating Bronze Schema in {environment}_catalog")
  spark.sql(f" CREATE SCHEMA IF NOT EXISTS `bronze` MANAGED LOCATION '{path}/bronze' ")
  print("Done!\n =======================================")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Silver Schema

# COMMAND ----------

def create_silver_schema(environment, path):
  print(f"Using {environment}_catalog")
  spark.sql(f" USE CATALOG '{environment}' ")
  print(f"Creating Silver Schema in {environment}_catalog")
  spark.sql(f" CREATE SCHEMA IF NOT EXISTS `silver` MANAGED LOCATION '{path}/silver' ")
  print("Done!\n =======================================")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Gold Schema

# COMMAND ----------

def create_gold_schema(environment, path):
  print(f"Using {environment}_catalog")
  spark.sql(f" USE CATALOG '{environment}' ")
  print(f"Creating Silver Schema in {environment}_catalog")
  spark.sql(f" CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '{path}/gold' ")
  print("Done!\n =======================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Call function

# COMMAND ----------

create_bronze_schema(env, BRONZE_PATH)
create_silver_schema(env, SILVER_PATH)
create_gold_schema(env, GOLD_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create Table in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Raw_Traffic table

# COMMAND ----------

def create_RawTraffic_Table(environment):
    print(f'Creating raw_Traffic table in {environment}_catalog')
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{environment}`.`bronze`.`raw_traffic`
                        (
                            Record_ID INT,
                            Count_point_id INT,
                            Direction_of_travel VARCHAR(255),
                            Year INT,
                            Count_date VARCHAR(255),
                            hour INT,
                            Region_id INT,
                            Region_name VARCHAR(255),
                            Local_authority_name VARCHAR(255),
                            Road_name VARCHAR(255),
                            Road_Category_ID INT,
                            Start_junction_road_name VARCHAR(255),
                            End_junction_road_name VARCHAR(255),
                            Latitude DOUBLE,
                            Longitude DOUBLE,
                            Link_length_km DOUBLE,
                            Pedal_cycles INT,
                            Two_wheeled_motor_vehicles INT,
                            Cars_and_taxis INT,
                            Buses_and_coaches INT,
                            LGV_Type INT,
                            HGV_Type INT,
                            EV_Car INT,
                            EV_Bike INT,
                            Extract_Time TIMESTAMP
                    );""")
    
    print("Done!\n =======================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Raw_Road Table

# COMMAND ----------

def create_RawRoad_Table(environment):
    print(f"Creating raw_road table in {environment}_catalog")
    spark.sql(f""" CREATE TABLE IF NOT EXISTS `{environment}`.`bronze`.`raw_road`
              (
                Road_ID INT,
                Road_Category_Id INT,
                Road_Category VARCHAR(255),
                Region_ID INT,
                Region_Name VARCHAR(255),
                Total_Link_Length_Km DOUBLE,
                Total_Link_Length_Miles DOUBLE,
                All_Motor_Vehicles DOUBLE
              );
              """)
    print("Done!\n =======================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Call function

# COMMAND ----------

create_RawRoad_Table(env)
create_RawTraffic_Table(env)
