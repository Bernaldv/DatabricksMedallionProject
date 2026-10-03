# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Ambiente (dev | prod)")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalog_base", "catalog_pr_db")
dbutils.widgets.text("esquema", "bronze")
dbutils.widgets.text("storageName", "adlsdvbtdev01")


env = dbutils.widgets.get("env").strip().lower()
container = dbutils.widgets.get("container")
catalogo = f"{dbutils.widgets.get('catalog_base')}_{env}"
esquema = dbutils.widgets.get("esquema")
storageName = dbutils.widgets.get("storageName")
 
assert env in ("dev", "prod"), f"env inválido: {env}"
 
ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/circuits.csv"
print(f"env={env} | catalogo={catalogo} | ruta={ruta}")


# COMMAND ----------

df_circuits = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)

# COMMAND ----------

df_circuits.display()

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_schema

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_circuits_final = spark.read\
.option('header', True)\
.schema(circuits_schema)\
.csv(ruta)

# COMMAND ----------

# DBTITLE 1,select only specific cols
circuits_selected_df = df_circuits_final.select(col("circuitId"), 
                                                col("circuitRef"), 
                                                col("name"), col("location"), 
                                                col("country"), 
                                                col("lat"), 
                                                col("lng"), 
                                                col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                            .withColumnRenamed("circuitRef", "circuit_ref") \
                                            .withColumnRenamed("lat", "latitude") \
                                            .withColumnRenamed("lng", "longitude") \
                                            .withColumnRenamed("alt", "altitude") 

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df.display()

# COMMAND ----------

print(f"catalogo = {catalogo}")
print(f"esquema = {esquema}")

# COMMAND ----------

spark.sql(f"USE CATALOG `{catalogo}`")
spark.sql(f"USE SCHEMA `{esquema}`")

# COMMAND ----------

print(f"current_catalog = {spark.sql('SELECT current_catalog()').collect()[0][0]}")
print(f"current_schema = {spark.sql('SELECT current_schema()').collect()[0][0]}")


# COMMAND ----------

circuits_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.circuits")