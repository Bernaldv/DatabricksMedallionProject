# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Ambiente (dev | prod)")
dbutils.widgets.text("catalog_base", "catalog_pr_db")
dbutils.widgets.text("esquema_bronze", "bronze")
dbutils.widgets.text("esquema_silver", "silver")

env = dbutils.widgets.get("env").strip().lower()
catalogo = f"{dbutils.widgets.get('catalog_base')}_{env}"
esquema_bronze = dbutils.widgets.get("esquema_bronze")
esquema_silver = dbutils.widgets.get("esquema_silver")

assert env in ("dev", "prod"), f"env inválido: {env}"
print(f"catalogo={catalogo} | bronze={esquema_bronze} | silver={esquema_silver}")

# COMMAND ----------

df_circuits_bronze = spark.table(f"`{catalogo}`.`{esquema_bronze}`.circuits")

# COMMAND ----------

dim_circuits_df = df_circuits_bronze.select(
    col("circuit_id"),
    col("name").alias("circuit_name"),
    col("location"),
    col("country"),
    col("latitude"),
    col("longitude"),
)

# COMMAND ----------

dim_circuits_df.display()

# COMMAND ----------

dim_circuits_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_silver}.dim_circuits")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_silver}`.dim_circuits").display()