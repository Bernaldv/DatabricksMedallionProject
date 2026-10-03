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

df_drivers_bronze = spark.table(f"`{catalogo}`.`{esquema_bronze}`.drivers")

# COMMAND ----------

dim_drivers_df = df_drivers_bronze.select(
    col("driver_id"),
    col("code"),
    col("forename"),
    col("surname"),
    concat_ws(" ", col("forename"), col("surname")).alias("full_name"),
    col("dob").alias("date_of_birth"),
    col("nationality"),
)

# COMMAND ----------

dim_drivers_df.display()

# COMMAND ----------

dim_drivers_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_silver}.dim_drivers")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_silver}`.dim_drivers LIMIT 20").display()