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

# Fuente: bronze.results (crudo). Apoyo: silver.dim_races y silver.dim_status ya transformadas antes
# -> por eso este script debe correr DESPUÉS de 2.Transformacion_dim_races y 2.Transformacion_dim_status.
df_results_bronze = spark.table(f"`{catalogo}`.`{esquema_bronze}`.results")
dim_races = spark.table(f"`{catalogo}`.`{esquema_silver}`.dim_races")
dim_status = spark.table(f"`{catalogo}`.`{esquema_silver}`.dim_status")

# COMMAND ----------

# bronze.results trae varias columnas numéricas como STRING por los \N (DNF, sin vuelta rápida, etc.)
# Las limpiamos y casteamos aquí, que es donde corresponde (no en bronze).
df_results_clean = (
    df_results_bronze
    .withColumn("position_order", col("position_order").cast("int"))
    .withColumn("points", col("points").cast("double"))
    .withColumn("laps", col("laps").cast("int"))
)

# COMMAND ----------

fact_results_df = (
    df_results_clean
    .join(dim_races.select("race_id", "race_year", "circuit_id"), "race_id", "left")
    .join(dim_status, "status_id", "left")
    .withColumn(
        "finished",
        when(col("status_description") == "Finished", True)
        .when(col("status_description").rlike("^\\+\\d+ Lap"), True)
        .otherwise(False),
    )
    .withColumn("is_podium", col("position_order").isin(1, 2, 3))
    .withColumn("is_win", col("position_order") == 1)
    .select(
        col("result_id"),
        col("race_id"),
        col("driver_id"),
        col("constructor_id"),
        col("grid").cast("int"),
        col("position_order").alias("finish_position"),
        col("points"),
        col("laps"),
        col("status_id"),
        col("race_year"),
        col("circuit_id"),
        col("finished"),
        col("is_podium"),
        col("is_win"),
    )
)

# COMMAND ----------

fact_results_df.display()

# COMMAND ----------

fact_results_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_silver}.fact_results")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_silver}`.fact_results LIMIT 20").display()