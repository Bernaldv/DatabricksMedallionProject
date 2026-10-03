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

# Debe correr DESPUÉS de 2.Transformacion_dim_races (usa esa tabla ya en silver).
df_standings_bronze = spark.table(f"`{catalogo}`.`{esquema_bronze}`.driver_standings")
dim_races = spark.table(f"`{catalogo}`.`{esquema_silver}`.dim_races")

# COMMAND ----------

fact_driver_standings_df = (
    df_standings_bronze
    .join(dim_races.select("race_id", "race_year", "round"), 
      df_standings_bronze["raceId"] == dim_races["race_id"], "left")
    .select(
        col("driverStandingsId").alias("driver_standings_id"),
        col("race_id"),
        col("driverId").alias("driver_id"),
        col("points").cast("double"),
        col("position").cast("int"),
        col("wins").cast("int"),
        col("race_year"),
        col("round"),
    )
)



# COMMAND ----------

fact_driver_standings_df.display()

# COMMAND ----------

fact_driver_standings_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_silver}.fact_driver_standings")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_silver}`.fact_driver_standings LIMIT 20").display()