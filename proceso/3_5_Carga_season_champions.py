# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Ambiente (dev | prod)")
dbutils.widgets.text("catalog_base", "catalog_pr_db")
dbutils.widgets.text("esquema_silver", "silver")
dbutils.widgets.text("esquema_gold", "gold")

env = dbutils.widgets.get("env").strip().lower()
catalogo = f"{dbutils.widgets.get('catalog_base')}_{env}"
esquema_silver = dbutils.widgets.get("esquema_silver")
esquema_gold = dbutils.widgets.get("esquema_gold")

assert env in ("dev", "prod"), f"env inválido: {env}"
print(f"catalogo={catalogo} | silver={esquema_silver} | gold={esquema_gold}")

# COMMAND ----------

fact_driver_standings = spark.table(f"`{catalogo}`.`{esquema_silver}`.fact_driver_standings")
dim_drivers = spark.table(f"`{catalogo}`.`{esquema_silver}`.dim_drivers")

# COMMAND ----------

# El campeón de cada temporada es quien queda en position = 1 en la ÚLTIMA ronda de ese año
# (no en cualquier ronda intermedia, que solo refleja una clasificación parcial).
w_ultima_ronda = Window.partitionBy("race_year")

standings_ultima_ronda = (
    fact_driver_standings
    .withColumn("max_round", max("round").over(w_ultima_ronda))
    .filter(col("round") == col("max_round"))
)

# COMMAND ----------

season_champions_df = (
    standings_ultima_ronda
    .filter(col("position") == 1)
    .join(dim_drivers.select("driver_id", "full_name"), "driver_id")
    .select(
        col("race_year"),
        col("driver_id"),
        col("full_name"),
        col("points").alias("champion_points"),
    )
    .orderBy("race_year")
)

# COMMAND ----------

season_champions_df.display()

# COMMAND ----------

season_champions_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_gold}.season_champions")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_gold}`.season_champions ORDER BY race_year DESC").display()