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

# NOTA: no tenemos la tabla oficial constructor_standings de Ergast, así que aproximamos la
# clasificación de constructores sumando los puntos de sus pilotos por temporada. Coincide con
# el campeonato real en casi todos los años (desde 1958), con alguna excepción histórica puntual
# (ej. reglas especiales de descarte de resultados en los años 50-60).
fact_results = spark.table(f"`{catalogo}`.`{esquema_silver}`.fact_results")
dim_constructors = spark.table(f"`{catalogo}`.`{esquema_silver}`.dim_constructors")

# COMMAND ----------

w_rank = Window.partitionBy("race_year").orderBy(desc("total_points"))

constructor_standings_df = (
    fact_results
    .join(dim_constructors, "constructor_id")
    .groupBy("race_year", "constructor_id", "constructor_name")
    .agg(
        sum("points").alias("total_points"),
        sum(col("is_win").cast("int")).cast("int").alias("wins"),        # <- el .cast("int") extra
        sum(col("is_podium").cast("int")).cast("int").alias("podiums"),  # <- el .cast("int") extra
    )
    .withColumn("season_rank", dense_rank().over(w_rank))
    .orderBy("race_year", "season_rank")
)

# COMMAND ----------

constructor_standings_df.display()

# COMMAND ----------

constructor_standings_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_gold}.constructor_standings")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_gold}`.constructor_standings WHERE season_rank = 1 ORDER BY race_year DESC").display()