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
dbutils.widgets.text("esquema_gold", "gold")

env = dbutils.widgets.get("env").strip().lower()
catalogo = f"{dbutils.widgets.get('catalog_base')}_{env}"
esquema_gold = dbutils.widgets.get("esquema_gold")

assert env in ("dev", "prod"), f"env inválido: {env}"
print(f"catalogo={catalogo} | gold={esquema_gold}")

# COMMAND ----------

# Debe correr DESPUÉS de 3.Carga_constructor_standings (lee esa tabla, ya en gold).
constructor_standings = spark.table(f"`{catalogo}`.`{esquema_gold}`.constructor_standings")

# COMMAND ----------

# "Dominante" = terminó primero en la clasificación de constructores de ese año (season_rank = 1).
lideres = constructor_standings.filter(col("season_rank") == 1).select(
    "constructor_id", "constructor_name", "race_year"
)

# COMMAND ----------

# Técnica de rachas con funciones de ventana: si restas al año un número de fila secuencial
# (particionado por constructor, ordenado por año), los años consecutivos de un mismo equipo
# obtienen el mismo resultado -> eso agrupa la racha.
w_equipo = Window.partitionBy("constructor_id").orderBy("race_year")

lideres_con_grupo = lideres.withColumn(
    "grupo_racha", col("race_year") - row_number().over(w_equipo)
)

# COMMAND ----------

constructor_dominance_df = (
    lideres_con_grupo
    .groupBy("constructor_id", "constructor_name", "grupo_racha")
    .agg(
        min("race_year").alias("streak_start_year"),
        max("race_year").alias("streak_end_year"),
        count("*").cast("int").alias("streak_length"),
    )
    .filter(col("streak_length") >= 2)  # solo nos interesan rachas de 2+ años consecutivos
    .drop("grupo_racha")
    .orderBy(desc("streak_length"))
)


# COMMAND ----------

constructor_dominance_df.display()

# COMMAND ----------

constructor_dominance_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_gold}.constructor_dominance")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_gold}`.constructor_dominance ORDER BY streak_length DESC").display()