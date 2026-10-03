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

# Debe correr DESPUÉS de 3.Carga_driver_career y 3.Carga_season_champions (lee ambas, ya en gold).
driver_career = spark.table(f"`{catalogo}`.`{esquema_gold}`.driver_career")
season_champions = spark.table(f"`{catalogo}`.`{esquema_gold}`.season_champions")

# COMMAND ----------

# Anti-join: pilotos que NUNCA aparecen como campeones en ningún año, pero sí tienen victorias.
w_wins = Window.orderBy(desc("total_wins"))

drivers_without_title_df = (
    driver_career
    .join(season_champions.select("driver_id").distinct(), "driver_id", "left_anti")
    .filter(col("total_wins") > 0)
    .withColumn("rank_by_wins", rank().over(w_wins))
    .select("driver_id", "full_name", "total_wins", "total_points", "rank_by_wins")
    .orderBy(desc("total_wins"))
)

# COMMAND ----------

drivers_without_title_df.display()

# COMMAND ----------

drivers_without_title_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_gold}.drivers_without_title")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_gold}`.drivers_without_title ORDER BY total_wins DESC LIMIT 20").display()