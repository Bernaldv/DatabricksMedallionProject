# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import *

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

fact_results = spark.table(f"`{catalogo}`.`{esquema_silver}`.fact_results")
dim_drivers = spark.table(f"`{catalogo}`.`{esquema_silver}`.dim_drivers")

# COMMAND ----------

driver_career_df = (
    fact_results
    .join(dim_drivers, "driver_id")
    .groupBy("driver_id", "full_name", "nationality")
    .agg(
        sum("points").alias("total_points"),
        sum(col("is_win").cast("int")).cast("int").alias("total_wins"),
        sum(col("is_podium").cast("int")).cast("int").alias("total_podiums"),
        countDistinct("race_id").cast("int").alias("races_disputed"),
        min("race_year").alias("first_season"),
        max("race_year").alias("last_season"),
    )
    .orderBy(desc("total_points"))
)


# COMMAND ----------

driver_career_df.display()

# COMMAND ----------

driver_career_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_gold}.driver_career")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_gold}`.driver_career ORDER BY total_points DESC LIMIT 20").display()