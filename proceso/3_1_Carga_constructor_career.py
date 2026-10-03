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
dim_constructors = spark.table(f"`{catalogo}`.`{esquema_silver}`.dim_constructors")
 

# COMMAND ----------

constructor_career_df = (
    fact_results
    .join(dim_constructors, "constructor_id")
    .groupBy("constructor_id", "constructor_name", "nationality")
    .agg(
        sum("points").alias("total_points"),
        sum(col("is_win").cast("int")).cast("int").alias("total_wins"),
        sum(col("is_podium").cast("int")).cast("int").alias("total_podiums"),
        min("race_year").alias("first_season"),
        max("race_year").alias("last_season"),
    )
    .orderBy(desc("total_points"))
)


# COMMAND ----------

constructor_career_df.display()

# COMMAND ----------

constructor_career_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_gold}.constructor_career")

# COMMAND ----------

spark.sql(f"SELECT * FROM `{catalogo}`.`{esquema_gold}`.constructor_career ORDER BY total_points DESC LIMIT 20").display()