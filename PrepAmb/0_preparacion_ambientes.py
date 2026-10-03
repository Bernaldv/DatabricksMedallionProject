# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 0. Preparación de ambiente
# MAGIC Crea external locations, catálogo y schemas del ambiente indicado por parámetros.
# MAGIC El mismo notebook corre en dev y en prod; solo cambian los parámetros que envía el workflow.
# MAGIC
# MAGIC Es idempotente (`IF NOT EXISTS`) y **no borra nada**. Para eliminar usar `reversion/drop_ambiente.py`.

# COMMAND ----------

# No usar dbutils.widgets.removeAll(): dentro de un job borraría los parámetros que envía el workflow.
dbutils.widgets.text("env", "dev", "Ambiente (dev | prod)")
dbutils.widgets.text("storage_name", "adlsdvbtdev01", "Storage account")
dbutils.widgets.text("credential_name", "credential_dev", "Storage credential")
dbutils.widgets.text("catalog_base", "catalog_pr_db", "Prefijo del catálogo")

env = dbutils.widgets.get("env").strip().lower()
storage = dbutils.widgets.get("storage_name").strip()
credential = dbutils.widgets.get("credential_name").strip()
catalog = f"{dbutils.widgets.get('catalog_base').strip()}_{env}"

# Validaciones: el metastore es compartido, así que evitamos que dev apunte a recursos de prod (y viceversa).
assert env in ("dev", "prod"), f"env inválido: {env}"
assert storage, "storage_name es obligatorio"
assert credential.endswith(f"_{env}"), f"El credential '{credential}' no corresponde al ambiente '{env}'"

print(f"env={env} | storage={storage} | credential={credential} | catalog={catalog}")

# COMMAND ----------

# External locations: una por contenedor. El nombre lleva el ambiente porque son objetos globales del metastore.
for layer in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-{env}-{layer}`
        URL 'abfss://{layer}@{storage}.dfs.core.windows.net/'
        WITH (STORAGE CREDENTIAL `{credential}`)
        COMMENT 'Ubicación externa {layer} ({env})'
    """)
    print(f"OK exlt-{env}-{layer}")

# COMMAND ----------

# Catálogo por ambiente + schemas con MANAGED LOCATION en el Data Lake de ese ambiente.
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}` COMMENT 'Catálogo del ambiente {env}'")

for layer in ["bronze", "silver", "gold"]:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{layer}`
        MANAGED LOCATION 'abfss://{layer}@{storage}.dfs.core.windows.net/'
    """)
    print(f"OK {catalog}.{layer}")

# COMMAND ----------

display(spark.sql("SHOW EXTERNAL LOCATIONS"))
display(spark.sql(f"SHOW SCHEMAS IN `{catalog}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Creacion de Tablas Bronze**

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.bronze.circuits")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.bronze.circuits (
circuit_id integer,
circuit_ref string,
name string,
location string,
country string,
latitude double,
longitude double,
altitude integer,
ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/circuits'
""")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.bronze.races")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.bronze.races (
race_id integer,
race_year integer,
round integer,
circuit_id integer,
name string,
date_race date,
ingestion_date timestamp
)
USING DELTA
PARTITIONED BY (race_year)
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/races'
""")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.bronze.constructors")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.bronze.constructors (
  constructor_id integer,
  constructor_ref string,
  name string,
  nationality string,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/constructors'
""")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.bronze.drivers")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.bronze.drivers (
  driver_id integer,
  driverRef string,
  number_driver integer,
  code string,
  forename string,
  surname string,
  dob date,
  nationality string,
  ingestion_date timestamp
)
USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/drivers'
""")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.bronze.results")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.bronze.results (
        result_id         INT,
        race_id            INT,
        driver_id          INT,
        constructor_id     INT,
        number_results     STRING,   -- puede venir \N
        grid               INT,
        position           STRING,   -- \N si no clasificó (DNF)
        position_text      STRING,   -- "R", "D", "W", o el número como texto
        position_order     INT,
        points             DOUBLE,
        laps               INT,
        `time`             STRING,   -- formato "1:34:50.616", "+13.411" o \N
        milliseconds       STRING,   -- puede venir \N
        fastest_lap        STRING,   -- número de vuelta, puede venir \N
        `rank`             STRING,   -- puede venir \N
        fastest_lap_time   STRING,   -- formato "1:27.452" o \N
        fastest_lap_speed  STRING,   -- puede venir \N
        status_id          INT,
        ingestion_date     TIMESTAMP
    )
    USING DELTA
    LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/results'
""")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.bronze.status")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.bronze.status (
        statusId         INT,
        status           STRING,
        ingestion_date     TIMESTAMP
    )
    USING DELTA
    LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/status'
""")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.bronze.driver_standings")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.bronze.driver_standings (
        driverStandingsId   INT,
        raceId              INT,
        driverId            INT,
        points              double,
        position            INT,
        positionText        STRING,
        wins                INT,
        ingestion_date     TIMESTAMP
    )
    USING DELTA    
    LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/driver_standings'
""")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.dim_circuits")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.silver.dim_circuits (
        circuit_id     INT,
        circuit_name   STRING,
        location       STRING,
        country        STRING,
        latitude       DOUBLE,
        longitude      DOUBLE
    )
    USING DELTA
    LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/dim_circuits'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.dim_drivers")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.silver.dim_drivers (
        driver_id       INT,
        code            STRING,
        forename        STRING,
        surname         STRING,
        full_name       STRING,
        date_of_birth   DATE,
        nationality     STRING
    )
    USING DELTA
    LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/dim_drivers'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.dim_constructors")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.silver.dim_constructors (
        constructor_id   INT,
        constructor_name STRING,
        nationality      STRING
    )
    USING DELTA
    LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/dim_constructors'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.dim_status")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.silver.dim_status (
        status_id          INT,
        status_description STRING
    )
    USING DELTA
    LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/dim_status'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.dim_races")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.silver.dim_races (
        race_id      INT,
        race_year    INT,
        round        INT,
        circuit_id   INT,
        race_name    STRING,
        race_date    DATE
    )
    USING DELTA
    PARTITIONED BY (race_year)
    LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/dim_races'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.fact_results")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.silver.fact_results (
        result_id        INT,
        race_id          INT,
        driver_id        INT,
        constructor_id   INT,
        grid             INT,
        finish_position  INT,
        points           DOUBLE,
        laps             INT,
        status_id        INT,
        race_year        INT,
        circuit_id       INT,
        finished         BOOLEAN,
        is_podium        BOOLEAN,
        is_win           BOOLEAN
    )
    USING DELTA
    PARTITIONED BY (race_year)
    LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/fact_results'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.fact_driver_standings")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.silver.fact_driver_standings (
        driver_standings_id  INT,
        race_id              INT,
        driver_id            INT,
        points               DOUBLE,
        position             INT,
        wins                 INT,
        race_year            INT,
        round                INT
    )
    USING DELTA
    PARTITIONED BY (race_year)
    LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/fact_driver_standings'
""")


# COMMAND ----------

# MAGIC %md
# MAGIC # creacion tablas gold
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.driver_career")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.gold.driver_career (
        driver_id       INT,
        full_name       STRING,
        nationality     STRING,
        total_points    DOUBLE,
        total_wins      INT,
        total_podiums   INT,
        races_disputed  INT,
        first_season    INT,
        last_season     INT
    )
    USING DELTA
    LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/driver_career'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.constructor_career")

# COMMAND ----------


spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.gold.constructor_career (
        constructor_id   INT,
        constructor_name STRING,
        nationality      STRING,
        total_points     DOUBLE,
        total_wins       INT,
        total_podiums    INT,
        first_season     INT,
        last_season      INT
    )
    USING DELTA
    LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/constructor_career'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.season_champions")

# COMMAND ----------


spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.gold.season_champions (
        race_year        INT,
        driver_id         INT,
        full_name         STRING,
        champion_points   DOUBLE
    )
    USING DELTA
    LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/season_champions'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.drivers_without_title")

# COMMAND ----------

 
spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.gold.drivers_without_title (
        driver_id       INT,
        full_name       STRING,
        total_wins      INT,
        total_points    DOUBLE,
        rank_by_wins    INT
    )
    USING DELTA
    LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/drivers_without_title'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.constructor_standings")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.gold.constructor_standings (
        race_year         INT,
        constructor_id    INT,
        constructor_name  STRING,
        total_points      DOUBLE,
        wins              INT,
        podiums           INT,
        season_rank       INT
    )
    USING DELTA
    PARTITIONED BY (race_year)
    LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/constructor_standings'
""")


# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.silver.constructor_dominance")

# COMMAND ----------

spark.sql(rf"""
    CREATE TABLE IF NOT EXISTS `{catalog}`.gold.constructor_dominance (
        constructor_id      INT,
        constructor_name    STRING,
        streak_start_year   INT,
        streak_end_year     INT,
        streak_length       INT
    )
    USING DELTA
    LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/constructor_dominance'
""")
