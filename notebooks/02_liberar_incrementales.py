# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Liberar los ficheros incrementales
# MAGIC
# MAGIC Los notebooks `00` y `01` dejan una segunda tanda de ficheros en
# MAGIC `_incremental_staging`, fuera de las rutas que vigila el Autoloader.
# MAGIC Este notebook los copia a landing para simular la llegada de datos nuevos.
# MAGIC
# MAGIC Sirve para demostrar que la ingesta es **incremental**: al volver a ejecutar el
# MAGIC motor después de esta copia, solo se procesan los ficheros recién llegados y
# MAGIC ninguno de los anteriores se reprocesa.
# MAGIC
# MAGIC Material de apoyo: no forma parte del motor de ingesta.

# COMMAND ----------

STORAGE = "masterob001sta"
LANDING = f"abfss://landing@{STORAGE}.dfs.core.windows.net/farmia"
STAGING = f"{LANDING}/_incremental_staging"

# field_images no tiene incremental: sirve para comprobar que un dataset sin
# ficheros nuevos reporta cero filas en lugar de reprocesar lo que ya ingirió.
DATASETS_INCREMENTALES = [
    "product_catalog",
    "inventory",
    "orders_cdc",
    "sensors",
    "app_events",
    "shipments",
    "weather",
]

# COMMAND ----------

for dataset in DATASETS_INCREMENTALES:
    dbutils.fs.cp(f"{STAGING}/{dataset}", f"{LANDING}/{dataset}", recurse=True)
    print(f"{dataset} liberado")

# COMMAND ----------

# MAGIC %md
# MAGIC Filas esperadas en la siguiente ejecución del motor, para esta tanda concreta.
# MAGIC El ejemplo del README corresponde a una carga posterior, con otras cifras.
# MAGIC
# MAGIC | Dataset | Filas nuevas |
# MAGIC |---|---|
# MAGIC | product_catalog | 40 |
# MAGIC | inventory | 270 |
# MAGIC | orders_cdc | 120 |
# MAGIC | sensors | 180 |
# MAGIC | app_events | 400 |
# MAGIC | shipments | 150 |
# MAGIC | weather | 64 |
# MAGIC | field_images | 0 |
