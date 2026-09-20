# Databricks notebook source
# MAGIC %md
# MAGIC # Motor de ingesta FarmIA: landing -> bronze
# MAGIC
# MAGIC Este notebook no contiene lógica: solo localiza el paquete `farmia_ingest`,
# MAGIC carga las configuraciones y lanza el motor. Toda la lógica vive en `src/`
# MAGIC y todo lo específico de cada dataset, en `configs/`.

# COMMAND ----------

# MAGIC %md
# MAGIC `confluent-kafka` solo hace falta para los datasets en Avro, que consultan el
# MAGIC esquema en el Schema Registry. Los extras traen `fastavro` y `authlib`, que
# MAGIC el cliente del registry necesita.

# COMMAND ----------

# MAGIC %pip install "confluent-kafka[avro,schemaregistry]"

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import os
import sys

# Raíz del proyecto en el workspace. Se deduce de la ruta de este notebook, que
# está en <proyecto>/notebooks/, para no tener que fijar rutas a mano.
_ruta_notebook = (dbutils.notebook.entry_point.getDbutils()
                  .notebook().getContext().notebookPath().get())
PROJECT_ROOT = "/Workspace" + os.path.dirname(os.path.dirname(_ruta_notebook))

SRC = f"{PROJECT_ROOT}/src"
CONFIGS = f"{PROJECT_ROOT}/configs"

if SRC not in sys.path:
    sys.path.insert(0, SRC)

print("proyecto:", PROJECT_ROOT)

# COMMAND ----------

# MAGIC %md
# MAGIC Recarga automática del paquete: sin esto, al editar un módulo habría que
# MAGIC reiniciar el intérprete de Python para que el notebook viera los cambios.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from farmia_ingest import EngineConfig, ejecutar, load_dataset_configs

engine = EngineConfig.load(f"{CONFIGS}/engine.json")

print("cuenta:   ", engine.storage_account)
print("catalogo: ", engine.catalog)
print("bronze:   ", engine.bronze_root)

# COMMAND ----------

# Permite lanzar un subconjunto desde un job: "farmia.sensors,farmia.weather".
dbutils.widgets.text("datasets", "", "Datasets (vacio = todos)")
seleccion = [d.strip() for d in dbutils.widgets.get("datasets").split(",") if d.strip()]

datasets = load_dataset_configs(f"{CONFIGS}/datasets", solo=seleccion or None)

for d in datasets:
    print(f"  {d.nombre:28s} {d.tipo_origen:6s} {d.source.get('format', '-')}")

# COMMAND ----------

resultados = ejecutar(spark, engine, datasets)

# COMMAND ----------

display(spark.createDataFrame([{
    "dataset": r.datasource + "." + r.dataset,
    "formato": r.source_format,
    "filas": r.rows_ingested,
    "estado": r.status,
    "segundos": round((r.finished_at - r.started_at).total_seconds(), 1),
    "error": r.error_message,
} for r in resultados]))
