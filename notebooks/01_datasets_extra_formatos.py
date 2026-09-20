# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Datasets extra: CSV, Avro e imágenes
# MAGIC
# MAGIC Los datasets base cubren **JSON** y **Parquet**. Este notebook añade las tres fuentes
# MAGIC que faltan para ejercitar los cinco formatos que soporta el motor, y con ellas quedan
# MAGIC representadas las seis fuentes de datos de FarmIA:
# MAGIC
# MAGIC | Fuente | Dataset | Formato |
# MAGIC |---|---|---|
# MAGIC | Ventas online | `orders_cdc` | JSON |
# MAGIC | Inventario local | `inventory`, `product_catalog` | Parquet |
# MAGIC | Sensores IoT | `sensors` (+ Kafka) | JSON / Avro |
# MAGIC | Eventos de clientes | `app_events` (+ Kafka) | JSON |
# MAGIC | **Proveedores y logística** | **`shipments`** | **CSV** |
# MAGIC | **Meteorología externa** | **`weather`** | **Avro** |
# MAGIC | *(extra)* Fotos de cultivo | **`field_images`** | **imágenes (binaryFile)** |

# COMMAND ----------

STORAGE = "masterob001sta"
LANDING_ROOT = f"abfss://landing@{STORAGE}.dfs.core.windows.net/farmia"
INCREMENTAL_STAGING_ROOT = f"{LANDING_ROOT}/_incremental_staging"

CATALOG = spark.catalog.currentCatalog()
OPS = f"{CATALOG}.farmia_ops"

from pyspark.sql import Row
from datetime import datetime, timedelta
import random

random.seed(7)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Proveedores y logística: CSV
# MAGIC CSV con cabecera. Se deja alguna fila con campos vacíos a propósito: sirve para demostrar en
# MAGIC la memoria cómo se comporta el motor con `nullValue` y con el esquema declarado.

# COMMAND ----------

SUPPLIERS = ["SUP-001", "SUP-002", "SUP-003", "SUP-004"]
CARRIERS = ["CARRIER-01", "CARRIER-02", "CARRIER-03", "CARRIER-04"]

def make_shipments(start_idx, n, base_day):
    rows = []
    for i in range(start_idx, start_idx + n):
        dispatch = base_day + timedelta(hours=(i % 48))
        delivered = dispatch + timedelta(hours=random.randint(12, 96))
        rows.append(Row(
            shipment_id=f"SHP-{i:06d}",
            supplier_id=random.choice(SUPPLIERS),
            carrier=random.choice(CARRIERS),
            warehouse_id=random.choice(["MAD-01", "SEV-01", "VAL-01"]),
            sku=f"SKU-{(i % 120) + 1:05d}",
            units=int(50 + (i % 200)),
            dispatch_ts=dispatch,
            delivered_ts=None if i % 11 == 0 else delivered,   # envíos aún en tránsito
            transit_hours=None if i % 11 == 0 else float(round((delivered - dispatch).total_seconds() / 3600, 2)),
            status="IN_TRANSIT" if i % 11 == 0 else "DELIVERED",
        ))
    return rows

(spark.createDataFrame(make_shipments(1, 240, datetime(2026, 4, 1, 5, 0, 0)))
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{LANDING_ROOT}/shipments"))

(spark.createDataFrame(make_shipments(241, 150, datetime(2026, 4, 3, 5, 0, 0)))
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{INCREMENTAL_STAGING_ROOT}/shipments"))

print("shipments CSV")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Meteorología externa: Avro
# MAGIC Fichero Avro "plano" (no Kafka): es el que llega del proveedor externo de datos meteorológicos.
# MAGIC Ojo: esto es Avro **de fichero**, sin el *wire format* de Confluent (los 5 bytes de cabecera
# MAGIC con el id de esquema) que sí llevan los mensajes de Kafka.

# COMMAND ----------

def make_weather(days, start_day):
    rows = []
    for d in range(days):
        day = start_day + timedelta(days=d)
        for station in ["EST-NORTE", "EST-SUR", "EST-ESTE", "EST-OESTE"]:
            for hour in range(0, 24, 3):
                rows.append(Row(
                    station_id=station,
                    field_id=station.replace("EST-", "FIELD-"),
                    observation_ts=day + timedelta(hours=hour),
                    temperature_c=float(round(random.uniform(4, 34), 1)),
                    humidity_pct=float(round(random.uniform(25, 95), 1)),
                    rainfall_mm=float(round(random.choice([0, 0, 0, 0.2, 1.5, 6.0]), 2)),
                    wind_speed_kmh=float(round(random.uniform(0, 45), 1)),
                    provider="WEATHER-PROVIDER-01",
                ))
    return rows

(spark.createDataFrame(make_weather(4, datetime(2026, 4, 1, 0, 0, 0)))
    .coalesce(1)
    .write.mode("overwrite")
    .format("avro")
    .save(f"{LANDING_ROOT}/weather"))

(spark.createDataFrame(make_weather(2, datetime(2026, 4, 5, 0, 0, 0)))
    .coalesce(1)
    .write.mode("overwrite")
    .format("avro")
    .save(f"{INCREMENTAL_STAGING_ROOT}/weather"))

print("weather Avro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fotos de cultivo: imágenes
# MAGIC Para escribir **ficheros binarios** en ADLS desde serverless lo más limpio es un
# MAGIC *external volume* de Unity Catalog apuntando a la carpeta de landing: el volumen se monta en
# MAGIC `/Volumes/...` y se puede usar `open(..., "wb")` de Python normal.
# MAGIC
# MAGIC Las imágenes se generan como PNG válidos de color plano con la librería estándar (`zlib`),
# MAGIC sin dependencias externas.

# COMMAND ----------

IMAGES_SUBPATH = "field_images"
VOLUME = f"{CATALOG}.farmia_ops.landing_images"

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS {VOLUME}
LOCATION '{LANDING_ROOT}/{IMAGES_SUBPATH}'
""")

VOLUME_PATH = f"/Volumes/{CATALOG}/farmia_ops/landing_images"
print("volumen listo:", VOLUME_PATH)

# COMMAND ----------

import os
import zlib
import struct

def png_bytes(width: int, height: int, rgb: tuple[int, int, int]) -> bytes:
    """PNG de color plano, generado a mano para no depender de Pillow."""
    raw = b"".join(b"\x00" + bytes(rgb) * width for _ in range(height))

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF))

    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw))
            + chunk(b"IEND", b""))

FIELDS = ["FIELD-NORTE", "FIELD-SUR", "FIELD-ESTE", "FIELD-OESTE"]

def write_images(n, start_idx, day):
    written = 0
    for i in range(start_idx, start_idx + n):
        field = FIELDS[i % len(FIELDS)]
        # particionado por campo y fecha: el motor lo puede aprovechar con partition columns
        folder = f"{VOLUME_PATH}/{field}/{day.strftime('%Y-%m-%d')}"
        os.makedirs(folder, exist_ok=True)
        color = (random.randint(20, 90), random.randint(90, 200), random.randint(20, 90))  # verdes
        with open(f"{folder}/crop_{i:04d}.png", "wb") as fh:
            fh.write(png_bytes(64, 64, color))
        written += 1
    return written

total = 0
total += write_images(24, 1, datetime(2026, 4, 2))
total += write_images(16, 25, datetime(2026, 4, 3))
print(f"imagenes escritas: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

print("--- shipments (CSV) ---")
display(spark.read.option("header", "true").csv(f"{LANDING_ROOT}/shipments").limit(5))

# COMMAND ----------

print("--- weather (Avro) ---")
display(spark.read.format("avro").load(f"{LANDING_ROOT}/weather").limit(5))

# COMMAND ----------

print("--- field_images (binaryFile) ---")
display(
    spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.png")
        .option("recursiveFileLookup", "true")
        .load(f"{LANDING_ROOT}/{IMAGES_SUBPATH}")
        .selectExpr("path", "modificationTime", "length")
        .limit(5)
)

# COMMAND ----------

display(dbutils.fs.ls(LANDING_ROOT))
