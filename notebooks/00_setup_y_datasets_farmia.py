# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup FarmIA + datasets base en landing
# MAGIC
# MAGIC Prepara el lakehouse y genera los datos de prueba.
# MAGIC
# MAGIC Crea los esquemas del medallion y la tabla de auditoría, y deja en landing:
# MAGIC `product_catalog` (parquet), `inventory` (parquet), `orders_cdc` (json),
# MAGIC `sensors` (json) y `app_events` (json), más una segunda tanda en staging.
# MAGIC
# MAGIC Está escrito para **cómputo Serverless con external locations de Unity Catalog**:
# MAGIC no hay cluster clásico, así que las rutas `abfss://` van explícitas en lugar de
# MAGIC resolverse con la spark conf `adls.account.name`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

STORAGE = "masterob001sta"

LANDING_CONTAINER = "landing"
LAKEHOUSE_CONTAINER = "lakehouse"

LANDING_ROOT = f"abfss://{LANDING_CONTAINER}@{STORAGE}.dfs.core.windows.net/farmia"
LAKEHOUSE_ROOT = f"abfss://{LAKEHOUSE_CONTAINER}@{STORAGE}.dfs.core.windows.net"
CHECKPOINT_ROOT = f"{LAKEHOUSE_ROOT}/_checkpoints"

INCREMENTAL_STAGING_ROOT = f"{LANDING_ROOT}/_incremental_staging"
WRITE_INCREMENTALS_TO_STAGING = True

CATALOG = spark.catalog.currentCatalog()

BRONZE = f"{CATALOG}.farmia_bronze"
SILVER = f"{CATALOG}.farmia_silver"
GOLD = f"{CATALOG}.farmia_gold"
OPS = f"{CATALOG}.farmia_ops"

print({"catalog": CATALOG, "landing_root": LANDING_ROOT, "lakehouse_root": LAKEHOUSE_ROOT})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprobación previa: ¿escribe y lee el serverless en ADLS?
# MAGIC Si esta celda falla, el problema está en las external locations / storage credential,
# MAGIC no en el resto del notebook.

# COMMAND ----------

_probe = f"{LANDING_ROOT}/_probe"
spark.range(1).write.mode("overwrite").format("delta").save(_probe)
print("escritura OK:", spark.read.format("delta").load(_probe).count())
dbutils.fs.rm(_probe, True)
print("borrado OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Esquemas y tablas de operación

# COMMAND ----------

for stmt in [
    f"CREATE SCHEMA IF NOT EXISTS {BRONZE}",
    f"CREATE SCHEMA IF NOT EXISTS {SILVER}",
    f"CREATE SCHEMA IF NOT EXISTS {GOLD}",
    f"CREATE SCHEMA IF NOT EXISTS {OPS}",
]:
    spark.sql(stmt)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OPS}.ingestion_audit (
  run_id STRING,
  datasource STRING,
  dataset STRING,
  source_format STRING,
  sink_path STRING,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  rows_ingested BIGINT,
  status STRING,
  error_message STRING
) USING DELTA
""")

print("esquemas y tabla de auditoria creados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datasets base

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime, timedelta
import random

random.seed(42)

# COMMAND ----------

# 1) Catálogo de productos: PARQUET
products = []
for i in range(1, 121):
    products.append(Row(
        product_id=f"P{i:04d}",
        sku=f"SKU-{i:05d}",
        product_name=f"Producto agricola {i}",
        category=random.choice(["Semillas", "Fertilizantes", "Riego", "Herramientas", "Proteccion de cultivos"]),
        brand=random.choice(["AgroMax", "VerdePlus", "CampoTech", "BioFarm", "TerraNova"]),
        unit_of_measure=random.choice(["kg", "l", "ud", "saco", "pack"]),
        price_eur=float(round(random.uniform(3.5, 120.0), 2)),
        is_active=True,
        effective_ts=datetime(2026, 3, 25, 0, 0, 0),
        extract_ts=datetime(2026, 3, 25, 2, 0, 0),
    ))
spark.createDataFrame(products).coalesce(1).write.mode("overwrite").parquet(f"{LANDING_ROOT}/product_catalog")

# COMMAND ----------

# 2) Inventario: PARQUET
inventory = []
for day in [datetime(2026, 4, 1, 6, 0, 0), datetime(2026, 4, 2, 6, 0, 0)]:
    for wh in ["MAD-01", "SEV-01", "VAL-01"]:
        for i in range(1, 81):
            inventory.append(Row(
                warehouse_id=wh,
                sku=f"SKU-{i:05d}",
                snapshot_ts=day,
                extract_ts=day + timedelta(minutes=15),
                available_units=int(100 + (i % 35)),
                reserved_units=int(10 + (i % 12)),
            ))
spark.createDataFrame(inventory).coalesce(1).write.mode("overwrite").parquet(f"{LANDING_ROOT}/inventory")

# COMMAND ----------

# 3) Pedidos (CDC de la tienda online): JSON
orders = []
channels = ["web", "mobile_app", "marketplace"]
for i in range(1, 181):
    ts = datetime(2026, 4, 1, 9, 0, 0) + timedelta(minutes=i * 3)
    orders.append(Row(
        order_id=f"O{i:06d}",
        customer_id=f"C{(i % 100) + 1:05d}",
        order_status="CREATED",
        sales_channel=random.choice(channels),
        order_total_eur=float(round(random.uniform(20, 450), 2)),
        item_count=int((i % 8) + 1),
        op_type="I",
        op_ts=ts,
        sequence_num=1,
        is_deleted=False,
    ))
spark.createDataFrame(orders).coalesce(1).write.mode("overwrite").json(f"{LANDING_ROOT}/orders_cdc")

# COMMAND ----------

# 4) Sensores IoT: JSON (además irán por Kafka en la parte streaming)
sensors = []
for i in range(1, 401):
    ts = datetime(2026, 4, 3, 8, 0, 0) + timedelta(minutes=i)
    sensors.append(Row(
        sensor_event_id=f"SEN-{i:06d}",
        field_id=random.choice(["FIELD-NORTE", "FIELD-SUR", "FIELD-ESTE", "FIELD-OESTE"]),
        event_ts=ts,
        temperature_c=float(20 + (i % 10)),
        soil_moisture_pct=float(35 + (i % 25)),
        ph_level=float(6.0 + ((i % 10) / 10.0)),
    ))
spark.createDataFrame(sensors).coalesce(1).write.mode("overwrite").json(f"{LANDING_ROOT}/sensors")

# COMMAND ----------

# 5) Eventos de la app móvil: JSON
app_events = []
for i in range(1, 701):
    ts = datetime(2026, 4, 3, 9, 0, 0) + timedelta(seconds=i * 15)
    app_events.append(Row(
        event_id=f"APP-{i:06d}",
        event_ts=ts,
        session_id=f"S{(i % 160) + 1:05d}",
        customer_id=f"C{(i % 100) + 1:05d}",
        event_type=random.choice(["session_start", "view_product", "add_to_cart", "checkout", "purchase"]),
        product_id=f"P{(i % 120) + 1:04d}",
        channel=random.choice(["android", "ios", "webview"]),
    ))
spark.createDataFrame(app_events).coalesce(1).write.mode("overwrite").json(f"{LANDING_ROOT}/app_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incrementales en staging
# MAGIC Se dejan fuera de las rutas de landing para poder "liberarlos" después y comprobar que
# MAGIC el Autoloader solo procesa los ficheros nuevos.

# COMMAND ----------

if WRITE_INCREMENTALS_TO_STAGING:
    products_inc = []
    for i in range(1, 26):
        products_inc.append(Row(
            product_id=f"P{i:04d}", sku=f"SKU-{i:05d}", product_name=f"Producto agricola {i}",
            category="Semillas", brand="AgroMax", unit_of_measure="kg", price_eur=float(15 + i),
            is_active=False if i % 7 == 0 else True,
            effective_ts=datetime(2026, 4, 2, 8, 0, 0), extract_ts=datetime(2026, 4, 2, 8, 30, 0),
        ))
    for i in range(121, 136):
        products_inc.append(Row(
            product_id=f"P{i:04d}", sku=f"SKU-{i:05d}", product_name=f"Producto agricola {i}",
            category="Herramientas", brand="CampoTech", unit_of_measure="ud", price_eur=float(25 + i),
            is_active=True, effective_ts=datetime(2026, 4, 3, 9, 0, 0), extract_ts=datetime(2026, 4, 3, 9, 20, 0),
        ))
    spark.createDataFrame(products_inc).coalesce(1).write.mode("overwrite").parquet(
        f"{INCREMENTAL_STAGING_ROOT}/product_catalog")

    inventory_inc = []
    for wh in ["MAD-01", "SEV-01", "VAL-01"]:
        for i in range(1, 91):
            inventory_inc.append(Row(
                warehouse_id=wh, sku=f"SKU-{i:05d}", snapshot_ts=datetime(2026, 4, 3, 6, 0, 0),
                extract_ts=datetime(2026, 4, 3, 6, 10, 0),
                available_units=int(110 + (i % 25)), reserved_units=int(12 + (i % 9)),
            ))
    spark.createDataFrame(inventory_inc).coalesce(1).write.mode("overwrite").parquet(
        f"{INCREMENTAL_STAGING_ROOT}/inventory")

    orders_inc = []
    for i in range(1, 121):
        orders_inc.append(Row(
            order_id=f"O{i:06d}", customer_id=f"C{(i % 100) + 1:05d}",
            order_status="DELIVERED" if i % 5 else "CANCELLED", sales_channel=random.choice(channels),
            order_total_eur=float(40 + i), item_count=int((i % 8) + 1), op_type="D" if i % 9 == 0 else "U",
            op_ts=datetime(2026, 4, 2, 10, 0, 0) + timedelta(minutes=i), sequence_num=2,
            is_deleted=True if i % 9 == 0 else False,
        ))
    spark.createDataFrame(orders_inc).coalesce(1).write.mode("overwrite").json(
        f"{INCREMENTAL_STAGING_ROOT}/orders_cdc")

    sensors_inc = []
    for i in range(401, 581):
        sensors_inc.append(Row(
            sensor_event_id=f"SEN-{i:06d}",
            field_id=random.choice(["FIELD-NORTE", "FIELD-SUR", "FIELD-ESTE", "FIELD-OESTE"]),
            event_ts=datetime(2026, 4, 3, 14, 0, 0) + timedelta(minutes=i), temperature_c=float(22 + (i % 12)),
            soil_moisture_pct=float(20 + (i % 20)), ph_level=float(5.1 + ((i % 6) / 10.0)),
        ))
    spark.createDataFrame(sensors_inc).coalesce(1).write.mode("overwrite").json(
        f"{INCREMENTAL_STAGING_ROOT}/sensors")

    app_inc = []
    for i in range(701, 1101):
        app_inc.append(Row(
            event_id=f"APP-{i:06d}", event_ts=datetime(2026, 4, 3, 12, 0, 0) + timedelta(seconds=i * 12),
            session_id=f"S{(i % 180) + 1:05d}", customer_id=f"C{(i % 100) + 1:05d}",
            event_type=random.choice(["session_start", "view_product", "add_to_cart", "checkout", "purchase"]),
            product_id=f"P{(i % 120) + 1:04d}", channel=random.choice(["android", "ios", "webview"]),
        ))
    spark.createDataFrame(app_inc).coalesce(1).write.mode("overwrite").json(
        f"{INCREMENTAL_STAGING_ROOT}/app_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

for d in ["product_catalog", "inventory", "orders_cdc", "sensors", "app_events"]:
    files = dbutils.fs.ls(f"{LANDING_ROOT}/{d}")
    data_files = [f.name for f in files if not f.name.startswith("_")]
    print(f"{d:20s} -> {len(data_files)} fichero(s) de datos")

display(dbutils.fs.ls(LANDING_ROOT))
