# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Productor de eventos a Confluent Cloud
# MAGIC
# MAGIC Simula los dos flujos en tiempo real de FarmIA:
# MAGIC
# MAGIC | Topic | Contenido | Formato |
# MAGIC |---|---|---|
# MAGIC | `farmia.app_events` | Eventos de la app móvil | JSON |
# MAGIC | `farmia.sensors.norte` / `.sur` | Lecturas de sensores de campo | Avro con Schema Registry |
# MAGIC
# MAGIC Los de sensores van en dos topics a propósito, para que el motor pueda
# MAGIC consumirlos con un patrón (`subscribePattern`) en lugar de nombrarlos uno a uno.
# MAGIC
# MAGIC Este notebook es material de apoyo: no forma parte del motor de ingesta.

# COMMAND ----------

# MAGIC %pip install "confluent-kafka[avro,schemaregistry]"

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

CLIENT_PROPERTIES = "/Workspace/Users/<usuario>/farmia-secrets/client.properties"

TOPIC_APP_EVENTS = "farmia.app_events"
TOPICS_SENSORES = ["farmia.sensors.norte", "farmia.sensors.sur"]

MENSAJES_APP = 300
MENSAJES_SENSORES = 200

# COMMAND ----------


def leer_properties(ruta):
    """Mismo formato clave=valor que consume el motor."""
    propiedades = {}
    with open(ruta, encoding="utf-8") as fh:
        for linea in fh:
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            clave, valor = linea.split("=", 1)
            propiedades[clave.strip()] = valor.strip()
    return propiedades


conf = leer_properties(CLIENT_PROPERTIES)

# El Producer solo admite claves suyas: las del Schema Registry se separan.
conf_productor = {
    "bootstrap.servers": conf["bootstrap.servers"],
    "security.protocol": conf["security.protocol"],
    "sasl.mechanisms": conf["sasl.mechanisms"],
    "sasl.username": conf["sasl.username"],
    "sasl.password": conf["sasl.password"],
}

conf_registry = {
    "url": conf["schema.registry.url"],
    "basic.auth.user.info": conf["basic.auth.user.info"],
}

print("bootstrap:", conf_productor["bootstrap.servers"])
print("registry: ", conf_registry["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilidades comunes
# MAGIC En su propia celda para que los dos productores (JSON y Avro) sean
# MAGIC independientes y se pueda ejecutar uno sin el otro.

# COMMAND ----------

import json
import random
from datetime import datetime, timedelta

from confluent_kafka import Producer

random.seed(11)

TIPOS_EVENTO = ["session_start", "view_product", "add_to_cart", "checkout", "purchase"]
CANALES = ["android", "ios", "webview"]

entregados = {"ok": 0, "error": 0}


def callback_entrega(err, msg):
    """Se invoca por cada mensaje cuando el broker confirma o rechaza."""
    if err is None:
        entregados["ok"] += 1
    else:
        entregados["error"] += 1
        print("error de entrega:", err)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Eventos de la app móvil (JSON)

# COMMAND ----------

entregados = {"ok": 0, "error": 0}
productor = Producer(conf_productor)
inicio = datetime(2026, 4, 4, 8, 0, 0)

for i in range(1, MENSAJES_APP + 1):
    customer_id = f"C{(i % 100) + 1:05d}"
    evento = {
        "event_id": f"APP-STR-{i:06d}",
        "event_ts": (inicio + timedelta(seconds=i * 7)).isoformat(),
        "session_id": f"S{(i % 150) + 1:05d}",
        "customer_id": customer_id,
        "event_type": random.choice(TIPOS_EVENTO),
        "product_id": f"P{(i % 120) + 1:04d}",
        "channel": random.choice(CANALES),
    }
    productor.produce(
        topic=TOPIC_APP_EVENTS,
        key=customer_id.encode("utf-8"),
        value=json.dumps(evento).encode("utf-8"),
        on_delivery=callback_entrega,
    )
    productor.poll(0)

productor.flush()
print(f"app_events -> entregados {entregados['ok']}, errores {entregados['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lecturas de sensores (Avro)
# MAGIC
# MAGIC `AvroSerializer` registra el esquema en el Schema Registry la primera vez y
# MAGIC antepone a cada mensaje la cabecera de 5 bytes de Confluent (byte mágico + id
# MAGIC del esquema). Es justo esa cabecera la que el motor descarta al decodificar.

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

ESQUEMA_SENSOR = """
{
  "type": "record",
  "name": "SensorReading",
  "namespace": "farmia",
  "fields": [
    {"name": "sensor_event_id",   "type": "string"},
    {"name": "field_id",          "type": "string"},
    {"name": "event_ts",          "type": "string"},
    {"name": "temperature_c",     "type": "double"},
    {"name": "soil_moisture_pct", "type": "double"},
    {"name": "ph_level",          "type": "double"}
  ]
}
"""

cliente_registry = SchemaRegistryClient(conf_registry)
serializador_avro = AvroSerializer(cliente_registry, ESQUEMA_SENSOR)
serializador_clave = StringSerializer("utf_8")

entregados = {"ok": 0, "error": 0}
productor = Producer(conf_productor)
inicio = datetime(2026, 4, 4, 9, 0, 0)

for i in range(1, MENSAJES_SENSORES + 1):
    topic = TOPICS_SENSORES[i % len(TOPICS_SENSORES)]
    campo = "FIELD-NORTE" if topic.endswith("norte") else "FIELD-SUR"
    lectura = {
        "sensor_event_id": f"SEN-STR-{i:06d}",
        "field_id": campo,
        "event_ts": (inicio + timedelta(minutes=i)).isoformat(),
        "temperature_c": float(round(random.uniform(8, 33), 2)),
        "soil_moisture_pct": float(round(random.uniform(18, 80), 2)),
        "ph_level": float(round(random.uniform(5.0, 7.5), 2)),
    }
    productor.produce(
        topic=topic,
        key=serializador_clave(lectura["sensor_event_id"],
                               SerializationContext(topic, MessageField.KEY)),
        value=serializador_avro(lectura,
                                SerializationContext(topic, MessageField.VALUE)),
        on_delivery=callback_entrega,
    )
    productor.poll(0)

productor.flush()
print(f"sensores -> entregados {entregados['ok']}, errores {entregados['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprobación de los esquemas registrados

# COMMAND ----------

for subject in cliente_registry.get_subjects():
    version = cliente_registry.get_latest_version(subject)
    print(f"{subject}  (version {version.version})")
