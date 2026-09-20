"""Lectores de origen: ficheros con Autoloader y eventos con Kafka.

Ambos devuelven un DataFrame de *streaming*. Unificar los dos casos bajo la misma
API de Structured Streaming es lo que permite que el resto del motor (escritura,
checkpoints, auditoría) sea idéntico para batch y para tiempo real.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from .config import ConfigError, DatasetConfig, EngineConfig
from .logging_utils import get_logger

log = get_logger(__name__)

# Número de bytes que Confluent antepone al mensaje: 1 byte mágico + 4 con el id
# del esquema. Hay que saltarselos antes de pasarle el payload a from_avro.
_CABECERA_CONFLUENT = 5


def leer_origen(spark: SparkSession,
                dataset: DatasetConfig,
                engine: EngineConfig) -> DataFrame:
    """Punto de entrada único: decide el lector según el tipo de origen."""
    if dataset.tipo_origen == "files":
        df = _leer_ficheros(spark, dataset, engine)
    else:
        df = _leer_kafka(spark, dataset, engine)

    # Metadato común a cualquier origen.
    return df.withColumn("_ingested_at", F.current_timestamp())


def _leer_ficheros(spark: SparkSession,
                   dataset: DatasetConfig,
                   engine: EngineConfig) -> DataFrame:
    """Ingesta incremental de ficheros de landing con Databricks Autoloader.

    Autoloader lleva su propio registro de los ficheros ya procesados, así que
    cada ejecución horaria del motor solo lee lo que ha llegado nuevo.
    """
    origen = dataset.source
    formato = origen["format"]
    ruta = engine.resolver_ruta(origen["path"])

    opciones: dict[str, Any] = {
        "cloudFiles.format": formato,
        "cloudFiles.schemaLocation": f"{engine.checkpoint_root}/schemas/{dataset.datasource}/{dataset.dataset}",
        "cloudFiles.schemaEvolutionMode": _modo_evolucion(origen, formato),
    }

    # Columna donde caen los campos que no encajan con el esquema esperado: así un
    # fichero mal formado no tumba la ingesta ni pierde datos. binaryFile no la
    # admite porque su esquema es fijo (path, content, length, modificationTime).
    if formato != "binaryFile":
        opciones["rescuedDataColumn"] = "_rescued_data"

    opciones.update(origen.get("options", {}))

    lector = spark.readStream.format("cloudFiles").options(**opciones)

    # Esquema esperado: si se declara, Autoloader no infiere y los tipos quedan
    # fijados por configuración. Si no, se infiere y se admiten schema hints.
    esquema = origen.get("schema")
    if esquema:
        lector = lector.schema(esquema)
        log.info("[%s] esquema declarado en configuracion", dataset.nombre)
    else:
        log.info("[%s] esquema inferido por Autoloader", dataset.nombre)

    log.info("[%s] leyendo %s desde %s", dataset.nombre, formato, ruta)

    df = lector.load(ruta)

    # Fichero de procedencia. Se usa _metadata en vez de input_file_name()
    # porque es lo soportado en cómputo serverless.
    return (df
            .withColumn("_ingested_filename", F.col("_metadata.file_name"))
            .withColumn("_ingested_filepath", F.col("_metadata.file_path")))


def _modo_evolucion(origen: dict[str, Any], formato: str) -> str:
    """Modo de evolución de esquema que Autoloader va a aceptar.

    Rechaza 'addNewColumns' cuando el esquema se declara, y en binaryFile solo
    admite 'none'. El motor lo deduce para no tener que acertarlo en cada JSON.
    """
    if origen.get("schema_evolution_mode"):
        return origen["schema_evolution_mode"]

    if origen.get("schema") or formato == "binaryFile":
        return "none"

    return "addNewColumns"


def _leer_kafka(spark: SparkSession,
                dataset: DatasetConfig,
                engine: EngineConfig) -> DataFrame:
    """Consumo de un topic (o patrón de topics) de Kafka."""
    if engine.kafka is None:
        raise ConfigError(
            f"[{dataset.nombre}] origen kafka pero engine.json no trae seccion 'kafka'"
        )

    origen = dataset.source
    opciones = engine.kafka.spark_options()

    if origen.get("subscribe_pattern"):
        opciones["subscribePattern"] = origen["subscribe_pattern"]
        descripcion = f"patron {origen['subscribe_pattern']}"
    else:
        opciones["subscribe"] = origen["subscribe"]
        descripcion = f"topic {origen['subscribe']}"

    opciones["startingOffsets"] = origen.get("starting_offsets", "earliest")
    opciones.update(origen.get("options", {}))

    log.info("[%s] suscribiendo a %s", dataset.nombre, descripcion)

    df = spark.readStream.format("kafka").options(**opciones).load()

    # Se conserva la metadata de Kafka: es la trazabilidad de la capa bronze.
    df = df.select(
        F.col("key").alias("_raw_key"),
        F.col("value").alias("_raw_value"),
        F.col("topic").alias("_topic"),
        F.col("partition").alias("_partition"),
        F.col("offset").alias("_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
    )

    df = df.withColumn(
        "key",
        _decodificar("_raw_key", origen.get("key_format", "string"),
                     origen.get("key_subject"), origen.get("key_json_schema"), engine, dataset),
    )
    df = df.withColumn(
        "value",
        _decodificar("_raw_value", origen["value_format"],
                     origen.get("value_subject"), origen.get("value_json_schema"), engine, dataset),
    )

    return df.drop("_raw_key", "_raw_value")


def _decodificar(columna: str,
                 formato: str,
                 subject: str | None,
                 json_schema: str | None,
                 engine: EngineConfig,
                 dataset: DatasetConfig):
    """Convierte los bytes de Kafka al formato declarado en la configuración."""
    if formato == "binary":
        return F.col(columna)

    if formato == "string":
        return F.col(columna).cast("string")

    if formato == "json":
        if not json_schema:
            raise ConfigError(
                f"[{dataset.nombre}] formato json sin esquema declarado para '{columna}'"
            )
        return F.from_json(F.col(columna).cast("string"), json_schema)

    if formato == "avro":
        if not subject:
            raise ConfigError(
                f"[{dataset.nombre}] formato avro sin 'subject' del Schema Registry"
            )
        esquema = _esquema_del_registry(subject, engine)
        # from_avro sobre el payload sin la cabecera de 5 bytes de Confluent.
        payload = F.expr(
            f"substring({columna}, {_CABECERA_CONFLUENT + 1}, length({columna}) - {_CABECERA_CONFLUENT})"
        )
        return from_avro(payload, esquema)

    raise ConfigError(f"[{dataset.nombre}] formato de mensaje no soportado: '{formato}'")


def _esquema_del_registry(subject: str, engine: EngineConfig) -> str:
    """Descarga del Schema Registry la última versión del esquema del subject.

    El import va aquí dentro y no arriba porque `confluent-kafka` solo hace falta
    si hay algún dataset en Avro: así el motor arranca sin la librería instalada.
    """
    from confluent_kafka.schema_registry import SchemaRegistryClient

    cliente = SchemaRegistryClient(engine.kafka.schema_registry_conf())
    esquema = cliente.get_latest_version(subject).schema.schema_str
    log.info("esquema '%s' recuperado del Schema Registry", subject)
    return esquema
