"""Escritura en la capa bronze.

Bronze guarda el dato tal y como llegó, enriquecido solo con metadatos de
procedencia. Ni se limpia ni se deduplica: eso es trabajo de silver.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from .config import DatasetConfig, EngineConfig
from .logging_utils import get_logger

log = get_logger(__name__)


def ruta_destino(dataset: DatasetConfig, engine: EngineConfig) -> str:
    """Ruta de la tabla bronze; se puede fijar en el JSON o derivar del nombre."""
    if dataset.sink.get("path"):
        return engine.resolver_ruta(dataset.sink["path"])
    return f"{engine.bronze_root}/{dataset.datasource}/{dataset.dataset}"


def ruta_checkpoint(dataset: DatasetConfig, engine: EngineConfig) -> str:
    """El checkpoint vive fuera de la tabla, para poder borrar uno sin el otro."""
    return f"{engine.checkpoint_root}/bronze/{dataset.datasource}/{dataset.dataset}"


def escribir_bronze(dataset: DatasetConfig,
                    df: DataFrame,
                    engine: EngineConfig) -> StreamingQuery:
    """Arranca la streaming query que vuelca el DataFrame en bronze."""
    destino = ruta_destino(dataset, engine)
    checkpoint = ruta_checkpoint(dataset, engine)

    df = _anadir_particiones_derivadas(dataset, df)

    opciones: dict[str, Any] = {
        # Permite que lleguen columnas nuevas sin reescribir la tabla, que es la
        # evolución de esquema compatible.
        "mergeSchema": "true",
        "checkpointLocation": checkpoint,
    }
    opciones.update(dataset.sink.get("options", {}))

    escritor = (df.writeStream
                  .format(dataset.sink.get("format", "delta"))
                  .outputMode("append")
                  .options(**opciones)
                  .queryName(dataset.nombre))

    particiones = dataset.sink.get("partition_by", [])
    if particiones:
        escritor = escritor.partitionBy(*particiones)
        log.info("[%s] particionando por %s", dataset.nombre, ", ".join(particiones))

    escritor = _aplicar_trigger(escritor, dataset)

    log.info("[%s] escribiendo en %s", dataset.nombre, destino)
    return escritor.start(destino)


def _anadir_particiones_derivadas(dataset: DatasetConfig, df: DataFrame) -> DataFrame:
    """Crea las columnas de particionado que no vienen en el origen.

    Ejemplo típico: particionar por día de ingesta sin que el dato traiga esa
    columna. Se declara en el JSON como
    `"derived_partitions": {"_ingested_date": "to_date(_ingested_at)"}`.
    """
    for nombre, expresion in dataset.sink.get("derived_partitions", {}).items():
        df = df.withColumn(nombre, F.expr(expresion))
    return df


def _aplicar_trigger(escritor, dataset: DatasetConfig):
    """Traduce la configuración de trigger a la llamada de Spark.

    Por defecto `availableNow`: procesa lo pendiente y termina, que es el modo
    que encaja con un motor que se ejecuta cada hora y el único disponible en
    cómputo serverless.
    """
    trigger = dataset.sink.get("trigger", {})

    if trigger.get("processing_time"):
        log.info("[%s] trigger continuo cada %s", dataset.nombre, trigger["processing_time"])
        return escritor.trigger(processingTime=trigger["processing_time"])

    return escritor.trigger(availableNow=True)


def registrar_tabla(spark: SparkSession,
                    dataset: DatasetConfig,
                    engine: EngineConfig) -> str:
    """Declara la tabla en Unity Catalog apuntando a la ruta de bronze.

    No es obligatorio para que la ingesta funcione, pero deja el dato accesible
    por SQL y visible en el catálogo, que es como se consume un lakehouse.
    """
    destino = ruta_destino(dataset, engine)
    nombre_tabla = f"{engine.catalog}.{engine.bronze_schema}.{dataset.datasource}_{dataset.dataset}"

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {nombre_tabla} USING DELTA LOCATION '{destino}'"
    )
    log.info("[%s] tabla registrada como %s", dataset.nombre, nombre_tabla)
    return nombre_tabla
