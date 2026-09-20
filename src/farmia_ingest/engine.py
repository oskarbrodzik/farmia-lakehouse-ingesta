"""Orquestación del motor de ingesta.

Cada ejecución: lee las configuraciones, crea una streaming query por dataset,
espera a que terminen y deja constancia del resultado en la tabla de auditoría.

Un dataset que falla no tumba al resto: se captura el error, se registra y el
motor sigue con los demás. En una ingesta nocturna es preferible perder una
fuente y saberlo, a perderlas todas por un fichero corrupto.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from .config import DatasetConfig, EngineConfig
from .logging_utils import get_logger
from .readers import leer_origen
from .writers import escribir_bronze, registrar_tabla, ruta_destino

log = get_logger(__name__)


@dataclass
class ResultadoIngesta:
    """Lo que se registra de cada dataset procesado."""

    run_id: str
    datasource: str
    dataset: str
    source_format: str
    sink_path: str
    started_at: datetime
    finished_at: datetime
    rows_ingested: int
    status: str
    error_message: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "OK"


def ejecutar(spark: SparkSession,
             engine: EngineConfig,
             datasets: list[DatasetConfig],
             run_id: str | None = None) -> list[ResultadoIngesta]:
    """Ejecuta una pasada completa del motor."""
    run_id = run_id or uuid.uuid4().hex[:12]
    activos = [d for d in datasets if d.enabled]
    omitidos = [d.nombre for d in datasets if not d.enabled]

    log.info("=" * 78)
    log.info("run_id=%s | %d dataset(s) a procesar", run_id, len(activos))
    if omitidos:
        log.info("desactivados en configuracion: %s", ", ".join(omitidos))
    log.info("=" * 78)

    resultados: list[ResultadoIngesta] = []
    en_vuelo: list[tuple[DatasetConfig, StreamingQuery, datetime, int | None]] = []

    # Fase 1: arrancar todas las queries. Se lanzan juntas para que las ingestas
    # independientes se solapen en lugar de ir una detrás de otra.
    for dataset in activos:
        inicio = datetime.now()
        try:
            # Versión de la tabla antes de escribir: es la referencia para saber
            # después cuántas filas ha añadido esta ejecución en concreto.
            version_previa = _version_delta(spark, ruta_destino(dataset, engine))
            df = leer_origen(spark, dataset, engine)
            query = escribir_bronze(dataset, df, engine)
            en_vuelo.append((dataset, query, inicio, version_previa))
        except Exception as exc:
            log.error("[%s] no se pudo arrancar: %s", dataset.nombre, exc)
            resultados.append(_resultado_error(run_id, dataset, engine, inicio, exc))

    # Fase 2: esperar. Las queries continuas no terminan solas, así que solo se
    # espera a las de tipo availableNow.
    for dataset, query, inicio, version_previa in en_vuelo:
        if dataset.sink.get("trigger", {}).get("processing_time"):
            log.info("[%s] query continua activa (id=%s)", dataset.nombre, query.id)
            resultados.append(_resultado(
                run_id, dataset, engine, inicio, datetime.now(), 0, "RUNNING"))
            continue

        try:
            query.awaitTermination()
            filas = _filas_escritas(spark, dataset, engine, version_previa)
            log.info("[%s] completado, %d fila(s)", dataset.nombre, filas)
            resultados.append(_resultado(
                run_id, dataset, engine, inicio, datetime.now(), filas, "OK"))
        except Exception as exc:
            log.error("[%s] fallo durante la ingesta: %s", dataset.nombre, exc)
            resultados.append(_resultado_error(run_id, dataset, engine, inicio, exc))
            continue

        # Fuera del try anterior: registrar la tabla es un paso posterior a la
        # ingesta, y que falle no invalida los datos que ya están escritos.
        if engine.register_tables:
            try:
                registrar_tabla(spark, dataset, engine)
            except Exception as exc:
                log.warning("[%s] datos escritos, pero no se pudo registrar la tabla: %s",
                            dataset.nombre, exc)

    _auditar(spark, engine, resultados)
    _resumen(resultados)
    return resultados


def _version_delta(spark: SparkSession, ruta: str) -> int | None:
    """Última versión de la tabla Delta, o None si aún no existe."""
    try:
        historial = spark.sql(f"DESCRIBE HISTORY delta.`{ruta}`")
        fila = historial.selectExpr("max(version) as v").first()
        return None if fila is None else fila["v"]
    except Exception:
        return None


def _filas_escritas(spark: SparkSession,
                    dataset: DatasetConfig,
                    engine: EngineConfig,
                    version_previa: int | None) -> int:
    """Filas escritas por esta ejecución, según el historial de Delta.

    No se usa `query.recentProgress` porque en cómputo serverless viene vacío en
    cuanto la query termina. El historial registra en cada commit cuántas filas
    se añadieron, y sirve en cualquier runtime.
    """
    ruta = ruta_destino(dataset, engine)
    try:
        historial = spark.sql(f"DESCRIBE HISTORY delta.`{ruta}`")
        if version_previa is not None:
            historial = historial.filter(f"version > {version_previa}")

        total = 0
        for fila in historial.select("operationMetrics").collect():
            metricas = fila["operationMetrics"] or {}
            total += int(metricas.get("numOutputRows", 0))
        return total
    except Exception as exc:
        log.warning("[%s] no se pudieron contar las filas escritas: %s",
                    dataset.nombre, exc)
        return 0


def _resultado(run_id, dataset, engine, inicio, fin, filas, estado, error="") -> ResultadoIngesta:
    return ResultadoIngesta(
        run_id=run_id,
        datasource=dataset.datasource,
        dataset=dataset.dataset,
        source_format=dataset.source.get("format", dataset.tipo_origen),
        sink_path=ruta_destino(dataset, engine),
        started_at=inicio,
        finished_at=fin,
        rows_ingested=filas,
        status=estado,
        error_message=error,
    )


def _resultado_error(run_id, dataset, engine, inicio, exc) -> ResultadoIngesta:
    return _resultado(run_id, dataset, engine, inicio, datetime.now(), 0, "ERROR",
                      f"{type(exc).__name__}: {exc}"[:1000])


def _auditar(spark: SparkSession,
             engine: EngineConfig,
             resultados: list[ResultadoIngesta]) -> None:
    """Escribe una fila por dataset en la tabla de auditoría.

    Si la tabla no existe o no hay permisos, se avisa pero no se falla: la
    auditoría no debe poder invalidar una ingesta que ya se completó.
    """
    if not resultados:
        return

    filas = [(r.run_id, r.datasource, r.dataset, r.source_format, r.sink_path,
              r.started_at, r.finished_at, r.rows_ingested, r.status, r.error_message)
             for r in resultados]

    columnas = ["run_id", "datasource", "dataset", "source_format", "sink_path",
                "started_at", "finished_at", "rows_ingested", "status", "error_message"]

    try:
        (spark.createDataFrame(filas, columnas)
              .write.mode("append")
              .saveAsTable(engine.audit_table_fqn))
        log.info("auditoria registrada en %s", engine.audit_table_fqn)
    except Exception as exc:
        log.warning("no se pudo escribir la auditoria: %s", exc)


def _resumen(resultados: list[ResultadoIngesta]) -> None:
    correctos = [r for r in resultados if r.ok]
    fallidos = [r for r in resultados if r.status == "ERROR"]
    activas = [r for r in resultados if r.status == "RUNNING"]
    total_filas = sum(r.rows_ingested for r in correctos)

    log.info("-" * 78)
    log.info("RESUMEN: %d correcto(s), %d con error, %d query(s) continua(s)",
             len(correctos), len(fallidos), len(activas))
    log.info("filas ingeridas: %d", total_filas)
    for r in fallidos:
        log.info("  ERROR en %s.%s -> %s", r.datasource, r.dataset, r.error_message)
    log.info("-" * 78)
