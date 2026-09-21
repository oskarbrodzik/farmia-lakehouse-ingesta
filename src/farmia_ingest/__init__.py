"""Motor de ingesta de FarmIA: de la capa landing a la capa bronze.

Uso típico desde un notebook:

    from farmia_ingest import EngineConfig, load_dataset_configs, ejecutar

    engine = EngineConfig.load("configs/engine.json")
    datasets = load_dataset_configs("configs/datasets")
    ejecutar(spark, engine, datasets)
"""

from .config import ConfigError, DatasetConfig, EngineConfig, load_dataset_configs

__all__ = [
    "ConfigError",
    "DatasetConfig",
    "EngineConfig",
    "ResultadoIngesta",
    "ejecutar",
    "load_dataset_configs",
]

__version__ = "1.0.0"


def __getattr__(nombre):
    """Importa el motor solo cuando se usa.

    `engine.py` necesita PySpark, pero la capa de configuración no. Retrasando
    ese import, la configuración se puede cargar y probar fuera de un cluster.
    """
    if nombre in ("ResultadoIngesta", "ejecutar"):
        from . import engine
        return getattr(engine, nombre)
    raise AttributeError(f"el módulo {__name__!r} no tiene el atributo {nombre!r}")
