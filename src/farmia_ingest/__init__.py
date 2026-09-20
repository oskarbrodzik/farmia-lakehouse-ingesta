"""Motor de ingesta de FarmIA: de la capa landing a la capa bronze.

Uso típico desde un notebook:

    from farmia_ingest import EngineConfig, load_dataset_configs, ejecutar

    engine = EngineConfig.load("configs/engine.json")
    datasets = load_dataset_configs("configs/datasets")
    ejecutar(spark, engine, datasets)
"""

from .config import ConfigError, DatasetConfig, EngineConfig, load_dataset_configs
from .engine import ResultadoIngesta, ejecutar

__all__ = [
    "ConfigError",
    "DatasetConfig",
    "EngineConfig",
    "ResultadoIngesta",
    "ejecutar",
    "load_dataset_configs",
]

__version__ = "1.0.0"
