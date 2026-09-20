"""Carga y validación de la configuración del motor.

Cada dataset se describe en un JSON de `configs/datasets/`; los ajustes comunes,
en `configs/engine.json`. No hay nada específico de un dataset en el código.

Se valida al cargar, no al ejecutar: mejor fallar antes de arrancar ninguna
query que a mitad de la ingesta.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    """Error de configuración, con el fichero y el campo que lo provocan."""


def _requerido(datos: dict[str, Any], clave: str, origen: str) -> Any:
    if clave not in datos or datos[clave] in (None, ""):
        raise ConfigError(f"[{origen}] falta el campo obligatorio '{clave}'")
    return datos[clave]


def _leer_properties(ruta: str) -> dict[str, str]:
    """Lee el `client.properties` que descarga Confluent, tal cual.

    Consumirlo sin tocarlo evita copiar las credenciales a mano a ningún sitio.
    """
    propiedades: dict[str, str] = {}
    with open(ruta, encoding="utf-8") as fh:
        for linea in fh:
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            clave, valor = linea.split("=", 1)
            propiedades[clave.strip()] = valor.strip()
    return propiedades


@dataclass
class KafkaConfig:
    """Conexión al cluster de Kafka y al Schema Registry.

    Las credenciales no se escriben en el JSON: se leen del `client.properties`
    que descarga Confluent, cuya ruta es lo único que se configura.
    """

    client_properties_path: str
    schema_registry_url: str = ""
    schema_registry_auth: str = ""
    _propiedades: dict[str, str] = field(default_factory=dict, repr=False)

    @classmethod
    def from_dict(cls, datos: dict[str, Any]) -> "KafkaConfig":
        ruta = _requerido(datos, "client_properties_path", "engine.json/kafka")
        propiedades = _leer_properties(ruta)
        return cls(
            client_properties_path=ruta,
            schema_registry_url=datos.get("schema_registry_url")
            or propiedades.get("schema.registry.url", ""),
            schema_registry_auth=datos.get("schema_registry_auth")
            or propiedades.get("basic.auth.user.info", ""),
            _propiedades=propiedades,
        )

    def spark_options(self) -> dict[str, str]:
        """Opciones `kafka.*` que espera el conector de Spark."""
        usuario = self._propiedades.get("sasl.username")
        password = self._propiedades.get("sasl.password")
        modulo = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        return {
            "kafka.bootstrap.servers": self._propiedades["bootstrap.servers"],
            "kafka.security.protocol": self._propiedades.get("security.protocol", "SASL_SSL"),
            "kafka.sasl.mechanism": self._propiedades.get("sasl.mechanisms", "PLAIN"),
            "kafka.sasl.jaas.config": (
                f'{modulo} required username="{usuario}" password="{password}";'
            ),
        }

    def schema_registry_conf(self) -> dict[str, str]:
        if not self.schema_registry_url:
            raise ConfigError(
                "no hay URL de Schema Registry: necesaria para los datasets en Avro"
            )
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": self.schema_registry_auth,
        }


@dataclass
class EngineConfig:
    """Ajustes comunes a todas las ingestas."""

    storage_account: str
    landing_container: str
    lakehouse_container: str
    catalog: str
    bronze_schema: str
    ops_schema: str
    audit_table: str
    register_tables: bool
    kafka: KafkaConfig | None = None

    @classmethod
    def from_dict(cls, datos: dict[str, Any]) -> "EngineConfig":
        kafka = datos.get("kafka")
        return cls(
            storage_account=_requerido(datos, "storage_account", "engine.json"),
            landing_container=datos.get("landing_container", "landing"),
            lakehouse_container=datos.get("lakehouse_container", "lakehouse"),
            catalog=_requerido(datos, "catalog", "engine.json"),
            bronze_schema=datos.get("bronze_schema", "farmia_bronze"),
            ops_schema=datos.get("ops_schema", "farmia_ops"),
            audit_table=datos.get("audit_table", "ingestion_audit"),
            register_tables=bool(datos.get("register_tables", True)),
            kafka=KafkaConfig.from_dict(kafka) if kafka else None,
        )

    @classmethod
    def load(cls, ruta: str | Path) -> "EngineConfig":
        with open(ruta, encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))

    @property
    def landing_root(self) -> str:
        return f"abfss://{self.landing_container}@{self.storage_account}.dfs.core.windows.net"

    @property
    def lakehouse_root(self) -> str:
        return f"abfss://{self.lakehouse_container}@{self.storage_account}.dfs.core.windows.net"

    @property
    def bronze_root(self) -> str:
        return f"{self.lakehouse_root}/bronze"

    @property
    def checkpoint_root(self) -> str:
        return f"{self.lakehouse_root}/_checkpoints"

    @property
    def audit_table_fqn(self) -> str:
        return f"{self.catalog}.{self.ops_schema}.{self.audit_table}"

    def resolver_ruta(self, ruta: str) -> str:
        """Permite escribir rutas relativas en los JSON de dataset.

        `{landing}/farmia/sensors` se expande a la ruta abfss completa, de forma
        que cambiar de cuenta de almacenamiento solo toca `engine.json`.
        """
        return (ruta
                .replace("{landing}", self.landing_root)
                .replace("{lakehouse}", self.lakehouse_root)
                .replace("{bronze}", self.bronze_root))


@dataclass
class DatasetConfig:
    """Un dataset a ingerir: de dónde se lee, cómo y dónde se escribe."""

    datasource: str
    dataset: str
    source: dict[str, Any]
    sink: dict[str, Any]
    enabled: bool = True

    FORMATOS_FICHERO = {"csv", "json", "parquet", "avro", "binaryFile"}

    @property
    def nombre(self) -> str:
        return f"{self.datasource}.{self.dataset}"

    @property
    def tipo_origen(self) -> str:
        """`files` para ingesta batch con Autoloader, `kafka` para streaming."""
        return self.source["type"]

    @classmethod
    def from_dict(cls, datos: dict[str, Any], origen: str) -> "DatasetConfig":
        cfg = cls(
            datasource=_requerido(datos, "datasource", origen),
            dataset=_requerido(datos, "dataset", origen),
            source=_requerido(datos, "source", origen),
            sink=_requerido(datos, "sink", origen),
            enabled=bool(datos.get("enabled", True)),
        )
        cfg._validar(origen)
        return cfg

    def _validar(self, origen: str) -> None:
        tipo = _requerido(self.source, "type", origen)

        if tipo == "files":
            formato = _requerido(self.source, "format", origen)
            if formato not in self.FORMATOS_FICHERO:
                raise ConfigError(
                    f"[{origen}] formato '{formato}' no soportado; "
                    f"validos: {sorted(self.FORMATOS_FICHERO)}"
                )
            _requerido(self.source, "path", origen)

        elif tipo == "kafka":
            if not (self.source.get("subscribe") or self.source.get("subscribe_pattern")):
                raise ConfigError(
                    f"[{origen}] un origen kafka necesita 'subscribe' o 'subscribe_pattern'"
                )
            _requerido(self.source, "value_format", origen)

        else:
            raise ConfigError(f"[{origen}] tipo de origen desconocido: '{tipo}'")

        if self.sink.get("layer", "bronze") != "bronze":
            raise ConfigError(
                f"[{origen}] esta version del motor solo escribe en la capa bronze"
            )


def load_dataset_configs(directorio: str | Path,
                         solo: list[str] | None = None) -> list[DatasetConfig]:
    """Carga todos los JSON de un directorio, ordenados por nombre de fichero.

    `solo` permite ejecutar un subconjunto ("farmia.sensors"), útil para probar
    un dataset sin lanzar los demás.
    """
    directorio = Path(directorio)
    if not directorio.is_dir():
        raise ConfigError(f"no existe el directorio de configuraciones: {directorio}")

    configuraciones = []
    for fichero in sorted(directorio.glob("*.json")):
        with open(fichero, encoding="utf-8") as fh:
            try:
                datos = json.load(fh)
            except json.JSONDecodeError as exc:
                raise ConfigError(f"[{fichero.name}] JSON invalido: {exc}") from exc
        configuraciones.append(DatasetConfig.from_dict(datos, fichero.name))

    if not configuraciones:
        raise ConfigError(f"no se encontro ninguna configuracion en {directorio}")

    if solo:
        configuraciones = [c for c in configuraciones if c.nombre in solo]
        if not configuraciones:
            raise ConfigError(f"ningun dataset coincide con {solo}")

    return configuraciones
