"""Tests de la carga y validación de la configuración.

No necesitan Spark: `config.py` es la parte del motor que se puede comprobar
sin levantar un cluster, y es donde un error tiene más alcance, porque una
configuración mal validada no se detecta hasta la mitad de la ingesta.
"""

import json

import pytest

from farmia_ingest.config import (
    ConfigError,
    DatasetConfig,
    EngineConfig,
    load_dataset_configs,
)

ENGINE_MINIMO = {"storage_account": "cuenta", "catalog": "catalogo"}

DATASET_FICHEROS = {
    "datasource": "farmia",
    "dataset": "shipments",
    "source": {"type": "files", "format": "csv", "path": "{landing}/farmia/shipments"},
    "sink": {"layer": "bronze"},
}

DATASET_KAFKA = {
    "datasource": "farmia",
    "dataset": "sensors_stream",
    "source": {"type": "kafka", "subscribe": "farmia.sensors", "value_format": "avro"},
    "sink": {"layer": "bronze"},
}


def escribir(directorio, nombre, datos):
    ruta = directorio / nombre
    ruta.write_text(json.dumps(datos), encoding="utf-8")
    return ruta


# --- EngineConfig ----------------------------------------------------------

def test_engine_aplica_los_valores_por_defecto():
    cfg = EngineConfig.from_dict(ENGINE_MINIMO)

    assert cfg.landing_container == "landing"
    assert cfg.lakehouse_container == "lakehouse"
    assert cfg.bronze_schema == "farmia_bronze"
    assert cfg.audit_table == "ingestion_audit"
    assert cfg.register_tables is True
    assert cfg.kafka is None


@pytest.mark.parametrize("campo", ["storage_account", "catalog"])
def test_engine_falla_si_falta_un_campo_obligatorio(campo):
    datos = {k: v for k, v in ENGINE_MINIMO.items() if k != campo}

    with pytest.raises(ConfigError, match=campo):
        EngineConfig.from_dict(datos)


def test_engine_construye_las_rutas_abfss():
    cfg = EngineConfig.from_dict(ENGINE_MINIMO)

    assert cfg.landing_root == "abfss://landing@cuenta.dfs.core.windows.net"
    assert cfg.bronze_root == "abfss://lakehouse@cuenta.dfs.core.windows.net/bronze"
    assert cfg.audit_table_fqn == "catalogo.farmia_ops.ingestion_audit"


def test_resolver_ruta_expande_los_marcadores():
    cfg = EngineConfig.from_dict(ENGINE_MINIMO)

    assert cfg.resolver_ruta("{landing}/farmia/sensors") == (
        "abfss://landing@cuenta.dfs.core.windows.net/farmia/sensors"
    )
    assert cfg.resolver_ruta("{bronze}/farmia/sensors").endswith("/bronze/farmia/sensors")
    assert cfg.resolver_ruta("sin marcadores") == "sin marcadores"


def test_engine_load_lee_el_json(tmp_path):
    ruta = escribir(tmp_path, "engine.json", ENGINE_MINIMO | {"register_tables": False})

    cfg = EngineConfig.load(ruta)

    assert cfg.storage_account == "cuenta"
    assert cfg.register_tables is False


# --- DatasetConfig ---------------------------------------------------------

def test_dataset_de_ficheros_valido():
    cfg = DatasetConfig.from_dict(DATASET_FICHEROS, "01_shipments.json")

    assert cfg.nombre == "farmia.shipments"
    assert cfg.tipo_origen == "files"
    assert cfg.enabled is True


def test_dataset_rechaza_un_formato_no_soportado():
    datos = {**DATASET_FICHEROS, "source": {**DATASET_FICHEROS["source"], "format": "xml"}}

    with pytest.raises(ConfigError, match="xml"):
        DatasetConfig.from_dict(datos, "01_shipments.json")


def test_dataset_de_ficheros_exige_path():
    source = {k: v for k, v in DATASET_FICHEROS["source"].items() if k != "path"}

    with pytest.raises(ConfigError, match="path"):
        DatasetConfig.from_dict({**DATASET_FICHEROS, "source": source}, "01.json")


def test_dataset_de_kafka_exige_subscribe_o_patron():
    source = {"type": "kafka", "value_format": "avro"}

    with pytest.raises(ConfigError, match="subscribe"):
        DatasetConfig.from_dict({**DATASET_KAFKA, "source": source}, "10.json")


def test_dataset_de_kafka_acepta_el_patron_de_topics():
    source = {"type": "kafka", "subscribe_pattern": r"farmia\.sensors\..*",
              "value_format": "avro"}

    cfg = DatasetConfig.from_dict({**DATASET_KAFKA, "source": source}, "10.json")

    assert cfg.tipo_origen == "kafka"


def test_dataset_rechaza_un_tipo_de_origen_desconocido():
    datos = {**DATASET_FICHEROS, "source": {"type": "jdbc"}}

    with pytest.raises(ConfigError, match="jdbc"):
        DatasetConfig.from_dict(datos, "01.json")


def test_dataset_rechaza_una_capa_distinta_de_bronze():
    datos = {**DATASET_FICHEROS, "sink": {"layer": "silver"}}

    with pytest.raises(ConfigError, match="bronze"):
        DatasetConfig.from_dict(datos, "01.json")


def test_el_error_nombra_el_fichero_que_lo_provoca():
    datos = {k: v for k, v in DATASET_FICHEROS.items() if k != "dataset"}

    with pytest.raises(ConfigError, match=r"\[07_weather\.json\]"):
        DatasetConfig.from_dict(datos, "07_weather.json")


# --- load_dataset_configs --------------------------------------------------

def test_los_datasets_se_cargan_en_orden_de_fichero(tmp_path):
    escribir(tmp_path, "02_kafka.json", DATASET_KAFKA)
    escribir(tmp_path, "01_ficheros.json", DATASET_FICHEROS)

    configuraciones = load_dataset_configs(tmp_path)

    assert [c.dataset for c in configuraciones] == ["shipments", "sensors_stream"]


def test_solo_filtra_por_nombre_completo(tmp_path):
    escribir(tmp_path, "01_ficheros.json", DATASET_FICHEROS)
    escribir(tmp_path, "02_kafka.json", DATASET_KAFKA)

    configuraciones = load_dataset_configs(tmp_path, solo=["farmia.sensors_stream"])

    assert [c.nombre for c in configuraciones] == ["farmia.sensors_stream"]


def test_falla_si_ningun_dataset_coincide_con_el_filtro(tmp_path):
    escribir(tmp_path, "01_ficheros.json", DATASET_FICHEROS)

    with pytest.raises(ConfigError, match="farmia.inexistente"):
        load_dataset_configs(tmp_path, solo=["farmia.inexistente"])


def test_falla_si_el_directorio_no_existe(tmp_path):
    with pytest.raises(ConfigError, match="no existe"):
        load_dataset_configs(tmp_path / "datasets")


def test_falla_si_el_directorio_esta_vacio(tmp_path):
    with pytest.raises(ConfigError, match="ninguna configuracion"):
        load_dataset_configs(tmp_path)


def test_un_json_invalido_identifica_el_fichero(tmp_path):
    (tmp_path / "03_roto.json").write_text("{ no es json", encoding="utf-8")

    with pytest.raises(ConfigError, match="03_roto.json"):
        load_dataset_configs(tmp_path)
