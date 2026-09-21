"""Tests de la conexión a Kafka.

Las credenciales nunca se escriben en el JSON: se leen del `client.properties`
que descarga Confluent. Lo que se comprueba aquí es que ese fichero se traduce
bien a las opciones que espera el conector de Spark.
"""

import pytest

from farmia_ingest.config import ConfigError, EngineConfig, KafkaConfig

PROPERTIES = """\
# Fichero de ejemplo, con el formato que entrega Confluent Cloud
bootstrap.servers=pkc-ejemplo.europe.azure.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=clave-de-cluster
sasl.password=secreto-de-cluster

schema.registry.url=https://psrc-ejemplo.europe.azure.confluent.cloud
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=clave-de-registry:secreto-de-registry
"""


@pytest.fixture
def client_properties(tmp_path):
    ruta = tmp_path / "client.properties"
    ruta.write_text(PROPERTIES, encoding="utf-8")
    return str(ruta)


def test_lee_el_schema_registry_del_properties(client_properties):
    cfg = KafkaConfig.from_dict({"client_properties_path": client_properties})

    assert cfg.schema_registry_url == "https://psrc-ejemplo.europe.azure.confluent.cloud"
    assert cfg.schema_registry_auth == "clave-de-registry:secreto-de-registry"


def test_el_json_puede_sobrescribir_el_schema_registry(client_properties):
    cfg = KafkaConfig.from_dict({
        "client_properties_path": client_properties,
        "schema_registry_url": "https://otro-registry",
    })

    assert cfg.schema_registry_url == "https://otro-registry"


def test_las_opciones_de_spark_llevan_el_jaas_completo(client_properties):
    opciones = KafkaConfig.from_dict(
        {"client_properties_path": client_properties}).spark_options()

    assert opciones["kafka.bootstrap.servers"].endswith(":9092")
    assert opciones["kafka.security.protocol"] == "SASL_SSL"
    assert opciones["kafka.sasl.mechanism"] == "PLAIN"
    assert 'username="clave-de-cluster"' in opciones["kafka.sasl.jaas.config"]
    assert 'password="secreto-de-cluster"' in opciones["kafka.sasl.jaas.config"]


def test_las_lineas_de_comentario_no_entran_como_propiedades(client_properties):
    cfg = KafkaConfig.from_dict({"client_properties_path": client_properties})

    assert all(not clave.startswith("#") for clave in cfg._propiedades)


def test_sin_url_de_registry_no_se_puede_decodificar_avro(tmp_path):
    ruta = tmp_path / "client.properties"
    ruta.write_text("bootstrap.servers=localhost:9092\n", encoding="utf-8")
    cfg = KafkaConfig.from_dict({"client_properties_path": str(ruta)})

    with pytest.raises(ConfigError, match="Schema Registry"):
        cfg.schema_registry_conf()


def test_falta_la_ruta_del_properties():
    with pytest.raises(ConfigError, match="client_properties_path"):
        KafkaConfig.from_dict({})


def test_el_engine_carga_kafka_cuando_esta_declarado(client_properties):
    cfg = EngineConfig.from_dict({
        "storage_account": "cuenta",
        "catalog": "catalogo",
        "kafka": {"client_properties_path": client_properties},
    })

    assert cfg.kafka is not None
    assert cfg.kafka.spark_options()["kafka.sasl.mechanism"] == "PLAIN"
