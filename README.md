# Motor de ingesta FarmIA

Este motor lleva los datos de FarmIA desde la capa **landing** hasta la capa **bronze** de un lakehouse en Azure Databricks.

Lee ficheros por lotes con Databricks Autoloader (CSV, JSON, Parquet, Avro e imágenes) y eventos desde Apache Kafka (JSON y Avro con Schema Registry). Todo lo que cambia de una fuente a otra vive en un fichero JSON, así que dar de alta una fuente nueva no toca el código.

El diseño de la arquitectura está en [`docs/arquitectura.md`](docs/arquitectura.md).

---

## Índice

1. [Estructura del proyecto](#1-estructura-del-proyecto)
2. [Requisitos previos](#2-requisitos-previos)
3. [Despliegue](#3-despliegue)
4. [Configuración](#4-configuración)
5. [Ejecución](#5-ejecución)
6. [Ejemplo de ejecución](#6-ejemplo-de-ejecución)
7. [Preparación de los datos de prueba](#7-preparación-de-los-datos-de-prueba)
8. [Decisiones técnicas](#8-decisiones-técnicas)

---

## 1. Estructura del proyecto

```
tarea-lakehouse/
├── configs/
│   ├── engine.json              Ajustes comunes: cuenta, catálogo, Kafka
│   └── datasets/                Un fichero por dataset (10 en total)
│       ├── 01_product_catalog.json
│       ├── ...
│       └── 10_sensors_stream.json
├── src/farmia_ingest/
│   ├── __init__.py              API pública del paquete
│   ├── config.py                Carga y validación de la configuración
│   ├── readers.py               Lectores: Autoloader y Kafka
│   ├── writers.py               Escritura Delta en bronze
│   ├── engine.py                Orquestación, errores y auditoría
│   └── logging_utils.py         Configuración de logs
├── notebooks/
│   ├── 00_setup_y_datasets_farmia.py   Catálogo, esquemas y datasets base
│   ├── 01_datasets_extra_formatos.py   CSV, Avro e imágenes
│   ├── 02_liberar_incrementales.py     Simula la llegada de ficheros nuevos
│   ├── 03_kafka_producer.py            Produce eventos a los topics
│   └── run_engine.py                   Lanzador del motor
└── docs/
    ├── arquitectura.md          Diseño del lakehouse
    ├── diagrama-arquitectura.png
    └── capturas/                Evidencias de ejecución
```

El paquete `farmia_ingest` es el núcleo del proyecto. Los notebooks `00` a `03` son material de apoyo para generar datos de prueba. `run_engine` es el lanzador y no contiene lógica.

### Responsabilidad de cada módulo

| Módulo | Qué hace |
|---|---|
| `config.py` | Convierte los JSON en objetos validados. Falla al cargar, no a mitad de ejecución |
| `readers.py` | Devuelve un DataFrame de streaming, venga de ficheros o de Kafka |
| `writers.py` | Escribe en Delta con particionado y registra la tabla en el catálogo |
| `engine.py` | Lanza las consultas, captura errores por dataset y escribe la auditoría |
| `logging_utils.py` | Un único formato de log para todos los módulos |

## 2. Requisitos previos

**En Azure:**

- Cuenta de almacenamiento **ADLS Gen2** con espacio de nombres jerárquico activado y dos contenedores: `landing` y `lakehouse`.
- Workspace de **Azure Databricks** con Unity Catalog habilitado.
- **Access Connector for Azure Databricks** con el rol *Storage Blob Data Contributor* sobre la cuenta de almacenamiento.

**En Unity Catalog:**

- Una *storage credential* que apunte al Access Connector.
- Dos *external locations* sobre los contenedores: `landing_loc` y `lakehouse_loc`.
- Un catálogo con cuatro esquemas: `farmia_bronze`, `farmia_silver`, `farmia_gold` y `farmia_ops`.

**Para la ingesta de eventos:**

- Un cluster de **Kafka** accesible. Este trabajo usa Confluent Cloud.
- **Schema Registry** con una API key propia. Importante: una key de ámbito *Global* no sirve para el Schema Registry, hay que crearla con ámbito *Schema Registry*.

**Cómputo:**

Funciona en cómputo **serverless**, que es lo que se usó aquí. También en un cluster clásico con Databricks Runtime 13.3 o superior.

## 3. Despliegue

Partiendo de cero, el orden es:

1. Subir el proyecto al workspace (3.1).
2. Dejar el `client.properties` fuera del proyecto y apuntarlo desde `engine.json` (3.2), solo si vas a usar Kafka.
3. Ejecutar los notebooks `00` y `01` para crear los esquemas y generar datos en landing (sección 7).
4. Crear los topics y ejecutar el notebook `03`, si vas a usar Kafka (sección 7).
5. Lanzar `run_engine` (sección 5).

### 3.1 Subir el proyecto al workspace

Con la [CLI de Databricks](https://docs.databricks.com/dev-tools/cli/index.html) autenticada:

```bash
databricks sync ./tarea-lakehouse /Users/<usuario>/tarea-lakehouse \
  --exclude "**/__pycache__/**"
```

Usa `sync` y no `workspace import-dir`: este último convierte los ficheros `.py` en notebooks, y entonces el paquete deja de poder importarse.

También vale copiar los ficheros a mano desde la interfaz (*Workspace → Create → File*), respetando la estructura de carpetas.

### 3.2 Credenciales de Kafka

El motor lee la conexión a Kafka de un fichero `client.properties` con el formato que descarga Confluent Cloud:

```properties
bootstrap.servers=pkc-xxxxx.region.azure.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=<api key del cluster>
sasl.password=<api secret del cluster>

schema.registry.url=https://psrc-xxxxx.region.azure.confluent.cloud
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<api key del registry>:<api secret del registry>
```

**Este fichero debe vivir fuera del proyecto.** En esta implementación está en `/Workspace/Users/<usuario>/farmia-secrets/client.properties`, y `engine.json` solo guarda su ruta. Así no hay credenciales en el código ni en el repositorio.

En un entorno de producción lo correcto sería usar *secret scopes* de Databricks en lugar de un fichero.

### 3.3 Dependencias

Solo se necesita una librería externa, y únicamente si hay datasets en Avro:

```python
%pip install "confluent-kafka[avro,schemaregistry]"
```

Los extras no son opcionales: el cliente del Schema Registry necesita `authlib` y `fastavro`, que no vienen en la instalación básica.

## 4. Configuración

### 4.1 `engine.json`

Ajustes comunes a todas las ingestas:

```json
{
  "storage_account": "masterob001sta",
  "landing_container": "landing",
  "lakehouse_container": "lakehouse",

  "catalog": "masterob001dbw",
  "bronze_schema": "farmia_bronze",
  "ops_schema": "farmia_ops",
  "audit_table": "ingestion_audit",

  "register_tables": true,

  "kafka": {
    "client_properties_path": "/Workspace/Users/<usuario>/farmia-secrets/client.properties"
  }
}
```

| Campo | Obligatorio | Descripción |
|---|---|---|
| `storage_account` | Sí | Cuenta de ADLS Gen2 |
| `landing_container` | No (`landing`) | Contenedor de la zona de aterrizaje |
| `lakehouse_container` | No (`lakehouse`) | Contenedor del lakehouse |
| `catalog` | Sí | Catálogo de Unity Catalog |
| `bronze_schema` | No (`farmia_bronze`) | Esquema donde se registran las tablas |
| `ops_schema` | No (`farmia_ops`) | Esquema de la tabla de auditoría |
| `audit_table` | No (`ingestion_audit`) | Nombre de la tabla de auditoría |
| `register_tables` | No (`true`) | Si se registran las tablas en el catálogo |
| `kafka` | No | Solo si hay datasets de tipo `kafka` |

### 4.2 Configuración de un dataset

Cada fichero de `configs/datasets/` describe una fuente. El nombre del fichero solo determina el orden de carga.

**Ingesta de ficheros:**

```json
{
  "datasource": "farmia",
  "dataset": "shipments",
  "description": "Envíos de proveedores y logística.",
  "enabled": true,

  "source": {
    "type": "files",
    "format": "csv",
    "path": "{landing}/farmia/shipments",
    "schema": "shipment_id STRING, supplier_id STRING, units INT, dispatch_ts TIMESTAMP",
    "options": { "header": "true" }
  },

  "sink": {
    "layer": "bronze",
    "derived_partitions": { "_ingested_date": "to_date(_ingested_at)" },
    "partition_by": ["_ingested_date"]
  }
}
```

**Ingesta de eventos:**

```json
{
  "datasource": "farmia",
  "dataset": "sensors_stream",
  "enabled": true,

  "source": {
    "type": "kafka",
    "subscribe_pattern": "farmia\\.sensors\\..*",
    "starting_offsets": "earliest",
    "key_format": "string",
    "value_format": "avro",
    "value_subject": "farmia.sensors.norte-value"
  },

  "sink": {
    "layer": "bronze",
    "partition_by": ["_topic"]
  }
}
```

### 4.3 Referencia de campos

**Comunes**

| Campo | Obligatorio | Descripción |
|---|---|---|
| `datasource` | Sí | Agrupa datasets de un mismo origen. Forma parte de la ruta y del nombre de tabla |
| `dataset` | Sí | Nombre del dataset |
| `enabled` | No (`true`) | Permite desactivar un dataset sin borrar su fichero |
| `description` | No | Documentación del propio fichero, el motor no la usa |

**`source` con `type: "files"`**

| Campo | Obligatorio | Descripción |
|---|---|---|
| `format` | Sí | `csv`, `json`, `parquet`, `avro` o `binaryFile` |
| `path` | Sí | Ruta en landing. Admite `{landing}` y `{lakehouse}` |
| `schema` | No | Esquema esperado en DDL. Si se declara, Autoloader no infiere |
| `schema_evolution_mode` | No | `addNewColumns`, `rescue`, `failOnNewColumns` o `none`. Si se omite, el motor lo deduce |
| `options` | No | Opciones adicionales de Autoloader, tal cual las espera Spark |

**`source` con `type: "kafka"`**

| Campo | Obligatorio | Descripción |
|---|---|---|
| `subscribe` | Uno de los dos | Topic concreto |
| `subscribe_pattern` | Uno de los dos | Expresión regular de topics |
| `value_format` | Sí | `json`, `avro`, `string` o `binary` |
| `key_format` | No (`string`) | Mismo juego de valores |
| `value_subject` | Si es Avro | Subject del Schema Registry. Con `subscribe_pattern` no se puede derivar de cada topic, así que se declara uno y vale para todos los del patrón, que comparten esquema |
| `key_subject` | Si la clave es Avro | Subject de la clave |
| `value_json_schema` | Si es JSON | Esquema DDL del mensaje |
| `key_json_schema` | Si la clave es JSON | Esquema DDL de la clave |
| `starting_offsets` | No (`earliest`) | `earliest`, `latest` o un JSON de offsets |
| `options` | No | Opciones adicionales del conector de Kafka |

**`sink`**

| Campo | Obligatorio | Descripción |
|---|---|---|
| `layer` | No (`bronze`) | Única capa soportada en esta versión |
| `path` | No | Ruta de destino. Si se omite: `{lakehouse}/bronze/{datasource}/{dataset}` |
| `format` | No (`delta`) | Formato de escritura |
| `partition_by` | No | Columnas de particionado |
| `derived_partitions` | No | Columnas calculadas con SQL antes de escribir |
| `trigger` | No | `{"processing_time": "30 seconds"}` para consulta continua. Por defecto `availableNow` |
| `options` | No | Opciones adicionales del escritor |

### 4.4 Columnas que añade el motor

| Columna | Origen | Contenido |
|---|---|---|
| `_ingested_at` | Todos | Momento de la ingesta |
| `_ingested_filename` | Ficheros | Nombre del fichero de origen |
| `_ingested_filepath` | Ficheros | Ruta completa del fichero |
| `_rescued_data` | Ficheros salvo `binaryFile` | Campos que no encajan con el esquema |
| `_topic`, `_partition`, `_offset`, `_kafka_timestamp` | Kafka | Coordenadas del mensaje |
| `key`, `value` | Kafka | Clave y contenido ya decodificados |

## 5. Ejecución

### Desde el notebook

Abrir `notebooks/run_engine` y ejecutarlo. El notebook localiza el paquete a partir de su propia ruta, carga las configuraciones y lanza el motor.

Para procesar solo algunos datasets, escribir sus nombres separados por comas en el widget `datasets` de la parte superior:

```
farmia.sensors_stream,farmia.weather
```

### Desde código

```python
import sys

PROYECTO = "/Workspace/Users/<usuario>/tarea-lakehouse"
sys.path.insert(0, f"{PROYECTO}/src")

from farmia_ingest import EngineConfig, load_dataset_configs, ejecutar

engine = EngineConfig.load(f"{PROYECTO}/configs/engine.json")
datasets = load_dataset_configs(f"{PROYECTO}/configs/datasets")

resultados = ejecutar(spark, engine, datasets)
```

`ejecutar` devuelve una lista de `ResultadoIngesta` con las filas escritas, la duración y el estado de cada dataset.

### Programar la ejecución horaria

El motor está pensado para ejecutarse cada hora. En Databricks se consigue creando un *Job* que apunte a `run_engine` con una programación `0 0 * * * ?`. El widget `datasets` se puede pasar como parámetro del job.

### Auditoría

Cada ejecución escribe una fila por dataset en `<catálogo>.farmia_ops.ingestion_audit`:

```sql
SELECT run_id, dataset, source_format, rows_ingested, status,
       timestampdiff(SECOND, started_at, finished_at) AS segundos
FROM masterob001dbw.farmia_ops.ingestion_audit
ORDER BY started_at DESC;
```

## 6. Ejemplo de ejecución

Ejecución de los diez datasets después de que llegaran ficheros nuevos a landing y mensajes nuevos a los topics.

![Ejecución del motor](docs/capturas/ejecucion-motor.png)

**(1)** Suscripción al topic `farmia.app_events` y al patrón `farmia\.sensors\..*`, con el esquema Avro recuperado del Schema Registry.
**(2)** Ingesta incremental de los ocho datasets de fichero. `field_images` reporta cero porque no llegó ninguna imagen nueva: el resto procesó **solo los ficheros nuevos**, sin reprocesar los de cargas anteriores. Esa es la prueba de que la ingesta es incremental.
**(3)** Ingesta de los dos datasets de eventos: 300 mensajes JSON y 200 en Avro. Las 200 lecturas de sensores vienen de dos topics distintos, 100 de cada uno, recogidos por el patrón sin nombrarlos.
**(4)** Auditoría registrada y resumen: diez datasets correctos, 1382 filas, ningún error.

Desglose de esa ejecución:

| Dataset | Origen | Formato | Filas nuevas |
|---|---|---|---|
| `product_catalog` | ficheros | parquet | 30 |
| `inventory` | ficheros | parquet | 180 |
| `orders_cdc` | ficheros | json | 90 |
| `sensors` | ficheros | json | 150 |
| `app_events` | ficheros | json | 300 |
| `shipments` | ficheros | csv | 100 |
| `weather` | ficheros | avro | 32 |
| `field_images` | ficheros | binaryFile | 0 |
| `app_events_stream` | kafka | json | 300 |
| `sensors_stream` | kafka | avro | 200 |

### Registro de auditoría

![Tabla de auditoría](docs/capturas/auditoria.png)

Dos ejecuciones distintas identificadas por su `run_id`. La de las 10:52 recogió las filas nuevas de cada dataset. La anterior, de las 09:31, devolvió cero en todos porque no había llegado nada nuevo desde la ejecución previa.

## 7. Preparación de los datos de prueba

Los notebooks de apoyo, en orden:

| Notebook | Qué hace |
|---|---|
| `00_setup_y_datasets_farmia` | Crea los cuatro esquemas y la tabla de auditoría, y genera cinco datasets en landing |
| `01_datasets_extra_formatos` | Genera los datasets de CSV, Avro e imágenes |
| `02_liberar_incrementales` | Copia una segunda tanda de ficheros a landing para probar la ingesta incremental |
| `03_kafka_producer` | Crea los mensajes de los topics, en JSON y en Avro |

Antes de ejecutar `03`, hay que crear los topics en Kafka: `farmia.app_events`, `farmia.sensors.norte` y `farmia.sensors.sur`.

Las imágenes se escriben a través de un volumen externo de Unity Catalog, porque en cómputo serverless no se puede escribir un fichero binario directamente en una ruta `abfss://` con Python.

## 8. Decisiones técnicas

**Structured Streaming para ambos casos.** Tanto Autoloader como Kafka se leen con la misma API. Eso hace que la escritura, los checkpoints, el registro de tablas y la auditoría sean idénticos para lotes y para eventos: el motor tiene dos lectores, no dos mitades.

**Un fallo no propaga.** Cada dataset se arranca y se espera de forma independiente. Si uno falla, se registra en el log y en la auditoría, y el resto continúa. En una ingesta nocturna es preferible perder una fuente y saberlo, a perderlas todas por un fichero corrupto.

**El modo de evolución de esquema se deduce.** Autoloader rechaza `addNewColumns` cuando se declara el esquema, y `binaryFile` solo admite `none`. En lugar de obligar a acertarlo en cada JSON, el motor lo deriva de la propia configuración. Se puede forzar con `schema_evolution_mode`.

**Las filas escritas se cuentan desde el historial de Delta.** En cómputo serverless, `query.recentProgress` viene vacío en cuanto la consulta termina. El historial de Delta registra en cada commit cuántas filas se añadieron, y funciona en cualquier runtime.

**Registrar la tabla no puede invalidar la ingesta.** El registro en el catálogo ocurre después de escribir y con su propio control de errores: si falla, se avisa, pero el dataset no se marca como fallido, porque los datos ya están escritos.

**Cabecera Confluent en Avro.** Los mensajes de Confluent llevan cinco bytes delante (un byte de control y cuatro con el identificador del esquema) que hay que descartar antes de decodificar. El motor lo hace al construir la expresión de deserialización.

## Procedencia

Trabajo realizado para la asignatura de diseño de ingestas y lagos de datos del Máster en Big Data & Data Engineering de la Universidad Complutense de Madrid.

El escenario de la empresa y los requisitos del motor los planteaba el curso. El diseño de la arquitectura, el motor de ingesta, la configuración de los datasets y la documentación son míos. Los datos son sintéticos: se generan con los notebooks `00` y `01`.

El diseño completo de la arquitectura está en [`docs/arquitectura.md`](docs/arquitectura.md).
