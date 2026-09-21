# FarmIA ingestion engine

*[Versión en español](README.es.md)*

This engine moves FarmIA's data from the **landing** zone to the **bronze** layer of a lakehouse on Azure Databricks.

It reads files in batch with Databricks Autoloader (CSV, JSON, Parquet, Avro and images) and events from Apache Kafka (JSON and Avro with Schema Registry). Everything that changes from one source to another lives in a JSON file, so onboarding a new source never touches the code.

The architecture design is in [`docs/arquitectura.md`](docs/arquitectura.md) (Spanish).

---

## Table of contents

1. [Project structure](#1-project-structure)
2. [Prerequisites](#2-prerequisites)
3. [Deployment](#3-deployment)
4. [Configuration](#4-configuration)
5. [Running the engine](#5-running-the-engine)
6. [Example run](#6-example-run)
7. [Test data](#7-test-data)
8. [Tests](#8-tests)
9. [Design decisions](#9-design-decisions)

---

## 1. Project structure

```
farmia-lakehouse-ingesta/
├── configs/
│   ├── engine.json              Shared settings: account, catalog, Kafka
│   └── datasets/                One file per dataset (10 in total)
│       ├── 01_product_catalog.json
│       ├── ...
│       └── 10_sensors_stream.json
├── src/farmia_ingest/
│   ├── __init__.py              Public API
│   ├── config.py                Loads and validates configuration
│   ├── readers.py               Readers: Autoloader and Kafka
│   ├── writers.py               Delta writes into bronze
│   ├── engine.py                Orchestration, error handling, audit
│   └── logging_utils.py         Logging setup
├── notebooks/
│   ├── 00_setup_y_datasets_farmia.py   Catalog, schemas and base datasets
│   ├── 01_datasets_extra_formatos.py   CSV, Avro and images
│   ├── 02_liberar_incrementales.py     Simulates new files arriving
│   ├── 03_kafka_producer.py            Produces events to the topics
│   └── run_engine.py                   Engine launcher
├── tests/                       Unit tests for the configuration layer
└── docs/
    ├── arquitectura.md          Lakehouse design
    ├── diagrama-arquitectura.png
    └── capturas/                Execution evidence
```

The `farmia_ingest` package is the core of the project. Notebooks `00` to `03` are supporting material that generates test data. `run_engine` is the launcher and holds no logic.

### What each module does

| Module | Responsibility |
|---|---|
| `config.py` | Turns the JSON files into validated objects. Fails on load, not halfway through a run |
| `readers.py` | Returns a streaming DataFrame, whether it comes from files or from Kafka |
| `writers.py` | Writes Delta with partitioning and registers the table in the catalog |
| `engine.py` | Starts the queries, captures per-dataset errors and writes the audit record |
| `logging_utils.py` | A single log format across every module |

## 2. Prerequisites

**On Azure:**

- An **ADLS Gen2** storage account with hierarchical namespace enabled and two containers: `landing` and `lakehouse`.
- An **Azure Databricks** workspace with Unity Catalog enabled.
- An **Access Connector for Azure Databricks** granted the *Storage Blob Data Contributor* role on the storage account.

**In Unity Catalog:**

- A storage credential pointing at the Access Connector.
- Two external locations over the containers: `landing_loc` and `lakehouse_loc`.
- A catalog with four schemas: `farmia_bronze`, `farmia_silver`, `farmia_gold` and `farmia_ops`.

**For event ingestion:**

- A reachable **Kafka** cluster. This project uses Confluent Cloud.
- A **Schema Registry** with its own API key. Note: a key scoped as *Global* does not work against the Schema Registry, it has to be created with the *Schema Registry* scope.

**Compute:**

Runs on **serverless** compute, which is what was used here. It also runs on a classic cluster with Databricks Runtime 13.3 or later.

## 3. Deployment

Starting from scratch, the order is:

1. Upload the project to the workspace (3.1).
2. Place `client.properties` outside the project and point `engine.json` at it (3.2), only if you are using Kafka.
3. Run notebooks `00` and `01` to create the schemas and generate data in landing (section 7).
4. Create the topics and run notebook `03`, if you are using Kafka (section 7).
5. Launch `run_engine` (section 5).

### 3.1 Upload the project to the workspace

With the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) authenticated:

```bash
databricks sync ./farmia-lakehouse-ingesta /Users/<user>/farmia-lakehouse-ingesta \
  --exclude "**/__pycache__/**"
```

Use `sync` rather than `workspace import-dir`: the latter turns `.py` files into notebooks, which breaks the package imports.

Copying the files by hand through the UI (*Workspace → Create → File*) also works, as long as the folder structure is preserved.

### 3.2 Kafka credentials

The engine reads the Kafka connection from a `client.properties` file in the format Confluent Cloud hands out:

```properties
bootstrap.servers=pkc-xxxxx.region.azure.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=<cluster api key>
sasl.password=<cluster api secret>

schema.registry.url=https://psrc-xxxxx.region.azure.confluent.cloud
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<registry api key>:<registry api secret>
```

**This file must live outside the project.** In this implementation it sits at `/Workspace/Users/<user>/farmia-secrets/client.properties`, and `engine.json` only stores its path, so no credentials end up in the code or in the repository.

In production the right answer would be Databricks secret scopes rather than a file.

### 3.3 Dependencies

Only one external library is needed, and only when there are Avro datasets:

```python
%pip install "confluent-kafka[avro,schemaregistry]"
```

The extras are not optional: the Schema Registry client needs `authlib` and `fastavro`, which the base install does not bring.

## 4. Configuration

### 4.1 `engine.json`

Settings shared by every ingestion:

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
    "client_properties_path": "/Workspace/Users/<user>/farmia-secrets/client.properties"
  }
}
```

| Field | Required | Description |
|---|---|---|
| `storage_account` | Yes | ADLS Gen2 account |
| `landing_container` | No (`landing`) | Landing zone container |
| `lakehouse_container` | No (`lakehouse`) | Lakehouse container |
| `catalog` | Yes | Unity Catalog catalog |
| `bronze_schema` | No (`farmia_bronze`) | Schema where tables are registered |
| `ops_schema` | No (`farmia_ops`) | Schema holding the audit table |
| `audit_table` | No (`ingestion_audit`) | Audit table name |
| `register_tables` | No (`true`) | Whether tables get registered in the catalog |
| `kafka` | No | Only needed when there are `kafka` datasets |

### 4.2 Dataset configuration

Each file under `configs/datasets/` describes one source. The file name only determines load order.

**File ingestion:**

```json
{
  "datasource": "farmia",
  "dataset": "shipments",
  "description": "Supplier and logistics shipments.",
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

**Event ingestion:**

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

### 4.3 Field reference

**Common**

| Field | Required | Description |
|---|---|---|
| `datasource` | Yes | Groups datasets from the same origin. Part of the path and the table name |
| `dataset` | Yes | Dataset name |
| `enabled` | No (`true`) | Disables a dataset without deleting its file |
| `description` | No | Documents the file itself, the engine does not read it |

**`source` with `type: "files"`**

| Field | Required | Description |
|---|---|---|
| `format` | Yes | `csv`, `json`, `parquet`, `avro` or `binaryFile` |
| `path` | Yes | Landing path. Accepts `{landing}` and `{lakehouse}` |
| `schema` | No | Expected schema as DDL. When declared, Autoloader does not infer |
| `schema_evolution_mode` | No | `addNewColumns`, `rescue`, `failOnNewColumns` or `none`. When omitted, the engine derives it |
| `options` | No | Extra Autoloader options, exactly as Spark expects them |

**`source` with `type: "kafka"`**

| Field | Required | Description |
|---|---|---|
| `subscribe` | One of the two | A specific topic |
| `subscribe_pattern` | One of the two | Regular expression over topic names |
| `value_format` | Yes | `json`, `avro`, `string` or `binary` |
| `key_format` | No (`string`) | Same set of values |
| `value_subject` | If Avro | Schema Registry subject. With `subscribe_pattern` it cannot be derived per topic, so one is declared and serves every topic in the pattern, which share a schema |
| `key_subject` | If the key is Avro | Subject for the key |
| `value_json_schema` | If JSON | DDL schema of the message |
| `key_json_schema` | If the key is JSON | DDL schema of the key |
| `starting_offsets` | No (`earliest`) | `earliest`, `latest` or an offsets JSON |
| `options` | No | Extra options for the Kafka connector |

**`sink`**

| Field | Required | Description |
|---|---|---|
| `layer` | No (`bronze`) | The only layer this version writes to |
| `path` | No | Target path. When omitted: `{lakehouse}/bronze/{datasource}/{dataset}` |
| `format` | No (`delta`) | Write format |
| `partition_by` | No | Partition columns |
| `derived_partitions` | No | Columns computed with SQL before writing |
| `trigger` | No | `{"processing_time": "30 seconds"}` for a continuous query. Defaults to `availableNow` |
| `options` | No | Extra writer options |

### 4.4 Columns the engine adds

| Column | Source | Content |
|---|---|---|
| `_ingested_at` | All | Ingestion timestamp |
| `_ingested_filename` | Files | Name of the source file |
| `_ingested_filepath` | Files | Full path of the source file |
| `_rescued_data` | Files except `binaryFile` | Fields that do not match the schema |
| `_topic`, `_partition`, `_offset`, `_kafka_timestamp` | Kafka | Message coordinates |
| `key`, `value` | Kafka | Key and payload, already decoded |

## 5. Running the engine

### From the notebook

Open `notebooks/run_engine` and run it. The notebook locates the package from its own path, loads the configuration and starts the engine.

To process only some datasets, type their names separated by commas in the `datasets` widget at the top:

```
farmia.sensors_stream,farmia.weather
```

### From code

```python
import sys

PROJECT = "/Workspace/Users/<user>/farmia-lakehouse-ingesta"
sys.path.insert(0, f"{PROJECT}/src")

from farmia_ingest import EngineConfig, load_dataset_configs, ejecutar

engine = EngineConfig.load(f"{PROJECT}/configs/engine.json")
datasets = load_dataset_configs(f"{PROJECT}/configs/datasets")

results = ejecutar(spark, engine, datasets)
```

`ejecutar` returns a list of `ResultadoIngesta` with rows written, duration and status per dataset.

### Scheduling an hourly run

The engine is meant to run every hour. In Databricks that means a Job pointing at `run_engine` with a `0 0 * * * ?` schedule. The `datasets` widget can be passed as a job parameter.

### Audit

Every run writes one row per dataset into `<catalog>.farmia_ops.ingestion_audit`:

```sql
SELECT run_id, dataset, source_format, rows_ingested, status,
       timestampdiff(SECOND, started_at, finished_at) AS seconds
FROM masterob001dbw.farmia_ops.ingestion_audit
ORDER BY started_at DESC;
```

## 6. Example run

All ten datasets, after new files landed and new messages reached the topics.

![Engine run](docs/capturas/ejecucion-motor.png)

**(1)** Subscription to the `farmia.app_events` topic and to the `farmia\.sensors\..*` pattern, with the Avro schema pulled from the Schema Registry.
**(2)** Incremental ingestion of the eight file datasets. `field_images` reports zero because no new image arrived: every other dataset processed **only the new files**, without reprocessing earlier loads. That is the proof that ingestion is incremental.
**(3)** Ingestion of the two event datasets: 300 JSON messages and 200 in Avro. The 200 sensor readings come from two different topics, 100 each, picked up by the pattern without naming them.
**(4)** Audit written and summary: ten datasets successful, 1382 rows, no errors.

Breakdown of that run:

| Dataset | Source | Format | New rows |
|---|---|---|---|
| `product_catalog` | files | parquet | 30 |
| `inventory` | files | parquet | 180 |
| `orders_cdc` | files | json | 90 |
| `sensors` | files | json | 150 |
| `app_events` | files | json | 300 |
| `shipments` | files | csv | 100 |
| `weather` | files | avro | 32 |
| `field_images` | files | binaryFile | 0 |
| `app_events_stream` | kafka | json | 300 |
| `sensors_stream` | kafka | avro | 200 |

### Audit record

![Audit table](docs/capturas/auditoria.png)

Two separate runs identified by their `run_id`. The 10:52 one picked up the new rows of each dataset. The earlier one, at 09:31, returned zero across the board because nothing new had arrived since the previous run.

## 7. Test data

The supporting notebooks, in order:

| Notebook | What it does |
|---|---|
| `00_setup_y_datasets_farmia` | Creates the four schemas and the audit table, and generates five datasets in landing |
| `01_datasets_extra_formatos` | Generates the CSV, Avro and image datasets |
| `02_liberar_incrementales` | Copies a second batch of files into landing to exercise incremental ingestion |
| `03_kafka_producer` | Produces the topic messages, in JSON and in Avro |

Before running `03`, the topics have to exist in Kafka: `farmia.app_events`, `farmia.sensors.norte` and `farmia.sensors.sur`.

Images are written through a Unity Catalog external volume, because serverless compute cannot write a binary file straight to an `abfss://` path from Python.

## 8. Tests

The configuration layer is the part of the engine that can be checked without a cluster, and the part where a mistake travels furthest: a badly validated dataset is not discovered until the ingestion is already halfway through. So that is what the tests cover.

```bash
pip install -r requirements-dev.txt
pytest
```

27 tests, no Spark and no cloud involved, running in well under a second: default values and required fields, the `abfss://` paths the engine builds, the expansion of `{landing}` and `{lakehouse}` in dataset paths, every branch of dataset validation (unsupported formats, a Kafka source with no topic, an unknown source type, a layer other than bronze), the loading of a directory of configurations with its ordering and its filter, and the translation of Confluent's `client.properties` into the options the Spark connector expects.

Importing the configuration does not pull in PySpark: `engine.py` is imported lazily, so the tests run on a plain Python install.

## 9. Design decisions

**Structured Streaming for both paths.** Autoloader and Kafka are read through the same API, which makes writing, checkpointing, table registration and auditing identical for batch and for events. The engine has two readers, not two halves.

**A failure does not propagate.** Each dataset starts and is awaited independently. If one fails it is logged and audited, and the rest carry on. In an overnight ingestion, losing one source and knowing about it beats losing all of them to a single corrupt file.

**Schema evolution mode is derived.** Autoloader rejects `addNewColumns` when the schema is declared, and `binaryFile` only accepts `none`. Rather than forcing every JSON to get it right, the engine derives it from the configuration itself. It can still be forced with `schema_evolution_mode`.

**Written rows are counted from the Delta history.** On serverless compute, `query.recentProgress` comes back empty as soon as the query ends. The Delta history records how many rows each commit added, and works on any runtime.

**Registering a table cannot invalidate an ingestion.** Catalog registration happens after the write and with its own error handling: if it fails it is logged, but the dataset is not marked as failed, because the data is already written.

**Confluent's Avro wire format.** Confluent messages carry five bytes up front (a magic byte plus four with the schema id) that have to be stripped before decoding. The engine does that when building the deserialization expression.

## About this project

FarmIA is a fictional company and the scenario is invented: an agricultural retailer expecting its data volume to triple in two years, with six sources that have nothing in common. All the data is synthetic, generated by notebooks `00` and `01`.

The architecture design, the ingestion engine, the dataset configuration, the tests and the documentation are my own work.

The full architecture design is in [`docs/arquitectura.md`](docs/arquitectura.md), in Spanish.
