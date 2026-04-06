# Data Weaver

**Declarative data pipelines on Apache Spark.** Define ETL, RAG, and LLM-powered pipelines in YAML. Validate, plan, and execute — locally or on any Spark cluster.

```bash
weaver init my-project && cd my-project
weaver validate pipelines/example_pipeline.yaml
weaver apply pipelines/example_pipeline.yaml
```

## Why Data Weaver?

- **Pure YAML** — No Scala/Python code required. One file defines sources, transforms, quality checks, and sinks
- **5-minute start** — `weaver init` scaffolds a working project. No Spark installation needed for local dev
- **14 connectors** — PostgreSQL, MySQL, Kafka, MongoDB, REST APIs, BigQuery, DeltaLake, Elasticsearch, files (CSV/Parquet/JSON)
- **LLM transforms** — Use Claude, OpenAI, or local Ollama models as transformation steps
- **RAG pipelines** — Chunking, embedding, and graph extraction in the same YAML
- **AI generation** — `weaver generate "description"` creates pipelines from natural language
- **Terraform workflow** — `validate` → `plan` → `apply` with human-readable error messages
- **Parallel execution** — DAG resolver automatically parallelizes independent transforms
- **Data quality** — Inline checks with `abort`, `warn`, or `skip` on failure

## Installation

**Requirements:** Java 17+

```bash
# One-line install
curl -fsSL https://raw.githubusercontent.com/netsirius/data-weaver/main/scripts/install.sh | bash

# Or with SDKMAN
sdk install java 17.0.13-zulu
```

**From source:**
```bash
git clone https://github.com/netsirius/data-weaver.git
cd data-weaver
sbt "cli/assembly"
```

## Quick Start

```bash
# Create a new project
weaver init my-project
cd my-project

# Check everything is correct
weaver doctor pipelines/example_pipeline.yaml

# See execution plan
weaver plan pipelines/example_pipeline.yaml

# Run the pipeline
weaver apply pipelines/example_pipeline.yaml
```

## Pipeline Example

```yaml
name: CustomerETL
engine: auto
tag: daily

dataSources:
  - id: customers
    type: PostgreSQL
    connection: pg-prod
    config:
      query: "SELECT * FROM customers WHERE updated_at > '${date.yesterday}'"

  - id: orders
    type: File
    config:
      path: s3://data-lake/orders/
      format: parquet

transformations:
  - id: enriched
    type: SQL
    sources: [customers, orders]
    query: >
      SELECT c.*, COUNT(o.id) as order_count
      FROM customers c
      LEFT JOIN orders o ON c.id = o.customer_id
      GROUP BY c.id

  - id: validated
    type: DataQuality
    sources: [enriched]
    checks:
      - row_count > 0
      - missing_count(email) = 0
      - duplicate_count(id) = 0
    onFail: abort

sinks:
  - id: warehouse
    type: DeltaLake
    source: validated
    config:
      path: s3://warehouse/customers
      saveMode: merge
      mergeKey: id

profiles:
  dev:
    engine: local
  prod:
    engine: spark

tests:
  - name: "has data"
    assert: warehouse.row_count > 0
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `weaver init <project>` | Scaffold a new project with example pipeline |
| `weaver init --interactive` | Step-by-step pipeline wizard (no LLM) |
| `weaver generate "<description>"` | AI: natural language to YAML pipeline |
| `weaver doctor <pipeline>` | Full system diagnostic |
| `weaver validate <pipeline>` | Validate YAML, schema, and DAG |
| `weaver plan <pipeline>` | Dry-run: show what will execute |
| `weaver explain <pipeline>` | Visualize the execution DAG |
| `weaver inspect <pipeline> <id>` | Show source or transform details |
| `weaver test <pipeline>` | Run inline tests |
| `weaver test --coverage` | Show test coverage report |
| `weaver apply <pipeline>` | Execute the pipeline |
| `weaver apply --env prod` | Execute with environment profile |
| `weaver install <artifact>` | Install a connector plugin |
| `weaver scaffold connector <name>` | Generate a connector project (ready to build) |
| `weaver scaffold transform <name>` | Generate a transform plugin project |
| `weaver scaffold registry <name>` | Generate a private connector registry |
| `weaver install <artifact>` | Install a connector plugin |
| `weaver list connectors` | List available source and sink connectors |
| `weaver list transforms` | List available transformation types |
| `weaver list plugins` | List installed plugin JARs |

## Connectors

### Sources
| Type | Description |
|------|-------------|
| `PostgreSQL` | JDBC reader with health check |
| `MySQL` | JDBC reader (MySQL + SQL Server) |
| `File` | CSV, Parquet, JSON, ORC with format auto-detection |
| `Kafka` | Batch + streaming modes |
| `MongoDB` | Collection reads with aggregation pipeline |
| `REST` | Generic API with pagination and auth (bearer, API key) |
| `BigQuery` | Table reads and SQL queries |
| `Test` | JSON file reader for testing |

### Sinks
| Type | Description |
|------|-------------|
| `BigQuery` | Write to BigQuery tables |
| `DeltaLake` | Overwrite, Append, and Merge (upsert) |
| `File` | CSV, Parquet, JSON, ORC with partitioning |
| `Kafka` | Batch + streaming with checkpointing |
| `Elasticsearch` | Spark ES connector + REST bulk fallback |
| `Test` | JSON file writer for testing |

## Transforms

| Type | Description |
|------|-------------|
| `SQL` | Standard SQL queries on registered temp views |
| `DataQuality` | Quality gate: row_count, missing_count, duplicate_count |
| `LLMTransform` | Generic LLM-as-transformation with prompt templating |
| `Chunking` | Split documents (fixed, sentence, recursive strategies) |
| `Embedding` | Vector embeddings via OpenAI, Vertex AI, Cohere |
| `GraphExtraction` | Entity/relationship extraction using LLM |

### LLM Provider Support

| Provider | Config | API Key | Default Model |
|----------|--------|---------|---------------|
| Claude | `provider: claude` | `ANTHROPIC_API_KEY` | `claude-sonnet-4-20250514` |
| OpenAI | `provider: openai` | `OPENAI_API_KEY` | `gpt-4o-mini` |
| Gemini | `provider: gemini` | `GOOGLE_API_KEY` or `GEMINI_API_KEY` | `gemini-2.0-flash` |
| Ollama (local) | `provider: local` | Not required | `llama3` |
| Custom | `baseUrl: http://...` | Optional | — |

## Deployment

```bash
# Local (default — no Spark installation needed)
weaver apply pipeline.yaml

# Docker
docker run data-weaver:latest apply pipeline.yaml --env prod

# Kubernetes
weaver apply --submit k8s --master k8s://host:6443 --image data-weaver:latest

# AWS EMR
weaver apply --submit emr --cluster-id j-XXXXX --env prod

# GCP Dataproc
weaver apply --submit dataproc --cluster my-cluster --region us-central1
```

### Airflow

```python
from data_weaver_airflow import DataWeaverOperator

etl_task = DataWeaverOperator(
    task_id="customer_etl",
    pipeline="pipelines/customer_etl.yaml",
    env="prod",
    dag=dag,
)
```

## Configuration

### Connections (`connections.yaml`)

```yaml
connections:
  pg-prod:
    type: PostgreSQL
    host: ${env.DB_HOST}
    port: ${env.DB_PORT}
    database: ${env.DB_NAME}
    user: ${env.DB_USER}
    password: ${env.DB_PASSWORD}
```

Credentials resolve from: environment variables → `.env` file (gitignored) → Vault/Secrets Manager.

### Variable Injection

| Variable | Example | Result |
|----------|---------|--------|
| `${env.VAR}` | `${env.DB_HOST}` | Environment variable value |
| `${date.today}` | — | `2026-04-06` |
| `${date.yesterday}` | — | `2026-04-05` |
| `${date.offset(-7)}` | — | `2026-03-30` |
| `${date.format('yyyy/MM')}` | — | `2026/04` |

## Building Custom Connectors

See the [Connector SDK](docs/connector-sdk/CONNECTOR_SDK.md) for a complete guide.

```scala
class MyConnector extends SourceConnector {
  def connectorType = "MyDB"
  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame = {
    spark.read.format("jdbc").option("url", config("url")).load()
  }
}
```

Package as JAR, drop in `plugins/`, and it's automatically discovered via ServiceLoader.

## Tutorials

Ready-to-run example pipelines in [`docs/tutorials/`](docs/tutorials/):

| Tutorial | Description | Concepts |
|----------|-------------|----------|
| [01 — CSV to Parquet](docs/tutorials/01-csv-to-parquet.yaml) | Read CSV, clean with SQL, validate, write Parquet | File I/O, SQL, DataQuality |
| [02 — Multi-source Join](docs/tutorials/02-multi-source-join.yaml) | Join two sources with parallel transforms | Parallel DAG, aggregation |
| [03 — RAG Pipeline](docs/tutorials/03-rag-pipeline.yaml) | Chunk documents for vector search | Chunking, RAG preparation |
| [04 — LLM Classification](docs/tutorials/04-llm-classification.yaml) | Classify tickets with Gemini/Ollama | LLMTransform, local models |
| [05 — Production ETL](docs/tutorials/05-production-etl.yaml) | Full pipeline: PostgreSQL → DeltaLake | Connections, profiles, merge |
| [06 — Web Scraping + LLM](docs/tutorials/06-web-scraping-llm.yaml) | Extract supermarket products from HTML with LLM | REST, LLMTransform, SQL |

```bash
# Run a tutorial
weaver validate docs/tutorials/01-csv-to-parquet.yaml
weaver plan docs/tutorials/01-csv-to-parquet.yaml
weaver apply docs/tutorials/01-csv-to-parquet.yaml
```

## Plugin Registry

Install connector plugins without recompiling:

```bash
# Install from Maven Central
weaver install connector-kafka
weaver install com.example:my-connector:1.0.0

# Install local JAR
weaver install /path/to/connector.jar

# List what's available
weaver list connectors
weaver list transforms
weaver list plugins
```

### Build Your Own Connector

```bash
# Scaffold a complete connector project (build.sbt, source code, tests, ServiceLoader)
weaver scaffold connector my-redis-connector
weaver scaffold connector my-api-connector --type both   # source + sink
weaver scaffold transform my-custom-transform

# For your company: create a private registry
weaver scaffold registry my-company-connectors
```

Plugins are loaded from `~/.weaver/plugins/` automatically. See the [Connector SDK](docs/connector-sdk/CONNECTOR_SDK.md) for the full guide.

## Architecture

```
data-weaver/
├── core/             Plugin traits, DAG resolver, config, executor
├── connectors/       Source and sink implementations
├── transformations/  SQL, DataQuality, LLM, RAG transforms
├── cli/              CLI commands, AI generation, wizard
└── airflow-operator/ Python Airflow operator
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Scala 2.13.14 |
| Build | SBT 1.9.7 |
| Engine | Apache Spark 4.0.2 |
| Local engine | DuckDB (embedded) |
| YAML | circe-yaml |
| CLI | scopt |
| Java | JDK 17+ |

## License

Apache License 2.0
