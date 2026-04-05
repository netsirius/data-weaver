# Data Weaver v2 — Design Specification

**Date:** 2026-04-05
**Status:** Draft
**Author:** hsantos

---

## 1. Vision and Positioning

Data Weaver is a declarative data pipeline framework built on Apache Spark that lets engineers define the full ETL+RAG cycle in a single YAML file: extraction, transformation, quality checks, and loading — including advanced transformations like chunking, embedding, and knowledge graph extraction.

**Positioning:** "Terraform for data pipelines" — pure YAML, validate/plan/apply workflow, AI-generatable, auto-selectable execution engine.

**What it is NOT:**
- Not an orchestrator (that's Airflow/Dagster — Data Weaver integrates with them)
- Not a data warehouse (that's BigQuery/Snowflake — Data Weaver writes to them)
- Not an ML framework (that's MLflow/Kubeflow)

**Competitive differentiation:**

| vs | Data Weaver wins on |
|---|---|
| Flowman | Spark 4, 5-min install, streaming, community, AI generation |
| dbt | Spark-native, no warehouse required, RAG transforms, multi-engine |
| Metorikku (archived) | Actively maintained, multi-engine, AI generation |
| Spark Declarative Pipelines | Pure YAML, vendor-neutral, embedded local engine |

**Key principle:** Simple by default, powerful when needed. The YAML hides Spark complexity; escape hatches expose full control.

---

## 2. Architecture

### 2.1 Module Structure

Monorepo with 4 SBT modules. Separation prepares for future extraction of core as independent library.

```
data-weaver/
├── core/                ← Engine, plugin system, DataFrame abstraction, engine selection
│   └── src/main/scala/com/dataweaver/core/
│       ├── engine/      ← SparkEngine, EmbeddedEngine, EngineSelector
│       ├── plugin/      ← PluginManager, PluginRegistry, traits
│       ├── dag/         ← DAGResolver, TopologicalSort, ParallelExecutor
│       ├── config/      ← YAMLParser, SchemaValidator, ConnectionResolver
│       ├── arrow/       ← ArrowDataset abstraction layer
│       └── quality/     ← DataQualityEngine, Check types
├── connectors/          ← All source + sink connectors
│   └── src/main/scala/com/dataweaver/connectors/
│       ├── sources/     ← PostgreSQL, MongoDB, Kafka, REST, File, S3, PDF
│       └── sinks/       ← BigQuery, DeltaLake, Kafka, Neo4j, VectorDB, Elastic
├── transformations/     ← All transformation types
│   └── src/main/scala/com/dataweaver/transformations/
│       ├── sql/         ← SQLTransformation
│       ├── rag/         ← Chunking, Embedding, GraphExtraction
│       └── custom/      ← CustomPlugin interface
├── cli/                 ← CLI, AI generation, interactive wizard
│   └── src/main/scala/com/dataweaver/cli/
│       ├── commands/    ← Validate, Plan, Apply, Test, Generate, Install, Inspect
│       ├── ai/          ← LLMClient, PromptBuilder, PipelineGenerator
│       └── wizard/      ← InteractiveWizard, StepByStep
└── build.sbt            ← Root multi-project build
```

**Module dependencies:**
- `connectors` depends on `core`
- `transformations` depends on `core`
- `cli` depends on `core`, `connectors`, `transformations`

### 2.2 Plugin System

Everything is a plugin. Connectors and transformations implement traits defined in core.

```scala
// core/plugin/SourceConnector.scala
trait SourceConnector {
  def id: String                            // "PostgreSQL"
  def configSchema: JsonSchema              // for YAML validation
  def read(config: Map[String, String],
           connectionResolver: ConnectionResolver
          ): ArrowDataset
}

// core/plugin/SinkConnector.scala
trait SinkConnector {
  def id: String                            // "BigQuery"
  def configSchema: JsonSchema
  def write(data: ArrowDataset,
            config: Map[String, String],
            connectionResolver: ConnectionResolver): Unit
}

// core/plugin/TransformPlugin.scala
trait TransformPlugin {
  def id: String                            // "SQLTransformation"
  def configSchema: JsonSchema
  def transform(inputs: Map[String, ArrowDataset],
                config: TransformConfig
               ): ArrowDataset
}
```

**Plugin discovery:**
1. Built-in plugins (in connectors/ and transformations/ modules) — registered at compile time
2. External JARs in `plugins/` directory — discovered at startup via ServiceLoader
3. Downloaded via `weaver install connector-X` from registry

### 2.3 Execution Engines

Two engines, same interface. Selected per-pipeline.

**SparkEngine:** Distributed processing via Apache Spark 4.0+. For production workloads, large datasets, cluster execution. Converts ArrowDataset to/from Spark DataFrame.

**EmbeddedEngine:** Local processing via DuckDB (SQL) or direct Arrow compute. For development, testing, small datasets. Zero infrastructure required. Bundled in the JAR.

**Engine selection logic:**
```
pipeline.engine = "spark"  → SparkEngine
pipeline.engine = "local"  → EmbeddedEngine
pipeline.engine = "auto"   → estimate input size:
                              > threshold (default 1GB) → SparkEngine
                              ≤ threshold              → EmbeddedEngine
```

**Size estimation for "auto" mode:**
- JDBC sources: `SELECT COUNT(*) * avg_row_size` or database table statistics
- File sources: file system metadata (file size from S3/GCS/local)
- Kafka: partition lag * avg message size
- If estimation is not possible: defaults to Spark (safe fallback)

### 2.4 DAG Resolution and Parallelism

The user defines `sources` for each transformation. The DAG resolver infers execution order and parallelism automatically.

**Two levels of parallelism:**

1. **Inter-transform parallelism:** Independent transformations (no dependency between them) execute concurrently via `Future` + controlled thread pool.

2. **Intra-transform parallelism:** Data within each transformation is partitioned by Spark automatically. Auto-tuner adjusts partition count based on data size.

**Auto-tuning defaults:**
- Partitions: `max(4, dataSize / 128MB)`
- Broadcast join threshold: auto for tables < 10MB
- Shuffle partitions: dynamic based on data size
- Executor memory/cores: configurable via profiles, sensible defaults

**The user never configures parallelism explicitly** unless they want to tune for specific workloads (via profiles).

### 2.5 Connection Management

Connections are defined separately from pipelines. Credentials never appear in pipeline YAML.

```yaml
# connections.yaml (git-tracked, no secrets)
connections:
  pg-prod:
    type: PostgreSQL
    host: ${env.PG_HOST}
    port: ${env.PG_PORT}
    database: ${env.PG_DB}
    user: ${env.PG_USER}
    password: ${env.PG_PASSWORD}

  gcs-data:
    type: CloudStorage
    provider: gcs
    bucket: ${env.GCS_BUCKET}
    credentials: ${env.GOOGLE_APPLICATION_CREDENTIALS}
```

**Credential resolution priority:**
1. Environment variables (`${env.VAR}`)
2. `.env` file (gitignored, local dev)
3. HashiCorp Vault (`${vault.secret/path}`)
4. AWS Secrets Manager (`${aws.secretName}`)
5. Kubernetes Secrets (injected as env vars)

### 2.6 Environment Profiles

Non-sensitive overrides live in profiles within the pipeline YAML. Sensitive config comes from environment variables resolved by the connection resolver.

```yaml
profiles:
  dev:
    engine: local
    dataSources.customers.config.query: "SELECT * FROM customers LIMIT 100"
    sinks.warehouse.config.saveMode: overwrite
  prod:
    engine: spark
    spark:
      executor.memory: 4g
      executor.cores: 4
      partitions: 200
```

Selected via: `weaver apply --env prod`

**Profile override syntax:** Dot-notation JSON path. `dataSources.customers.config.query` targets the `query` field inside the `config` of the dataSource with id `customers`. Only non-sensitive values should go in profiles; secrets always come from environment/vault.

---

## 3. Pipeline YAML Contract

A complete pipeline in a single file:

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
      readMode: incremental
      watermark: updated_at

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
    assert: sinks.warehouse.row_count > 0
```

**Key design decisions:**
- `source` in sinks is EXPLICIT — each sink declares which transformation it reads from (fixes the current bug of always using last)
- `readMode: incremental` with `watermark` enables CDC without full reload
- `checks` with `onFail: abort | warn | skip` provides inline data quality
- `profiles` for non-sensitive environment overrides
- `tests` section for inline quick checks

---

## 4. LLM Transformations and RAG

### 4.1 LLMTransform — Generic LLM-as-Transformation

The core building block: use any LLM as a transformation step. The prompt is the "query", the inputColumns are the "parameters", and the outputSchema defines the structured return type.

```yaml
transformations:
  - id: classify_tickets
    type: LLMTransform
    sources: [jira_tickets]
    config:
      provider: claude                          # claude | openai | vertex-ai | local (Ollama)
      model: claude-sonnet-4-20250514
      prompt: |
        Analyze this Jira ticket and classify it:
        Title: {summary}
        Description: {description}
        Labels: {labels}
      inputColumns: [summary, description, labels]
      outputSchema:
        - {name: category, type: string}
        - {name: complexity, type: string}
        - {name: key_concepts, type: "array<string>"}
      batchSize: 5                # rows per LLM call (reduces cost)
      maxConcurrent: 10           # parallel LLM calls per executor
      retryOnError: 3             # retries with exponential backoff
      cache: true                 # content-hash cache, skip identical inputs
```

**LLMTransform capabilities:**

| Feature | Purpose |
|---------|---------|
| `batchSize` | Group N rows into a single LLM call (5-10x cost reduction) |
| `maxConcurrent` | Parallel LLM calls per Spark executor (throughput) |
| `cache` | Content-hash based — if input unchanged, skip LLM call |
| `outputSchema` | LLM returns structured JSON, parsed into DataFrame columns |
| `retryOnError` | LLMs fail — retry with exponential backoff |
| `inputColumns` | Which DataFrame columns are injected into the prompt template |
| `provider` | claude, openai, vertex-ai, local (Ollama) |
| `{column_name}` | Prompt templating — replaced with row values at runtime |

**Execution on Spark (distributed LLM processing):**

On Spark, LLMTransform uses `mapPartitions` to distribute LLM calls across executors. Each executor maintains its own HTTP connection pool and processes its partition of rows with controlled concurrency.

```
500K rows → repartition(200) → 50 executors × 10 concurrent calls each
= 500 LLM calls/second → 500K rows processed in ~5 minutes
vs single-process: ~46 hours
```

Spark handles fault tolerance at the partition level: if one executor fails, only its partition is reprocessed.

**Execution on local engine (DuckDB):**

On the local engine, LLMTransform uses a thread pool with `maxConcurrent` threads. Same API calls, same prompt, same output — just single-process instead of distributed. Ideal for development and datasets < 10K rows.

### 4.2 ACE Pattern — Context Playbook Generation

ACE (Analyze → Contextualize → Enrich) is a pipeline pattern for generating structured context playbooks from unstructured data using chained LLM transformations.

**Example: Jira tickets → Context Playbook**

```yaml
name: JiraContextPlaybook
engine: auto
tag: weekly

dataSources:
  - id: jira_tickets
    type: REST
    connection: jira-prod
    config:
      endpoint: /rest/api/3/search
      params:
        jql: "project = MYPROJ AND updated >= -7d"
      pagination:
        type: offset
        pageSize: 100
      flatten: issues[]

  - id: confluence_docs
    type: REST
    connection: confluence-prod
    config:
      endpoint: /wiki/rest/api/content
      params:
        spaceKey: ENGINEERING

transformations:
  # ── ANALYZE: extract structured info from each ticket/doc ──
  - id: ticket_analysis
    type: LLMTransform
    sources: [jira_tickets]
    config:
      provider: claude
      model: claude-sonnet-4-20250514
      prompt: |
        Analyze this Jira ticket. Extract structured information:
        Title: {summary}
        Description: {description}
        Comments: {comments}
        Labels: {labels}
      inputColumns: [summary, description, comments, labels]
      outputSchema:
        - {name: category, type: string}
        - {name: domain, type: string}
        - {name: technical_decisions, type: "array<string>"}
        - {name: systems_involved, type: "array<string>"}
        - {name: key_learnings, type: "array<string>"}
      batchSize: 3
      maxConcurrent: 10

  - id: doc_analysis
    type: LLMTransform
    sources: [confluence_docs]
    config:
      provider: claude
      model: claude-haiku-3-20250514
      prompt: |
        Extract key concepts and architecture decisions from:
        Title: {title}
        Content: {body}
      inputColumns: [title, body]
      outputSchema:
        - {name: concepts, type: "array<string>"}
        - {name: architecture_decisions, type: "array<string>"}
        - {name: domain, type: string}
      batchSize: 5

  # ── CONTEXTUALIZE: group by domain, synthesize patterns ──
  - id: grouped_context
    type: SQL
    sources: [ticket_analysis, doc_analysis]
    query: >
      SELECT
        ta.domain,
        COLLECT_LIST(ta.technical_decisions) as all_decisions,
        COLLECT_LIST(ta.key_learnings) as all_learnings,
        COLLECT_LIST(ta.systems_involved) as all_systems,
        COLLECT_LIST(da.architecture_decisions) as arch_decisions,
        COUNT(*) as ticket_count
      FROM ticket_analysis ta
      LEFT JOIN doc_analysis da ON ta.domain = da.domain
      GROUP BY ta.domain

  - id: contextualized
    type: LLMTransform
    sources: [grouped_context]
    config:
      provider: claude
      model: claude-sonnet-4-20250514
      prompt: |
        Build a context playbook for domain: {domain}
        Based on {ticket_count} tickets, synthesize:
        Technical decisions: {all_decisions}
        Learnings: {all_learnings}
        Systems: {all_systems}
        Architecture: {arch_decisions}

        Generalize patterns. Do NOT reference individual tickets.
        Identify: principles, dependency map, pitfalls, terminology.
      inputColumns: [domain, all_decisions, all_learnings, all_systems, arch_decisions, ticket_count]
      outputSchema:
        - {name: domain, type: string}
        - {name: patterns, type: string}
        - {name: principles, type: string}
        - {name: dependency_map, type: string}
        - {name: pitfalls, type: string}
        - {name: terminology, type: string}
      batchSize: 1

  # ── ENRICH: generate final playbook document ──
  - id: playbook
    type: LLMTransform
    sources: [contextualized]
    config:
      provider: claude
      model: claude-sonnet-4-20250514
      prompt: |
        Generate a Context Playbook in markdown for domain: {domain}
        Patterns: {patterns}
        Principles: {principles}
        Dependencies: {dependency_map}
        Pitfalls: {pitfalls}
        Terminology: {terminology}

        The playbook must be:
        - Self-contained (no ticket references)
        - Actionable (clear do's and don'ts)
        - Structured (headers, sections)
        - Usable as AI grounding context
      inputColumns: [domain, patterns, principles, dependency_map, pitfalls, terminology]
      outputSchema:
        - {name: domain, type: string}
        - {name: playbook_markdown, type: string}

  - id: quality_check
    type: DataQuality
    sources: [playbook]
    checks:
      - row_count > 0
      - missing_count(playbook_markdown) = 0
    onFail: warn

sinks:
  - id: playbook_files
    type: File
    source: playbook
    config:
      path: output/playbooks/
      format: json
      partitionBy: domain

  - id: vector_store
    type: BigQueryVector
    source: playbook
    connection: bq-prod
    config:
      table: project.rag.context_playbooks
      embeddingColumn: playbook_markdown
      embeddingProvider: vertex-ai
      embeddingModel: text-embedding-004

  - id: knowledge_graph
    type: Neo4j
    source: contextualized
    connection: neo4j-prod

profiles:
  dev:
    engine: local
    dataSources.jira_tickets.config.params.jql: "project = MYPROJ AND updated >= -1d"
    transformations.ticket_analysis.config.batchSize: 1
    transformations.ticket_analysis.config.maxConcurrent: 2
  prod:
    engine: spark
    transformations.ticket_analysis.config.maxConcurrent: 50
```

### 4.3 RAG Data Preparation

RAG-specific transformations are specialized variants of LLMTransform with predefined behavior:

```yaml
transformations:
  - id: chunks
    type: Chunking
    sources: [documents]
    config:
      strategy: semantic          # semantic | fixed | recursive | sentence
      size: 512
      overlap: 50

  - id: vectors
    type: Embedding
    sources: [chunks]
    config:
      provider: vertex-ai
      model: text-embedding-004

  - id: entities
    type: GraphExtraction
    sources: [chunks]
    config:
      provider: claude
      model: claude-sonnet-4-20250514
      entityTypes: [Person, Product, Organization]
```

**Transformation type hierarchy:**
- **LLMTransform** — generic, user provides prompt and outputSchema
- **Chunking** — predefined logic (no LLM needed), configurable strategy
- **Embedding** — predefined API call pattern, returns vectors
- **GraphExtraction** — predefined prompt for entity/relationship extraction
- **ContextPlaybook** — predefined ACE pattern, generates structured playbook

All LLM-based transforms (LLMTransform, GraphExtraction, ContextPlaybook) share the same execution engine: `mapPartitions` on Spark, thread pool on local. Same `batchSize`, `maxConcurrent`, `cache`, `retryOnError` config.

---

## 5. CLI Commands

```bash
# Project lifecycle
weaver init <project-name>          # Scaffold with working example
weaver doctor                       # Full system diagnostic (YAML + connections + env + permissions)
weaver validate                     # Parse YAML, validate schema, test connections
weaver plan                         # Dry-run: show what will be read/transformed/written
weaver plan --diff                  # Show changes vs last execution
weaver apply                        # Execute pipeline
weaver apply --env prod             # Execute with prod profile
weaver apply --pipeline <name>      # Execute specific pipeline

# Testing
weaver test                         # Run tests defined in YAML
weaver test --auto-generate         # Generate tests from schema inference
weaver test --coverage              # Show which transforms have tests

# AI Generation
weaver generate "<description>"     # Natural language to YAML pipeline
weaver init --interactive           # Step-by-step wizard (no LLM required)

# Connectors
weaver install connector-kafka      # Download from registry
weaver list connectors              # Show available connectors
weaver list transforms              # Show available transformation types

# Debugging
weaver inspect <transform-id>       # Show schema + sample rows of a transform
weaver explain                      # Show resolved DAG and execution plan
weaver logs                         # Show last execution logs

# Deployment
weaver apply --submit k8s --master k8s://host:6443 --image img:tag
weaver apply --submit emr --cluster-id j-XXXXX
weaver apply --submit dataproc --cluster name --region region
```

---

## 5.1 Doctor Command

`weaver doctor` performs a full system diagnostic across all pipelines in the project. It checks progressively deeper: syntax → schema → DAG → environment → connections → permissions.

```
$ weaver doctor

  Data Weaver Doctor v0.2.0
  ─────────────────────────

  Pipeline: pipelines/customer_etl.yaml
  ✓ YAML syntax valid
  ✓ Schema valid (3 sources, 2 transforms, 2 sinks)
  ✓ DAG resolved (3 levels, 2 parallelizable)
  ✓ No duplicate ids
  ✓ All sink sources exist

  Connections:
  ✓ pg-prod        PostgreSQL  db.example.com:5432    connected (23ms)
  ✗ elastic-prod   Elastic     search.example.com:9200 Connection refused
                   Hint: Is Elasticsearch running? Check ${env.ELASTIC_HOST}
  ✓ bq-prod        BigQuery    project: my-project     authenticated

  Environment:
  ✓ ${env.PG_HOST} = db.example.com
  ✓ ${env.PG_PASSWORD} = ******* (set)
  ✗ ${env.ELASTIC_HOST} = NOT SET
                   Hint: export ELASTIC_HOST=<value> or add to .env file

  Engine:
  ✓ Spark 4.0.2 available (SPARK_HOME=/opt/spark)
  ✓ Java 17.0.8 (meets minimum: 17+)
  ✓ Local engine (DuckDB) embedded

  Permissions:
  ✓ s3://warehouse/customers   WRITE OK
  ✗ s3://data-lake/orders/     READ DENIED
                   Hint: Check IAM role or GOOGLE_APPLICATION_CREDENTIALS

  Summary: 2 errors found. Fix them before running weaver apply.
```

**Doctor check categories (progressive by phase):**

| Category | Phase 0 | Phase 1+ |
|----------|---------|----------|
| YAML syntax | ✓ | ✓ |
| Schema validation | ✓ | ✓ |
| DAG resolution | ✓ | ✓ |
| Environment variables | ✓ | ✓ |
| Java version | ✓ | ✓ |
| Connection test (ping) | — | ✓ |
| Read/write permissions | — | ✓ |
| Spark availability | — | ✓ |
| Plugin availability | — | ✓ |

Phase 0 delivers `weaver doctor` with offline checks. Phase 1 adds live connection and permission checks as connectors become available. Each new connector registers its own health check method.

**Connector health check interface (added to plugin traits):**

```scala
trait SourceConnector extends Serializable {
  def connectorType: String
  def read(config: Map[String, String])(implicit spark: SparkSession): DataFrame

  /** Optional health check. Returns Right(latencyMs) on success, Left(error) on failure. */
  def healthCheck(config: Map[String, String]): Either[String, Long] =
    Left("Health check not implemented for this connector")
}
```

---

## 6. Testing Framework

### 6.1 Inline tests (in pipeline YAML)

```yaml
tests:
  - name: "has data"
    assert: sinks.warehouse.row_count > 0
  - name: "no duplicates"
    assert: sinks.warehouse.duplicate_count(id) = 0
```

### 6.2 External test files (pipeline.test.yaml)

```yaml
tests:
  - name: "enriched schema is correct"
    transform: enriched
    mock_sources:
      customers:
        - {id: 1, name: "Alice", email: "alice@test.com"}
      orders:
        - {id: 100, customer_id: 1, amount: 50.0}
    expect:
      schema:
        - {name: id, type: integer, nullable: false}
        - {name: order_count, type: long}
      row_count: 1
      values:
        - {id: 1, order_count: 1}
```

### 6.3 Auto-generated tests

`weaver test --auto-generate` runs the pipeline with real data (or sample), infers output schemas, generates assertions (not_null for PKs, row_count > 0, type checks), and saves as `pipeline.test.yaml`.

---

## 7. AI Pipeline Generation

### 7.1 CLI Generation

```bash
weaver generate "Extract active users from PostgreSQL,
                 enrich with order count from Parquet in S3,
                 validate emails not null,
                 load to Delta Lake merged by user_id"
```

### 7.2 Internal flow

1. Load JSON Schema of pipeline YAML format
2. Load available connector + transformation schemas
3. Build grounded prompt with schema + user description
4. Call LLM API (Claude/OpenAI, configurable)
5. Parse response as YAML
6. Run `weaver validate` on generated YAML
7. If validation fails, iterate with LLM (max 3 attempts)
8. Auto-generate `pipeline.test.yaml`
9. Run `weaver test` to verify
10. Present result to user

### 7.3 Interactive Wizard (no LLM required)

```bash
weaver init --interactive
> What data sources? [PostgreSQL, MySQL, CSV, Parquet, API, Kafka, MongoDB]
> PostgreSQL connection details? [host, port, db, user]
> What query? [SELECT * FROM ...]
> Transformations? [SQL filter, aggregate, join, deduplicate, custom SQL]
> Quality checks? [not_null, unique, row_count, custom]
> Where to write? [BigQuery, DeltaLake, Parquet, Elasticsearch, Kafka]
> Generate tests? [yes/no]
```

### 7.4 Configuration

```yaml
# ~/.weaver/config.yaml
ai:
  provider: claude
  apiKey: ${env.ANTHROPIC_API_KEY}
  model: claude-sonnet-4-20250514
  maxRetries: 3
```

---

## 8. Deployment Targets

### 8.1 Local development (default)

`weaver apply` — uses embedded engine (DuckDB). No Spark installation required. Bundled in JAR.

### 8.2 Spark standalone cluster

`weaver apply --env prod` — submits to cluster defined in profile's `spark.master`.

### 8.3 Kubernetes

```bash
weaver apply --submit k8s \
  --master k8s://https://cluster:6443 \
  --image data-weaver:latest \
  --namespace data-pipelines
```

### 8.4 AWS EMR

```bash
weaver apply --submit emr \
  --cluster-id j-XXXXX \
  --env prod
```

### 8.5 GCP Dataproc

```bash
weaver apply --submit dataproc \
  --cluster my-cluster \
  --region us-central1 \
  --env prod
```

### 8.6 Airflow integration

```python
from data_weaver.airflow import DataWeaverOperator

etl_task = DataWeaverOperator(
    task_id="customer_etl",
    pipeline="pipelines/customer_etl.yaml",
    env="prod",
    conn_id="spark_cluster",
    dag=dag,
)
```

### 8.7 Docker

```dockerfile
FROM data-weaver:latest
COPY pipelines/ /app/pipelines/
COPY connections.yaml /app/
CMD ["weaver", "apply", "--env", "prod"]
```

---

## 9. Error Handling

### 9.1 YAML validation errors

Human-readable messages with line numbers and suggestions:
```
ERROR pipeline.yaml:14 — field "soruce" is not valid in dataSources
  Did you mean: "source"?
  Valid fields: id, type, query, config, connection
```

### 9.2 Connection errors

```
ERROR connection "pg-prod" failed: Connection refused
  Host: localhost:5432
  Hint: Is PostgreSQL running? Check ${env.PG_HOST}
```

### 9.3 Pipeline execution errors

```
ERROR transform "enriched" failed: Column "email" not found
  Available columns: id, name, order_count
  Source: customers (PostgreSQL)
  Query line 3: SELECT email FROM customers
```

### 9.4 Data quality failures

```
QUALITY CHECK FAILED in transform "validated":
  ✓ row_count > 0 (actual: 1523)
  ✗ missing_count(email) = 0 (actual: 47)
  ✓ duplicate_count(id) = 0
  Action: abort (pipeline stopped)
```

---

## 10. Implementation Plan

### Phase 0: Foundation (Weeks 1-3)

Restructure existing code into multi-module SBT build. Establish core abstractions.

**Tasks:**
- Restructure into core/, connectors/, transformations/, cli/ SBT modules
- Define SourceConnector, SinkConnector, TransformPlugin traits in core
- Implement PluginManager with ServiceLoader discovery
- Implement ConnectionResolver with env var + dotenv support
- Implement DAG resolver with topological sort
- Implement parallel executor (Future-based) for independent transforms
- Move existing SQLReader, BigQuerySink, TestReader, TestSink to connector module
- Move SQLTransformation to transformations module
- Move CLI to cli module
- Fix per-sink source routing (replace `last._2` with explicit `source` field)
- JSON Schema for pipeline YAML format
- Schema-based YAML validator with human-readable errors

**Deliverable:** Same functionality as today but properly modularized. `weaver validate` works.

### Phase 1: Local Engine + Core Connectors (Weeks 4-7)

Add embedded engine and priority connectors.

**Tasks:**
- Integrate DuckDB as embedded engine (Arrow interop)
- Implement EngineSelector (auto/spark/local)
- Implement ArrowDataset abstraction layer
- Connector: PostgreSQL source
- Connector: File source (CSV, Parquet, JSON)
- Connector: S3/GCS cloud storage source
- Connector: DeltaLake sink
- Connector: Parquet/CSV sink
- Implement `weaver plan` (dry-run with schema inference)
- Implement `weaver inspect <transform-id>`
- Implement `weaver explain` (show DAG)
- Implement variable injection (${env.X}, ${date.yesterday})
- Implement profiles (dev/prod overrides)
- Implement connections.yaml with ConnectionResolver
- Implement incremental read mode with watermark tracking

**Deliverable:** Full ETL pipelines running locally without Spark. PostgreSQL to DeltaLake working. `weaver plan` and `weaver apply` working.

### Phase 2: Testing + Quality (Weeks 8-10)

**Tasks:**
- Implement DataQuality transformation type (checks DSL)
- Implement inline tests runner (tests: section in YAML)
- Implement external test files (pipeline.test.yaml) with mock data
- Implement `weaver test` command
- Implement `weaver test --auto-generate` (schema inference + assertion generation)
- Implement `weaver test --coverage`
- Implement onFail: abort | warn | skip behavior
- CI pipeline (GitHub Actions: build, test, lint, assembly)

**Deliverable:** Full testing framework. Auto-generated tests. Data quality checks in pipelines.

### Phase 3: AI Generation + Wizard (Weeks 11-13)

**Tasks:**
- Generate JSON Schema from pipeline YAML contract
- Implement LLMClient (Claude API, OpenAI API, configurable)
- Implement PromptBuilder (schema-grounded prompts)
- Implement `weaver generate "<description>"` command
- Implement validation loop (generate → validate → retry)
- Implement auto test generation for AI-generated pipelines
- Implement `weaver init --interactive` wizard (no LLM)
- Implement ~/.weaver/config.yaml for AI settings
- Publish JSON Schema for IDE autocomplete (VS Code, IntelliJ)

**Deliverable:** Natural language to validated YAML pipeline with auto-tests. Interactive wizard for users without API key.

### Phase 4: Streaming + Advanced Connectors (Weeks 14-17)

**Tasks:**
- Connector: Kafka source (batch + streaming)
- Connector: MongoDB source
- Connector: REST API generic source (with pagination, auth)
- Connector: Elasticsearch sink
- Connector: Kafka sink
- Implement Spark Structured Streaming execution mode
- Implement streaming in YAML (mode: streaming in dataSources)
- Implement connector plugin registry (weaver install connector-X)
- Connector: BigQuery source (not just sink)

**Deliverable:** Streaming support. Kafka, MongoDB, REST, Elasticsearch connectors. Plugin installation from registry.

### Phase 5: LLM Transforms + RAG (Weeks 18-22)

**Tasks:**
- Implement LLMTransform core engine (prompt templating, structured output parsing)
- Implement LLM provider abstraction (Claude, OpenAI, Vertex AI, Ollama/local)
- Implement Spark distributed execution via mapPartitions
- Implement local execution via thread pool
- Implement batchSize grouping (N rows per LLM call)
- Implement maxConcurrent control (per executor on Spark, per process on local)
- Implement content-hash cache (skip LLM call for identical inputs)
- Implement retryOnError with exponential backoff
- Implement outputSchema → JSON parsing → DataFrame columns
- Transform: Chunking (semantic, fixed, recursive, sentence strategies)
- Transform: Embedding (Vertex AI, OpenAI, Cohere, local providers)
- Transform: GraphExtraction (predefined prompt for entity/relationship extraction)
- Transform: ContextPlaybook (predefined ACE pattern)
- Connector: Vector DB sinks (Pinecone, Qdrant, ChromaDB, Weaviate)
- Connector: BigQueryVector sink (with inline embedding support)
- Connector: Neo4j sink (knowledge graph)
- Connector: PDF/document reader source
- Connector: REST API source with pagination and auth (needed for Jira, Confluence, etc.)
- Predefined RAG and ACE pipeline templates
- Rate limiting awareness (respect LLM API rate limits across executors)

**Deliverable:** Full LLM-as-transformation on Spark. ACE pattern for context playbooks. RAG data preparation. Documents to vectors + knowledge graph.

### Phase 6: Deployment + Production (Weeks 22-25)

**Tasks:**
- Implement --submit modes (k8s, emr, dataproc, standalone)
- Implement Airflow DataWeaverOperator (Python package published to PyPI)
- Implement Dagster asset integration
- Dockerfile + docker-compose for dev/CI
- Helm chart for Kubernetes deployment
- Implement `weaver logs` command
- Implement execution history tracking
- Implement retry policies per source/sink
- Implement notifications (webhook on failure)
- brew install formula / install script
- Landing page + documentation site

**Deliverable:** Production-ready deployment to any Spark target. Airflow operator. Docker. Kubernetes.

### Phase 7: Community + Ecosystem (Weeks 26-30)

**Tasks:**
- Connector developer SDK + documentation
- Connector registry (public + private)
- 5 real-world tutorial pipelines with public data
- Discord server
- Stack Overflow tag + canonical answers
- GitHub Actions for pipeline CI/CD (reusable workflow)
- Performance benchmarks vs Flowman
- Blog post: "Why we built Data Weaver"
- Public roadmap (GitHub Projects)

**Deliverable:** Community infrastructure. Connector ecosystem. Public presence.

---

## Appendix A: Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Scala | 2.13.14 |
| Build | SBT | 1.9.7+ |
| Distributed engine | Apache Spark | 4.0.2 |
| Embedded engine | DuckDB | Latest |
| Data interchange | Apache Arrow | Latest |
| YAML parsing | circe-yaml | 0.15.x |
| JSON Schema | everit-org/json-schema | Latest |
| CLI | scopt | 4.1.0 |
| HTTP client | sttp or http4s-client | Latest |
| Testing | ScalaTest + Mockito | Latest |
| Logging | Log4j2 (via Spark) | 2.24.x |
| Java | JDK 17+ | Required by Spark 4 |

## Appendix B: File Layout Convention

```
my-project/
├── pipelines/
│   ├── customer_etl.yaml
│   ├── customer_etl.test.yaml
│   ├── order_processing.yaml
│   └── rag_knowledge_base.yaml
├── connections.yaml
├── .env                          ← gitignored
├── .weaver/
│   └── config.yaml               ← AI settings, defaults
└── plugins/                      ← external connector JARs
```
