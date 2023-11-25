# Data Weaver

Data Weaver is a data processing and ETL (Extract, Transform, Load) tool built on Apache Spark. It allows you to define
data pipelines using YAML configuration files and execute them using Spark for data transformation and integration.

## Table of Contents

- [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
- [Usage](#usage)
    - [Defining Data Pipelines](#defining-data-pipelines)
    - [Running Data Pipelines](#running-data-pipelines)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

### Prerequisites

Before using Data Weaver, make sure you have the following prerequisites installed:

- Apache Spark 3.5.0: [Download and install Apache Spark](https://spark.apache.org/downloads.html).
- Java 11 or later: Data Weaver requires Java to run.

### Installation

1. Clone the Data Weaver repository to your local machine:

   ```bash
   git clone https://github.com/yourusername/data-weaver.git

## Usage

### Defining Data Pipelines

Data pipelines are defined using YAML configuration files. You can create your pipeline configurations and place them in
a directory of your choice. Each configuration should define data sources, transformations, and sinks.

Here's an example of a simple pipeline configuration:

```yaml
name: ExamplePipeline
tag: example
dataSources:
  - id: testSource
    type: MySQL
    query: >
      SELECT name
      FROM test_table
    config:
      readMode: ReadOnce # ReadOnce, Incremental..
      connection: testConnection # Connection name related to the defined connections inside application.conf
transformations:
  - id: transform1
    type: SQLTransformation
    sources:
      - source1 # Source name related to the defined data sources inside pipeline.yaml
    query: >
      SELECT name as id
      FROM testSource
      WHERE column1 = 'value'
  - id: transform2
    type: ScalaTransformation
    sources:
      - transform1 # Source name related to the defined data sources or transformations inside pipeline.yaml
    action: dropDuplicates
sinks:
  - id: sink1
    type: BigQuery
    config:
      saveMode: Append # Append, Overwrite, Merge...
      profile: testProfile # Profile name related to the defined profiles inside application.conf
```

### Running Data Pipelines

To run data pipelines, you can use the Data Weaver command-line interface (CLI). Here's how to execute a pipeline:

```bash
weaver run --pipelines /path/to/pipelines/folder --tag 1d
```

## Configuration

You can configure Data Weaver by editing the flow.conf file located in the config directory. This configuration file
contains various settings for Data Weaver, including Spark configuration.