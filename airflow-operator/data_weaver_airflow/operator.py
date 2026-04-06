"""Data Weaver Airflow Operator.

Usage in DAG:
    from data_weaver_airflow import DataWeaverOperator

    etl_task = DataWeaverOperator(
        task_id="customer_etl",
        pipeline="pipelines/customer_etl.yaml",
        env="prod",
        weaver_jar="/opt/weaver/data-weaver.jar",
        dag=dag,
    )
"""

import subprocess
from typing import Optional

from airflow.models import BaseOperator


class DataWeaverOperator(BaseOperator):
    """Execute a Data Weaver pipeline.

    Args:
        pipeline: Path to the pipeline YAML file
        env: Environment profile (dev, prod)
        command: Weaver command (default: apply)
        weaver_jar: Path to data-weaver.jar
        java_opts: JVM options (default: -Xmx1g)
        spark_master: Spark master URL (default: local[*])
        extra_args: Additional CLI arguments
    """

    template_fields = ("pipeline", "env", "extra_args")

    def __init__(
        self,
        pipeline: str,
        env: Optional[str] = None,
        command: str = "apply",
        weaver_jar: str = "/opt/weaver/data-weaver.jar",
        java_opts: str = "-Xmx1g",
        spark_master: Optional[str] = None,
        extra_args: str = "",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pipeline = pipeline
        self.env = env
        self.command = command
        self.weaver_jar = weaver_jar
        self.java_opts = java_opts
        self.spark_master = spark_master
        self.extra_args = extra_args

    def execute(self, context):
        cmd = ["java", self.java_opts, "-jar", self.weaver_jar, self.command, self.pipeline]

        if self.env:
            cmd.extend(["--env", self.env])

        if self.extra_args:
            cmd.extend(self.extra_args.split())

        self.log.info(f"Executing Data Weaver: {' '.join(cmd)}")

        env = {}
        if self.spark_master:
            env["SPARK_MASTER"] = self.spark_master

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**dict(__import__("os").environ), **env},
        )

        self.log.info(f"stdout: {result.stdout}")

        if result.returncode != 0:
            self.log.error(f"stderr: {result.stderr}")
            raise RuntimeError(
                f"Data Weaver pipeline failed with exit code {result.returncode}: {result.stderr}"
            )

        return result.stdout
