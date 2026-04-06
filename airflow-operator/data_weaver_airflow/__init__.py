"""Data Weaver Airflow Operator - Execute Data Weaver pipelines from Airflow DAGs."""

from data_weaver_airflow.operator import DataWeaverOperator

__all__ = ["DataWeaverOperator"]
__version__ = "0.2.0"
