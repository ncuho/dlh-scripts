# Контекст миграции: AS IS / TO BE.
# Назначение: Native Airflow DAG for reviewing each SQL entity as its own task. Requires a separately captured immutable canonical_event and the source boundary held by the operator. Load the reviewed SQL and Spark transport into the configured S3 DAG di.
# AS IS: Source DAG обновляет GP-историю и обе витрины.
# TO BE: Целевой Airflow DAG оркестрирует release и расчёты Cedrus/Spark.
# Отличие: Имена и состав tasks задаёт этот вариант DAG; универсальный Bridge разворачивает отдельный reviewed project DAG.
# Конец контекста миграции.
"""Materialize reviewed business SQL after baseline and canonical_event preparation."""

from datetime import datetime, timedelta
from pathlib import Path
import re
import sys
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path("/opt/airflow/dags/managed/native_oil_spark")
sys.path.insert(0, str(ROOT))

from bridge_project_spark import SparkClient

ORDER = [
    "hub_field",
    "hub_well",
    "hub_production_day",
    "hub_downtime",
    "link_well_field",
    "link_production_day_well",
    "link_downtime_well",
    "sat_field",
    "sat_well",
    "sat_production_day",
    "sat_downtime",
    "dim_field",
    "dim_well",
    "fact_well_day",
    "agg_field_day",
    "dim_downtime_reason",
    "fact_downtime_interval",
    "agg_field_downtime_day",
    "field_performance",
]
EXTRA = {
    "hub_downtime",
    "link_downtime_well",
    "sat_downtime",
    "dim_downtime_reason",
    "fact_downtime_interval",
    "agg_field_downtime_day",
    "field_performance",
}


def materialize(name, **context):
    """Запускает согласованный Spark расчёт, сохраняя полученные snapshots и проверки выпуска."""
    conf = context["dag_run"].conf or {}
    schema = conf.get("schema", "")
    chain = conf.get("chain")
    if not re.fullmatch(r"oil_r_[a-z0-9_]{1,42}", schema):
        raise ValueError("An explicit release schema is required")
    if chain not in ("production", "performance"):
        raise ValueError("Select production or performance explicitly")
    if chain == "production" and name in EXTRA:
        return {"status": "OUTSIDE_SELECTED_CHAIN", "entity": name}

    query = (ROOT / "sql" / name / (name + ".sql")).read_text()
    query = query.replace("${schema}", schema)
    sql = "CREATE TABLE dlh." + schema + "." + name + " USING iceberg AS " + query
    print(sql, flush=True)

    # A partial previous run must remain visible; never silently reuse its table.
    spark = SparkClient("bridge_" + uuid.uuid4().hex)
    try:
        spark.open()
        spark.sql(sql)
        return {"entity": name, "status": "MATERIALIZED"}
    finally:
        spark.close()


with DAG(
    dag_id="oil_spark_business_release",
    description="Расчёт измерений, фактов и отчётных агрегатов в Spark",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["oil", "spark"],
) as dag:
    previous = None
    for name in ORDER:
        task = PythonOperator(
            task_id=name,
            python_callable=materialize,
            op_kwargs={"name": name},
        )
        if previous is not None:
            previous >> task
        previous = task
