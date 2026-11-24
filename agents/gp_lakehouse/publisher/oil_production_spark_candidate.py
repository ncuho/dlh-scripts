# TO BE: raw Iceberg → Stage → Vault → Kimball; AS IS called GP procedures from oil_gp_release.
from datetime import datetime,timedelta
from pathlib import Path
import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0,'/opt/airflow/dags/managed/oil_layered_migration')
from oil_dlh_jobs import execute


def release(**context):
    """Apply one bounded raw window with Spark; existing table names/UUIDs and BI dataset stay unchanged."""
    conf=context['dag_run'].conf or {}
    if set(conf)-{'overlap_offsets'}:raise ValueError('Use only optional overlap_offsets; no schemas or _next names')
    return execute('release',context['run_id'],conf.get('overlap_offsets',10))


with DAG(dag_id='oil_production_spark_candidate',description='Обработка добычи Spark; кандидат переноса первой цепочки',
         start_date=datetime(2026,1,1),schedule='*/1 * * * *',catchup=False,max_active_runs=1,
         is_paused_upon_creation=True,default_args={'retries':0,'execution_timeout':timedelta(minutes=15)},
         tags=['oil','spark','inplace']) as dag:
    # Shared pool also excludes concurrent manual catchup and INITIAL writes.
    PythonOperator(task_id='apply_events_and_refresh_marts',python_callable=release,pool='oil_dlh_writer')
