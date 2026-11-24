# TO BE: raw Iceberg → Stage → Vault → Kimball; AS IS called GP procedures from oil_gp_release.
from datetime import datetime,timedelta
from pathlib import Path
import json
import sys
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0,'/opt/airflow/dags/managed/oil_layered_migration')
from oil_dlh_jobs import ROOT, execute


def release(**context):
    """Manually reconcile C1 with mode=catchup; scheduled/default runs process DLH raw only."""
    conf=context['dag_run'].conf or {}
    if set(conf)-{'mode','overlap_offsets'}:raise ValueError('Use only mode and overlap_offsets')
    mode=conf.get('mode','release')
    if mode not in ('catchup','release'):raise ValueError('mode must be catchup or release')
    if mode=='release':
        # Unpausing to trigger catchup can also queue a scheduled run. Until C1
        # is accepted, that run must skip without writing or opening Spark/GP.
        pointer=ROOT/'ready_dlh.json'
        accepted=json.loads(pointer.read_text()) if pointer.exists() else {}
        if accepted.get('status')!='READY' or accepted.get('mode') not in ('catchup','release'):
            raise AirflowSkipException('First trigger this DAG with {"mode":"catchup"}; accept C1 before recurring processing')
    return execute(mode,context['run_id'],conf.get('overlap_offsets',10))


with DAG(dag_id='oil_dlh_release',description='Догон с GP-сверкой и регулярная обработка нефтедобычи Spark',
         start_date=datetime(2026,1,1),schedule='*/1 * * * *',catchup=False,max_active_runs=1,
         is_paused_upon_creation=True,default_args={'retries':0,'execution_timeout':timedelta(minutes=15)},
         tags=['oil','spark','inplace']) as dag:
    # Shared pool also excludes concurrent manual catchup and INITIAL writes.
    PythonOperator(task_id='apply_events_and_refresh_marts',python_callable=release,pool='oil_dlh_writer')
