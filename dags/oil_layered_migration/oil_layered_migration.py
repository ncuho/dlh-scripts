# Одна task удерживает одно native GP-подключение для всей миграции; Airflow-процессы не делят транзакцию.
from datetime import datetime, timedelta
from pathlib import Path
import json
import os
import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

ROOT=Path('/opt/airflow/dags/managed/oil_layered_migration')
sys.path.insert(0,str(ROOT))
from oil_layered_plan import OPERATIONS
from oil_layered_runtime import Runner
from oil_layered_cedrus import MCP
from oil_direct_source import DirectSource


def migrate_and_validate(**context):
    """Copy all twenty GP objects with Cedrus only, validate the shared C0 and accept READY."""
    conf=context['dag_run'].conf or {}
    if set(conf)-{'mode'}:
        raise ValueError('INITIAL accepts only mode=initial; use oil_dlh_release with mode=catchup for changes')
    mode=conf.get('mode','initial')
    if mode!='initial':raise ValueError('Use oil_dlh_release with mode=catchup for in-place catchup; _next is retired')
    client=MCP(os.environ['OIL_TRINO_MCP_URL'],os.environ.get('OIL_MCP_CA_FILE'),os.environ.get('OIL_MCP_BEARER_FILE'))
    source=DirectSource(client,mode,context['run_id'])
    runner=Runner(client,'initial',context['run_id'],source_guard=source)
    try:
        runner.prepare()
        for index,op in enumerate(OPERATIONS,1):
            # Runner requires every parent VALIDATED before a child INSERT and checks keys/FK after each object.
            print(json.dumps({'phase':'OBJECT_START','position':index,'total':len(OPERATIONS),'object_symbol':op['symbol'],'parents':op['dependencies']}),flush=True)
            runner.materialize(op['symbol'])
        runner.validate_all()
        result=runner.finish()
    except BaseException:
        # Confirmed candidate writes are cleared in reverse order; unknown/foreign writes require reconciliation.
        try:runner.compensate()
        except Exception as error:print(json.dumps({'phase':'COMPENSATION_REQUIRES_RECONCILIATION','error_type':type(error).__name__}),flush=True)
        raise
    finally:
        source.close()
    print(json.dumps({'phase':'SOURCE_TRANSACTION_CLOSED','source_transaction_closed':True}),flush=True)
    return result


with DAG(dag_id='oil_layered_migration',description='Первичная загрузка 20 таблиц GP через CedrusData Engine и сверки C0',
         start_date=datetime(2026,1,1),schedule=None,catchup=False,max_active_runs=1,
         is_paused_upon_creation=True,default_args={'retries':0,'execution_timeout':timedelta(minutes=60)},
         tags=['oil','initial','cedrus']) as dag:
    # Последовательность двадцати объектов выполняется в одном процессе ради общего snapshot, а не параллельно.
    PythonOperator(task_id='migrate_and_validate',python_callable=migrate_and_validate,pool='oil_dlh_writer')
