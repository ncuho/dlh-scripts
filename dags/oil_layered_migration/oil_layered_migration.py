# Одна task удерживает одно native GP-подключение для всей миграции; Airflow-процессы не делят транзакцию.
from datetime import datetime, timedelta
from pathlib import Path
import json
import os
import sys
import uuid
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

ROOT=Path('/opt/airflow/dags/managed/oil_layered_migration')
sys.path.insert(0,str(ROOT))
from oil_layered_plan import OPERATIONS
from oil_layered_runtime import Runner
from oil_layered_cedrus import MCP
from oil_layered_spark import SparkClient
from oil_direct_source import DirectSource


def migrate_and_validate(**context):
    """Read one GP cut, load parents before children, run Spark and all quality gates, then accept READY."""
    conf=context['dag_run'].conf or {}
    if set(conf)-{'mode','overlap_offsets'}:
        raise ValueError('Use only mode and optional overlap_offsets; export/lock IDs are obsolete')
    mode=conf.get('mode')
    client=MCP(os.environ['OIL_TRINO_MCP_URL'],os.environ.get('OIL_MCP_CA_FILE'),os.environ.get('OIL_MCP_BEARER_FILE'))
    source=DirectSource(client,mode,context['run_id'])
    runner=Runner(client,mode,context['run_id'],spark_factory=lambda:SparkClient('bridge_'+uuid.uuid4().hex),
                  overlap_offsets=conf.get('overlap_offsets',10),source_guard=source)
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


with DAG(dag_id='oil_layered_migration',description='Прямая миграция GP в Bronze/Silver/Gold, Spark, ключи и сверки',
         start_date=datetime(2026,1,1),schedule=None,catchup=False,max_active_runs=1,
         is_paused_upon_creation=True,default_args={'retries':0,'execution_timeout':timedelta(minutes=60)},
         tags=['oil','bronze','silver','gold']) as dag:
    # Последовательность двадцати объектов выполняется в одном процессе ради общего snapshot, а не параллельно.
    PythonOperator(task_id='migrate_and_validate',python_callable=migrate_and_validate)
