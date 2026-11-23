# Извлекает исторический вход из одного snapshot GP без остановки producer, GP sink или source DAG.
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path('/opt/airflow/dags/managed/oil_layered_migration')
sys.path.insert(0, str(ROOT))


def export_source(**context):
    """Read a fully applied native snapshot, seal its export and print config for the next DAG."""
    from oil_layered_cedrus import MCP
    from oil_layered_plan import OPERATIONS
    from oil_source_snapshot import SourceSnapshot
    import json
    mode = (context['dag_run'].conf or {}).get('mode')
    client = MCP(os.environ['OIL_TRINO_MCP_URL'], os.environ.get('OIL_MCP_CA_FILE'), os.environ.get('OIL_MCP_BEARER_FILE'))
    source = SourceSnapshot(client, mode, context['run_id'])
    manifest = source.inspect(mode, OPERATIONS, client)
    result = {'status': 'SEALED', 'boundary_id': manifest['boundary_id'], 'export_id': manifest['export_id'],
              'input_snapshot': manifest['input_snapshot'], 'source_transaction_closed': True,
              'next_dag_config': {'mode': mode, 'source_export_run_id': context['run_id']}}
    print(json.dumps(result), flush=True)
    return result


with DAG(dag_id='oil_source_snapshot', description='Нативный snapshot GP и проверенный исторический вход Iceberg',
         start_date=datetime(2026,1,1), schedule=None, catchup=False, max_active_runs=1,
         is_paused_upon_creation=True, default_args={'retries':0, 'execution_timeout':timedelta(minutes=45)},
         tags=['oil','online','snapshot']) as dag:
    # GP-транзакция живёт только в этой task; downstream расчёты читают уже закреплённый экспорт.
    PythonOperator(task_id='export_source', python_callable=export_source)
