# Загружает Bronze, Silver и Gold с проверкой каждого объекта и компенсирующим откатом кандидатного выпуска.
# GP-функции заменяются SQL/Spark-расчётами; Airflow контролирует зависимости, проверки и публикационную готовность.
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path('/opt/airflow/dags/managed/oil_layered_migration')
sys.path.insert(0, str(ROOT))
from oil_layered_plan import OPERATIONS
from oil_layered_runtime import Runner
from oil_layered_cedrus import MCP
from oil_layered_spark import SparkClient
from oil_source_lock import SourceLock


# Один task выполняет только выбранную операцию; журнал привязан к фактическому Airflow run_id.
def execute(action, symbol=None, **context):
    """Open service clients and execute preflight, one table, a gate or compensation."""
    mode = (context['dag_run'].conf or {}).get('mode')
    client = MCP(os.environ['OIL_TRINO_MCP_URL'], os.environ.get('OIL_MCP_CA_FILE'), os.environ.get('OIL_MCP_BEARER_FILE'))
    runner = Runner(client, mode, context['run_id'], spark_factory=lambda: SparkClient('bridge_' + uuid.uuid4().hex),
                    overlap_offsets=(context['dag_run'].conf or {}).get('overlap_offsets', 10),
                    source_guard=SourceLock((context['dag_run'].conf or {}).get('source_lock')))
    if action == 'table':
        return runner.materialize(symbol)
    return getattr(runner, action)()


with DAG(
    dag_id='oil_layered_migration',
    description='Перенос добычи в Bronze/Silver/Gold с проверками ключей и откатом кандидата',
    start_date=datetime(2026, 1, 1), schedule=None, catchup=False,
    is_paused_upon_creation=True, max_active_runs=1,
    default_args={'retries': 0, 'execution_timeout': timedelta(minutes=30)},
    tags=['oil', 'bronze', 'silver', 'gold'],
) as dag:
    prepare = PythonOperator(task_id='prepare_candidate', python_callable=execute, op_kwargs={'action': 'prepare'})
    previous = prepare
    for operation in OPERATIONS:
        # Последовательность включает ссылки FK из DQ: родитель должен быть загружен и проверен до дочерней таблицы.
        current = PythonOperator(task_id=operation['symbol'], python_callable=execute,
                                 op_kwargs={'action': 'table', 'symbol': operation['symbol']})
        previous >> current
        previous = current
    validate = PythonOperator(task_id='validate_all_20_tables', python_callable=execute, op_kwargs={'action': 'validate_all'})
    previous >> validate
    # all_done запускает компенсацию после сбоя; UNKNOWN_OUTCOME остаётся закрытым до сверки, а не очищается вслепую.
    rollback = PythonOperator(task_id='compensate_if_failed', python_callable=execute,
                              op_kwargs={'action': 'compensate'}, trigger_rule='all_done')
    validate >> rollback
    # Финальный task специально завершится ошибкой после отката: зелёная компенсация не делает неуспешный перенос успешным.
    finish = PythonOperator(task_id='accept_ready_release', python_callable=execute, op_kwargs={'action': 'finish'})
    rollback >> finish
