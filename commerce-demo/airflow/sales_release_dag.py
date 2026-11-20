"""Draft manual DAG for an isolated staged release; never a migration trigger.

Worker must supply shared protected paths and a Python/Spark runtime. The external
lock and approved consistent cut are operator responsibilities, not implemented
by this DAG. Install paused only after reviewing deployment and connectivity.
"""
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator


def stage(**context):
    config = context['dag_run'].conf or {}
    release_id = config.get('release_id', '')
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,30}', release_id):
        raise ValueError('invalid release_id')
    if config.get('external_fence_attested') is not True:
        raise ValueError('an independently acquired single-writer fence must be attested')
    if config.get('consistent_cut_approved') is not True:
        raise ValueError('source snapshot cut must be independently validated and approved')
    root = Path(os.environ['DEMO_RELEASE_ROOT']).resolve(strict=True)
    job = Path(os.environ['DEMO_SPARK_JOB']).resolve(strict=True)
    # Only predetermined filenames under a configured protected directory.
    input_path = root / (release_id + '.input.json')
    output_path = root / (release_id + '.release.json')
    if input_path.is_symlink() or output_path.exists() or output_path.is_symlink():
        raise ValueError('input symlink or output already exists')
    if not input_path.is_file():
        raise ValueError('approved input manifest missing')
    python = os.environ.get('DEMO_SPARK_PYTHON', '/usr/bin/python3')
    result = subprocess.run([python, str(job), '--input-manifest', str(input_path),
                             '--stage-release', release_id, '--release-manifest', str(output_path),
                             '--sample-rows', '0'], check=False, timeout=3500)
    if result.returncode:
        raise RuntimeError('Spark staging failed; inspect logs and partial release tables')
    manifest = json.loads(output_path.read_text(encoding='utf-8'))
    if manifest.get('status') != 'staged' or manifest.get('release_id') != release_id:
        raise RuntimeError('release manifest invalid')
    # XCom carries references only; publication and reconciliation remain separate gates.
    return {'release_id': release_id, 'release_manifest': str(output_path)}


with DAG(
    dag_id='demo_sales_release_staging',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={'retries': 0, 'execution_timeout': timedelta(hours=1)},
    tags=['demo', 'manual', 'staging-only'],
) as dag:
    PythonOperator(task_id='stage_release', python_callable=stage)
