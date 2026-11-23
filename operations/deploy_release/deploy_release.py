#!/usr/bin/env python3
# Контекст миграции: AS IS / TO BE.
# Назначение: python3 deploy_release.py загружает проверенный DAG и библиотеки в настроенный S3-префикс Airflow DAGs/managed/. Учётные данные используются внутри существующего sidecar; старый файл этого DAG удаляется после проверки новых объектов. Бизнес.
# AS IS: Исходный source release применяется внутри GP.
# TO BE: Разворачивает явно выбранный target runtime/выпуск.
# Отличие: Не смешивайте старый deployment с текущими namespaces проекта и его принятым SQL.
# Конец контекста миграции.
"""Publish reviewed release code to the configured Airflow S3 DAG source."""
import hashlib,json,shlex,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
SSH=['ssh','-o','BatchMode=yes','redos@79.137.175.114']
files={}
for rel in ['pipelines/sql_plan/sql_plan.py','pipelines/dq_sql/dq_sql.py','pipelines/cedrus_release_runner/cedrus_release_runner.py','catalog/runtime_catalog/runtime_catalog.py']:
 p=ROOT/rel;compile(p.read_text(),str(p),'exec');files['managed/oil_wave2/'+p.name]=p.read_text()
dag=(ROOT/'dags/oil_wave2_release/oil_wave2_release.py').read_text();compile(dag,'oil_wave2_release.py','exec')
name='managed/oil_wave2_release__'+hashlib.sha256(dag.encode()).hexdigest()+'.py';files[name]=dag
program="""import os,json,sys,hashlib,botocore.session
x=json.load(sys.stdin);s=botocore.session.get_session().create_client('s3',endpoint_url=os.environ['AWS_ENDPOINT_URL_S3']);bucket=os.environ['S3_BUCKET'];prefix=os.environ['S3_BUCKET_FOLDER'].rstrip('/')+'/'
if prefix!='DAGs/':raise RuntimeError('unexpected DAG prefix')
for k,v in x.items():
 s.put_object(Bucket=bucket,Key=prefix+k,Body=v.encode(),ContentType='text/x-python')
 if s.get_object(Bucket=bucket,Key=prefix+k)['Body'].read()!=v.encode():raise RuntimeError('upload verification failed')
for o in s.list_objects_v2(Bucket=bucket,Prefix=prefix+'managed/oil_wave2_release__').get('Contents',[]):
 if o['Key'] not in {prefix+k for k in x}:s.delete_object(Bucket=bucket,Key=o['Key'])
print(json.dumps({'uploaded':list(x),'dag_source':prefix}))
"""
cmd=['kubectl','exec','-i','-n','airflow-atomcluster-4jn2dw24','airflow-worker-0','-c','s3-sync','--','python3','-c',program]
r=subprocess.run(SSH+[shlex.join(cmd)],input=json.dumps(files),text=True,capture_output=True,timeout=120)
if r.returncode:raise RuntimeError('DAG source upload failed; inspect service logs')
print(r.stdout)
