# Контекст миграции: AS IS / TO BE.
# Назначение: Исполняет reviewed INITIAL/REFRESH в Airflow и блокирует выпуск при ошибке DQ.
# AS IS: Исходная цепочка работает в GP; инженер вручную исследует, переносит и сверяет сущности.
# TO BE: Bridge оркестрирует те же сервисы по принятому плану, коду и проверкам.
# Отличие: Агент предлагает план/SQL; разрешённый исполнитель применяет принятые операции. Source не удаляется автоматически.
# Конец контекста миграции.
"""Installed Airflow execution library for compiled migration contracts.

This file deliberately has no application database, agent, recipe or business IDs.
Only a reviewed manifest is executed; statements and target names are journaled.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

OWNED=re.compile(r'^pb_[a-f0-9]{20}_[a-f0-9]{8}$')
SYMBOL=re.compile(r'^[a-z][a-z0-9_]{0,62}$')


def hashed(value):
    """Вычисляет SHA-256 сериализованного контракта для проверки неизменности исполнения."""
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def write(path,value):
    """Атомарно заменяет файл execution receipt, чтобы не оставлять половину JSON после сбоя."""
    tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2));os.replace(tmp,path)


def snap(client,table):
    """Возвращает текущий Iceberg snapshot; непустая таблица без history считается ошибкой."""
    catalog,namespace,name=table.split('.')
    rows=client.sql(f'SELECT snapshot_id FROM {catalog}.{namespace}."{name}$history" ORDER BY made_current_at DESC LIMIT 1')
    if rows:return rows[0]['snapshot_id']
    if client.sql('SELECT count(*) n FROM '+table)[0]['n']!=0:raise ValueError('Nonempty table has no snapshot')
    return None


def raw_window(client, table, partitions, initial, timeout=120):
    """Pin a committed raw snapshot only after the complete C0–C1 window exists.

    Kafka may continue growing beyond C1. Those later coordinates do not enter
    this release. A bounded wait covers sink commit latency without holding GP.
    """
    start = {p['partition']: p['end'] for p in initial['manifest']['partitions']} if initial else {p['partition']: p['end'] for p in partitions}
    topic = (initial['manifest']['topic'] if initial else None)
    deadline = time.monotonic() + timeout
    while True:
        snapshot = snap(client, table)
        complete = True
        for part in partitions:
            begin, end = start[part['partition']], part['end']
            if begin > end:
                raise ValueError('Kafka offsets moved backwards')
            if begin == end:
                continue
            if snapshot is None:
                complete = False
                continue
            literal = topic.replace("'", "''")
            rows = client.sql(f"SELECT count(DISTINCT kafka_offset) n FROM {table} FOR VERSION AS OF {snapshot} WHERE topic='{literal}' AND kafka_partition={part['partition']} AND kafka_offset>={begin} AND kafka_offset<{end}")
            if rows[0]['n'] != end - begin:
                complete = False
        if complete:
            return snapshot
        if time.monotonic() >= deadline:
            raise ValueError('Raw C0–C1 window incomplete after bounded catch-up wait')
        time.sleep(2)


def verify_source_readiness(conn, binding, cut, client=None):
    """Deployment-specific read gates, executed while the source fence is held."""
    contract = binding['boundary'].get('readiness')
    if not contract or not contract.get('checkpoint_query') or not contract.get('zero_queries'):
        raise ValueError('Source completeness contract is not configured')
    evidence = []
    with conn.cursor() as cur:
        cur.execute(contract['checkpoint_query'])
        rows = cur.fetchall()
        actual = {int(partition): int(offset) for partition, offset in rows}
        expected = {int(p['partition']): int(p['end']) for p in cut['partitions']}
        if len(actual) != len(rows) or actual != expected:
            raise ValueError('Source checkpoints do not match the fenced Kafka boundary')
        evidence.append({'name': 'source_checkpoints', 'status': 'PASS', 'offsets': actual})
        for check in contract['zero_queries']:
            if check.get('cedrus_query'):
                if client is None:raise ValueError('Registered Cedrus readiness reader required')
                result = [tuple(row.values()) for row in client.sql(check['cedrus_query'])]
            else:
                cur.execute(check['query'])
                result = cur.fetchall()
            if len(result) != 1 or len(result[0]) != 1 or result[0][0] != 0:
                raise ValueError('Source completeness gate failed: ' + check['name'])
            evidence.append({'name': check['name'], 'status': 'PASS', 'violations': 0})
    return evidence


def run(spec,conf,run_id):
    """Выполняет согласованный контракт в порядке зависимостей и блокирует READY при любом нарушении сверки."""
    expected=spec['hash'];body={k:v for k,v in spec.items() if k!='hash'}
    if hashed(body)!=expected or conf.get('spec_hash')!=expected:raise ValueError('Spec hash differs')
    ns=conf.get('namespace','');pid=spec['project_id']
    if not OWNED.fullmatch(ns) or not ns.startswith('pb_'+pid[:20]+'_'):raise ValueError('Foreign namespace')
    mode=conf.get('mode')
    if mode not in ('initial','refresh'):raise ValueError('Invalid mode')
    base=Path('/opt/airflow/logs/platform_bridge')/pid;base.mkdir(mode=0o700,parents=True,exist_ok=True)
    path=base/(ns+'.json')
    if path.exists():raise FileExistsError('Existing execution receipt; reconcile instead of replay')
    state={'status':'STARTING','spec_hash':expected,'namespace':ns,'mode':mode,'run_id':run_id,'steps':[],'checks':[],'snapshots':{}}
    write(path,state)
    binding=spec['connection'];sys.path.insert(0,binding['boundary']['library_dir'])
    from cedrus_release_runner import MCP
    m=MCP(os.environ['OIL_TRINO_MCP_URL'],os.environ.get('OIL_MCP_CA_FILE'),os.environ.get('OIL_MCP_BEARER_FILE'))
    cut={'cut_id':'pb_'+hashed([pid,ns])[:24],'_capture':True}
    initial=None
    if mode=='refresh':
        initial_ns=conf['initial_namespace']
        if not OWNED.fullmatch(initial_ns) or not initial_ns.startswith('pb_'+pid[:20]+'_'):raise ValueError('Foreign INITIAL')
        initial=json.loads((base/(initial_ns+'.json')).read_text())
        if initial['status']!='READY' or initial['spec_hash']!=expected:raise ValueError('INITIAL is not the approved baseline')
        for symbol,expected_snapshot in initial['snapshots'].items():
            if snap(m,binding['target_catalog']+'.'+initial_ns+'.'+symbol)!=expected_snapshot:raise ValueError('INITIAL snapshot changed')
        cut.update(_initial_partitions=initial['manifest']['partitions'],_initial_topic_id=initial['manifest']['topic_id'],_initial_cluster_id=initial['manifest']['cluster_id'])
    spark_client=None
    try:
        from bridge_online_capture import capture
        capture_id='pbc_'+pid[:20]+'_'+ns[-8:]
        state.update(status='CAPTURING_SOURCE',capture_intent=capture_id)
        write(path,state)
        manifest=capture(spec,ns,initial['manifest'] if initial else None)
        state['source_capture']=manifest
        state['manifest']={k:v for k,v in manifest.items() if k!='tables'}
        cut=state['manifest']
        cut_time=manifest['captured_at']
        state['cut_time']=cut_time
        state['source_checks']=[{'name':'source_capture','status':'PASS','violations':0}]
        state.update(status='SOURCE_CAPTURED',source_locks_released=True)
        write(path,state)
        print(json.dumps({'kind':'source_capture','state':'COMMITTED',
                          'capture_id':capture_id,'producer_paused':False,
                          'source_locks_released':True}),flush=True)
        topics=spec['topics']
        if topics!=binding['boundary']['topics']:
            raise ValueError('Source capture does not cover approved topics')
        raw=binding['target_catalog']+'.'+spec['raw_namespace']+'.__raw_events'
        raw_snapshot=raw_window(m,raw,cut['partitions'],initial)
        windows=[]
        for part in cut['partitions']:
            start=next(p['end'] for p in initial['manifest']['partitions'] if p['partition']==part['partition']) if initial else part['end']
            end=part['end'];p=part['partition']
            if start>end:raise ValueError('Kafka offsets moved backwards')
            topic=topics[0].replace("'","''")
            if raw_snapshot is None and start!=end:raise ValueError('Raw snapshot required for a nonempty window')
            count=0 if raw_snapshot is None else m.sql(f"SELECT count(DISTINCT kafka_offset) n FROM {raw} FOR VERSION AS OF {raw_snapshot} WHERE topic='{topic}' AND kafka_partition={p} AND kafka_offset>={start} AND kafka_offset<{end}")[0]['n']
            if count!=end-start:raise ValueError('Raw Kafka window contains a gap')
            windows.append(f'(kafka_partition={p} AND kafka_offset>={start} AND kafka_offset<{end})')
        topic=topics[0].replace("'","''")
        delta=f"(SELECT * FROM {raw} FOR VERSION AS OF {raw_snapshot} WHERE topic='{topic}' AND ("+' OR '.join(windows)+'))'
        if raw_snapshot is None:delta='(SELECT * FROM '+raw+' WHERE false)'
        state['raw_snapshot']=raw_snapshot
        objects=spec['objects']
        spark_client=None
        if any(o.get('dialect')=='spark' for o in spec['operations']):
            from bridge_project_spark import SparkClient
            spark_client=SparkClient('bridge_'+pid+'_'+ns[-8:]);spark_client.open()
        def render(query,dialect='trino'):
            """Подставляет только разрешённые имена объектов, закреплённые snapshots и параметры границы в SQL."""
            def replace(match):
                """Раскрывает один разрешённый placeholder SQL через проверенное сопоставление объектов."""
                side,symbol=match.groups();obj=objects[symbol]
                if side=='source':
                    captured=state['source_capture']['tables'][symbol]
                    return '.'.join('"'+v.replace('"','""')+'"' for v in [binding['source_catalog'],captured['schema'],captured['table']])
                if side=='target':return binding['target_catalog']+'.'+ns+'.'+symbol
                if not initial:raise ValueError('INITIAL reference outside refresh')
                table=binding['target_catalog']+'.'+initial['namespace']+'.'+symbol
                snapshot=initial['snapshots'][symbol]
                return '(SELECT * FROM '+table+' WHERE false)' if snapshot is None else table+(' VERSION AS OF ' if dialect=='spark' else ' FOR VERSION AS OF ')+str(snapshot)
            value=re.sub(r'\{\{(source|target|initial):([a-z][a-z0-9_]*)\}\}',replace,query).replace('{{delta}}',delta.replace(' FOR VERSION AS OF ',' VERSION AS OF ') if dialect=='spark' else delta)
            cut_expr=("TO_TIMESTAMP('"+cut_time+"')") if dialect=='spark' else ("CAST(FROM_ISO8601_TIMESTAMP_NANOS('"+cut_time+"') AS timestamp(6) with time zone)")
            value=value.replace('{{cut_time}}',cut_expr)
            if '{{' in value:raise ValueError('Unresolved template')
            return value
        state['status']='BUILDING'
        for op in spec['operations']:
            symbol=op['symbol']
            if not SYMBOL.fullmatch(symbol):raise ValueError('Invalid symbol')
            table=binding['target_catalog']+'.'+ns+'.'+symbol
            # No reuse of partially filled tables or implicit retry.
            if m.sql('SELECT count(*) n FROM '+table)[0]['n']!=0:raise ValueError('Target table is not pristine')
            dialect=op.get('initial_dialect',op.get('dialect','trino')) if mode=='initial' else op.get('dialect','trino')
            query=render(op['initial_query'] if mode=='initial' else op['refresh_query'],dialect)
            step={'symbol':symbol,'query_hash':hashed(query),'state':'ATTEMPTING'};state['steps'].append(step);write(path,state)
            quote='`' if dialect=='spark' else '"'
            fields=', '.join(quote+c['name'].replace(quote,quote+quote)+quote for c in op['columns'])
            ordered='SELECT '+fields+' FROM ('+query+') reviewed_projection'
            if dialect=='spark':spark_client.sql('INSERT INTO '+table+' '+ordered)
            else:m.sql('INSERT INTO '+table+' '+ordered)
            step['state']='DONE';state['snapshots'][symbol]=snap(m,table);write(path,state)
            print(json.dumps({'kind':'materialization','symbol':symbol,'state':'DONE'}),flush=True)
        for op in spec['operations']:
            for check in op['checks']:
                if check.get('phase','both') not in ('both',mode):continue
                rows=m.sql(render(check['query']))
                if len(rows)!=1 or 'n' not in rows[0] or not isinstance(rows[0]['n'],(int,float)):raise ValueError('Check contract requires one numeric n')
                result={'symbol':op['symbol'],'name':check['name'],'violations':rows[0]['n'],'purpose':check['purpose']};state['checks'].append(result);write(path,state)
                if result['violations']!=0:raise ValueError('Reconciliation failed: '+check['name'])
        for symbol,expected_snapshot in state['snapshots'].items():
            if snap(m,binding['target_catalog']+'.'+ns+'.'+symbol)!=expected_snapshot:raise ValueError('Target changed during checks')
        if spark_client:spark_client.close();spark_client=None
        state.update(status='READY',completed_at=time.time());write(path,state)
    except Exception as exc:
        state.update(status='FAILED_RECONCILE_BEFORE_RETRY',error_type=type(exc).__name__);write(path,state);raise
    finally:
        if spark_client:
            try:spark_client.close()
            except Exception:pass  # Preserve the execution outcome; reconcile the owned kernel separately.
    return {'namespace':ns,'status':state['status'],'checks':len(state['checks'])}
