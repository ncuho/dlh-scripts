"""Bounded, immutable, release-scoped event canonicalization for the oil demo.

Does not publish a dashboard, execute migration into oil_* schemas or guarantee CDC.
Inputs MUST be pre-frozen Iceberg snapshots and externally verified Kafka offsets.
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
from datetime import datetime, timezone

FQN = re.compile(r'^dlh\.[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$')
RELEASE = re.compile(r'^oil_r_[a-z0-9_]{1,42}$')

def manifest(path):
    x = json.loads(Path(path).read_text())
    for name in ('baseline_table', 'delta_table'):
        if not isinstance(x.get(name), str) or not FQN.fullmatch(x[name]):
            raise ValueError('invalid '+name)
    for name in ('baseline_snapshot', 'delta_snapshot'):
        if type(x.get(name)) is not int or x[name] <= 0:
            raise ValueError('positive snapshot ID required: '+name)
    if not isinstance(x.get('cut_id'), str) or not x['cut_id']:
        raise ValueError('cut_id required')
    for key in ('topic_id','cluster_id'):
        if not isinstance(x.get(key),str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',x[key]):
            raise ValueError('valid '+key+' required')
    offsets = x.get('partitions')
    if not isinstance(offsets, list) or not offsets:
        raise ValueError('nonempty Kafka partitions required')
    seen = set()
    for part in offsets:
        if not isinstance(part, dict) or type(part.get('partition')) is not int or type(part.get('start')) is not int or type(part.get('end')) is not int:
            raise ValueError('offset bounds must be integers')
        if part['partition'] < 0 or part['partition'] in seen or not 0 <= part['start'] <= part['end']:
            raise ValueError('invalid or duplicate partition')
        seen.add(part['partition'])
    return x

def write_json(path, obj):
    tmp = path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(obj,ensure_ascii=False,indent=2)+'\n')
    os.replace(tmp,path)

def run(args):
    x=manifest(args.manifest)
    if not RELEASE.fullmatch(args.schema):
        raise ValueError('target schema must be isolated oil_r_<id>')
    target='dlh.'+args.schema
    evidence=Path(args.evidence)
    if evidence.exists():
        raise FileExistsError('evidence exists; reconcile partial release, do not retry blindly')
    with evidence.open('x',encoding='utf-8') as f:
        json.dump({'status':'starting','cut_id':x['cut_id'],'schema':args.schema,'steps':[]},f)
    safe_inputs={k:x[k] for k in ('cut_id','topic_id','cluster_id','baseline_table','delta_table','baseline_snapshot','delta_snapshot','partitions')}
    state={'status':'running','cut_id':x['cut_id'],'schema':args.schema,'steps':[],'checks':{},'inputs':safe_inputs}
    write_json(evidence,state)
    try:
        sys.path.insert(0,'/home/admin')
        from dp103_spark import connect_dlh
        from pyspark.sql import functions as F
        spark=connect_dlh();spark.conf.set('spark.sql.session.timeZone','UTC')
        # Immutable input snapshots prevent a moving raw table during batch processing.
        baseline=spark.sql(f"SELECT event_document FROM {x['baseline_table']} VERSION AS OF {x['baseline_snapshot']}")
        raw=spark.sql(f"SELECT topic,kafka_partition,kafka_offset,event_json FROM {x['delta_table']} VERSION AS OF {x['delta_snapshot']}")
        windows=[]
        for p in x['partitions']:
            windows.append((F.col('kafka_partition')==p['partition']) & (F.col('kafka_offset')>=p['start']) & (F.col('kafka_offset')<p['end']))
        from functools import reduce
        from operator import or_
        bounded=raw.where((F.col('topic')==F.lit(x.get('topic','oil.events.v1'))) & reduce(or_,windows))
        conflicts=bounded.groupBy('topic','kafka_partition','kafka_offset').agg(F.countDistinct('event_json').alias('n')).where('n <> 1').count()
        if conflicts: raise ValueError('different payloads for one Kafka position')
        positions=bounded.dropDuplicates(['topic','kafka_partition','kafka_offset']).groupBy('kafka_partition').count().collect()
        counts={r['kafka_partition']:r['count'] for r in positions}
        for p in x['partitions']:
            if counts.get(p['partition'],0)!=p['end']-p['start']:
                raise ValueError('Kafka offset gap or missing raw rows: '+str(p['partition']))
        docs=baseline.select(F.col('event_document').alias('document')).unionByName(bounded.select(F.col('event_json').alias('document')))
        for col,path in [('event_id','$.event_id'),('event_type','$.event_type'),('entity_id','$.entity_id'),('entity_version','$.entity_version'),('op','$.op'),('event_time','$.event_time'),('schema_version','$.schema_version')]:
            docs=docs.withColumn(col,F.get_json_object('document',path))
        bad=docs.where(F.col('document').isNull() | F.col('event_id').isNull() | (F.col('event_id')=='') | F.col('entity_id').isNull() | F.col('event_type').isNull() | (~F.col('event_type').isin('field','well','production_day','downtime')) | F.col('op').isNull() | (~F.col('op').isin('U','D')) | F.col('schema_version').isNull() | (F.col('schema_version')!='1') | F.col('entity_version').cast('bigint').isNull() | (F.col('entity_version').cast('bigint')<1) | F.to_timestamp('event_time').isNull()).count()
        if bad: raise ValueError('invalid event envelopes: '+str(bad))
        conflict=docs.groupBy('event_id').agg(F.countDistinct('document').alias('n')).where('n > 1').count()
        if conflict: raise ValueError('event_id reused with different document: '+str(conflict))
        unique=docs.dropDuplicates(['event_id','document']).withColumn('entity_version',F.col('entity_version').cast('bigint'))
        conflict=unique.groupBy('event_type','entity_id','entity_version').agg(F.countDistinct('event_id').alias('n')).where('n > 1').count()
        if conflict: raise ValueError('entity version has conflicting events: '+str(conflict))
        unique=unique.withColumn('event_time',F.to_timestamp('event_time'))
        state['checks']={'positions_contiguous':True,'event_id_conflicts':0,'entity_version_conflicts':0,'invalid_envelopes':0}
        # A unique namespace and explicit new table make writes immutable and retry-safe:
        # any failure/timeout leaves a quarantined partial release, never a silent overwrite.
        existing=spark.sql(f"SHOW TABLES IN {target}").collect()
        if existing: raise ValueError('release namespace must be empty')
        name=target+'.canonical_event'
        state['steps'].append({'table':name,'status':'attempting'});write_json(evidence,state)
        unique.writeTo(name).using('iceberg').create()
        count=spark.table(name).count()
        history=spark.sql(f'SELECT snapshot_id FROM {target}.`canonical_event$snapshots` ORDER BY committed_at DESC LIMIT 1').collect()
        state['steps'][-1]={'table':name,'status':'created','rows':count,'snapshot_id':int(history[0][0]) if history else None}
        if not history: raise RuntimeError('no snapshot evidence')
        # Explicitly fail closed: DDS/MART rebuilding and reconciliation must be present
        # before this release is eligible for BI promotion.
        state['status']='canonicalized_not_publishable'
        state['blockers']=['DDS/MART rebuild and independent reconciliation not executed']
        write_json(evidence,state)
        print(json.dumps({'status':state['status'],'rows':count,'evidence':str(evidence)}))
        return 3
    except Exception as e:
        state['status']='failed_reconcile_before_retry';state['error_type']=type(e).__name__;write_json(evidence,state)
        raise

def main():
    p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);p.add_argument('--schema',required=True);p.add_argument('--evidence',required=True);a=p.parse_args()
    try:
        return run(a)
    except Exception as exc:
        print(json.dumps({'status':'failed','error_type':type(exc).__name__}),file=sys.stderr)
        return 2
if __name__=='__main__':raise SystemExit(main())
