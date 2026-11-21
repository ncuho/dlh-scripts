"""Draft: stage 8 DDS tables from an externally frozen GP clone into new Iceberg tables.
No data movement unless --execute is explicit. Does not create or freeze the clone,
prove a Kafka cut, implement CDC, activate consumers or guarantee multi-table atomicity.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit, parse_qs

SOURCES = ('hub_field','hub_well','hub_production_day','link_well_field',
           'link_production_day_well','sat_field','sat_well','sat_daily_production')
CALCULATION_SOURCES = ('sat_field','sat_well','sat_daily_production','link_well_field','link_production_day_well')


def validate_plan(path):
    raw = Path(path).read_bytes()
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError('plan must be a JSON object')
    for key in ('cut_id','snapshot_id','freeze_evidence','source_database','source_cluster'):
        if not isinstance(data.get(key), str) or not data[key].strip():
            raise ValueError(f'plan needs non-empty {key}')
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,62}',data['source_database']):
        raise ValueError('invalid source database identifier')
    if data['source_database'] in ('og_dwh','postgres','template0','template1'):
        raise ValueError('source must be an isolated frozen clone, never a live database')
    offsets=data.get('kafka_end_offsets_exclusive')
    if not isinstance(offsets,dict) or not offsets or any(not isinstance(t,str) or not t.strip() or not isinstance(p,dict) or not p or any(not str(k).isdigit() or type(v) is not int or v<0 for k,v in p.items()) for t,p in offsets.items()):
        raise ValueError('plan requires Kafka topic, partition and non-negative exclusive offsets')
    return data,hashlib.sha256(raw).hexdigest()


def validate_url(url, database):
    if not url.startswith('jdbc:postgresql://'):
        raise ValueError('JDBC URL must use PostgreSQL')
    parsed=urlsplit(url[5:]); params=parse_qs(parsed.query)
    if parsed.username or parsed.password or parsed.path != '/' + database:
        raise ValueError('JDBC URL must identify the approved frozen clone without credentials')
    if params.get('sslmode') != ['verify-full'] or len(params.get('sslrootcert',[])) != 1:
        raise ValueError('JDBC requires sslmode=verify-full and a CA certificate path')
    if any(key.lower() in ('user','password') for key in params):
        raise ValueError('credentials must not be embedded in JDBC URL')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan',required=True)
    p.add_argument('--release-id',required=True)
    p.add_argument('--output-manifest',required=True)
    p.add_argument('--execute',action='store_true')
    a=p.parse_args()
    plan,plan_hash=validate_plan(a.plan)
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,30}',a.release_id):
        p.error('release ID must match [a-z][a-z0-9_]{0,30}')
    if Path(a.output_manifest).exists():
        p.error('output manifest already exists')
    if not a.execute:
        print(json.dumps({'status':'plan_only','cut_id':plan['cut_id'],'source_tables':list(SOURCES),
                          'plan_sha256':plan_hash,'notice':'No service connection or data movement.'}))
        return
    url=os.environ['OIL_GP_JDBC_URL']; user=os.environ['OIL_GP_USER']; password=os.environ['OIL_GP_PASSWORD']
    validate_url(url,plan['source_database'])
    if not user or not password: raise ValueError('missing GP read-only credentials')
    import sys
    sys.path.insert(0,'/home/admin')
    from dp103_spark import connect_dlh
    spark=connect_dlh(); spark.conf.set('spark.sql.session.timeZone','UTC')
    created={}
    try:
        for name in SOURCES:
            # No partitionColumn: one JDBC partition/connection per read. Repeated
            # reads are safe only if operator's source freeze is actually enforced.
            reader=(spark.read.format('jdbc').option('url',url).option('driver','org.postgresql.Driver')
                    .option('dbtable',f'oil_dds.{name}').option('user',user).option('password',password)
                    .option('fetchsize','10000'))
            frame=reader.load()
            source_count=frame.count()
            target=f'dlh.oil_dds.r_{a.release_id}_{name}'
            frame.writeTo(target).using('iceberg').create()
            created[name]={'table':target,'source_rows':source_count}
            target_count=spark.table(target).count()
            if target_count != source_count: raise RuntimeError(f'row count mismatch for {target}')
            rows=spark.sql(f'SELECT snapshot_id FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1').collect()
            if len(rows)!=1: raise RuntimeError(f'no snapshot in {target}')
            created[name].update({'target_rows':target_count,'snapshot_id':int(rows[0][0])})
        result={'status':'staged','release_id':a.release_id,'created_utc':datetime.now(timezone.utc).isoformat(),
                'cut_id':plan['cut_id'],'source_plan_sha256':plan_hash,'objects':created,
                'calculation_input':{'cut_id':plan['cut_id'],
                 'snapshots':{n:created[n]['snapshot_id'] for n in CALCULATION_SOURCES},
                 'tables':{n:created[n]['table'] for n in CALCULATION_SOURCES}},
                'notice':'Independent commits. Counts are not reconciliation. Freeze and fencing external.'}
        with open(a.output_manifest,'x',encoding='utf-8') as f:
            json.dump(result,f,ensure_ascii=False,indent=2);f.write('\n')
        print(json.dumps({'status':'staged','release_id':a.release_id,'objects':created},ensure_ascii=False))
    except Exception as exc:
        # JDBC errors can contain URLs; keep logs free of exception messages.
        print(json.dumps({'status':'stage_failed','release_id':a.release_id,'created':created,
                          'error_type':type(exc).__name__,'notice':'Partial tables may remain; inspect all eight names.'}))
        raise SystemExit(2)


if __name__ == '__main__': main()
