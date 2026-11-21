"""Draft wave-1 calculation. Dry-run default; never activates BI or moves source data.

Optional staging writes four independently committed Iceberg tables. The caller
must provide a coherent source cut, hold a single-writer fence and protect all
input snapshots; none of these guarantees is supplied by this script.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

SOURCES = ('sat_field', 'sat_well', 'sat_daily_production', 'link_well_field', 'link_production_day_well')


def read_manifest(path):
    raw = Path(path).read_bytes()
    data = json.loads(raw)
    if isinstance(data, dict) and isinstance(data.get('calculation_input'), dict):
        data = data['calculation_input']
    if not isinstance(data, dict) or not isinstance(data.get('cut_id'), str) or not data['cut_id'].strip():
        raise ValueError('manifest requires a non-empty cut_id')
    snapshots = data.get('snapshots')
    if not isinstance(snapshots, dict) or set(snapshots) != set(SOURCES):
        raise ValueError('manifest must contain exactly the five source tables')
    if any(type(v) is not int or v <= 0 or v >= 2**63 for v in snapshots.values()):
        raise ValueError('snapshot IDs must be positive signed 64-bit integers')
    tables = data.get('tables')
    if tables is not None:
        if not isinstance(tables, dict) or set(tables) != set(SOURCES):
            raise ValueError('manifest tables must contain exactly five input tables')
        for name, table in tables.items():
            if not isinstance(table, str) or not re.fullmatch(r'dlh\.oil_dds\.r_[a-z][a-z0-9_]{0,30}_' + re.escape(name), table):
                raise ValueError('invalid staged table identifier')
    return data, hashlib.sha256(raw).hexdigest()


def run(input_manifest=None, release_id=None, release_manifest=None, sample_rows=3):
    manifest, manifest_hash = read_manifest(input_manifest) if input_manifest else (None, None)
    if release_id and (not manifest or not release_manifest or not re.fullmatch(r'[a-z][a-z0-9_]{0,30}', release_id)):
        raise ValueError('release requires valid release_id, input manifest and output manifest')
    if release_manifest and Path(release_manifest).exists():
        raise FileExistsError('release manifest already exists')
    sys.path.insert(0, '/home/admin')
    from dp103_spark import connect_dlh
    spark = connect_dlh()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    for source in SOURCES:
        version = f" VERSION AS OF {manifest['snapshots'][source]}" if manifest else ''
        table = manifest.get('tables',{}).get(source, f'dlh.oil_dds.{source}') if manifest else f'dlh.oil_dds.{source}'
        spark.sql(f'CREATE OR REPLACE TEMP VIEW src_{source} AS SELECT * FROM {table}{version}')
    sql = spark.sql
    checks = {
        'field_effective_date_duplicate': 'SELECT count(*) FROM (SELECT field_hk,event_time FROM src_sat_field GROUP BY field_hk,event_time HAVING count(*)>1)',
        'well_effective_date_duplicate': 'SELECT count(*) FROM (SELECT well_hk,valid_from FROM src_sat_well GROUP BY well_hk,valid_from HAVING count(*)>1)',
        'field_event_duplicate': 'SELECT count(*) FROM (SELECT event_id FROM src_sat_field GROUP BY event_id HAVING count(*)>1)',
        'well_event_duplicate': 'SELECT count(*) FROM (SELECT event_id FROM src_sat_well GROUP BY event_id HAVING count(*)>1)',
        'production_event_duplicate': 'SELECT count(*) FROM (SELECT event_id FROM src_sat_daily_production GROUP BY event_id HAVING count(*)>1)',
        'production_version_duplicate': 'SELECT count(*) FROM (SELECT production_hk,entity_version FROM src_sat_daily_production GROUP BY production_hk,entity_version HAVING count(*)>1)',
        'well_link_duplicate': 'SELECT count(*) FROM (SELECT link_hk FROM src_link_well_field GROUP BY link_hk HAVING count(*)>1)',
        'production_link_duplicate': 'SELECT count(*) FROM (SELECT link_hk FROM src_link_production_day_well GROUP BY link_hk HAVING count(*)>1)',
    }
    results = {name: sql(query).collect()[0][0] for name, query in checks.items()}
    if any(results.values()):
        print(json.dumps({'status': 'quality_failed', 'checks': results})); return 2
    sql('''CREATE OR REPLACE TEMP VIEW calc_dim_field AS
      SELECT field_hk,entity_version,event_time valid_from,
      lead(event_time) OVER(PARTITION BY field_hk ORDER BY event_time,entity_version) valid_to,
      field_name,region,(op='D') is_deleted FROM src_sat_field''')
    sql('''CREATE OR REPLACE TEMP VIEW calc_dim_well AS
      SELECT w.well_hk,l.field_hk,w.entity_version,w.valid_from,
      lead(w.valid_from) OVER(PARTITION BY w.well_hk ORDER BY w.valid_from,w.entity_version) valid_to,
      w.well_name,w.status,(w.op='D') is_deleted
      FROM src_sat_well w JOIN src_link_well_field l ON l.link_hk=w.link_hk''')
    sql('''CREATE OR REPLACE TEMP VIEW current_production AS SELECT * FROM (
      SELECT *,row_number() OVER(PARTITION BY production_hk ORDER BY entity_version DESC,event_id DESC) rn
      FROM src_sat_daily_production) q WHERE rn=1 AND op='U' ''')
    sql('''CREATE OR REPLACE TEMP VIEW calc_fact AS
      SELECT p.production_hk,l.well_hk,w.field_hk,p.production_date,w.entity_version well_version,
      f.entity_version field_version,p.status,p.oil_tonnes
      FROM current_production p JOIN src_link_production_day_well l ON l.link_hk=p.link_hk
      JOIN calc_dim_well w ON w.well_hk=l.well_hk AND NOT w.is_deleted AND w.valid_from<=p.production_date
       AND (w.valid_to IS NULL OR p.production_date<w.valid_to)
      JOIN calc_dim_field f ON f.field_hk=w.field_hk AND NOT f.is_deleted AND f.valid_from<=CAST(p.production_date AS TIMESTAMP)
       AND (f.valid_to IS NULL OR CAST(p.production_date AS TIMESTAMP)<f.valid_to)''')
    checks2 = {
      'production_not_one_fact': '''SELECT count(*) FROM current_production p LEFT JOIN
        (SELECT production_hk,count(*) n FROM calc_fact GROUP BY production_hk) f ON f.production_hk=p.production_hk WHERE coalesce(f.n,0)<>1''',
      'status_mismatch': '''SELECT count(*) FROM calc_fact p JOIN calc_dim_well w
        ON p.well_hk=w.well_hk AND p.well_version=w.entity_version
        WHERE p.status<>w.status OR (p.status='INACTIVE' AND p.oil_tonnes<>0)''',
      'duplicate_well_day': '''SELECT count(*) FROM (SELECT well_hk,production_date FROM calc_fact GROUP BY well_hk,production_date HAVING count(*)>1)''',
      'missing_well_link': '''SELECT count(*) FROM src_sat_well w LEFT JOIN src_link_well_field l ON l.link_hk=w.link_hk WHERE l.link_hk IS NULL''',
      'invalid_oil': '''SELECT count(*) FROM current_production WHERE oil_tonnes IS NULL OR oil_tonnes<0 OR status NOT IN ('ACTIVE','INACTIVE') OR status IS NULL''',
    }
    results.update({name: sql(query).collect()[0][0] for name, query in checks2.items()})
    if any(results.values()):
        print(json.dumps({'status': 'quality_failed', 'checks': results})); return 2
    outputs = {
      'dim_field': sql('SELECT * FROM calc_dim_field'),
      'dim_well': sql('SELECT * FROM calc_dim_well'),
      'fact_well_day': sql('SELECT * FROM calc_fact'),
      'agg_field_day': sql('''SELECT p.production_date,p.field_hk,f.field_name,f.region,
        CAST(sum(p.oil_tonnes) AS DECIMAL(22,3)) oil_tonnes,
        CAST(sum(CASE WHEN p.status='ACTIVE' THEN 1 ELSE 0 END) AS BIGINT) producing_well_days,
        CASE WHEN sum(CASE WHEN p.status='ACTIVE' THEN 1 ELSE 0 END)=0 THEN CAST(NULL AS DECIMAL(22,6))
        ELSE CAST(sum(p.oil_tonnes)/sum(CASE WHEN p.status='ACTIVE' THEN 1 ELSE 0 END) AS DECIMAL(22,6)) END oil_per_well_day
        FROM calc_fact p JOIN calc_dim_field f ON f.field_hk=p.field_hk AND f.entity_version=p.field_version
        GROUP BY p.production_date,p.field_hk,f.field_name,f.region'''),
    }
    summary = {name: {'rows': df.count(), 'columns': df.columns,
               'sample': [row.asDict(recursive=True) for row in df.limit(sample_rows).collect()] if sample_rows else []}
               for name, df in outputs.items()}
    if not summary['fact_well_day']['rows']:
        print(json.dumps({'status':'empty_result','checks':results,'outputs':summary},default=str)); return 2
    created = {}
    if release_id:
        try:
            for name, frame in outputs.items():
                table = f'dlh.oil_mart.r_{release_id}_{name}'
                frame.writeTo(table).using('iceberg').create()
                created[name] = {'table': table}
                rows = spark.table(table).count()
                if rows != summary[name]['rows']:
                    raise RuntimeError(f'row count mismatch: {table}')
                snapshots = sql(f'SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1').collect()
                if len(snapshots) != 1:
                    raise RuntimeError(f'no snapshot: {table}')
                created[name].update({'snapshot_id': int(snapshots[0][0]), 'rows': rows})
            release = {'status':'staged','release_id':release_id,'created_utc':datetime.now(timezone.utc).isoformat(),
                       'input_cut_id':manifest['cut_id'],'input_manifest_sha256':manifest_hash,
                       'input_snapshots':manifest['snapshots'],'outputs':created,'checks':results,
                       'notice':'Separate table commits; cut, fencing and activation are external.'}
            with open(release_manifest, 'x', encoding='utf-8') as f:
                json.dump(release, f, ensure_ascii=False, indent=2); f.write('\n')
        except Exception as exc:
            print(json.dumps({'status':'stage_failed','created':created,'error':str(exc),
                              'notice':'Partial output tables may remain; inspect before cleanup.'}))
            raise
    print(json.dumps({'status':'staged' if release_id else 'dry_run','checks':results,'outputs':summary,
                      'created':created,'input_manifest_sha256':manifest_hash,
                      'notice':'Pinned snapshots do not prove cross-table consistency.' if manifest else 'Inputs not pinned.'},
                     ensure_ascii=False, default=str))
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-manifest')
    parser.add_argument('--stage-release')
    parser.add_argument('--release-manifest')
    parser.add_argument('--sample-rows', type=int, default=3)
    args = parser.parse_args()
    if not 0 <= args.sample_rows <= 20:
        parser.error('--sample-rows must be between 0 and 20')
    if bool(args.stage_release) != bool(args.release_manifest):
        parser.error('--stage-release and --release-manifest must be used together')
    if args.stage_release and not args.input_manifest:
        parser.error('--stage-release requires --input-manifest')
    raise SystemExit(run(args.input_manifest, args.stage_release, args.release_manifest, args.sample_rows))
