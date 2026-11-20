"""Плановый Spark Connect расчёт. По умолчанию только чтение.

Dry-run по умолчанию. --stage-release создаёт пять отдельных таблиц по manifest
снимков. Каждая таблица фиксируется отдельно; переключения BI и CDC здесь нет.
Согласованность исходного cut и единственность писателя обеспечивает оператор.
"""
import argparse
import json
import sys
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone

SOURCES = ("sat_product", "sat_store", "sat_shipment", "sat_return",
           "hub_shipment", "hub_return", "link_shipment", "link_return")

def read_manifest(path):
    raw = Path(path).read_bytes()
    data = json.loads(raw)
    if not isinstance(data, dict) or not isinstance(data.get("cut_id"), str) or not data["cut_id"].strip():
        raise ValueError("manifest requires non-empty cut_id")
    snapshots = data.get("snapshots")
    if not isinstance(snapshots, dict) or set(snapshots) != set(SOURCES):
        raise ValueError("manifest must contain exactly the eight declared source snapshots")
    if any(type(v) is not int or not 0 < v < 2**63 for v in snapshots.values()):
        raise ValueError("snapshot IDs must be positive signed 64-bit integers")
    return data, hashlib.sha256(raw).hexdigest()


def run(sample_rows=5, manifest_path=None, release_id=None, release_manifest=None):
    sys.path.insert(0, '/home/admin')
    from dp103_spark import connect_dlh
    from pyspark.sql import functions as F

    spark = connect_dlh()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    manifest, manifest_sha = read_manifest(manifest_path) if manifest_path else (None, None)
    if release_id and not manifest:
        raise ValueError('A snapshot manifest is mandatory for staging a release')
    if release_id and not re.fullmatch(r'[a-z][a-z0-9_]{0,30}', release_id):
        raise ValueError('release_id must match [a-z][a-z0-9_]{0,30}')
    if release_id and not release_manifest:
        raise ValueError('A release manifest output path is mandatory')
    if release_manifest and Path(release_manifest).exists():
        raise FileExistsError('release manifest already exists')
    if manifest:
        for name in SOURCES:
            spark.sql(f"CREATE OR REPLACE TEMP VIEW source_{name} AS SELECT * FROM dlh.commerce_vault.{name} VERSION AS OF {manifest['snapshots'][name]}")
    def sql(query):
        if manifest:
            for name in SOURCES:
                query = query.replace(f'dlh.commerce_vault.{name}', f'source_{name}')
        return spark.sql(query)
    sql('''CREATE OR REPLACE TEMP VIEW q_product AS
      SELECT *, LEAD(event_time) OVER (PARTITION BY product_hk ORDER BY event_time,entity_version,event_id) AS valid_to
      FROM dlh.commerce_vault.sat_product''')
    sql('''CREATE OR REPLACE TEMP VIEW q_store AS
      SELECT *, LEAD(event_time) OVER (PARTITION BY store_hk ORDER BY event_time,entity_version,event_id) AS valid_to
      FROM dlh.commerce_vault.sat_store''')
    sql('''CREATE OR REPLACE TEMP VIEW q_ship_current AS
      SELECT * FROM (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY shipment_hk ORDER BY entity_version DESC,event_id DESC) AS rn
        FROM dlh.commerce_vault.sat_shipment
      ) q WHERE rn=1 AND op='U' ''')
    sql('''CREATE OR REPLACE TEMP VIEW q_return_current AS
      SELECT * FROM (
        SELECT *,ROW_NUMBER() OVER(PARTITION BY return_hk ORDER BY entity_version DESC,event_id DESC) AS rn
        FROM dlh.commerce_vault.sat_return
      ) q WHERE rn=1 AND op='U' ''')
    sql('''CREATE OR REPLACE TEMP VIEW q_shipment AS
      SELECT s.shipment_hk,h.shipment_id,l.product_hk,l.store_hk,p.entity_version AS product_version,
             t.entity_version AS store_version,p.category,t.region,s.business_date,s.quantity,s.unit_price,
             s.currency,CAST(ROUND(s.quantity*s.unit_price,2) AS DECIMAL(20,2)) AS amount
      FROM q_ship_current s
      JOIN dlh.commerce_vault.hub_shipment h ON h.shipment_hk=s.shipment_hk
      JOIN dlh.commerce_vault.link_shipment l ON l.link_hk=s.link_hk
      JOIN q_product p ON p.product_hk=l.product_hk AND p.op <> 'D'
       AND p.event_time <= CAST(s.business_date AS TIMESTAMP)
       AND (p.valid_to IS NULL OR CAST(s.business_date AS TIMESTAMP)<p.valid_to)
      JOIN q_store t ON t.store_hk=l.store_hk AND t.op <> 'D'
       AND t.event_time <= CAST(s.business_date AS TIMESTAMP)
       AND (t.valid_to IS NULL OR CAST(s.business_date AS TIMESTAMP)<t.valid_to)''')
    sql('''CREATE OR REPLACE TEMP VIEW q_return AS
      SELECT r.return_hk,h.return_id,l.shipment_hk,s.product_hk,s.store_hk,s.product_version,s.store_version,
             s.category,s.region,r.business_date,r.quantity,r.unit_price,r.currency,
             CAST(ROUND(r.quantity*r.unit_price,2) AS DECIMAL(20,2)) AS amount
      FROM q_return_current r
      JOIN dlh.commerce_vault.hub_return h ON h.return_hk=r.return_hk
      JOIN dlh.commerce_vault.link_return l ON l.link_hk=r.link_hk
      JOIN q_shipment s ON s.shipment_hk=l.shipment_hk''')
    checks = {
      'duplicate_dimension_timestamp': '''SELECT COUNT(*) FROM (
        SELECT product_hk,event_time FROM dlh.commerce_vault.sat_product GROUP BY product_hk,event_time HAVING COUNT(*)>1
        UNION ALL SELECT store_hk,event_time FROM dlh.commerce_vault.sat_store GROUP BY store_hk,event_time HAVING COUNT(*)>1) q''',
      'invalid_shipment_join': '''SELECT COUNT(*) FROM q_ship_current s LEFT JOIN
        (SELECT shipment_hk,COUNT(*) n FROM q_shipment GROUP BY shipment_hk) f ON s.shipment_hk=f.shipment_hk
        WHERE COALESCE(f.n,0)<>1''',
      'invalid_return_join': '''SELECT COUNT(*) FROM q_return_current r LEFT JOIN
        (SELECT return_hk,COUNT(*) n FROM q_return GROUP BY return_hk) f ON r.return_hk=f.return_hk
        WHERE COALESCE(f.n,0)<>1''',
      'over_return': '''SELECT COUNT(*) FROM q_shipment s JOIN
        (SELECT shipment_hk,SUM(quantity) qty FROM q_return GROUP BY shipment_hk) r ON s.shipment_hk=r.shipment_hk
        WHERE r.qty>s.quantity''',
      'currency_mismatch': '''SELECT COUNT(*) FROM q_return r JOIN q_shipment s ON s.shipment_hk=r.shipment_hk
        WHERE r.currency<>s.currency OR r.business_date<s.business_date'''
    }
    outcomes={name:sql(query).collect()[0][0] for name,query in checks.items()}
    if any(outcomes.values()):
        print(json.dumps({'status':'quality_failed','checks':outcomes},ensure_ascii=False));return 2
    result=sql('''WITH shipments AS (
        SELECT business_date,region,category,currency,SUM(amount) amt,SUM(quantity) qty
        FROM q_shipment GROUP BY business_date,region,category,currency
      ), returns AS (
        SELECT business_date,region,category,currency,SUM(amount) amt,SUM(quantity) qty
        FROM q_return GROUP BY business_date,region,category,currency
      ) SELECT COALESCE(s.business_date,r.business_date) AS business_date,
        COALESCE(s.region,r.region) AS region,COALESCE(s.category,r.category) AS category,
        COALESCE(s.currency,r.currency) AS currency,
        CAST(COALESCE(s.amt,0) AS DECIMAL(22,2)) AS shipment_amount,
        CAST(COALESCE(r.amt,0) AS DECIMAL(22,2)) AS return_amount,
        CAST(COALESCE(s.amt,0)-COALESCE(r.amt,0) AS DECIMAL(22,2)) AS net_amount,
        CAST(COALESCE(s.qty,0) AS BIGINT) AS shipped_qty,CAST(COALESCE(r.qty,0) AS BIGINT) AS returned_qty
      FROM shipments s FULL OUTER JOIN returns r
      ON s.business_date=r.business_date AND s.region=r.region AND s.category=r.category AND s.currency=r.currency''')
    # Представления имеют те же колонки и зерно, что соответствующие GP-таблицы.
    outputs = {
        'dim_product': sql("SELECT product_hk,entity_version,event_time AS valid_from,valid_to,product_name,category,(op='D') AS is_deleted FROM q_product"),
        'dim_store': sql("SELECT store_hk,entity_version,event_time AS valid_from,valid_to,store_name,region,(op='D') AS is_deleted FROM q_store"),
        'fact_shipment': sql("SELECT shipment_hk,shipment_id,product_hk,store_hk,product_version,store_version,business_date,quantity,unit_price,currency,amount FROM q_shipment"),
        'fact_return': sql("SELECT return_hk,return_id,shipment_hk,product_hk,store_hk,product_version,store_version,business_date,quantity,unit_price,currency,amount FROM q_return"),
        'sales_day': result,
    }
    # Порядок образцов не гарантируется; полная сверка выполняется отдельно.
    summary = {}
    for name, frame in outputs.items():
        count = frame.count()
        sample = [row.asDict(recursive=True) for row in frame.limit(sample_rows).collect()] if sample_rows else []
        summary[name] = {'rows': count, 'columns': frame.columns, 'sample': sample}
    if not summary['sales_day']['rows']:
        print(json.dumps({'status':'empty_result','checks':outcomes,'outputs':summary}, ensure_ascii=False, default=str)); return 2
    created = {}
    if release_id:
        # Каждая CREATE — отдельный commit. Существующие таблицы не перезаписываются.
        # Внешний orchestrator обеспечивает single writer и срок жизни snapshots.
        try:
            for name, frame in outputs.items():
                table = f'dlh.commerce_dm.r_{release_id}_{name}'
                frame.writeTo(table).using('iceberg').create()
                created[name] = {'table': table}
                observed = spark.table(table).count()
                if observed != summary[name]['rows']:
                    raise RuntimeError(f'row count mismatch for {table}: {observed} != {summary[name]["rows"]}')
                snapshots = spark.sql(f'SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1').collect()
                if len(snapshots) != 1:
                    raise RuntimeError(f'output snapshot not found for {table}')
                created[name].update({'rows': observed, 'snapshot_id': int(snapshots[0][0])})
            release = {'status': 'staged', 'release_id': release_id,
                       'created_utc': datetime.now(timezone.utc).isoformat(),
                       'input_cut_id': manifest['cut_id'], 'input_manifest_sha256': manifest_sha,
                       'input_snapshots': manifest['snapshots'], 'outputs': created, 'checks': outcomes,
                       'notice': 'Independent table commits; no atomic activation or proof of cross-table source consistency.'}
            # Создаём итоговый файл только после всех проверок и никогда не затираем его.
            with open(release_manifest, 'x', encoding='utf-8') as out:
                json.dump(release, out, ensure_ascii=False, indent=2)
                out.write('\n')
        except Exception as exc:
            print(json.dumps({'status':'stage_failed','created':created, 'error':str(exc),
                              'notice':'Partial tables may remain; inspect before cleanup.'}, ensure_ascii=False))
            raise
    print(json.dumps({'status':'staged' if release_id else 'dry_run',
                      'checks':outcomes,'outputs':summary,'created':created,
                      'input_manifest_sha256':manifest_sha,
                      'notice':'Snapshots pin each source but do not prove a coherent cut; publication is not atomic.' if manifest else
                               'Inputs are not pinned; counts and samples are not a reconciliation.'},
                     ensure_ascii=False, default=str))
    return 0


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-manifest',help='JSON с cut_id и snapshot ID каждой входной таблицы')
    parser.add_argument('--stage-release',metavar='RELEASE_ID',help='Создать отдельные release-таблицы без переключения потребителей')
    parser.add_argument('--release-manifest',help='Новый локальный файл для manifest успешной подготовки')
    parser.add_argument('--sample-rows',type=int,default=5,help='Число примеров на объект, от 0 до 20')
    args=parser.parse_args()
    if not 0 <= args.sample_rows <= 20:
        parser.error('--sample-rows должен быть от 0 до 20')
    if bool(args.stage_release) != bool(args.release_manifest):
        parser.error('--stage-release и --release-manifest требуются вместе')
    if args.stage_release and not args.input_manifest:
        parser.error('--stage-release требует --input-manifest')
    raise SystemExit(run(args.sample_rows,args.input_manifest,args.stage_release,args.release_manifest))
