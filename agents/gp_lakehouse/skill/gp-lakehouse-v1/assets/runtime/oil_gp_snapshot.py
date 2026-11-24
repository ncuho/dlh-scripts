"""Read the applied prefix inside one native Greenplum transaction without stopping ingestion.

GP is read through psycopg2 in REPEATABLE READ READ ONLY. Cedrus writes typed
rows directly into their final candidate tables; there is no Iceberg export table. No source DML,
writer locks, connector pause or application of source batches happens here.
"""
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import time

SCHEMAS = ('oil_stg', 'oil_dds', 'oil_mart')


def connect_source():
    """Use the existing Airflow connection; its password never enters code or receipts."""
    from airflow.models.connection import Connection
    import psycopg2
    c = Connection.get_connection_from_secrets('webinar_greenplum')
    options = {k: v for k, v in c.extra_dejson.items()
               if k in {'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'sslcrl', 'options'}}
    return psycopg2.connect(host=c.host, port=c.port or 5432, dbname=c.schema,
                           user=c.login, password=c.password, connect_timeout=15, **options)


def identifier(value):
    """Accept reviewed simple identifiers only; SQL text cannot come from a DAG config."""
    if not isinstance(value, str) or not re.fullmatch(r'[a-z_][a-z0-9_]{0,62}', value):
        raise ValueError('Invalid snapshot object identifier')
    return '"' + value + '"'


def literal(value):
    """Quote one transport string for Trino without changing JSON escapes or whitespace."""
    return "'" + value.replace("'", "''") + "'"


def normalized(value):
    """Preserve decimal scale, UTC microseconds, NULLs and exact source text bytes."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise ValueError('Unreviewed GP value type: ' + type(value).__name__)


def row_document(row):
    """Use a positional array so duplicate JSON names or column reordering cannot hide a mismatch."""
    return json.dumps([normalized(v) for v in row], ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def digest(value):
    """Hash a complete metadata document without including credentials."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def save(path, value):
    """Publish private receipts atomically, never a half-written SEALED file."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str))
    temp.chmod(0o600)
    temp.replace(path)


def applied_frontier(rows):
    """Separate received offsets from the contiguous prefix committed to DDS/Marts.

    Pending deliveries are normal. Gaps, duplicate positions and an APPLIED row
    beyond a pending position are not one coherent prefix and block the export.
    """
    applied, delivered, seen = [], [], set()
    for part, end, pending, last_applied, first, count, unique in rows:
        part, end, count, unique = int(part), int(end), int(count), int(unique)
        if part < 0 or part in seen or end < 0 or count != unique:
            raise ValueError('Duplicate or invalid source delivery coordinates')
        seen.add(part)
        if count and (first is None or int(first) < 0 or count != end - int(first)):
            raise ValueError('Source delivery contains an offset gap')
        if not count and end != 0:
            raise ValueError('Source checkpoint has no retained event history')
        cut = end if pending is None else int(pending)
        if not 0 <= cut <= end or (pending is not None and cut == end):
            raise ValueError('Pending position is outside the received range')
        if last_applied is not None and int(last_applied) >= cut:
            raise ValueError('Applied events are not a contiguous source prefix')
        applied.append({'partition': part, 'end': cut})
        delivered.append({'partition': part, 'end': end})
    if not applied:
        raise ValueError('Source checkpoints absent')
    return {'partitions': sorted(applied, key=lambda p: p['partition']),
            'delivery_partitions': sorted(delivered, key=lambda p: p['partition'])}


def source_readiness(cur):
    """Take the already applied prefix from one MVCC snapshot without waiting for READY=0.

    Source release commits APPLIED, DDS and both marts in one transaction. READY
    is only the received tail; its events must not enter the historical business
    cut. The same snapshot reads the applied prefix and all business relations.
    """
    cur.execute("SELECT status,count(*) FROM oil_stg.batch_control WHERE source_name='kafka:oil.events.v1' AND status IN ('OPEN','READY','FAILED') GROUP BY status")
    status = dict(cur.fetchall())
    if status.get('FAILED') or status.get('OPEN'):
        raise ValueError('Source contains failed or incomplete batches')
    cur.execute('SELECT count(*) FROM oil_stg.event_reject')
    if cur.fetchone()[0]:
        raise ValueError('Source contains rejected events')
    cur.execute("SELECT count(*) FROM oil_stg.batch_control WHERE source_name='kafka:oil.events.v1' AND batch_id<=0")
    if cur.fetchone()[0]:
        raise ValueError('Source batch IDs overlap the reserved negative Lakehouse namespace')
    # All retained deliveries must have a known batch and lie below its received checkpoint.
    cur.execute("""SELECT count(*) FROM oil_stg.event_log s
        LEFT JOIN oil_stg.batch_control b ON b.batch_id=s.batch_id
        LEFT JOIN oil_stg.kafka_checkpoint c ON c.topic=s.topic AND c.kafka_partition=s.kafka_partition
        WHERE s.topic<>'oil.events.v1' OR c.topic IS NULL OR b.batch_id IS NULL
          OR b.source_name<>'kafka:oil.events.v1' OR b.status NOT IN ('APPLIED','READY')
          OR s.kafka_offset<0 OR s.kafka_offset>=c.next_offset""")
    if cur.fetchone()[0]:
        raise ValueError('Source delivery has an orphan batch or invalid checkpoint')
    cur.execute("""SELECT c.kafka_partition,c.next_offset,
        min(CASE WHEN b.status<>'APPLIED' THEN s.kafka_offset END) AS first_pending,
        max(CASE WHEN b.status='APPLIED' THEN s.kafka_offset END) AS last_applied,
        min(s.kafka_offset),count(s.kafka_offset),count(DISTINCT s.kafka_offset)
        FROM oil_stg.kafka_checkpoint c
        LEFT JOIN oil_stg.event_log s ON s.topic=c.topic AND s.kafka_partition=c.kafka_partition
        LEFT JOIN oil_stg.batch_control b ON b.batch_id=s.batch_id
        WHERE c.topic='oil.events.v1'
        GROUP BY c.kafka_partition,c.next_offset ORDER BY c.kafka_partition""")
    boundary = applied_frontier(cur.fetchall())
    # Compare the APPLIED deliveries in both directions with the entire source ledger.
    cur.execute("""SELECT count(*) FROM oil_stg.event_log s
        JOIN oil_stg.batch_control b ON b.batch_id=s.batch_id AND b.status='APPLIED'
        LEFT JOIN oil_dds.event d ON d.event_id=(s.event_json::json->>'event_id')
        WHERE s.topic='oil.events.v1' AND (d.event_id IS NULL OR d.event_document<>s.event_json)""")
    if cur.fetchone()[0]:
        raise ValueError('Applied Stage and DDS ledger differ')
    cur.execute("""SELECT count(*) FROM oil_dds.event d WHERE NOT EXISTS (
        SELECT 1 FROM oil_stg.event_log s JOIN oil_stg.batch_control b
        ON b.batch_id=s.batch_id AND b.status='APPLIED'
        WHERE s.topic='oil.events.v1' AND (s.event_json::json->>'event_id')=d.event_id
        AND s.event_json=d.event_document)""")
    if cur.fetchone()[0]:
        raise ValueError('DDS ledger contains an event outside the applied source prefix')
    cur.execute("""SELECT count(*) FROM oil_stg.batch_coalesce_map m
        LEFT JOIN oil_stg.batch_control b ON b.batch_id=m.coalesced_batch_id
        WHERE b.batch_id IS NULL OR b.status NOT IN ('APPLIED','READY')""")
    if cur.fetchone()[0]:
        raise ValueError('Source coalesced batch provenance is invalid')
    cur.execute("""SELECT count(*) FROM oil_stg.batch_coalesce_event_map a
        LEFT JOIN oil_stg.batch_coalesce_map m ON m.original_batch_id=a.original_batch_id
          AND m.coalesced_batch_id=a.coalesced_batch_id
        LEFT JOIN oil_stg.event_log e ON e.topic=a.topic AND e.kafka_partition=a.kafka_partition
          AND e.kafka_offset=a.kafka_offset
        WHERE m.original_batch_id IS NULL OR e.kafka_offset IS NULL
          OR e.batch_id IS DISTINCT FROM a.coalesced_batch_id""")
    if cur.fetchone()[0]:
        raise ValueError('Source coalesced event provenance is invalid')
    return {**boundary, 'topic': 'oil.events.v1', 'source_checks': 'PASS',
            'source_selection': 'applied_prefix_v1', 'pending_batches': int(status.get('READY', 0)),
            'application_caught_up_to_delivery_at_snapshot': boundary['partitions'] == boundary['delivery_partitions']}


def source_relation(symbol, boundary):
    """Project the six Stage objects to C0/C1; DDS/Marts remain exact MVCC snapshot rows.

    The physical received checkpoint is retained in delivery_partitions. Exported
    Stage checkpoint values describe only the applied prefix, not an empty queue.
    No view, temporary table or migration schema is created inside Greenplum.
    """
    schema, table = symbol.split('__', 1)
    physical = identifier(schema) + '.' + identifier(table)
    if boundary.get('source_selection') != 'applied_prefix_v1' or schema != 'oil_stg':
        return physical
    if table in ('event_log', 'v_event_parsed'):
        select = "SELECT s.* FROM " + physical + " s JOIN oil_stg.batch_control b ON b.batch_id=s.batch_id WHERE b.status='APPLIED' AND s.topic='oil.events.v1'"
    elif table == 'batch_control':
        select = """SELECT b.* FROM oil_stg.batch_control b WHERE b.source_name='kafka:oil.events.v1'
            AND (b.status='APPLIED' OR (b.status='COALESCED' AND EXISTS (
            SELECT 1 FROM oil_stg.batch_coalesce_map m JOIN oil_stg.batch_control a
            ON a.batch_id=m.coalesced_batch_id AND a.status='APPLIED'
            WHERE m.original_batch_id=b.batch_id)))"""
    elif table in ('batch_coalesce_map', 'batch_coalesce_event_map'):
        select = "SELECT m.* FROM " + physical + " m JOIN oil_stg.batch_control b ON b.batch_id=m.coalesced_batch_id WHERE b.status='APPLIED'"
    elif table == 'kafka_checkpoint':
        positions = []
        for p in boundary['partitions']:
            part, end = p['partition'], p['end']
            if type(part) is not int or type(end) is not int or part < 0 or end < 0:
                raise ValueError('Invalid applied source checkpoint')
            positions.append('(' + str(part) + ',' + str(end) + '::bigint)')
        select = "SELECT c.topic,c.kafka_partition,v.applied_end::bigint AS next_offset,c.updated_at FROM oil_stg.kafka_checkpoint c JOIN (VALUES " + ','.join(positions) + ") v(partition_id,applied_end) ON v.partition_id=c.kafka_partition WHERE c.topic='oil.events.v1'"
    else:
        raise ValueError('Stage object has no reviewed applied-prefix projection')
    return '(' + select + ') source_applied_cut'


