"""Read one native GP snapshot and its applied prefix while ingestion continues; seal the export.

GP is read through psycopg2 in REPEATABLE READ READ ONLY. Cedrus only writes
the exported values and later reads the sealed Iceberg snapshot, so its GP
connector is never mistaken for a shared-snapshot reader. No source DML,
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

INPUT = 'dlh.bronze.__source_snapshots'
CHUNK = 500
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


def current_snapshot(client):
    """Return the committed input-table snapshot, allowing an initially empty table."""
    rows = client.sql('SELECT snapshot_id FROM ' + metadata_relation('history') + ' ORDER BY made_current_at DESC LIMIT 1')
    return int(rows[0]['snapshot_id']) if rows else None


def metadata_relation(kind):
    """Address the input table's own Iceberg metadata, including isolated fixture tables."""
    catalog, schema, name = INPUT.split('.')
    if kind not in ('history', 'snapshots'):
        raise ValueError('Unsupported metadata relation')
    return catalog + '.' + schema + '."' + name + '$' + kind + '"'


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


def projection(manifest, symbol, dialect):
    """Read typed original columns from the sealed export, never from the moving live GP."""
    table = manifest['tables'][symbol]
    types = table['trino_types'] if dialect == 'trino' else table['spark_types']
    fields = []
    for index, (name, kind) in enumerate(zip(table['columns'], types)):
        if not re.fullmatch(r'(?:varchar|integer|bigint|boolean|date|decimal\(\d+,\d+\)|timestamp(?:\(6\))?(?: with time zone)?)', kind):
            raise ValueError('Unreviewed snapshot projection type')
        # A declared array<string> preserves SQL NULL separately from the literal text "null" in Spark.
        value = ('json_extract_scalar(row_json, ' + literal('$[' + str(index) + ']') + ')' if dialect == 'trino'
                 else 'element_at(from_json(row_json, '+literal('array<string>')+'), '+str(index+1)+')')
        if dialect == 'trino' and 'with time zone' in kind:
            value = 'CAST(from_iso8601_timestamp_nanos(' + value + ') AS timestamp(6) with time zone)'
        else:
            value = 'CAST(' + value + ' AS ' + ('string' if dialect == 'spark' and kind == 'varchar' else kind) + ')'
        quote = '"' if dialect == 'trino' else '`'
        fields.append(value + ' AS ' + quote + name + quote)
    version = ' FOR VERSION AS OF ' if dialect == 'trino' else ' VERSION AS OF '
    return '(SELECT ' + ', '.join(fields) + ' FROM ' + INPUT + version + str(manifest['input_snapshot']) + \
        ' WHERE export_id=' + literal(manifest['export_id']) + ' AND object_symbol=' + literal(symbol) + ')'


class SourceSnapshot:
    def __init__(self, client, mode, run_id, state_dir=None, connection_factory=connect_source,
                 readiness=source_readiness, allowed_schemas=SCHEMAS, readiness_timeout=600):
        """Bind one export to one DAG run; fixture overrides are not exposed through DAG config."""
        if mode not in ('initial', 'refresh'):
            raise ValueError('Unknown snapshot mode')
        self.client, self.mode, self.run_id = client, mode, str(run_id)
        self.export_id = 'snp_' + digest([mode, self.run_id])[:32]
        self.root = Path(state_dir or os.environ.get('OIL_LAYERED_STATE_DIR', '/opt/airflow/logs/oil_layered_migration')) / 'exports'
        self.path = self.root / (self.export_id + '.json')
        self.connection_factory, self.readiness = connection_factory, readiness
        self.allowed_schemas, self.readiness_timeout = allowed_schemas, readiness_timeout

    def load(self):
        """Accept only a sealed export belonging to this exact mode/run."""
        value = json.loads(self.path.read_text())
        if value.get('status') != 'SEALED' or value.get('export_id') != self.export_id or value.get('run_id') != self.run_id:
            raise ValueError('Snapshot export is incomplete or belongs to another run')
        if value.get('manifest_hash') != digest(value['manifest']):
            raise ValueError('Snapshot manifest changed')
        return value

    def assert_held(self):
        """Verify retained immutable input; the GP transaction has already been closed safely."""
        manifest = self.load()['manifest']
        rows = self.client.sql('SELECT count(*) n FROM ' + metadata_relation('snapshots') + ' WHERE snapshot_id=' + str(manifest['input_snapshot']))
        if rows != [{'n': 1}]:
            raise ValueError('Sealed input snapshot expired or disappeared')

    def validate_manifest(self, manifest):
        """Prevent another task from silently substituting a different historical input."""
        if self.load()['manifest'] != manifest:
            raise ValueError('Execution manifest differs from sealed source export')

    def relation(self, symbol, dialect, manifest):
        """Expose a typed historical relation for both Cedrus and Spark SQL."""
        self.validate_manifest(manifest)
        return projection(manifest, symbol, dialect)

    def inspect(self, mode, operations, client):
        """Export all reviewed objects in one native read-only transaction and verify every chunk."""
        if self.path.exists():
            sealed = self.load()['manifest']
            expected = {op['symbol'] for op in operations}
            if set(sealed['tables']) != expected or any(
                sealed['tables'][op['symbol']][key] != op[key]
                for op in operations for key in ['columns','trino_types','spark_types']):
                raise ValueError('Sealed export differs from the reviewed object/type contract')
            self.assert_held()
            return sealed
        if mode != self.mode or client is not self.client:
            raise ValueError('Foreign snapshot execution')
        if client.sql('SELECT count(*) n FROM ' + INPUT + ' WHERE export_id=' + literal(self.export_id)) != [{'n': 0}]:
            raise ValueError('Source export identifier is not empty')
        state = {'status': 'STARTING', 'run_id': self.run_id, 'export_id': self.export_id, 'chunks': [], 'writes': []}
        save(self.path, state)
        db = None
        try:
            deadline = time.monotonic() + self.readiness_timeout
            while True:
                db = self.connection_factory()
                db.set_session(isolation_level='REPEATABLE READ', readonly=True, autocommit=False)
                with db.cursor() as cur:
                    cur.execute("SET LOCAL TIME ZONE 'UTC'")
                    cur.execute("SET LOCAL lock_timeout='3s'")
                    cur.execute("SET LOCAL statement_timeout='120s'")
                    boundary = self.readiness(cur)
                    if boundary is not None:
                        cur.execute('SELECT pg_export_snapshot(),clock_timestamp(),pg_backend_pid()')
                        snapshot_id, captured_at, backend_pid = cur.fetchone()
                        break
                db.rollback()
                db.close()
                db = None
                if time.monotonic() >= deadline:
                    raise TimeoutError('No fully applied business cut; source continues running, export not started')
                time.sleep(1)
            manifest = {**boundary, 'provider': 'native_gp_repeatable_read', 'boundary_id': 'C0' if mode == 'initial' else 'C1',
                        'export_id': self.export_id, 'gp_snapshot_id': snapshot_id, 'gp_backend_pid': int(backend_pid),
                        'captured_at': captured_at.isoformat(), 'tables': {}, 'producer_paused': False,
                        'gp_sink_paused': False, 'source_dag_paused': False, 'source_write_locks': False}
            state.update(status='EXPORTING', manifest=manifest)
            save(self.path, state)
            print(json.dumps({'phase': 'NATIVE_GP_SNAPSHOT', 'export_id': self.export_id, 'gp_snapshot_id': snapshot_id,
                              'gp_backend_pid': backend_pid, 'partitions': boundary['partitions'],
                              'delivery_partitions': boundary.get('delivery_partitions', boundary['partitions']),
                              'pending_batches': boundary.get('pending_batches', 0), 'source_write_locks': False}), flush=True)
            for op in operations:
                symbol = op['symbol']
                schema, table = symbol.split('__', 1)
                if schema not in self.allowed_schemas:
                    raise ValueError('Source schema outside reviewed snapshot scope')
                relation = source_relation(symbol, boundary)
                fields = ','.join(identifier(c) for c in op['columns'])
                with db.cursor() as cur:
                    cur.execute('SELECT ' + fields + ' FROM ' + relation + ' LIMIT 0')
                    source_types = [int(c.type_code) for c in cur.description]
                    # JSON types are read as their native lexical text, not decoded and reserialized by psycopg2.
                    fields = ','.join(identifier(c) + ('::text AS ' + identifier(c) if oid in (114, 3802) else '')
                                      for c, oid in zip(op['columns'], source_types))
                    cur.execute('SELECT oid::bigint FROM pg_class WHERE oid=%s::regclass', (schema + '.' + table,))
                    oid = int(cur.fetchone()[0])
                count = 0
                with db.cursor(name='snapshot_' + digest(symbol)[:16]) as cur:
                    cur.itersize = CHUNK
                    cur.execute('SELECT ' + fields + ' FROM ' + relation)
                    while True:
                        rows = cur.fetchmany(CHUNK)
                        if not rows:
                            break
                        docs = [row_document(r) for r in rows]
                        chunk = count // CHUNK
                        expected_hash = hashlib.sha256('\n'.join(docs).encode()).hexdigest().upper()
                        values = ','.join('(' + literal(self.export_id) + ',' + literal(symbol) + ',' + str(count + i) + ',' + literal(doc) + ')'
                                          for i, doc in enumerate(docs))
                        statement = '/* source_export:' + self.export_id + ':' + symbol + ':' + str(chunk) + ' */ INSERT INTO ' + INPUT + ' (export_id,object_symbol,row_no,row_json) VALUES ' + values
                        write = {'symbol': symbol, 'chunk': chunk, 'state': 'ATTEMPTING', 'query_hash': digest(statement)}
                        state['writes'].append(write)
                        save(self.path, state)
                        try:
                            client.sql(statement)
                        except Exception:
                            write['state'] = 'UNKNOWN_OUTCOME'
                            raise
                        write['state'] = 'COMMITTED'
                        state['chunks'].append({'object_symbol': symbol, 'chunk': chunk, 'n': len(docs), 'hash': expected_hash,
                                                'first_row': count, 'last_row': count + len(docs) - 1})
                        count += len(docs)
                        save(self.path, state)
                        print(json.dumps({'phase':'EXPORT_CHUNK','object_symbol':symbol,'chunk':chunk,'exported_rows':count}),flush=True)
                manifest['tables'][symbol] = {'schema': schema, 'table': table, 'oid': oid, 'rows': count,
                    'columns': op['columns'], 'source_type_oids': source_types,
                    'trino_types': op['trino_types'], 'spark_types': op['spark_types'],
                    'source_projection': relation}
                save(self.path, state)
                print(json.dumps({'phase':'SOURCE_OBJECT_EXPORTED','object_symbol':symbol,'rows':count}),flush=True)
            # The extractor never commits source changes. All source reads are complete before ending its snapshot.
            db.rollback()
            db.close()
            db = None
            pinned = current_snapshot(client)
            if pinned is None and state['chunks']:
                raise ValueError('Export data has no committed Iceberg snapshot')
            manifest['input_snapshot'] = pinned
            query = 'SELECT object_symbol, row_no / ' + str(CHUNK) + ' AS chunk, count(*) AS n, count(DISTINCT row_no) AS unique_rows, min(row_no) AS first_row, max(row_no) AS last_row, to_hex(sha256(to_utf8(array_join(array_agg(row_json ORDER BY row_no),chr(10))))) AS hash FROM ' + INPUT + ' FOR VERSION AS OF ' + str(pinned) + ' WHERE export_id=' + literal(self.export_id) + ' GROUP BY object_symbol,row_no / ' + str(CHUNK)
            actual = client.sql(query) if pinned is not None else []
            found = {(r['object_symbol'], int(r['chunk'])): r for r in actual}
            if len(found) != len(state['chunks']):
                raise ValueError('Export contains missing or unexpected chunks')
            for expected in state['chunks']:
                row = found[(expected['object_symbol'], expected['chunk'])]
                if any(int(row[k]) != expected[k] for k in ['n', 'first_row', 'last_row']) or int(row['unique_rows']) != expected['n'] or row['hash'].upper() != expected['hash']:
                    raise ValueError('Source export content/uniqueness checksum failed')
            if pinned is None:
                raise ValueError('Empty snapshot export has no input snapshot')
            manifest['transport_checks'] = {'status': 'PASS', 'sha256_chunks': len(actual), 'rows': sum(t['rows'] for t in manifest['tables'].values())}
            manifest['source_transaction_closed'] = True
            state.update(status='SEALED', manifest=manifest, manifest_hash=digest(manifest), export_check_sql=query)
            save(self.path, state)
            print(json.dumps({'phase': 'SOURCE_EXPORT_SEALED', 'export_id': self.export_id, 'input_snapshot': pinned,
                              'tables': len(manifest['tables']), 'source_transaction_closed': True}), flush=True)
            return manifest
        except Exception as error:
            state.update(status='FAILED_RECONCILE', error_type=type(error).__name__)
            save(self.path, state)
            raise
        finally:
            if db is not None:
                db.rollback()
                db.close()
