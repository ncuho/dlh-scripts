"""Verify an operator-held GP transaction instead of materializing source copies.

Cedrus connections do not share an external snapshot. Native SHARE locks keep
the committed source stable for every reader until INITIAL/REFRESH is checked.
"""
from datetime import datetime, timezone
import re

SCHEMAS = ('oil_stg', 'oil_dds', 'oil_mart')


def connect_source():
    """Use the existing Airflow identity only for read-only lock inspection."""
    from airflow.models.connection import Connection
    import psycopg2
    c = Connection.get_connection_from_secrets('webinar_greenplum')
    options = {k: v for k, v in c.extra_dejson.items()
               if k in {'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'sslcrl', 'options'}}
    return psycopg2.connect(host=c.host, port=c.port or 5432, dbname=c.schema,
                            user=c.login, password=c.password, connect_timeout=15, **options)


class SourceLock:
    def __init__(self, token, connection_factory=connect_source):
        """Bind the exact backend and virtual transaction, never just a reusable PID."""
        if not isinstance(token, dict) or set(token) != {'pid', 'database_oid', 'virtualtransaction'}:
            raise ValueError('Provide source_lock JSON printed by the GP lock script')
        if any(type(token[k]) is not int or token[k] <= 0 for k in ['pid', 'database_oid']):
            raise ValueError('Invalid source lock identity')
        if not re.fullmatch(r'[0-9]+/[0-9]+', str(token['virtualtransaction'])):
            raise ValueError('Invalid source virtual transaction')
        self.token, self.connection_factory = dict(token), connection_factory

    def assert_held(self):
        """Require all source tables to retain granted write-blocking locks in that transaction."""
        db = self.connection_factory()
        try:
            with db.cursor() as cur:
                cur.execute("SELECT oid FROM pg_database WHERE datname=current_database()")
                if int(cur.fetchone()[0]) != self.token['database_oid']:
                    raise ValueError('Source database identity changed')
                cur.execute("""SELECT n.nspname,c.relname,c.oid,
                    EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=%s AND l.database=%s
                        AND l.virtualtransaction=%s AND l.relation=c.oid AND l.granted
                        AND (l.mode IN ('ShareLock','ShareRowExclusiveLock','ExclusiveLock','AccessExclusiveLock')
                             OR (c.relkind='v' AND l.mode='AccessShareLock')))
                    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname IN ('oil_stg','oil_dds','oil_mart')
                        AND (c.relkind='r' OR (n.nspname='oil_stg' AND c.relname='v_event_parsed')
                             OR (n.nspname='oil_mart' AND c.relname='v_field_production'))
                    ORDER BY n.nspname,c.relname""",
                    (self.token['pid'], self.token['database_oid'], self.token['virtualtransaction']))
                relations = cur.fetchall()
                if not relations or any(not row[3] for row in relations):
                    raise ValueError('Source write lock was released or does not cover every source table')
                cur.execute("""SELECT n.nspname,c.relname,c.oid FROM pg_class c
                    JOIN pg_namespace n ON n.oid=c.relnamespace
                    WHERE n.nspname IN ('oil_stg','oil_dds','oil_mart') AND c.relkind IN ('r','v')""")
                return {(r[0], r[1]): int(r[2]) for r in cur.fetchall()}
        finally:
            db.rollback()
            db.close()

    def inspect(self, mode, operations, client):
        """Read committed, fully applied source data while the separate operator transaction holds locks."""
        identities = self.assert_held()
        checks = {
            'unapplied_batches': "SELECT count(*) n FROM greenplum_gp.oil_stg.batch_control WHERE source_name='kafka:oil.events.v1' AND status IN ('OPEN','READY','FAILED')",
            'rejects': 'SELECT count(*) n FROM greenplum_gp.oil_stg.event_reject',
            'stage_ledger': "SELECT count(*) n FROM greenplum_gp.oil_stg.event_log s LEFT JOIN greenplum_gp.oil_dds.event d ON d.event_id=json_extract_scalar(s.event_json,'$.event_id') WHERE s.topic='oil.events.v1' AND (d.event_id IS NULL OR d.event_document<>s.event_json)",
            'coalesced_batches': "SELECT count(*) n FROM greenplum_gp.oil_stg.batch_coalesce_map m LEFT JOIN greenplum_gp.oil_stg.batch_control b ON b.batch_id=m.coalesced_batch_id WHERE b.batch_id IS NULL OR b.status<>'APPLIED'",
        }
        for name, query in checks.items():
            if client.sql(query) != [{'n': 0}]:
                raise ValueError('Source is not fully applied: ' + name + '; release locks and finish the source DAG')
        positions = client.sql("SELECT kafka_partition,next_offset FROM greenplum_gp.oil_stg.kafka_checkpoint WHERE topic='oil.events.v1' ORDER BY kafka_partition")
        if not positions or len({r['kafka_partition'] for r in positions}) != len(positions):
            raise ValueError('Source checkpoints missing or ambiguous')
        tables = {}
        for op in operations:
            schema, table = op['symbol'].split('__', 1)
            if schema not in SCHEMAS or not re.fullmatch(r'[a-z][a-z0-9_]*', table):
                raise ValueError('Source outside reviewed scope')
            tables[op['symbol']] = {'schema': schema, 'table': table,
                'oid': identities[(schema, table)], 'columns': op['columns'],
                'rows': client.sql('SELECT count(*) n FROM greenplum_gp.' + schema + '.' + table)[0]['n']}
        self.assert_held()
        return {'boundary_id': 'C0' if mode == 'initial' else 'C1', 'provider': 'gp_locked_source',
                'source_lock': self.token, 'source_checks': 'PASS', 'topic': 'oil.events.v1',
                'captured_at': datetime.now(timezone.utc).isoformat(), 'tables': tables,
                'partitions': [{'partition': int(r['kafka_partition']), 'end': int(r['next_offset'])} for r in positions]}
