# Фиксирует и проверяет согласованные GP captures C0/C1 и их безопасную очистку.
# Агент предлагает план/SQL; разрешённый исполнитель применяет принятые операции. Source не удаляется автоматически.
"""Materialize one consistent GP image without stopping a Kafka producer.

The guarded SQL service queues GP writes only for the short capture transaction.
Cedrus and Spark subsequently read immutable tables after that transaction has
committed. Every relation identity is recorded, checked, and deleted explicitly.
"""
import json
import os
import re
import subprocess


def observe(binding):
    """Read broker identity/watermarks through the existing forced JSON relay."""
    controller = os.environ['OIL_FENCE_CONTROLLER']
    command = [controller] if not controller.endswith('.py') else [__import__('sys').executable, controller]
    result = subprocess.run(command, input=json.dumps({
        'action': 'observe', 'cut_id': 'online-observation',
        'topic': binding['boundary']['topics'][0],
    }), text=True, capture_output=True, timeout=45)
    if result.returncode:
        raise ValueError('Kafka observation failed; no producer fence was requested')
    value = json.loads(result.stdout)
    if value.get('status') != 'observed' or not value.get('topic_id') or not value.get('cluster_id'):
        raise ValueError('Incomplete broker identity')
    if value.get('producer_denied'):
        raise ValueError('A legacy producer fence is active; online capture will not bypass it')
    return value


def connect_source(binding):
    """Reuse the configured Airflow identity in tasks and maintenance workers.

    Airflow 3's SDK connection lookup requires task supervisor communications.
    A maintenance subprocess has no supervisor, so it reads the same encrypted
    metadata connection through Airflow's server-side secrets API instead.
    Credentials never enter a receipt, manifest, log, or project configuration.
    """
    connection_id = binding['boundary'].get('postgres_connection', 'webinar_greenplum')
    try:
        from airflow.sdk import get_current_context
        get_current_context()
    except (RuntimeError, ImportError):
        from airflow.models.connection import Connection
        import psycopg2
        configured = Connection.get_connection_from_secrets(connection_id)
        options = {key: value for key, value in configured.extra_dejson.items()
                   if key in {'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'sslcrl', 'options'}}
        return psycopg2.connect(host=configured.host, port=configured.port or 5432,
                                dbname=configured.schema, user=configured.login,
                                password=configured.password, connect_timeout=15,
                                application_name='platform_bridge_capture', **options)
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=connection_id).get_conn()


def capture(spec, namespace, initial=None):
    """Commit a source image, then return with all GP writer locks released.

    Only symbols actually materialized by the approved contract are copied.
    Source readiness and application happen inside the same database transaction.
    No broker end offset is mistaken for an already applied GP checkpoint.
    """
    binding = spec['connection']
    expected = observe(binding)
    planned = {operation['symbol'] for operation in spec['operations']}
    objects = [{
        'symbol': symbol,
        'source_schema': spec['objects'][symbol]['source_schema'],
        'source_table': spec['objects'][symbol]['source_table'],
        'columns': [c['name'] for c in next(o for o in spec['operations'] if o['symbol'] == symbol)['columns']],
    } for symbol in sorted(planned)]
    ident = 'pbc_' + spec['project_id'][:20] + '_' + namespace[-8:]
    if not re.fullmatch(r'pbc_[a-f0-9]{20}_[a-f0-9]{8}', ident):
        raise ValueError('Invalid capture identity')
    conn = connect_source(binding)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout='10s'")
            cur.execute("SET LOCAL statement_timeout='180s'")
            cur.execute('SELECT bridge_capture.capture(%s,%s,%s::json)', (
                ident, binding['boundary'].get('source_contract', 'oil'), json.dumps(objects),
            ))
            manifest = cur.fetchone()[0]
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
    # From this point the data image is durable and GP accepts new batches while
    # Target copying, transformations, and comparisons may take minutes or hours.
    actual = observe(binding)
    verify(manifest, expected, actual, planned, initial)
    manifest.update(topic_id=actual['topic_id'], cluster_id=actual['cluster_id'],
                    capture_committed=True, source_locks_released=True,
                    broker_end_at_capture={str(p['partition']): p['high'] for p in actual['partitions']})
    return manifest


def verify(manifest, before, after, planned, initial=None):
    """Reject identity changes, retention loss, missing tables and invalid cuts."""
    if (before['topic_id'], before['cluster_id']) != (after['topic_id'], after['cluster_id']):
        raise ValueError('Kafka identity changed during source capture')
    if manifest.get('source_checks') != 'PASS' or set(manifest.get('tables', {})) != planned:
        raise ValueError('Source capture incomplete')
    parts = manifest['partitions']
    end = {int(p['partition']): int(p['end']) for p in parts}
    broker = {int(p['partition']): p for p in after['partitions']}
    if len(end) != len(parts) or set(end) != set(broker):
        raise ValueError('Ambiguous source checkpoints')
    if initial and (initial['topic_id'], initial['cluster_id']) != (after['topic_id'], after['cluster_id']):
        raise ValueError('Kafka identity differs from INITIAL')
    starts = {int(p['partition']): int(p['end']) for p in initial['partitions']} if initial else end
    if set(starts) != set(end):
        raise ValueError('Partition set changed')
    for p in parts:
        key = int(p['partition']);start = starts[key]
        if not int(broker[key]['low']) <= start <= end[key] <= int(broker[key]['high']):
            raise ValueError('Source cut or retention is invalid')
        p.update(start=start, low=int(broker[key]['low']))
    for symbol, table in manifest['tables'].items():
        if table['schema'] != 'bridge_capture' or not table['table'].startswith(manifest['capture_id'] + '_'):
            raise ValueError('Foreign captured table')
        if int(table['rows']) < 0 or not int(table['oid']):
            raise ValueError('Invalid capture evidence')


def cleanup(binding, capture_ids):
    """Delete registered source images through their guarded SQL service only."""
    conn = connect_source(binding)
    try:
        with conn.cursor() as cur:
            for ident in capture_ids:
                if not re.fullmatch(r'pbc_[a-f0-9]{20}_[a-f0-9]{8}', ident):
                    raise ValueError('Invalid capture cleanup ID')
                cur.execute('SELECT bridge_capture.cleanup(%s)', (ident,))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
