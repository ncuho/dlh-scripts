"""Build one isolated layered candidate; compensate confirmed writes on failure.

Airflow orders the operations. SQL/Spark retain key and history semantics.
Rollback restores empty candidate contents, not a cross-table ACID transaction.
"""
import hashlib
import json
import os
import re
import time
from pathlib import Path
from oil_layered_plan import OPERATIONS

LAYERS = {'oil_stg': 'bronze', 'oil_dds': 'silver', 'oil_mart': 'gold'}
BOUNDARIES = {'initial': 'C0', 'refresh': 'C1'}
RAW = 'dlh.bronze.__raw_events'


def overlap_window(p0, p1, overlap_offsets=10):
    """Keep the logical C0/C1 interval and replay an explicit prefix by Kafka position."""
    if type(overlap_offsets) is not int or not 0 <= overlap_offsets <= 10000:
        raise ValueError('overlap_offsets must be an integer from 0 to 10000')
    if set(p0) != set(p1) or any(p1[p] < p0[p] for p in p0):
        raise ValueError('Invalid partition boundary')
    return [{'partition': p, 'start': p0[p], 'read_start': max(0, p0[p]-overlap_offsets), 'end': p1[p]} for p in sorted(p0)]


# Сохраняем исходные имена таблиц внутри слоёв; следующий выпуск получает суффикс _next.
def target(symbol, mode='initial'):
    """Resolve a fixed source symbol to its explicit physical Lakehouse table."""
    schema, name = symbol.split('__', 1)
    if schema not in LAYERS or mode not in BOUNDARIES or not re.fullmatch(r'[a-z][a-z0-9_]*', name):
        raise ValueError('Unknown object or release')
    return 'dlh.' + LAYERS[schema] + '.' + name + ('_next' if mode == 'refresh' else '')


def fingerprint(value):
    """Hash a code/identity value without placing credentials in the journal."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def atomic_write(path, state):
    """Replace the durable receipt atomically; incomplete JSON must never be accepted."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str))
    temporary.chmod(0o600)
    temporary.replace(path)


class Runner:
    def __init__(self, client, mode, run_id, state_dir=None, sql_dir=None, spark_factory=None, overlap_offsets=10, source_guard=None):
        """Bind a single DAG run to reviewed SQL, private receipts and fixed target names."""
        if mode not in BOUNDARIES:
            raise ValueError('mode must be initial or refresh')
        self.client, self.mode, self.run_id = client, mode, str(run_id)
        self.root = Path(state_dir or os.environ.get('OIL_LAYERED_STATE_DIR', '/opt/airflow/logs/oil_layered_migration'))
        self.path = self.root / (fingerprint(self.run_id)[:32] + '.json')
        self.sql_dir = Path(sql_dir or Path(__file__).parent / 'sql')
        self.spark_factory = spark_factory
        self.overlap_offsets = overlap_offsets
        if source_guard is None:
            raise ValueError('A verified native source lock is required')
        self.source_guard = source_guard

    def contract_hash(self):
        """Pin the plan, runtime and exact native SQL before any target write."""
        sources = {p.relative_to(self.sql_dir).as_posix(): p.read_text() for p in sorted(self.sql_dir.rglob('*.sql'))}
        guard_path = Path(__file__).with_name('oil_source_lock.py')
        return fingerprint({'operations': OPERATIONS, 'sql': sources, 'runtime': Path(__file__).read_text(), 'source_guard': guard_path.read_text()})

    def load(self):
        """Reject another run's receipt before trusting any completed operation."""
        state = json.loads(self.path.read_text())
        if state['run_id'] != self.run_id or state['mode'] != self.mode or state['contract_hash'] != self.contract_hash():
            raise ValueError('Foreign receipt')
        return state

    def save(self, state):
        """Persist each intent before a mutation and each acknowledged outcome afterwards."""
        atomic_write(self.path, state)

    def count(self, table):
        """Read current row count; this is not a permission or uniqueness guarantee."""
        return self.client.sql('SELECT count(*) n FROM ' + table)[0]['n']

    def snapshot(self, table):
        """Return the committed table snapshot, allowing an unpopulated table to have none."""
        catalog, schema, name = table.split('.')
        rows = self.client.sql('SELECT snapshot_id FROM ' + catalog + '.' + schema + '."' + name + '$history" ORDER BY made_current_at DESC LIMIT 1')
        return int(rows[0]['snapshot_id']) if rows else None

    def identity(self, table):
        """Pin the complete CREATE definition, including the UUID-based storage location."""
        rows = self.client.sql('SHOW CREATE TABLE ' + table)
        return fingerprint(rows)

    def manifest(self):
        """Describe live committed source tables protected by the operator's native locks."""
        value = self.source_guard.inspect(self.mode, OPERATIONS, self.client)
        if value.get('source_checks') != 'PASS' or value.get('topic') != 'oil.events.v1':
            raise ValueError('Locked source did not pass its contract')
        if set(value['tables']) != {o['symbol'] for o in OPERATIONS}:
            raise ValueError('Boundary must contain exactly the approved 20 objects')
        return value

    def prepare(self):
        """Require empty candidate tables and pin C0/C1 before permitting any INSERT."""
        if self.path.exists():
            raise FileExistsError('Do not Clear/retry an existing run; inspect its receipt')
        pointer = self.root / ('ready_' + self.mode + '.json')
        if pointer.exists():
            raise FileExistsError('This release is already accepted; do not overwrite it')
        state = {'run_id': self.run_id, 'mode': self.mode, 'status': 'PREPARED',
                 'manifest': self.manifest(), 'steps': {}, 'targets': {}, 'checks': [], 'contract_hash': self.contract_hash()}
        for op in OPERATIONS:
            table = target(op['symbol'], self.mode)
            if self.count(table) != 0:
                raise ValueError('Candidate is not empty: ' + table)
            state['targets'][op['symbol']] = {'table': table, 'identity': self.identity(table), 'before_snapshot': self.snapshot(table)}
        if self.mode == 'refresh':
            state['initial'] = json.loads((self.root / 'ready_initial.json').read_text())
            if state['initial'].get('status') != 'READY':
                raise ValueError('INITIAL is not accepted')
            for symbol, step in state['initial']['steps'].items():
                if self.snapshot(target(symbol)) != step['snapshot']:
                    raise ValueError('INITIAL changed after acceptance')
            p0 = {p['partition']: p['end'] for p in state['initial']['manifest']['partitions']}
            p1 = {p['partition']: p['end'] for p in state['manifest']['partitions']}
            if set(p0) != set(p1) or any(p1[p] < p0[p] for p in p0):
                raise ValueError('Invalid partition boundary')
            state['window'] = overlap_window(p0, p1, self.overlap_offsets)
            state['overlap_offsets'] = self.overlap_offsets
            if sum(p['end'] - p['start'] for p in state['window']) == 0:
                raise ValueError('No changes between C0 and C1')
            deadline = time.monotonic() + 120
            while True:
                snapshot = self.snapshot(RAW)
                gaps = []
                for part in state['window']:
                    expected = part['end'] - part['read_start']
                    n = 0 if snapshot is None else self.client.sql('SELECT count(DISTINCT kafka_offset) n FROM ' + RAW + ' FOR VERSION AS OF ' + str(snapshot) + " WHERE topic='oil.events.v1' AND kafka_partition=" + str(part['partition']) + ' AND kafka_offset>=' + str(part['read_start']) + ' AND kafka_offset<' + str(part['end']))[0]['n']
                    if n != expected:
                        gaps.append(part)
                if not gaps:
                    state['raw_snapshot'] = snapshot
                    break
                if time.monotonic() >= deadline:
                    raise ValueError('Raw window is incomplete; no target data was written')
                time.sleep(2)
        self.source_guard.assert_held()
        self.save(state)
        print(json.dumps({'status': 'PREPARED', 'mode': self.mode, 'boundary_id': state['manifest']['boundary_id'], 'source_lock': state['manifest']['source_lock'], 'receipt': str(self.path), 'window': state.get('window'), 'raw_snapshot': state.get('raw_snapshot')}))
        return {'receipt': str(self.path), 'targets': 20}

    def render(self, query, dialect, state):
        """Resolve original GP objects under native locks and pinned INITIAL/raw snapshots."""
        def replace(match):
            """Substitute one fixed source, target or historical reference."""
            side, symbol = match.groups()
            if symbol not in state['targets']:
                raise ValueError('Reference outside the migration contract')
            if side == 'target':
                return target(symbol, self.mode)
            if side == 'initial':
                snap = state['initial']['steps'][symbol]['snapshot']
                return '(SELECT * FROM ' + target(symbol) + ' WHERE false)' if snap is None else target(symbol) + (' VERSION AS OF ' if dialect == 'spark' else ' FOR VERSION AS OF ') + str(snap)
            source = state['manifest']['tables'][symbol]
            name = source['table']
            if (source['schema'], name) != tuple(symbol.split('__', 1)):
                raise ValueError('Source relation differs from the reviewed object')
            return 'greenplum_gp.' + source['schema'] + '.' + name
        value = re.sub(r'\{\{(source|target|initial):([^}]+)\}\}', replace, query)
        if '{{delta}}' in value:
            windows = ['(kafka_partition={partition} AND kafka_offset>={read_start} AND kafka_offset<{end})'.format(**p) for p in state['window']]
            delta = '(SELECT * FROM ' + RAW + (' VERSION AS OF ' if dialect == 'spark' else ' FOR VERSION AS OF ') + str(state['raw_snapshot']) + " WHERE topic='oil.events.v1' AND (" + ' OR '.join(windows) + '))'
            value = value.replace('{{delta}}', delta)
        stamp = state['manifest']['captured_at'].replace("'", "''")
        expression = "TO_TIMESTAMP('" + stamp + "')" if dialect == 'spark' else "CAST(FROM_ISO8601_TIMESTAMP_NANOS('" + stamp + "') AS timestamp(6) with time zone)"
        value = value.replace('{{cut_time}}', expression)
        if '{{' in value:
            raise ValueError('Unresolved SQL reference')
        return value.rstrip().rstrip(';')

    def materialize(self, symbol):
        """Write one table, validate its keys/data, then permit dependent tasks to proceed."""
        state = self.load()
        self.source_guard.assert_held()
        op = next(o for o in OPERATIONS if o['symbol'] == symbol)
        if state['status'] not in ('PREPARED', 'BUILDING') or symbol in state['steps']:
            raise ValueError('Run is not writable or the task was already attempted')
        if any(state['steps'].get(d, {}).get('state') != 'VALIDATED' for d in op['dependencies']):
            raise ValueError('A parent table has not passed validation')
        table = target(symbol, self.mode)
        if self.identity(table) != state['targets'][symbol]['identity'] or self.count(table) != 0:
            raise ValueError('Candidate changed since preflight')
        dialect = op[self.mode + '_dialect']
        query = self.render((self.sql_dir / symbol / (self.mode + '.sql')).read_text(), dialect, state)
        quote = '`' if dialect == 'spark' else '"'
        fields = ', '.join(quote + c + quote for c in op['columns'])
        tag = 'oil_layered:' + fingerprint(self.run_id)[:16] + ':' + symbol
        statement = '/* ' + tag + ' */ INSERT INTO ' + table + ' (' + fields + ') SELECT ' + fields + ' FROM (' + query + ') reviewed_projection'
        state['status'] = 'BUILDING'
        state['steps'][symbol] = {'state': 'ATTEMPTING', 'dialect': dialect, 'query_hash': fingerprint(statement), 'tag': tag, 'query': statement}
        self.save(state)
        print(statement, flush=True)
        spark = None
        try:
            if dialect == 'spark':
                spark = self.spark_factory()
                spark.open()
                spark.sql(statement)
            else:
                self.client.sql(statement)
            # Only an acknowledged commit permits an automatic compensating DELETE.
            state['steps'][symbol].update(state='COMMITTED', snapshot=self.snapshot(table))
            self.save(state)
        except Exception as error:
            state['steps'][symbol]['state'] = 'UNKNOWN_OUTCOME'
            state['steps'][symbol]['error_type'] = type(error).__name__
            self.save(state)
            # An unknown Spark command may still be running; preserve its kernel/receipts for reconciliation.
            if spark:
                state['steps'][symbol].update(kernel=spark.kernel, notebook=spark.name)
                self.save(state)
                spark = None
            raise
        finally:
            if spark:
                try:
                    spark.close()
                except Exception:
                    pass
        self.source_guard.assert_held()
        for check in op['checks']:
            if check['phase'] not in ('both', self.mode):
                continue
            query = self.render((self.sql_dir / symbol / check['path']).read_text(), 'trino', state)
            result = self.client.sql(query)
            if len(result) != 1 or 'n' not in result[0]:
                raise ValueError('Invalid quality check result')
            state['checks'].append({'symbol': symbol, 'name': check['name'], 'violations': result[0]['n'], 'query': query})
            self.save(state)
            if result[0]['n'] != 0:
                raise ValueError('Quality gate failed: ' + symbol + '.' + check['name'])
        self.source_guard.assert_held()
        state['steps'][symbol]['state'] = 'VALIDATED'
        self.save(state)
        print(json.dumps({'table': table, 'status': 'VALIDATED', 'snapshot': state['steps'][symbol]['snapshot']}))
        return state['steps'][symbol]

    def validate_all(self):
        """Accept all twenty tables only when all phase checks and snapshot identities match."""
        state = self.load()
        self.source_guard.assert_held()
        if set(state['steps']) != {o['symbol'] for o in OPERATIONS}:
            raise ValueError('Not all twenty tables were materialized')
        for symbol, step in state['steps'].items():
            if step['state'] != 'VALIDATED' or self.snapshot(target(symbol, self.mode)) != step['snapshot']:
                raise ValueError('Unvalidated or changed candidate')
        expected = sum(c['phase'] in ('both', self.mode) for o in OPERATIONS for c in o['checks'])
        if len(state['checks']) != expected or any(c['violations'] != 0 for c in state['checks']):
            raise ValueError('Incomplete reconciliation')
        state['status'] = 'VALIDATED'
        self.save(state)
        return {'tables': 20, 'checks': expected, 'status': 'VALIDATED'}

    def compensate(self):
        """Clear only confirmed owned candidate writes, in reverse dependency order."""
        if not self.path.exists():
            return {'status': 'NO_WRITES'}
        state = self.load()
        if state['status'] == 'VALIDATED':
            return {'status': 'NO_ROLLBACK_REQUIRED'}
        errors = []
        for op in reversed(OPERATIONS):
            symbol = op['symbol']
            step = state['steps'].get(symbol)
            if not step:
                continue
            table = target(symbol, self.mode)
            try:
                if step['state'] in ('ATTEMPTING', 'UNKNOWN_OUTCOME'):
                    raise ValueError('Unknown outcome: reconcile the query/kernel before compensation')
                if self.identity(table) != state['targets'][symbol]['identity'] or self.snapshot(table) != step['snapshot']:
                    raise ValueError('Foreign change: compensation refused')
                # Candidate tables were empty before this run and have a single permitted writer.
                self.client.sql('DELETE FROM ' + table)
                if self.count(table) != 0:
                    raise ValueError('Rollback did not restore empty candidate contents')
                step['state'] = 'ROLLED_BACK'
                self.save(state)
            except Exception as error:
                errors.append({'table': table, 'error': str(error)})
        state['status'] = 'MANUAL_RECONCILE' if errors else 'ROLLED_BACK'
        state['rollback_errors'] = errors
        self.save(state)
        print(json.dumps({'status': state['status'], 'rollback_errors': errors}))
        if errors:
            raise RuntimeError('Automatic rollback refused for unconfirmed/foreign writes')
        return {'status': 'ROLLED_BACK'}

    def finish(self):
        """Keep a compensated failure failed; publish only a READY receipt, never a BI switch."""
        state = self.load()
        if state['status'] != 'VALIDATED':
            raise RuntimeError('Release not accepted: ' + state['status'])
        self.validate_all()
        state = self.load()
        state['status'] = 'READY'
        self.save(state)
        atomic_write(self.root / ('ready_' + self.mode + '.json'), state)
        print(json.dumps({'status': 'READY', 'mode': self.mode, 'receipt': str(self.path), 'serving': target('oil_mart__v_field_production', self.mode)}))
        return {'status': 'READY', 'receipt': str(self.path)}
