"""Keep one GP snapshot while copying typed rows directly and reconciling every object.

No intermediary Iceberg table is created. Initial copy uses the reviewed pure
projections; Spark runs the six calculations. Source and target row signatures
preserve NULL, JSON text, decimal scale and UTC microseconds. A replayable target
checksum query records the expected source signature without retaining source rows.
"""
from datetime import datetime, timezone, date
from decimal import Decimal
import hashlib
import json
import re

from oil_gp_snapshot import connect_source, source_readiness, source_relation, identifier, literal

CHUNK = 500
TYPE = re.compile(r'(?:varchar|integer|bigint|boolean|date|decimal\(\d+,\d+\)|timestamp(?:\(6\))?(?: with time zone)?)\Z')


def canonical(value, kind):
    """Normalize both database representations to one lossless string or SQL NULL."""
    if not TYPE.fullmatch(kind):
        raise ValueError('Unreviewed column type')
    if value is None:
        return None
    if kind == 'varchar':
        return str(value)
    if kind in ('integer', 'bigint'):
        return str(int(value))
    if kind == 'boolean':
        if isinstance(value, bool):return str(value).lower()
        if str(value).lower() not in ('true','false'):raise ValueError('Invalid boolean')
        return str(value).lower()
    if kind.startswith('decimal'):
        scale=int(kind.rstrip(')').split(',')[1])
        number=Decimal(str(value));quantized=number.quantize(Decimal(10)**-scale)
        if number!=quantized:raise ValueError('Decimal scale would lose source precision')
        return format(quantized,'.'+str(scale)+'f')
    if kind == 'date':
        return value.isoformat() if isinstance(value,date) else str(value)
    stamp=value if isinstance(value,datetime) else datetime.fromisoformat(str(value).replace(' UTC','+00:00'))
    if 'with time zone' in kind:
        if stamp.tzinfo is None:raise ValueError('Timezone missing')
        return stamp.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f UTC')
    return stamp.strftime('%Y-%m-%d %H:%M:%S.%f')


def typed_literal(value, kind):
    """Produce one explicitly typed Cedrus value; never interpolate arbitrary identifiers."""
    text=canonical(value,kind)
    if text is None:return 'CAST(NULL AS '+kind+')'
    if 'with time zone' in kind:
        stamp=value if isinstance(value,datetime) else datetime.fromisoformat(text.replace(' UTC','+00:00'))
        return 'CAST(from_iso8601_timestamp_nanos('+literal(stamp.astimezone(timezone.utc).isoformat())+') AS '+kind+')'
    return 'CAST('+literal(text)+' AS '+kind+')'


def row_hash(row, types):
    """Hash canonical ordered values, distinguishing NULL from the literal text null."""
    text=json.dumps([canonical(v,t) for v,t in zip(row,types)],ensure_ascii=False,separators=(',',':'),allow_nan=False)
    return hashlib.sha256(text.encode()).hexdigest().upper()


def multiset_hash(hashes):
    """Order-independent SHA-256 of all row digests; duplicate multiplicity is retained."""
    return hashlib.sha256('\n'.join(sorted(hashes)).encode()).hexdigest().upper()


def target_signature_query(table, snapshot, op):
    """Compute the same typed row/multiset signatures entirely in Cedrus on a pinned version."""
    values=[]
    for column,kind in zip(op['columns'],op['trino_types']):
        if not TYPE.fullmatch(kind):raise ValueError('Unreviewed column type')
        name=identifier(column)
        if 'with time zone' in kind:
            value='CAST((CAST('+name+' AS timestamp(6) with time zone) AT TIME ZONE \'UTC\') AS varchar)'
        elif kind.startswith('timestamp'):
            value='CAST(CAST('+name+' AS timestamp(6)) AS varchar)'
        else:value='CAST('+name+' AS varchar)'
        values.append(value)
    relation=table+' FOR VERSION AS OF '+str(int(snapshot)) if snapshot is not None else '(SELECT * FROM '+table+' WHERE false)'
    each='SELECT to_hex(sha256(to_utf8(json_format(CAST(ARRAY['+','.join(values)+'] AS JSON))))) h FROM '+relation
    return "SELECT count(*) AS row_count,to_hex(sha256(to_utf8(coalesce(array_join(array_agg(h ORDER BY h),chr(10)),'')))) AS row_digest FROM ("+each+') canonical_rows'


class DirectSource:
    """One native connection is owned by one Airflow task for INITIAL or REFRESH."""
    def __init__(self, client, mode, run_id, connection_factory=connect_source, readiness=source_readiness):
        self.client,self.mode,self.run_id=client,mode,run_id
        self.factory,self.readiness=connection_factory,readiness
        self.db=None;self.value=None;self.signatures={}

    def inspect(self, mode, operations, client):
        """Open C0/C1 once, validate the applied prefix, and describe the twenty source objects."""
        if self.value is not None:
            self.assert_held();return self.value
        if mode!=self.mode or client is not self.client:raise ValueError('Foreign source request')
        self.db=self.factory()
        self.db.set_session(isolation_level='REPEATABLE READ',readonly=True,autocommit=False)
        with self.db.cursor() as cur:
            cur.execute("SET LOCAL TIME ZONE 'UTC'")
            cur.execute("SET LOCAL statement_timeout='120s'")
            cur.execute("SET LOCAL lock_timeout='3s'")
            boundary=self.readiness(cur)
            if boundary is None:raise ValueError('Source contract returned no applied cut')
            cur.execute('SELECT pg_export_snapshot(),clock_timestamp(),pg_backend_pid()')
            snapshot,stamp,pid=cur.fetchone()
            tables={}
            for op in operations:
                symbol=op['symbol'];schema,table=symbol.split('__',1)
                fields=','.join(identifier(c) for c in op['columns'])
                projection=source_relation(symbol,boundary)
                cur.execute('SELECT '+fields+' FROM '+projection+' LIMIT 0')
                oids=[int(c.type_code) for c in cur.description]
                cur.execute('SELECT oid::bigint FROM pg_class WHERE oid=%s::regclass',(schema+'.'+table,))
                oid=int(cur.fetchone()[0])
                tables[symbol]={'schema':schema,'table':table,'oid':oid,'columns':op['columns'],
                    'trino_types':op['trino_types'],'spark_types':op['spark_types'],
                    'source_type_oids':oids,'source_projection':projection}
        self.value={**boundary,'provider':'native_gp_direct','boundary_id':'C0' if mode=='initial' else 'C1',
            'gp_snapshot_id':snapshot,'gp_backend_pid':int(pid),'captured_at':stamp.isoformat(),'tables':tables,
            'producer_paused':False,'gp_sink_paused':False,'source_dag_paused':False,'source_write_locks':False}
        print(json.dumps({'phase':'NATIVE_GP_SNAPSHOT',**{k:self.value[k] for k in ['boundary_id','gp_snapshot_id','gp_backend_pid','partitions','delivery_partitions','pending_batches']}}),flush=True)
        return self.value

    def assert_held(self):
        """A lost source connection invalidates the candidate instead of silently opening a new cut."""
        if self.db is None or self.db.closed or self.db.get_backend_pid()!=self.value['gp_backend_pid']:
            raise ValueError('The original GP snapshot connection was lost')
        from psycopg2.extensions import TRANSACTION_STATUS_INTRANS
        if self.db.get_transaction_status()!=TRANSACTION_STATUS_INTRANS:
            raise ValueError('The original GP read-only transaction is no longer active')

    def validate_manifest(self, value):
        """Do not let a later operation replace its source boundary or object/type contract."""
        if self.value!=value:raise ValueError('Source boundary changed')

    def select_sql(self, op):
        """Return the actual native source SELECT for receipts and Catalog lineage, without row data."""
        self.assert_held();meta=self.value['tables'][op['symbol']]
        fields=','.join(identifier(c)+('::text AS '+identifier(c) if oid in (114,3802) else '')
                        for c,oid in zip(op['columns'],meta['source_type_oids']))
        return 'SELECT '+fields+' FROM '+meta['source_projection']

    def rows(self, op):
        """Stream source values through the same native session, preserving lexical JSON fields."""
        query=self.select_sql(op)
        with self.db.cursor(name='direct_'+hashlib.sha256(op['symbol'].encode()).hexdigest()[:16]) as cur:
            cur.itersize=CHUNK;cur.execute(query)
            while True:
                batch=cur.fetchmany(CHUNK)
                if not batch:break
                yield batch

    def copy_into(self, runner, op, state, table):
        """Write directly into the proper layer table; journal intent before every chunk."""
        original=(runner.sql_dir/op['symbol']/'initial.sql').read_text()
        cleaned=re.sub(r'--[^\n]*','',original).strip().rstrip(';')
        match=re.fullmatch(r'SELECT\s+([a-z_,\s]+)\s+FROM\s+\{\{source:'+re.escape(op['symbol'])+r'\}\}',cleaned,re.I)
        if not match or sorted(c.strip() for c in match[1].split(','))!=sorted(op['columns']):
            raise ValueError('Native direct copy only accepts the reviewed pure projection')
        step=state['steps'][op['symbol']];step['chunks']=[]
        step.update(query=self.select_sql(op),write_kind='INSERT_VALUES',write_target=table,write_columns=op['columns'])
        hashes=[];count=0
        fields=','.join(identifier(c) for c in op['columns'])
        for batch in self.rows(op):
            values=','.join('('+','.join(typed_literal(v,t) for v,t in zip(row,op['trino_types']))+')' for row in batch)
            statement='/* direct_gp:'+op['symbol']+' */ INSERT INTO '+table+' ('+fields+') VALUES '+values
            chunk={'number':len(step['chunks']),'rows':len(batch),'state':'ATTEMPTING',
                'query_hash':hashlib.sha256(statement.encode()).hexdigest()}
            step['chunks'].append(chunk);runner.save(state)
            self.client.sql(statement)
            chunk['state']='COMMITTED';count+=len(batch)
            hashes.extend(row_hash(row,op['trino_types']) for row in batch)
            runner.save(state)
            print(json.dumps({'phase':'DIRECT_COPY_CHUNK','object_symbol':op['symbol'],'copied_rows':count,'target':table}),flush=True)
        self.signatures[op['symbol']]={'rows':count,'hash':multiset_hash(hashes)}
        step['source_signature']=self.signatures[op['symbol']]

    def compare(self, op, table, snapshot):
        """Reconcile source and target full row multisets, not only totals or foreign keys."""
        self.assert_held()
        if op['symbol'] not in self.signatures:
            hashes=[];count=0
            for batch in self.rows(op):
                count+=len(batch);hashes.extend(row_hash(row,op['trino_types']) for row in batch)
            self.signatures[op['symbol']]={'rows':count,'hash':multiset_hash(hashes)}
        expected=self.signatures[op['symbol']]
        signature=target_signature_query(table,snapshot,op)
        query='SELECT CASE WHEN row_count='+str(expected['rows'])+' AND row_digest='+literal(expected['hash'])+' THEN 0 ELSE 1 END n FROM ('+signature+') source_comparison'
        return query,self.client.sql(query)

    def relation(self, *args):
        """Reject accidental Cedrus GP reads: those would not inherit this native transaction."""
        raise ValueError('Native GP source must be read through its owning Python task')

    def close(self):
        """Close the read-only source transaction; this never commits or rolls back Iceberg writes."""
        if self.db is not None:
            self.db.rollback();self.db.close();self.db=None
