"""Catch up and continuously process the first oil chain in the existing Iceberg tables.

AS IS oil_gp_release invokes GP procedures in one database transaction. TO BE
uses one Spark session, explicit PK/FK gates, table snapshots and compensating
rollback. Iceberg has no cross-table transaction: the single BI serving table
is written last. A shared writer pool and filesystem lock serialize all writers.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

from oil_layered_plan import OPERATIONS
from oil_layered_runtime import atomic_write, fingerprint, target
from oil_layered_cedrus import MCP
from oil_layered_spark import SparkClient
from oil_direct_source import DirectSource

RAW = 'dlh.bronze.__raw_events'
MERGE_KEYS={'oil_stg__batch_control': ['batch_id'], 'oil_stg__batch_coalesce_map': ['original_batch_id'], 'oil_stg__event_log': ['topic', 'kafka_partition', 'kafka_offset'], 'oil_stg__batch_coalesce_event_map': ['topic', 'kafka_partition', 'kafka_offset'], 'oil_stg__kafka_checkpoint': ['topic', 'kafka_partition'], 'oil_stg__v_event_parsed': ['topic', 'kafka_partition', 'kafka_offset'], 'oil_dds__event': ['event_id'], 'oil_dds__hub_field': ['field_hk'], 'oil_dds__hub_production_day': ['production_hk'], 'oil_dds__hub_well': ['well_hk'], 'oil_dds__link_production_day_well': ['link_hk'], 'oil_dds__link_well_field': ['link_hk'], 'oil_dds__sat_daily_production': ['event_id'], 'oil_dds__sat_field': ['event_id'], 'oil_dds__sat_well': ['event_id']}
ROOT = Path(os.environ.get('OIL_DLH_STATE_DIR', '/opt/airflow/logs/oil_dlh_release'))
INITIAL = Path(os.environ.get('OIL_INITIAL_RECEIPT', '/opt/airflow/logs/oil_layered_migration/ready_initial.json'))
DEFAULT_SQL=Path(__file__).parent/'business_sql'
if not DEFAULT_SQL.exists():DEFAULT_SQL=Path(__file__).parent.parent/'oil_dlh_release'
SQL = Path(os.environ.get('OIL_DLH_SQL_DIR',str(DEFAULT_SQL)))
# Checkpoint and serving publication follow the accepted Vault/Marts calculations.
ORDER = [o for o in OPERATIONS if o['symbol'] not in ('oil_stg__kafka_checkpoint','oil_mart__v_field_production')]
ORDER += [next(o for o in OPERATIONS if o['symbol']==name) for name in ('oil_stg__kafka_checkpoint','oil_mart__v_field_production')]


def client():
    """Use registered Cedrus credentials; no secrets enter source code or job receipts."""
    return MCP(os.environ['OIL_TRINO_MCP_URL'], os.environ.get('OIL_MCP_CA_FILE'), os.environ.get('OIL_MCP_BEARER_FILE'))


def code_hash():
    """Freeze Python, object plan and reviewed business SQL for the whole run."""
    files={p.relative_to(SQL).as_posix():p.read_text() for p in SQL.rglob('*.sql')}
    return fingerprint({'runtime':Path(__file__).read_text(),'operations':OPERATIONS,'sql':files})


@contextmanager
def writer_lock(root):
    """Serialize manual catchup, scheduled release and direct CLI calls on the shared worker storage."""
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    with (root/'.writer.lock').open('a+') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('Another DLH writer is running')
        try:yield
        finally:fcntl.flock(lock,fcntl.LOCK_UN)


class Processor:
    def __init__(self, engine, spark, mode, run_id, root=ROOT, sql_dir=SQL, source=None):
        """Bind one bounded run to one Spark session and optional GP C1 reference snapshot."""
        if mode not in ('catchup','release'):raise ValueError('mode must be catchup or release')
        self.c,self.spark,self.mode,self.run_id=engine,spark,mode,str(run_id)
        self.root,self.sql_dir,self.source=Path(root),Path(sql_dir),source
        self.pointer=self.root/'ready_dlh.json'
        self.path=self.root/(fingerprint(self.run_id)[:32]+'.json')
        self.state=None

    def snapshot(self, table):
        """Read the current Iceberg snapshot; require a materialized baseline for recurring writes."""
        catalog,schema,name=table.split('.')
        rows=self.c.sql('SELECT snapshot_id FROM '+catalog+'.'+schema+'."'+name+'$refs" WHERE name=\'main\'')
        return int(rows[0]['snapshot_id']) if rows else None

    def identity(self, table):
        """Detect table recreation or schema changes before writing/rolling back."""
        return fingerprint(self.c.sql('SHOW CREATE TABLE '+table))

    def save(self):
        """Persist write intent before an external command and its acknowledged result afterwards."""
        atomic_write(self.path,self.state)

    def baseline(self):
        """Adopt the already accepted INITIAL once; keep the original passport unchanged."""
        if (self.root/'blocked.json').exists():raise RuntimeError('An unresolved writer exists; reconcile blocked.json first')
        for path in self.root.glob('*.json'):
            if re.fullmatch(r'[a-f0-9]{32}\.json',path.name):
                pending=json.loads(path.read_text())
                if pending.get('status') not in ('READY','ROLLED_BACK'):
                    raise RuntimeError('An unfinished writer receipt exists: '+str(path))
        previous=json.loads((self.pointer if self.pointer.exists() else INITIAL).read_text())
        if previous.get('status')!='READY' or len(previous['steps'])!=20:
            raise ValueError('Accept the complete INITIAL first')
        for op in OPERATIONS:
            symbol=op['symbol'];table=target(symbol)
            if self.identity(table)!=previous['targets'][symbol]['identity']:
                raise ValueError('Table identity changed: '+table)
            if self.snapshot(table)!=previous['steps'][symbol]['snapshot']:
                raise ValueError('Data changed outside the accepted release: '+table)
        return previous

    def raw_boundary(self, previous, raw_snapshot):
        """Advance only across a contiguous committed raw prefix; GP is not read by scheduled releases."""
        positions=[]
        for part in previous['manifest']['partitions']:
            p,start=int(part['partition']),int(part['end'])
            rows=self.c.sql('SELECT max(kafka_offset) n FROM '+RAW+' FOR VERSION AS OF '+str(raw_snapshot)+" WHERE topic='oil.events.v1' AND kafka_partition="+str(p))
            end=start if rows[0]['n'] is None else max(start,int(rows[0]['n'])+1)
            positions.append({'partition':p,'end':end})
        observed=self.c.sql('SELECT DISTINCT kafka_partition FROM '+RAW+' FOR VERSION AS OF '+str(raw_snapshot)+" WHERE topic='oil.events.v1'")
        if {int(x['kafka_partition']) for x in observed}!={p['partition'] for p in positions}:
            raise ValueError('Kafka partition set changed; review the new partition contract')
        return positions

    def prepare(self, overlap_offsets):
        """Pin previous tables, C1/raw versions and complete overlap before the first mutation."""
        if type(overlap_offsets) is not int or not 0<=overlap_offsets<=10000:raise ValueError('Invalid overlap_offsets')
        if self.path.exists():raise FileExistsError('Use a new run ID; do not Clear/Retry a previous write')
        previous=self.baseline()
        raw_snapshot=self.snapshot(RAW)
        if raw_snapshot is None:raise ValueError('Raw is empty; check the Iceberg Kafka sink')
        if self.source:
            manifest=self.source.inspect('refresh',OPERATIONS,self.c)
            end=manifest['partitions']
        else:
            end=self.raw_boundary(previous,raw_snapshot)
            manifest={'provider':'dlh_raw_offsets_v1','topic':'oil.events.v1','partitions':end,
                      'captured_at':datetime.now(timezone.utc).isoformat(),'source_write_locks':False}
        start={p['partition']:p['end'] for p in previous['manifest']['partitions']}
        if set(start)!={p['partition'] for p in end}:raise ValueError('Kafka partition contract changed')
        window=[]
        for p in end:
            partition,last=int(p['partition']),int(p['end'])
            if partition<0 or partition>=9000 or last<start[partition] or last>=10**15:raise ValueError('Invalid Kafka frontier')
            window.append({'partition':partition,'start':start[partition],'read_start':max(0,start[partition]-overlap_offsets),'end':last})
        fresh_positions=' OR '.join('(kafka_partition={partition} AND kafka_offset>={start} AND kafka_offset<{end})'.format(**p) for p in window)
        type_rows=self.c.sql("SELECT DISTINCT json_extract_scalar(event_json,'$.event_type') event_type FROM "+RAW+' FOR VERSION AS OF '+str(raw_snapshot)+" WHERE topic='oil.events.v1' AND ("+fresh_positions+')')
        event_types={row['event_type'] for row in type_rows}
        if event_types-{'field','well','production_day','downtime'}:raise ValueError('Unknown event type; review event contract before processing')
        advanced=sum(p['end']-p['start'] for p in window)
        # A manual migration acceptance still needs the GP checks when C1=C0.
        # Only ordinary scheduled releases may finish before those checks.
        if not advanced and not self.source:
            return {'status':'NO_CHANGES','mode':self.mode,'partitions':end,'tables_written':0}
        # The raw snapshot may lag GP. Refuse partial replay; wait for a fresh complete snapshot on a new run.
        for p in window:
            rows=self.c.sql('SELECT count(DISTINCT kafka_offset) n FROM '+RAW+' FOR VERSION AS OF '+str(raw_snapshot)+" WHERE topic='oil.events.v1' AND kafka_partition="+str(p['partition'])+' AND kafka_offset>='+str(p['read_start'])+' AND kafka_offset<'+str(p['end']))
            if rows[0]['n']!=p['end']-p['read_start']:raise ValueError('Raw window is incomplete; wait for the connector commit and trigger a new run')
        self.state={'status':'PREPARED','mode':self.mode,'run_id':self.run_id,'contract_hash':code_hash(),
                    'manifest':manifest,'window':window,'raw_snapshot':raw_snapshot,'previous_run_id':previous['run_id'],
                    'previous':{s:step['snapshot'] for s,step in previous['steps'].items()},
                    'targets':previous['targets'],'steps':{},'checks':[],
                    'expected_checks':58 if self.source else 53,'advanced_events':advanced,'event_types':sorted(event_types)}
        self.save()
        print(json.dumps({'phase':'PREPARED','run_id':self.run_id,'mode':self.mode,'window':window,'raw_snapshot':raw_snapshot,'advanced_events':advanced}),flush=True)
        return None

    def render(self, query, dialect):
        """Resolve exact current/previous/raw tables; no temporary _next schema/table exists."""
        state=self.state
        def relation(match):
            side,symbol=match.groups()
            if symbol not in state['targets']:raise ValueError('Relation outside the approved chain')
            table=target(symbol)
            if side=='target':return table
            if side in ('previous','initial'):
                snap=state['previous'][symbol]
                if snap is None:return '(SELECT * FROM '+table+' WHERE false)'
                return table+(' VERSION AS OF ' if dialect=='spark' else ' FOR VERSION AS OF ')+str(snap)
            raise ValueError('Source reads must use the native C1 owner')
        value=re.sub(r'\{\{(source|target|previous|initial):([^}]+)\}\}',relation,query)
        positions=' OR '.join('(kafka_partition={partition} AND kafka_offset>={read_start} AND kafka_offset<{end})'.format(**p) for p in state['window'])
        raw='(SELECT * FROM '+RAW+(' VERSION AS OF ' if dialect=='spark' else ' FOR VERSION AS OF ')+str(state['raw_snapshot'])+" WHERE topic='oil.events.v1' AND ("+positions+'))'
        value=value.replace('{{raw_window}}',raw).replace('{{delta}}',raw)
        stamp=state['manifest']['captured_at'].replace("'","''")
        timestamp="TO_TIMESTAMP('"+stamp+"')" if dialect=='spark' else "CAST(FROM_ISO8601_TIMESTAMP_NANOS('"+stamp+"') AS timestamp(6) with time zone)"
        value=value.replace('{{release_time}}',timestamp).replace('{{cut_time}}',timestamp)
        if '{{' in value:raise ValueError('Unresolved SQL placeholder')
        return value.strip().rstrip(';')

    def validate(self, op):
        """Run PK/FK, retention and semantic gates; catchup also compares all Gold values with GP C1."""
        symbol=op['symbol'];step=self.state['steps'][symbol]
        for check in op['checks']:
            if check['phase'] not in ('both','refresh'):continue
            text=(self.sql_dir/symbol/check['path']).read_text()
            if '{{source:' in text:
                if not self.source:continue
                query,rows=self.source.compare(op,target(symbol),step['snapshot'])
                step['source_signature']=self.source.signatures[symbol]
            else:
                query=self.render(text,'trino');rows=self.c.sql(query)
            if len(rows)!=1 or 'n' not in rows[0]:raise ValueError('Invalid DQ response')
            self.state['checks'].append({'symbol':symbol,'name':check['name'],'violations':rows[0]['n'],'query':query})
            self.save()
            if rows[0]['n']!=0:raise ValueError('Quality gate failed: '+symbol+'.'+check['name'])

    def materialize(self, op):
        """Overwrite one existing table atomically; a VALIDATED parent is required before its child."""
        if code_hash()!=self.state['contract_hash']:raise ValueError('Code changed during the run')
        if self.source:self.source.assert_held()
        symbol=op['symbol'];table=target(symbol)
        for parent in op['dependencies']:
            if self.state['steps'].get(parent,{}).get('state')!='VALIDATED':raise ValueError('Parent not accepted: '+parent)
        if self.identity(table)!=self.state['targets'][symbol]['identity'] or self.snapshot(table)!=self.state['previous'][symbol]:
            raise ValueError('Foreign change before write: '+table)
        types=set(self.state['event_types'])
        unchanged=symbol in {'oil_stg__batch_coalesce_map','oil_stg__batch_coalesce_event_map'}
        unchanged=unchanged or (symbol in {'oil_dds__hub_field','oil_dds__sat_field','oil_mart__dim_field'} and 'field' not in types)
        unchanged=unchanged or (symbol in {'oil_dds__hub_well','oil_dds__link_well_field','oil_dds__sat_well','oil_mart__dim_well'} and 'well' not in types)
        unchanged=unchanged or (symbol in {'oil_dds__hub_production_day','oil_dds__link_production_day_well','oil_dds__sat_daily_production'} and 'production_day' not in types)
        unchanged=unchanged or (symbol in {'oil_mart__fact_well_day','oil_mart__agg_field_day','oil_mart__v_field_production'} and not types.intersection({'field','well','production_day'}))
        if unchanged:
            # Like GP apply_batch branches: unchanged entities retain their snapshot, but still pass every DQ gate.
            self.state['steps'][symbol]={'state':'COMMITTED','table':table,'snapshot':self.state['previous'][symbol],
                'before_snapshot':self.state['previous'][symbol],'write_kind':'UNCHANGED',
                'query':'SELECT * FROM '+self.render('{{previous:'+symbol+'}}','trino')}
            self.save();self.validate(op);self.state['steps'][symbol]['state']='VALIDATED';self.save()
            print(json.dumps({'phase':'VALIDATED','object_symbol':symbol,'write_kind':'UNCHANGED'}),flush=True)
            return
        query=self.render((self.sql_dir/symbol/'apply.sql').read_text(),'spark')
        fields=', '.join('`'+c+'`' for c in op['columns'])
        tag='/* oil_dlh:'+fingerprint(self.run_id)[:16]+':'+symbol+' */ '
        if symbol in MERGE_KEYS:
            # PK/FK constraints move from GP into explicit key contracts and DQ gates.
            # Exact overlap replays become no-ops instead of duplicate INSERTs.
            on=' AND '.join('t.`'+key+'`=s.`'+key+'`' for key in MERGE_KEYS[symbol])
            identical=' AND '.join('t.`'+col+'` <=> s.`'+col+'`' for col in op['columns'])
            updates=', '.join('t.`'+col+'`=s.`'+col+'`' for col in op['columns'])
            values=', '.join('s.`'+col+'`' for col in op['columns'])
            delta_query='SELECT '+fields+' FROM ('+query+') desired EXCEPT ALL SELECT '+fields+' FROM '+self.render('{{previous:'+symbol+'}}','spark')
            statement=tag+'MERGE INTO '+table+' t USING ('+delta_query+') s ON '+on+' WHEN MATCHED AND NOT ('+identical+') THEN UPDATE SET '+updates+' WHEN NOT MATCHED THEN INSERT ('+fields+') VALUES ('+values+')'
            kind='SPARK_MERGE'
        else:
            # GP refresh_production rebuilds the current Marts; atomic overwrite also removes deleted facts.
            statement=tag+'INSERT OVERWRITE TABLE '+table+' ('+fields+') SELECT '+fields+' FROM ('+query+') reviewed_projection'
            kind='SPARK_OVERWRITE'
        step={'state':'ATTEMPTING','table':table,'query':statement,'write_kind':kind,'before_snapshot':self.state['previous'][symbol]}
        self.state['steps'][symbol]=step;self.state['status']='BUILDING';self.save()
        print(json.dumps({'phase':'OBJECT_START','object_symbol':symbol,'parents':op['dependencies'],'write_kind':kind}),flush=True)
        print(statement,flush=True)
        try:
            self.spark.sql(statement)
            step.update(state='COMMITTED',snapshot=self.snapshot(table));self.save()
        except BaseException:
            step.update(state='UNKNOWN_OUTCOME',kernel=self.spark.kernel,notebook=self.spark.name);self.save();raise
        self.validate(op)
        step['state']='VALIDATED';self.save()
        print(json.dumps({'phase':'VALIDATED','object_symbol':symbol,'snapshot':step['snapshot']}),flush=True)

    def accept(self):
        """Advance the durable business frontier only after every table and check has passed."""
        if len(self.state['steps'])!=20 or any(x['state']!='VALIDATED' for x in self.state['steps'].values()):raise ValueError('Incomplete tables')
        if len(self.state['checks'])!=self.state['expected_checks'] or any(c['violations']!=0 for c in self.state['checks']):raise ValueError('Incomplete DQ')
        for symbol,step in self.state['steps'].items():
            if self.snapshot(target(symbol))!=step['snapshot']:raise ValueError('Foreign change after validation')
        if self.source:self.source.assert_held()
        self.state['status']='READY';self.save()
        # Retain the migration C1 receipt when the same DAG starts its next
        # scheduled release and replaces ready_dlh.json.
        if self.mode=='catchup':atomic_write(self.root/'ready_catchup.json',self.state)
        atomic_write(self.pointer,self.state)
        print(json.dumps({'phase':'READY','mode':self.mode,'run_id':self.run_id,'partitions':self.state['manifest']['partitions'],'checks':len(self.state['checks']),'tables':20}),flush=True)
        return {'status':'READY','run_id':self.run_id,'partitions':self.state['manifest']['partitions'],'checks':len(self.state['checks'])}

    def rollback(self):
        """Restore only confirmed own snapshots in reverse order; ambiguous outcomes block further writers."""
        if not self.state:return
        errors=[]
        for symbol,step in reversed(list(self.state['steps'].items())):
            try:
                if step['state'] in ('ATTEMPTING','UNKNOWN_OUTCOME'):raise ValueError('Command outcome unknown; inspect the Spark receipt')
                table=target(symbol)
                if self.identity(table)!=self.state['targets'][symbol]['identity'] or self.snapshot(table)!=step['snapshot']:
                    raise ValueError('Foreign change; refusing rollback')
                before=step['before_snapshot']
                if before is None:
                    self.spark.sql('DELETE FROM '+table)
                    if self.c.sql('SELECT count(*) n FROM '+table)!=[{'n':0}]:raise ValueError('Empty baseline was not restored')
                    step.update(state='ROLLED_BACK',restored_empty_snapshot=self.snapshot(table));self.save();continue
                if before==step['snapshot']:
                    step['state']='ROLLED_BACK';self.save();continue
                _,schema,name=table.split('.')
                self.spark.sql("CALL dlh.system.rollback_to_snapshot(table => '"+schema+'.'+name+"', snapshot_id => "+str(before)+')')
                if self.snapshot(table)!=before:raise ValueError('Rollback snapshot not current')
                step['state']='ROLLED_BACK';self.save()
            except Exception as error:errors.append({'symbol':symbol,'error':str(error)})
        if not errors and any('restored_empty_snapshot' in x for x in self.state['steps'].values()):
            previous=json.loads((self.pointer if self.pointer.exists() else INITIAL).read_text())
            for symbol,step in self.state['steps'].items():
                if 'restored_empty_snapshot' in step:previous['steps'][symbol]['snapshot']=step['restored_empty_snapshot']
            previous['recovered_empty_tables_from']=self.run_id
            atomic_write(self.pointer,previous)
        self.state['status']='MANUAL_RECONCILE' if errors else 'ROLLED_BACK'
        self.state['rollback_errors']=errors;self.save()
        if errors:atomic_write(self.root/'blocked.json',{'run_id':self.run_id,'receipt':str(self.path),'errors':errors})
        print(json.dumps({'phase':self.state['status'],'rollback_errors':errors}),flush=True)


def execute(mode, run_id, overlap_offsets=10):
    """Shared entry point for Airflow and CLI; scheduled release never connects to Greenplum."""
    with writer_lock(ROOT):
        engine=client();spark=SparkClient('bridge_'+uuid.uuid4().hex)
        source=DirectSource(engine,'refresh',run_id) if mode=='catchup' else None
        runner=Processor(engine,spark,mode,run_id,source=source)
        try:
            no_changes=runner.prepare(overlap_offsets)
            if no_changes:
                print(json.dumps(no_changes),flush=True);return no_changes
            # One Spark session handles all twenty entities, instead of twenty independent kernel startups.
            spark.open()
            spark.execute("spark.conf.set('spark.sql.shuffle.partitions','8')\nspark.conf.set('spark.sql.adaptive.enabled','true')\nprint('PB_RESULT:{}')")
            runner.state['spark']={'notebook':spark.name,'kernel':spark.kernel};runner.save()
            for op in ORDER:runner.materialize(op)
            return runner.accept()
        except BaseException:
            runner.rollback();raise
        finally:
            if source:source.close()
            # Unknown outcomes retain the kernel for exact command reconciliation, never blind re-execution.
            if not runner.state or runner.state['status']!='MANUAL_RECONCILE':
                try:spark.close()
                except Exception:pass
