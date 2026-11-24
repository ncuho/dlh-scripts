#!/usr/bin/env python3
"""Reset the first oil migration to step 1; default mode only reads live state."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import sys
import time
import uuid

REPO = Path(__file__).resolve().parents[2]
BRIDGE = Path(os.environ.get('PLATFORM_BRIDGE_ROOT', str(REPO.parents[1]/'platform-bridge'))).resolve()
CONNECTOR = 'manual-oil-production-a'
DAGS = ['oil_layered_migration', 'oil_dlh_catchup', 'oil_dlh_release']
OPTIONAL_DAGS = ['oil_production_spark_candidate']
TABLES = {
    'bronze': ['__raw_events', 'batch_control', 'batch_coalesce_map', 'batch_coalesce_event_map', 'event_log', 'kafka_checkpoint', 'v_event_parsed'],
    'silver': ['event', 'hub_field', 'hub_production_day', 'hub_well', 'link_production_day_well', 'link_well_field', 'sat_daily_production', 'sat_field', 'sat_well'],
    'gold': ['dim_field', 'dim_well', 'fact_well_day', 'agg_field_day', 'v_field_production'],
}
SOURCE_DASHBOARDS = {3, 4}
LOCAL_FILES = ['oil-ready-initial.json', 'oil-ready-dlh.json', 'oil-ready-catchup.json', 'oil-ready-release.json',
               'oil-initial-checks.sql', 'oil-dlh-checks.sql', 'oil-catchup-checks.sql', 'oil-release-checks.sql']


def write_json(path, data):
    """Atomically record progress in a private local journal; never store auth tokens."""
    temporary = path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str)+'\n')
    temporary.chmod(0o600)
    temporary.replace(path)


def dashboard_ids(chart):
    """Read Superset chart membership; reject unknown API shapes rather than guess ownership."""
    result = set()
    for entry in chart.get('dashboards', []):
        ident = entry.get('id') if isinstance(entry, dict) else entry
        if type(ident) is not int:
            raise ValueError('Unknown Superset dashboard reference')
        result.add(ident)
    return result


def select_bi(raw):
    """Discover new native IDs from dataset bindings; refuse mixed/shared dashboards."""
    datasets = []
    for item in raw['datasets']:
        if item.get('database', {}).get('id') != 2 or item.get('schema') not in TABLES:
            continue
        if item.get('catalog') != 'dlh':
            raise ValueError('Ambiguous catalog for target namespace: '+str(item['id']))
        if item.get('table_name') not in TABLES[item['schema']] or item.get('sql'):
            raise ValueError('Unreviewed dataset in migration namespace: '+str(item['id']))
        datasets.append(item)
    ids = {item['id'] for item in datasets}
    charts = [item for item in raw['charts'] if item.get('datasource_type')=='table' and item.get('datasource_id') in ids]
    chart_ids = {item['id'] for item in charts}
    selected_dashboards = set().union(*(dashboard_ids(item) for item in charts)) if charts else set()
    if selected_dashboards & SOURCE_DASHBOARDS:
        raise ValueError('Target charts are shared with a protected source dashboard')
    for item in raw['charts']:
        if dashboard_ids(item) & selected_dashboards and item['id'] not in chart_ids:
            raise ValueError('Mixed dashboard contains unrelated charts; separate it before reset')
    dashboards = [item for item in raw['dashboards'] if item['id'] in selected_dashboards]
    if {item['id'] for item in dashboards} != selected_dashboards:
        raise ValueError('Incomplete dashboard inventory')
    if not SOURCE_DASHBOARDS <= {item['id'] for item in raw['dashboards']}:
        raise ValueError('Source dashboards are unavailable')
    return {'datasets':datasets, 'charts':charts, 'dashboards':dashboards}


def validate_plan(plan):
    """Permit only this chain's fixed physical names; table/namespace UUIDs bind each reset."""
    if set(plan['lakekeeper']) != set(TABLES):
        raise ValueError('Incomplete namespace inventory')
    for namespace, entry in plan['lakekeeper'].items():
        if namespace not in TABLES:
            raise ValueError('Unowned namespace')
        if entry.get('absent'):
            continue
        unknown = {item['name'] for item in entry['tables']} - set(TABLES[namespace])
        if unknown:
            raise ValueError('Unowned tables; target retained: '+str(sorted(unknown)))
    config = plan['kafka'].get('target_config')
    if config:
        expected = {'topics':'oil.events.v1', 'iceberg.tables':'bronze.__raw_events',
                    'iceberg.control.topic':'manual-oil-production-a-control',
                    'iceberg.control.group-id-prefix':'manual-oil-production-a-control'}
        if any(config.get(key)!=value for key,value in expected.items()):
            raise ValueError('Target connector is bound to unexpected resources')
        if config.get('consumer.override.group.id') not in (None, 'connect-'+CONNECTOR):
            raise ValueError('Unreviewed custom consumer group')
    if SOURCE_DASHBOARDS & {x['id'] for x in plan['bi']['dashboards']}:
        raise ValueError('Protected BI resource')
    datasets=plan['bi']['datasets']
    if any(d.get('database',{}).get('id')!=2 or d.get('catalog')!='dlh' or d.get('schema') not in TABLES
           or d.get('table_name') not in TABLES[d['schema']] or d.get('sql') for d in datasets):
        raise ValueError('Unowned dataset in journal')
    ids={d['id'] for d in datasets}
    if any(c.get('datasource_id') not in ids or c.get('datasource_type')!='table' for c in plan['bi']['charts']):
        raise ValueError('Unowned chart in journal')
    source=plan['kafka']['source']
    if source['connector']['state']!='RUNNING' or not source['tasks'] or any(t['state']!='RUNNING' for t in source['tasks']):
        raise ValueError('Source Kafka sink is unhealthy; leave target intact')
    if plan['airflow']['oil_gp_release']['paused'] or any(x['import_errors'] for x in plan['airflow'].values()):
        raise ValueError('Source DAG paused or DAG import errors; leave target intact')


class Backend:
    """Use the stand's existing authenticated adapters, native service APIs and SSH routes."""
    def __init__(self):
        sys.path[:0] = [str(BRIDGE), str(BRIDGE/'scripts')]
        from bridge.config import Settings
        from bridge.operations import WaveServices
        from bridge.project_services import ProjectServices
        from bridge.project_registry import connection
        from verify_direct_followup import ssadmin
        settings = Settings()
        self.wave = WaveServices(settings)
        self.services = ProjectServices(settings, connection(settings, 'dp103'))
        self.ss = ssadmin

    def airflow(self):
        """Read paused/import states and all active runs, including an old queued run."""
        return self.wave.air('names='+repr(DAGS+OPTIONAL_DAGS)+'\noptional='+repr(OPTIONAL_DAGS)+'''
out={}
for name in ['oil_gp_release']+names:
 try:d=call('/api/v2/dags/'+name)
 except urllib.error.HTTPError as e:
  if e.code==404 and name in optional:continue
  raise
 active=[];offset=0
 while True:
  page=call('/api/v2/dags/'+name+'/dagRuns?limit=100&offset='+str(offset))
  rows=page['dag_runs'];active += [r['dag_run_id'] for r in rows if r['state'] in ['running','queued']]
  offset+=len(rows)
  if offset>=page.get('total_entries',offset) or not rows:break
 out[name]={'paused':d['is_paused'],'import_errors':d['has_import_errors'],'active':active}
print(json.dumps(out))''')

    def kafka(self):
        """Read connector ownership and producer state without stopping or resetting anything."""
        return self.services.kafka('''import subprocess
source=call(18084,'/connectors/webinar-oil-gp-ingest/status')
names=call(18085,'/connectors');name='manual-oil-production-a'
config=call(18085,'/connectors/'+name+'/config') if name in names else None
safe={k:v for k,v in (config or {}).items() if not any(s in k.lower() for s in ['password','secret','token','credential']) or k.endswith('credential-file')}
print(json.dumps({'source':source,'target_config':safe or None,'producer_running':subprocess.run(['pgrep','-f','^python3 /opt/kafka-demo/oil-generator/live_oil_updates.py'],capture_output=True).returncode==0}))''')

    def lakekeeper(self):
        """Read namespace/table UUIDs via the real REST catalog."""
        return self.services.lk("print(json.dumps({ns:inventory(ns) for ns in ['bronze','silver','gold']}))")

    def bi(self):
        """Read paginated native BI definitions; IDs are discovered every new run."""
        raw = self.ss('''out={}
for kind,key in [('dataset','datasets'),('chart','charts'),('dashboard','dashboards')]:
 items=[];page=0
 while True:
  batch=call('/api/v1/'+kind+'/?q=(page:'+str(page)+',page_size:100)')
  rows=batch['result'];items+=rows
  if len(items)>=batch.get('count',len(items)) or not rows:break
  page+=1
 # Full definitions are both deletion guards and a configuration backup, not a data backup.
 out[key]=[call('/api/v1/'+kind+'/'+str(item['id']))['result'] for item in items]
print('RESULT:'+json.dumps(out,default=str))''')
        return select_bi(raw)

    def runtime(self):
        """Inspect unresolved write intents; an unknown Spark outcome must not race a reset."""
        return self.wave.worker('''import json,re
from pathlib import Path
out={}
for name,ready in [('oil_layered_migration','ready_initial.json'),('oil_dlh_release','ready_dlh.json')]:
 p=Path('/opt/airflow/logs')/name;unfinished=[]
 for file in p.glob('*.json'):
  if re.fullmatch('[a-f0-9]{32}[.]json',file.name):
   r=json.loads(file.read_text())
   if r.get('status') not in ['READY','ROLLED_BACK']:unfinished.append({'run_id':r.get('run_id'),'status':r.get('status')})
 out[name]={'exists':p.exists(),'blocked':(p/'blocked.json').exists(),'unfinished':unfinished,'ready':json.loads((p/ready).read_text()) if (p/ready).exists() else None}
print(json.dumps(out))''')

    def inventory(self):
        """Batch independent reads; no resource writes occur during the default preview."""
        reads = {'airflow':self.airflow, 'kafka':self.kafka, 'lakekeeper':self.lakekeeper, 'bi':self.bi, 'runtime':self.runtime}
        with ThreadPoolExecutor(max_workers=3) as pool:
            jobs = {key:pool.submit(fn) for key,fn in reads.items()}
            plan = {key:job.result() for key,job in jobs.items()}
        plan['bi']['protected_dashboards'] = sorted(SOURCE_DASHBOARDS)
        validate_plan(plan)
        return plan

    def quiesce(self, wait_seconds):
        """Pause only target scheduling, gracefully stop the producer, and await current jobs."""
        self.wave.air('names='+repr(DAGS+OPTIONAL_DAGS)+'''
out=[]
for name in names+['oil_source_snapshot']:
 try:x=call('/api/v2/dags/'+name,'PATCH',{'is_paused':True})
 except urllib.error.HTTPError as e:
  if e.code==404:continue
  raise
 out.append(name)
print(json.dumps(out))''')
        stopped = self.services.kafka('''import subprocess,time,hashlib
from pathlib import Path
result=subprocess.run(['sudo','-n','-u','kafka','pkill','-INT','-f','^python3 /opt/kafka-demo/oil-generator/live_oil_updates.py'],capture_output=True)
if result.returncode not in [0,1]:raise RuntimeError('Cannot stop producer')
for _ in range(70):
 if subprocess.run(['pgrep','-f','^python3 /opt/kafka-demo/oil-generator/live_oil_updates.py'],capture_output=True).returncode==1:break
 time.sleep(0.5)
else:raise RuntimeError('Producer has not stopped')
p=Path('/tmp/oil-live-production.json')
print(json.dumps({'state_exists':p.exists(),'state_sha256':hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None}))''')
        deadline = time.monotonic()+wait_seconds
        while True:
            dags = self.airflow()
            active = {name:dags[name]['active'] for name in DAGS+OPTIONAL_DAGS if name in dags and dags[name]['active']}
            if not active:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError('Target writer still active; no target objects deleted. Resume this journal later.')
            print('Ожидаю завершения целевого DAG: '+json.dumps(active), flush=True)
            time.sleep(min(15, max(0, deadline-time.monotonic())))
        runtime = self.runtime()
        if any(r['blocked'] or r['unfinished'] for r in runtime.values()):
            raise RuntimeError('Unresolved writer receipt; reconcile its outcome before reset')
        return {'producer':stopped, 'runtime':runtime}

    def guard_idle(self):
        """Reject a new writer started after the reset paused scheduling."""
        states=self.airflow()
        if any(not states[n]['paused'] or states[n]['active'] for n in DAGS+OPTIONAL_DAGS if n in states):
            raise RuntimeError('Target writer resumed during cleanup')

    def transport(self, plan):
        """Stop/delete only this connector, its groups and control topic; preserve source offsets."""
        expected = plan['kafka']['target_config']
        return self.services.kafka('expected='+repr(expected)+'''
import subprocess,time
name='manual-oil-production-a';removed=[]
if name in call(18085,'/connectors'):
 config=call(18085,'/connectors/'+name+'/config')
 if expected is None or any(config.get(k)!=v for k,v in expected.items()):raise RuntimeError('Connector changed since inventory')
 call(18085,'/connectors/'+name+'/stop','PUT')
 for _ in range(40):
  status=call(18085,'/connectors/'+name+'/status')
  if status['connector']['state']=='STOPPED' and not status['tasks']:break
  time.sleep(0.5)
 else:raise RuntimeError('Target connector did not stop')
 call(18085,'/connectors/'+name,'DELETE')
def cli(binary,args):
 command=['sudo','-n','-u','kafka','env','JAVA_HOME=/opt/kafka-demo/jdk','/opt/kafka-demo/kafka_2.13-3.9.1/bin/'+binary,'--bootstrap-server','10.77.25.20:9093','--command-config','/etc/kafka-demo/client.properties']+args
 r=subprocess.run(command,capture_output=True,text=True,timeout=40)
 if r.returncode:raise RuntimeError('Kafka operation rejected: '+r.stdout[-700:]+r.stderr[-700:])
 return r.stdout
for group in cli('kafka-consumer-groups.sh',['--list']).splitlines():
 if group in ['connect-'+name,'connect-'+name+'-coord',name+'-control'] or group.startswith(name+'-control-'):
  cli('kafka-consumer-groups.sh',['--delete','--group',group]);removed.append(group)
cli('kafka-topics.sh',['--delete','--if-exists','--topic',name+'-control'])
print(json.dumps({'target_connector_absent':name not in call(18085,'/connectors'),'removed_groups':removed,'control_topic_delete_requested':True}))''')

    def delete_bi(self, plan):
        """Revalidate bindings/UUIDs before each deletion; a 404 permits safe continuation."""
        return self.ss('expected='+repr(plan['bi'])+'''
import urllib.error
removed=[]
for kind,key in [('dashboard','dashboards'),('chart','charts'),('dataset','datasets')]:
 for item in expected[key]:
  path='/api/v1/'+kind+'/'+str(item['id'])
  r=client.get(path)
  if r.status_code==404:continue
  if r.status_code!=200:raise RuntimeError('BI lookup rejected')
  actual=r.get_json()['result']
  if actual.get('uuid')!=item.get('uuid'):raise RuntimeError('BI identity changed')
  if kind=='chart' and (actual['datasource_id']!=item['datasource_id'] or actual['datasource_type']!=item['datasource_type']):raise RuntimeError('Chart binding changed')
  if kind=='dataset' and (actual.get('database',{}).get('id')!=item.get('database',{}).get('id') or any(actual.get(k)!=item.get(k) for k in ['catalog','schema','table_name','sql'])):raise RuntimeError('Dataset binding changed')
  if kind=='dashboard':
   current=call(path+'/charts')['result']
   if {c['id'] for c in current}-{c['id'] for c in expected['charts']}:raise RuntimeError('Dashboard gained unrelated charts')
  call(path,'DELETE');removed.append([kind,item['id']])
print('RESULT:'+json.dumps({'removed':removed}))''')

    def delete_catalog(self, plan):
        """Delete known target FQNs; recursive schema deletion removes only their own lineage."""
        selected={'dashboards':['webinar_superset.dashboard_'+str(x['id']) for x in plan['bi']['dashboards']],
                  'charts':['webinar_superset.chart_'+str(x['id']) for x in plan['bi']['charts']],
                  'pipelines':['webinar_airflow.'+name for name in DAGS]+['webinar_connect.'+CONNECTOR],
                  'databaseSchemas':['dlh_trino.dlh.'+ns for ns in TABLES]}
        return self.wave.catalog('selected='+repr(selected)+'''
removed=[]
for kind,names in selected.items():
 for name in names:
  try:item=call(kind+'/name/'+urllib.parse.quote(name,safe=''))
  except urllib.error.HTTPError as e:
   if e.code==404:continue
   raise
  if item['fullyQualifiedName']!=name:raise RuntimeError('Catalog identity mismatch')
  call(kind+'/'+item['id']+'?recursive=true&hardDelete=true','DELETE');removed.append(name)
print(json.dumps({'removed':removed}))''')

    def delete_tables(self, plan):
        """Request native Iceberg purge by recorded UUIDs; never delete arbitrary S3 prefixes."""
        return self.services.lk('expected='+repr(plan['lakekeeper'])+'''
removed=[]
for ns,recorded in expected.items():
 current=inventory(ns)
 if current.get('absent'):continue
 if recorded.get('absent') or current['id']!=recorded['id']:raise RuntimeError('Namespace recreated during reset')
 known={t['name']:t['uuid'] for t in recorded['tables']}
 if any(known.get(t['name'])!=t['uuid'] for t in current['tables']):raise RuntimeError('Foreign/recreated table; stop reset')
 for table in current['tables']:
  status,_=req(root+ns+'/tables/'+table['name']+'?purgeRequested=true','DELETE')
  if status not in [200,204,404]:raise RuntimeError('Native table purge rejected')
  removed.append(ns+'.'+table['name'])
 status,_=req(root+ns,'DELETE')
 if status not in [200,204,404]:raise RuntimeError('Namespace deletion rejected')
print(json.dumps({'removed':removed,'physical_s3_purge':'requested_asynchronously'}))''')

    def archive_runtime(self, journal_id):
        """Move active receipts to audit history; keep Airflow task logs and old DAG runs."""
        return self.wave.worker('journal='+repr(journal_id)+'''
import json,shutil,fcntl
from pathlib import Path
logs=Path('/opt/airflow/logs');destination=logs/'rehearsals'/('reset_demo_'+journal);destination.mkdir(parents=True,exist_ok=True)
out=[]
for name in ['oil_layered_migration','oil_dlh_release']:
 source=logs/name;target=destination/name
 if not source.exists():continue
 if target.exists():raise RuntimeError('New runtime appeared after archival')
 with (source/'.writer.lock').open('a+') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  shutil.move(str(source),str(target));out.append(name)
print(json.dumps({'archived':out,'path':str(destination)}))''')

    def verify(self, plan=None):
        """Read all final services; require an empty scoped target and a healthy source."""
        observed=self.inventory()
        source=observed['kafka']['source']
        if source['connector']['state']!='RUNNING' or not source['tasks'] or any(t['state']!='RUNNING' for t in source['tasks']):
            raise RuntimeError('Source Kafka sink is unhealthy')
        if observed['kafka']['target_config'] or observed['kafka']['producer_running']:
            raise RuntimeError('Target connector or producer is still active')
        if any(not entry.get('absent') for entry in observed['lakekeeper'].values()):
            raise RuntimeError('Target namespaces remain')
        if any(observed['bi'][key] for key in ['datasets','charts','dashboards']):
            raise RuntimeError('Target BI remains')
        if observed['airflow']['oil_gp_release']['paused'] or any(d['import_errors'] for d in observed['airflow'].values()):
            raise RuntimeError('DAG status/import error')
        self.guard_idle()
        if any(entry['exists'] for entry in observed['runtime'].values()):
            raise RuntimeError('Active runtime receipts remain')
        schemas=self.services.sql('SHOW SCHEMAS FROM dlh')
        if any(next(iter(row.values())) in TABLES for row in schemas):
            raise RuntimeError('Cedrus still lists target schemas; wait for metadata cache and retry --check')
        bi=plan['bi'] if plan else {'dashboards':[], 'charts':[]}
        selected={'databaseSchemas':['dlh_trino.dlh.'+ns for ns in TABLES],
                  'pipelines':['webinar_airflow.'+name for name in DAGS]+['webinar_connect.'+CONNECTOR],
                  'dashboards':['webinar_superset.dashboard_'+str(x['id']) for x in bi['dashboards']],
                  'charts':['webinar_superset.chart_'+str(x['id']) for x in bi['charts']]}
        catalog=self.wave.catalog('selected='+repr(selected)+'''
for kind,names in selected.items():
 for name in names:
  try:call(kind+'/name/'+urllib.parse.quote(name,safe=''))
  except urllib.error.HTTPError as e:
   if e.code==404:continue
   raise
  raise RuntimeError('Target Catalog entity remains: '+name)
source=call('dashboards/name/webinar_superset.dashboard_3')
print(json.dumps({'target_entities_absent':True,'source_report_preserved':source['fullyQualifiedName']}))''')
        baseline=self.services.sql('SELECT count(*) n,cast(sum(oil_tonnes) AS varchar) oil_tonnes FROM greenplum_gp.oil_mart.fact_well_day')
        return {'status':'READY_FOR_STEP_1','observed_at_utc':datetime.now(timezone.utc).isoformat(),
                'source':baseline,'catalog':catalog,'airflow':observed['airflow'],'schemas':schemas,
                'producer_stopped':True,'source_data_and_offsets_not_reset':True,'s3_purge':'asynchronous'}


class Reset:
    """Execute resumable phases against one immutable ownership inventory."""
    def __init__(self, backend, folder, plan=None):
        self.backend,self.folder=backend,Path(folder)
        self.path=self.folder/'journal.json'
        if self.path.exists():
            self.journal=json.loads(self.path.read_text())
            if self.journal.get('format')!=1:raise ValueError('Unknown reset journal')
        else:
            if plan is None:raise ValueError('A new reset needs a recorded inventory')
            self.folder.mkdir(mode=0o700,parents=True,exist_ok=False)
            self.journal={'format':1,'id':self.folder.name,'plan':plan,'done':{},'status':'PREPARED'}
            write_json(self.path,self.journal)
        validate_plan(self.journal['plan'])

    def phase(self, name, operation):
        """Repeat an interrupted idempotent phase; never silently skip an unknown command outcome."""
        if name in self.journal['done']:return
        print('Сброс: '+name,flush=True)
        self.journal.update(status='RUNNING',current_phase=name)
        write_json(self.path,self.journal)
        try:result=operation()
        except BaseException as error:
            self.journal.update(status='INTERRUPTED',error_type=type(error).__name__)
            write_json(self.path,self.journal)
            raise
        self.journal['done'][name]=result
        write_json(self.path,self.journal)

    def local_archive(self):
        """Archive only the documented Desktop receipts, without deleting the user's other files."""
        destination=self.folder/'desktop-passports';destination.mkdir(exist_ok=True)
        moved=[]
        for name in LOCAL_FILES:
            source=Path.home()/'Desktop'/name
            if source.is_file():
                if (destination/name).exists():raise RuntimeError('New local passport appeared after archival')
                shutil.move(str(source),str(destination/name));moved.append(name)
        return {'archived':moved}

    def run(self, wait_seconds=1800):
        """Quiesce every attempt, then remove transport/BI/catalog/tables and accept final checks."""
        if self.journal['status']=='COMPLETE':
            return self.backend.verify(self.journal['plan'])
        # Re-check quiescence even on resume: a user may have restarted a DAG meanwhile.
        self.journal.update(status='QUIESCING',current_phase='quiescence')
        write_json(self.path,self.journal)
        try:result=self.backend.quiesce(wait_seconds)
        except BaseException as error:
            self.journal.update(status='INTERRUPTED',error_type=type(error).__name__)
            write_json(self.path,self.journal)
            raise
        self.journal['last_quiescence']=result
        write_json(self.path,self.journal)
        plan=self.journal['plan']
        for name,operation in [('kafka',lambda:self.backend.transport(plan)),
                               ('superset',lambda:self.backend.delete_bi(plan)),
                               ('catalog',lambda:self.backend.delete_catalog(plan)),
                               ('tables',lambda:self.backend.delete_tables(plan)),
                               ('runtime',lambda:self.backend.archive_runtime(self.journal['id'])),
                               ('local',self.local_archive)]:
            self.backend.guard_idle()
            self.phase(name,operation)
        report=self.backend.verify(plan)
        self.journal.update(status='COMPLETE',report=report)
        write_json(self.path,self.journal)
        return report


@contextmanager
def reset_lock(root):
    """Exclude two concurrent reset commands; this does not replace Airflow/worker writer checks."""
    root.mkdir(mode=0o700,parents=True,exist_ok=True)
    with (root/'.reset.lock').open('a+') as handle:
        try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('Another reset command is running')
        try:yield
        finally:fcntl.flock(handle,fcntl.LOCK_UN)


def summary(plan):
    """Show resource names and new BI IDs without printing private payloads or credentials."""
    return {'mode':'DRY_RUN','tables':{ns:[t['name'] for t in entry.get('tables',[])] for ns,entry in plan['lakekeeper'].items()},
            'superset':{key:[x['id'] for x in plan['bi'][key]] for key in ['datasets','charts','dashboards']},
            'connector':CONNECTOR if plan['kafka']['target_config'] else None,'target_dags':DAGS,
            'producer_running':plan['kafka']['producer_running'],
            'preserved':['GP data/functions/reports','oil.events.v1 and source offsets','CIAM roles and warehouse','service credentials/connections','DAG code/pool and Airflow run history','producer state file']}


def main(argv=None):
    """Preview by default; --apply performs reset, --resume continues one saved journal, --check only reads."""
    parser=argparse.ArgumentParser(description=__doc__)
    mode=parser.add_mutually_exclusive_group()
    mode.add_argument('--dry-run',action='store_true',help='Read and print cleanup scope; default')
    mode.add_argument('--apply',action='store_true',help='Reset this first-chain target')
    mode.add_argument('--resume',type=Path,metavar='JOURNAL_FOLDER',help='Continue the saved reset, using its original IDs/UUIDs')
    mode.add_argument('--check',action='store_true',help='Only verify step-1 state')
    parser.add_argument('--wait-seconds',type=int,default=1800,help='Maximum wait for active DAGs; default 1800')
    args=parser.parse_args(argv)
    if args.wait_seconds<=0:parser.error('--wait-seconds must be positive')
    backend=Backend()
    if args.check:
        latest=BRIDGE/'var/demo-resets/latest.json'
        plan=None
        if latest.exists():
            path=Path(json.loads(latest.read_text())['folder'])/'journal.json'
            if path.is_file():plan=json.loads(path.read_text())['plan']
        print(json.dumps(backend.verify(plan),ensure_ascii=False,indent=2));return
    if not args.apply and args.resume is None:
        print(json.dumps(summary(backend.inventory()),ensure_ascii=False,indent=2));return
    root=BRIDGE/'var/demo-resets'
    with reset_lock(root):
        if args.resume:
            if not (args.resume/'journal.json').is_file():raise ValueError('Reset journal not found')
            run=Reset(backend,args.resume.resolve())
        else:
            plan=backend.inventory()
            stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            run=Reset(backend,root/(stamp+'_'+uuid.uuid4().hex[:8]),plan)
        print('Журнал: '+str(run.folder),flush=True)
        write_json(root/'latest.json',{'folder':str(run.folder)})
        try:report=run.run(args.wait_seconds)
        except BaseException:
            print('Сброс не завершён. Для продолжения: --resume '+str(run.folder),file=sys.stderr)
            raise
        print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__=='__main__':
    try:main()
    except (Exception,KeyboardInterrupt) as error:
        print('Ошибка: '+type(error).__name__+': '+str(error),file=sys.stderr)
        sys.exit(1)
