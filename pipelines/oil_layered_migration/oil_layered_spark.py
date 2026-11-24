# Подключает Spark через отдельный Jupyter kernel и сохраняет durable результаты команд.
# Агент предлагает план/SQL; разрешённый исполнитель применяет принятые операции. Source не удаляется автоматически.
"""Airflow-side Spark Connect adapter using an isolated Jupyter kernel per run."""
import json
import os
from pathlib import Path
import re
import urllib.request


def json_payload(document):
    """Return the exact JSON value span, preserving GP json text hash inputs."""
    if document is None:
        return None
    import json
    def pairs(items):
        """Собирает JSON object и отклоняет дублирующиеся ключи вместо неоднозначного выбора значения."""
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    def constant(value):
        """Отклоняет NaN/Infinity и другие значения вне стандарта JSON."""
        raise ValueError('Non-JSON numeric constant')
    decoder = json.JSONDecoder(object_pairs_hook=pairs, parse_constant=constant)
    parsed = decoder.decode(document)
    if not isinstance(parsed, dict) or not isinstance(parsed.get('payload'), dict):
        raise ValueError('Object envelope and payload required')
    pos = 0
    while document[pos].isspace():pos += 1
    pos += 1
    while True:
        while document[pos].isspace():pos += 1
        key, pos = decoder.raw_decode(document, pos)
        while document[pos].isspace():pos += 1
        if document[pos] != ':':raise ValueError('JSON colon required')
        pos += 1
        while document[pos].isspace():pos += 1
        start = pos
        _, pos = decoder.raw_decode(document, pos)
        if key == 'payload':return document[start:pos]
        while document[pos].isspace():pos += 1
        if document[pos] != ',':raise ValueError('Payload not found')
        pos += 1


class SparkClient:
    def __init__(self,name):
        """Сохраняет зависимости и настройки экземпляра; секреты остаются в штатных сервисах."""
        if not re.fullmatch(r'bridge_[a-f0-9_]{10,90}',name):raise ValueError('Invalid owned notebook')
        self.name=name;self.session=None;self.seq=0;self.kernel=None
        self.url=os.environ['BRIDGE_JUPYTER_MCP_URL']
        if self.url!='http://bridge-jupyter-mcp.jupytermcpcluster-czs7ssnx.svc.cluster.local:8000/mcp':raise ValueError('Unregistered Jupyter endpoint')
        self.token=Path(os.environ['BRIDGE_JUPYTER_MCP_TOKEN_FILE']).read_text().strip()
        self.rpc('initialize',{'protocolVersion':'2024-11-05','capabilities':{},'clientInfo':{'name':'platform-bridge-spark','version':'1'}})
        self.rpc('notifications/initialized',{},False)

    def rpc(self,method,params,answer=True,timeout=1300):
        # The registered MCP deployment serves its localhost virtual host.
        # Network destination remains the allowlisted dedicated Kubernetes service.
        """Отправляет JSON-RPC запрос с текущей сессией и проверяет протокол ответа."""
        self.seq+=1;h={'Content-Type':'application/json','Accept':'application/json, text/event-stream','Authorization':'Bearer '+self.token,'Host':'localhost:8000'}
        if self.session:h['Mcp-Session-Id']=self.session
        body={'jsonrpc':'2.0','method':method,'params':params}
        if answer:body['id']=self.seq
        req=urllib.request.Request(self.url,data=json.dumps(body).encode(),headers=h)
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req,timeout=timeout) as r:
            self.session=r.headers.get('Mcp-Session-Id',self.session)
            if not answer:return None
            if 'text/event-stream' in r.headers.get('Content-Type',''):
                result=None
                for line in r:
                    if line.startswith(b'data:'):
                        row=json.loads(line[5:])
                        if row.get('id')==self.seq:result=row;break
                if result is None:raise RuntimeError('Jupyter outcome unknown')
            else:result=json.load(r)
        if result.get('error') or result.get('result',{}).get('isError'):raise RuntimeError('Jupyter rejected tool')
        return result['result']

    def tool(self,name,args):
        """Вызывает конкретный инструмент сервиса и проверяет структурированный результат."""
        result=self.rpc('tools/call',{'name':name,'arguments':args})
        value=result.get('structuredContent',{}).get('result')
        if value is None:
            value='\n'.join(x.get('text','') for x in result.get('content',[]) if x.get('type')=='text')
        if isinstance(value,str):
            try:return json.loads(value)
            except ValueError:return value
        return value

    def open(self):
        """Создаёт отдельную сессию исполнения с проверенными настройками и авторизацией."""
        result=self.tool('use_notebook',{'notebook_name':self.name,'notebook_path':self.name+'.ipynb','mode':'create'})
        text=json.dumps(result)
        match=re.search(r"kernel(?:[_ ]id)?[\s\"':]+([a-f0-9-]{36})",text,re.I)
        if not match:raise RuntimeError('No explicit kernel ID; shared notebook execution is forbidden')
        self.kernel=match[1]
        self.execute("from dp103_spark import connect_dlh\nspark=connect_dlh()\nspark.conf.set('spark.sql.session.timeZone','UTC')\nspark.addArtifact('/tmp/bridge-json-payload-v1.jar')\nspark.udf.registerJavaFunction('bridge_json_payload','bridge.runtime.JsonPayload','string')\nprint('PB_RESULT:'+__import__('json').dumps({'spark_version':spark.version}))")

    def execute(self,code):
        """Execute once and recover a lost MCP reply from a private durable receipt.

        The SQL is never resubmitted after an ambiguous acknowledgement. A read
        of this command's exact request ID/hash can establish committed success
        even when progress output displaced stdout in the MCP response.
        """
        import hashlib
        import uuid
        import time
        token=uuid.uuid4().hex
        fingerprint=hashlib.sha256(code.encode()).hexdigest()
        receipt='/home/admin/.platform_bridge_results/'+self.name+'/'+token+'.json'
        # Only the PB_RESULT JSON enters the durable record: no arbitrary stdout,
        # Connection configuration, credentials or agent code is archived here.
        wrapped="""import contextlib as _pb_ctx,io as _pb_io,json as _pb_json,os as _pb_os
from pathlib import Path as _pb_Path
_pb_file=_pb_Path(RECEIPT)
_pb_file.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
_pb_stdout=_pb_io.StringIO()
try:
    with _pb_ctx.redirect_stdout(_pb_stdout):
        exec(CODE,globals(),globals())
    _pb_text=_pb_stdout.getvalue()
    _pb_at=_pb_text.rfind('PB_RESULT:')
    if _pb_at<0:raise RuntimeError('Command did not produce a structured result')
    _pb_value=_pb_json.JSONDecoder().raw_decode(_pb_text[_pb_at+10:])[0]
    _pb_receipt={'request_id':TOKEN,'code_hash':FINGERPRINT,'status':'SUCCEEDED','result':_pb_value}
except Exception as _pb_error:
    _pb_receipt={'request_id':TOKEN,'code_hash':FINGERPRINT,'status':'FAILED','error_type':type(_pb_error).__name__}
_pb_tmp=_pb_file.with_suffix('.tmp')
_pb_tmp.write_text(_pb_json.dumps(_pb_receipt,default=str));_pb_tmp.chmod(0o600)
_pb_os.replace(_pb_tmp,_pb_file)
print('PB_COMMAND_RESULT:'+_pb_json.dumps(_pb_receipt,default=str),flush=True)
"""
        for key,value in [('RECEIPT',repr(receipt)),('TOKEN',repr(token)),('FINGERPRINT',repr(fingerprint)),('CODE',repr(code))]:
            wrapped=wrapped.replace(key,value)
        def result_from(value):
            """Accept only an exact matching execution receipt, never another cell's output."""
            def texts(item):
                """Yield result text blocks without treating any embedded source as executable code."""
                if isinstance(item,str):yield item
                elif isinstance(item,list):
                    for child in item:yield from texts(child)
                elif isinstance(item,dict):
                    for child in item.values():yield from texts(child)
            for text in texts(value):
                for match in re.finditer('PB_COMMAND_RESULT:',text):
                    try:document=json.JSONDecoder().raw_decode(text[match.end():].lstrip())[0]
                    except (ValueError,TypeError):continue
                    if isinstance(document,dict) and document.get('request_id')==token and document.get('code_hash')==fingerprint:return document
            return None
        try:
            response=self.tool('execute_code',{'code':wrapped,'timeout':1200,'kernel_id':self.kernel})
            document=result_from(response)
        except (RuntimeError,OSError,ValueError):
            document=None
        for _ in range(3):
            if document and document.get('status') in ('SUCCEEDED','FAILED'):break
            reader="from pathlib import Path\nimport json\np=Path("+repr(receipt)+")\nprint('PB_COMMAND_RESULT:'+(p.read_text() if p.exists() else '{}'),flush=True)"
            try:document=result_from(self.tool('execute_code',{'code':reader,'timeout':30,'kernel_id':self.kernel}))
            except (RuntimeError,OSError,ValueError):document=None
            if not document:time.sleep(1)
        if not document or document.get('status') not in ('SUCCEEDED','FAILED'):
            raise RuntimeError('Spark command outcome unknown; reconcile receipt '+token+' before retry')
        if document['status']=='FAILED':
            raise RuntimeError('Spark command failed: '+document.get('error_type','unknown')+'; receipt '+token)
        return document['result']

    def sql(self,query):
        """Dispatch SQL once in a background thread, then poll its durable receipt.

        Long Spark MERGE commands outlive the MCP HTTP response deadline. The
        SQL runs outside the kernel's foreground execution slot, so polling is
        responsive and never interrupts/re-submits an in-flight table commit.
        """
        import hashlib,time,uuid
        request_id=uuid.uuid4().hex
        digest=hashlib.sha256(query.encode()).hexdigest()
        path='/home/admin/.platform_bridge_results/'+self.name+'/'+request_id+'.json'
        code="""import threading as _oil_threading,json as _oil_json,os as _oil_os
from pathlib import Path as _oil_Path
def _oil_run_sql(_sql,_path,_id,_hash,_session):
    _file=_oil_Path(_path)
    try:
        _rows=[r.asDict(recursive=True) for r in _session.sql(_sql).collect()]
        _value={'request_id':_id,'code_hash':_hash,'status':'SUCCEEDED','result':{'rows':_rows}}
    except BaseException as _error:
        _value={'request_id':_id,'code_hash':_hash,'status':'FAILED','error_type':type(_error).__name__}
    _tmp=_file.with_suffix('.tmp')
    _tmp.write_text(_oil_json.dumps(_value,default=str));_tmp.chmod(0o600)
    _oil_os.replace(_tmp,_file)
_oil_file=_oil_Path(PATH)
_oil_file.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
if _oil_file.exists():raise FileExistsError('Never dispatch the same SQL request twice')
_oil_file.write_text(_oil_json.dumps({'request_id':REQUEST,'code_hash':HASH,'status':'RUNNING'}));_oil_file.chmod(0o600)
_oil_threading.Thread(target=_oil_run_sql,args=(QUERY,PATH,REQUEST,HASH,spark),daemon=True).start()
print('PB_RESULT:'+_oil_json.dumps({'request_id':REQUEST,'dispatched':True}))
"""
        # Replace only exact uppercase sentinels, before any external execution.
        for key,value in [('PATH',repr(path)),('REQUEST',repr(request_id)),('HASH',repr(digest)),('QUERY',repr(query))]:
            code=code.replace(key,value)
        try:self.execute(code)
        except (RuntimeError,OSError,ValueError):pass
        reader="import json;from pathlib import Path\np=Path("+repr(path)+")\nprint('PB_SQL_RESULT:'+(p.read_text() if p.exists() else '{}'),flush=True)"
        deadline=time.monotonic()+1200;last_log=0
        while time.monotonic()<deadline:
            try:response=self.tool('execute_code',{'code':reader,'timeout':20,'kernel_id':self.kernel})
            except (RuntimeError,OSError,ValueError):response=None
            texts=[]
            def collect(value):
                if isinstance(value,str):texts.append(value)
                elif isinstance(value,dict):
                    for child in value.values():collect(child)
                elif isinstance(value,list):
                    for child in value:collect(child)
            collect(response)
            for text in texts:
                for match in re.finditer('PB_SQL_RESULT:',text):
                    try:value=json.JSONDecoder().raw_decode(text[match.end():].lstrip())[0]
                    except (ValueError,TypeError):continue
                    if value.get('request_id')!=request_id or value.get('code_hash')!=digest:continue
                    if value.get('status')=='SUCCEEDED':return value['result']['rows']
                    if value.get('status')=='FAILED':raise RuntimeError('Spark SQL failed: '+value.get('error_type','unknown')+'; receipt '+request_id)
            if time.monotonic()-last_log>=15:
                print(json.dumps({'phase':'SPARK_SQL_RUNNING','request_id':request_id,'notebook':self.name}),flush=True);last_log=time.monotonic()
            time.sleep(2)
        raise RuntimeError('Spark SQL outcome unknown; inspect receipt '+request_id+' without resubmitting')

    def close(self):
        """Закрывает принадлежащую запуску сессию или ресурс; бизнес-данные не удаляет."""
        if self.kernel:
            try:
                self.execute("spark.sql('DROP TEMPORARY FUNCTION IF EXISTS bridge_json_payload')\nspark.stop()\nprint('PB_RESULT:{}')")
            finally:
                self.tool('unuse_notebook',{'notebook_name':self.name})
                self.kernel=None
