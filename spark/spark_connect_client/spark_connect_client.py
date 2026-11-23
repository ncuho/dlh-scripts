"""Airflow-side Spark Connect adapter using an isolated Jupyter kernel per run."""
import json
import os
from pathlib import Path
import re
import urllib.request


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
        self.execute("from dp103_spark import connect_dlh\nspark=connect_dlh()\nspark.conf.set('spark.sql.session.timeZone','UTC')\nprint('PB_RESULT:'+__import__('json').dumps({'spark_version':spark.version}))")

    def execute(self,code):
        """Исполняет выбранную операцию через зарегистрированный runtime; результат проверяется вызывающим кодом."""
        result=self.tool('execute_code',{'code':code,'timeout':1200,'kernel_id':self.kernel})
        def strings(x):
            """Извлекает текстовые ответы из структурированных content blocks сервиса."""
            if isinstance(x,str):yield x
            elif isinstance(x,list):
                for v in x:yield from strings(v)
            elif isinstance(x,dict):
                if x.get('output_type')=='error' or x.get('status')=='error':raise RuntimeError('Spark execution failed')
                for v in x.values():yield from strings(v)
        for text in strings(result):
            for line in text.splitlines():
                if line.startswith('PB_RESULT:'):return json.loads(line[len('PB_RESULT:'):])
        raise RuntimeError('Spark result marker missing; inspect execution before retry')

    def sql(self,query):
        """Выполняет SQL через зарегистрированный движок и возвращает фактические строки результата."""
        return self.execute('import json\n_result=spark.sql('+repr(query)+')\n_rows=[r.asDict(recursive=True) for r in _result.collect()]\nprint("PB_RESULT:"+json.dumps({"rows":_rows},default=str))')['rows']

    def close(self):
        """Закрывает принадлежащую запуску сессию или ресурс; бизнес-данные не удаляет."""
        if self.kernel:self.tool('unuse_notebook',{'notebook_name':self.name})
