# Read-only verifier подтверждает table UUID, snapshot и effective SELECT CIAM-backed reader. Для production проверяет agg_field_day, для performance также field_performance; запрещает расширенные права на родительских объектах. Развёрнут sid.
# Права не копируются SQL GRANT из GP; новый UUID выпуска требует нового назначения и проверки.
"""Read-only serving grant verifier. No credentials or arbitrary URLs in output."""
import json
import hmac
import ssl
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WAREHOUSE = 'be4599ce-dbba-11f1-a520-7382628b06be'
ROLE = '01a1fffd-bdc0-7c31-8738-c6d32c823dc7'
BASE = 'http://lakekeeper-service:8181'
SCHEMA = re.compile(r'oil_r_[a-z0-9_]{1,42}\Z')
UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')
NAMES = ('agg_field_day', 'field_performance')
MAX_BODY = 1024 * 1024
NAMESPACE_ALLOWED = {'get_metadata'}
WAREHOUSE_ALLOWED = {'get_config', 'get_metadata', 'list_namespaces', 'include_in_list', 'list_deleted_tabulars', 'get_all_tasks', 'get_endpoint_statistics'}
TABLE_ALLOWED = {'read_data', 'get_metadata', 'get_tasks'}
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def validate(value):
    """Проверяет входные идентификаторы и ограничения до обращения к данным."""
    if not isinstance(value, dict) or value.get('status') != 'ready':
        raise ValueError('ready evidence required')
    schema = value.get('schema')
    snapshots = value.get('snapshots')
    if not isinstance(schema, str) or not SCHEMA.fullmatch(schema) or not isinstance(snapshots, dict):
        raise ValueError('invalid schema or snapshots')
    names = ('agg_field_day',) if value.get('chain') == 'production' else NAMES
    if any(type(snapshots.get(name)) is not int or snapshots[name] <= 0 for name in names):
        raise ValueError('positive snapshot ids required')
    return schema, {name: snapshots[name] for name in names}


def read_json(url, *, token=None, body=None):
    """Читает и разбирает JSON с проверкой формата входного документа."""
    headers = {'Accept': 'application/json'}
    if token is not None:
        headers['Authorization'] = 'Bearer ' + token
    if body is not None:
        headers['Content-Type'] = 'application/x-www-form-urlencoded'
    request = urllib.request.Request(url, data=body, headers=headers, method='POST' if body is not None else 'GET')
    with OPENER.open(request, timeout=15) as response:
        raw = response.read(MAX_BODY + 1)
        if len(raw) > MAX_BODY:
            raise RuntimeError('response too large')
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise RuntimeError('unexpected response')
        return value


def verify(value):
    """Проверяет фактические права, идентичность ресурса и принятые критерии доступа."""
    schema, expected = validate(value)
    # This private call is exclusively to the existing loopback token proxy.
    token = read_json('http://127.0.0.1:8080/token', body=b'grant_type=client_credentials')['access_token']
    if not isinstance(token, str) or not token:
        raise RuntimeError('token unavailable')
    def get(path):
        """Читает выбранный ресурс через авторизованный сервисный API."""
        return read_json(BASE + path, token=token)
    def uuid(value):
        """Проверяет канонический UUID; произвольный путь или SQL вместо ID отклоняется."""
        if not isinstance(value, str) or not UUID.fullmatch(value):
            raise RuntimeError('invalid object id')
        return value
    def assignments(path):
        """Читает назначения ролей и пользователей на конкретный объект LakeKeeper."""
        out = get(path + '/assignments').get('assignments')
        if not isinstance(out, list):
            raise RuntimeError('missing assignments')
        return [a.get('type') for a in out if isinstance(a, dict) and a.get('role') == ROLE]
    def actions(path, allowed):
        """Проверяет эффективные действия identity на выбранном объекте."""
        out = get(path + '/access?principalRole=' + urllib.parse.quote(ROLE, safe='')).get('allowed-actions')
        if not isinstance(out, list) or not all(isinstance(x, str) for x in out) or not set(out).issubset(allowed):
            raise RuntimeError('unexpected effective permissions')
        return set(out)
    ns = get('/catalog/v1/' + WAREHOUSE + '/namespaces/' + schema)
    ns_id = uuid(ns.get('properties', {}).get('namespace_id'))
    ns_path = '/management/v1/permissions/namespace/' + ns_id
    wh_path = '/management/v1/permissions/warehouse/' + WAREHOUSE
    # Explicit select/modify/create/ownership grants at parent scope would grant
    # Access beyond serving tables; only optional describe is permitted.
    if not set(assignments(ns_path)).issubset({'describe'}) or not set(assignments(wh_path)).issubset({'describe'}):
        raise RuntimeError('unexpected parent assignment')
    actions(ns_path, NAMESPACE_ALLOWED)
    actions(wh_path, WAREHOUSE_ALLOWED)
    tables = []
    for name in expected:
        table = get('/catalog/v1/' + WAREHOUSE + '/namespaces/' + schema + '/tables/' + name)
        metadata = table.get('metadata', {})
        table_id = uuid(metadata.get('table-uuid'))
        if type(metadata.get('current-snapshot-id')) is not int or metadata['current-snapshot-id'] != expected[name]:
            raise RuntimeError('snapshot differs')
        table_path = wh_path + '/table/' + table_id
        grants = assignments(table_path)
        if 'select' not in grants or not set(grants).issubset({'select', 'describe'}):
            raise RuntimeError('missing or excessive table grant')
        if 'read_data' not in actions(table_path, TABLE_ALLOWED):
            raise RuntimeError('table read is not effective')
        tables.append({'table': name, 'uuid': table_id, 'role': ROLE, 'relation': 'select',
                       'already_existed': True, 'snapshot_id': expected[name]})
    return {'schema': schema, 'role': ROLE, 'serving_select_verified': True, 'tables': tables}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        """Пишет HTTP access log без возвращения секретных заголовков."""
        pass  # Do not log input, response, authorization headers or tokens.
    def reply(self, status, value):
        """Возвращает структурированный HTTP ответ с заданным статусом."""
        payload = json.dumps(value, separators=(',', ':')).encode()
        self.send_response(status); self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload))); self.end_headers()
        self.wfile.write(payload)
    def do_GET(self):
        """Обрабатывает разрешённый HTTP GET и возвращает наблюдаемое состояние сервиса."""
        if self.path == '/healthz': self.reply(200, {'status': 'ok'})
        else: self.reply(404, {'error': 'not found'})
    def do_POST(self):
        """Проверяет тело HTTP POST и выполняет только предусмотренную сервисом операцию."""
        if self.path != '/verify': return self.reply(404, {'error': 'not found'})
        try:
            expected = 'Bearer ' + Path('/opt/oil-verifier-auth/token').read_text().strip()
            supplied = self.headers.get('Authorization', '')
            if not expected[7:] or not hmac.compare_digest(supplied.encode(), expected.encode()):
                return self.reply(401, {'error': 'unauthorized'})
        except (OSError, ValueError):
            return self.reply(401, {'error': 'unauthorized'})
        if self.headers.get('Transfer-Encoding') is not None: return self.reply(400, {'error': 'invalid request'})
        try:
            n = int(self.headers.get('Content-Length', ''))
            if n < 1 or n > MAX_BODY: raise ValueError()
            self.connection.settimeout(15)
            payload = self.rfile.read(n)
            if len(payload) != n: raise ValueError()
            value = json.loads(payload)
            validate(value)
        except (ValueError, TypeError, UnicodeError, OSError):
            return self.reply(400, {'error': 'invalid request'})
        try:
            return self.reply(200, verify(value))
        except Exception:
            return self.reply(403, {'error': 'verification failed'})


if __name__ == '__main__':
    server = ThreadingHTTPServer(('0.0.0.0', 8091), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain('/opt/oil-verifier-tls/tls.crt', '/opt/oil-verifier-tls/tls.key')
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()
