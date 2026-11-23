"""Use the existing authenticated Cedrus MCP without publishing service secrets."""
import json, ssl, urllib.request, urllib.parse
from pathlib import Path

class MCP:
    def __init__(self, url, ca, bearer_file):
        """Сохраняет зависимости и настройки экземпляра; секреты остаются в штатных сервисах."""
        p = urllib.parse.urlparse(url)
        if p.scheme != "https" and not (
            p.scheme == "http" and p.hostname in ("127.0.0.1", "localhost", "::1")
        ):
            raise ValueError("MCP requires HTTPS or loopback")
        self.url = url
        self.session = None
        self.seq = 0
        self.ca = ca
        self.bearer_file = bearer_file
        self.rpc(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "oil-bounded-release", "version": "1"},
            },
            True,
        )
        self.rpc("notifications/initialized", {}, False)

    def rpc(self, method, params, answer=True):
        """Отправляет JSON-RPC запрос с текущей сессией и проверяет протокол ответа."""
        self.seq += 1
        body = {"jsonrpc": "2.0", "method": method, "params": params}
        if answer:
            body["id"] = self.seq
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        if self.bearer_file:
            headers["Authorization"] = (
                "Bearer " + Path(self.bearer_file).read_text().strip()
            )
        req = urllib.request.Request(
            self.url, data=json.dumps(body).encode(), headers=headers
        )
        with urllib.request.urlopen(
            req, timeout=180, context=ssl.create_default_context(cafile=self.ca)
        ) as r:
            self.session = r.headers.get("Mcp-Session-Id", self.session)
            if not answer:
                return None
            if "text/event-stream" in r.headers.get("Content-Type", ""):
                result = None
                for line in r:
                    if line.startswith(b"data:"):
                        x = json.loads(line[5:])
                        if x.get("id") == self.seq:
                            result = x
                            break
                if result is None:
                    raise RuntimeError("MCP response missing")
            else:
                result = json.load(r)
        if result.get("error") or result.get("result", {}).get("isError"):
            raise RuntimeError("MCP tool error")
        return result.get("result", {})

    def sql(self, query):
        """Выполняет SQL через зарегистрированный движок и возвращает фактические строки результата."""
        r = self.rpc(
            "tools/call", {"name": "execute_query", "arguments": {"query": query}}
        )
        value = r.get("structuredContent", {}).get("result")
        if value is None:
            raise RuntimeError("MCP result missing")
        try:
            return json.loads(value) if isinstance(value, str) else value
        except json.JSONDecodeError:
            return []
