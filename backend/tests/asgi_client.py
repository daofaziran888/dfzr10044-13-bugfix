"""最小 ASGI 测试客户端：不引入额外依赖，直接驱动 FastAPI 应用。

与同系列任务仓库保持一致，便于交接接口测试以 ASGI 直连方式运行。
"""
from __future__ import annotations

import asyncio
import json
from typing import Any


class AsgiResponse:
    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self.body = body

    def json(self) -> Any:
        return json.loads(self.body) if self.body else None


class AsgiClient:
    """用 asyncio.run 逐次调用 ASGI 应用，够测试用且零依赖。"""

    def __init__(self, app: Any) -> None:
        self.app = app

    def request(self, method: str, path: str, json_body: Any = None) -> AsgiResponse:
        return asyncio.run(self._request(method, path, json_body))

    async def _request(self, method: str, path: str, json_body: Any) -> AsgiResponse:
        raw_path, _, query = path.partition("?")
        body = b""
        headers = [(b"host", b"testserver")]
        if json_body is not None:
            body = json.dumps(json_body).encode()
            headers.append((b"content-type", b"application/json"))
            headers.append((b"content-length", str(len(body)).encode()))

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method.upper(),
            "scheme": "http",
            "path": raw_path,
            "raw_path": raw_path.encode(),
            "query_string": query.encode(),
            "root_path": "",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }

        status_code = 500
        chunks: list[bytes] = []

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message: dict[str, Any]) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            elif message["type"] == "http.response.body":
                chunks.append(message.get("body", b""))

        await self.app(scope, receive, send)
        return AsgiResponse(status_code, b"".join(chunks))
