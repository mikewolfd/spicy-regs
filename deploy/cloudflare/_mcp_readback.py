"""Bounded public MCP session; retained from the qualified local delivery client."""

import http.client
import json
import re
import time
from urllib.parse import urlsplit

PROTOCOL = "2025-11-25"
LIMITS = {
    "response_bytes": 1048576,
    "output_bytes": 16777216,
    "request_seconds": 60,
    "wall_seconds": 240,
    "cleanup_reserve_seconds": 5,
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


class Client:
    def __init__(self, endpoint, run, limits):
        url = urlsplit(endpoint)
        require(endpoint == "https://mcp.spicygov.ai/mcp", "Exact MCP endpoint required")
        self.host = url.hostname
        self.path = url.path
        self.run = run
        self.limits = limits
        self.started = time.monotonic()
        self.session = None
        self.protocol = None
        self.sequence = 0
        self.retained = 0
        self.records = []

    def save(self, name, raw):
        require(self.retained + len(raw) <= self.limits["output_bytes"] - 65536, "Complete MCP output cap exceeded")
        with (self.run / name).open("xb") as out:
            out.write(raw)
        self.retained += len(raw)

    def remaining(self, cleanup=False):
        return (
            self.limits["wall_seconds"]
            - (0 if cleanup else self.limits["cleanup_reserve_seconds"])
            - (time.monotonic() - self.started)
        )

    def exchange(self, message=None, cleanup=False):
        timeout = min(5 if cleanup else self.limits["request_seconds"], self.remaining(cleanup))
        require(timeout > 0, "Complete MCP readback deadline exceeded")
        self.sequence += 1
        prefix = f"{self.sequence:03d}"
        body = None if message is None else json.dumps(message, separators=(",", ":")).encode()
        if body is not None:
            self.save(prefix + ".request.json", body)
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
            "Cache-Control": "no-cache",
        }
        if self.session is not None:
            headers["MCP-Session-Id"] = self.session
        if self.protocol is not None:
            headers["MCP-Protocol-Version"] = self.protocol
        connection = http.client.HTTPSConnection(self.host, timeout=timeout)
        deadline = time.monotonic() + timeout
        raw = bytearray()
        response = None
        identifier = None if message is None else message.get("id")
        answer = None
        pending = b""
        event = []
        try:
            connection.request("DELETE" if cleanup else "POST", self.path, body=body, headers=headers)
            response = connection.getresponse()
            content_type = response.getheader("Content-Type", "").split(";")[0].strip().lower()
            returned_session = response.getheader("MCP-Session-Id")
            if returned_session is not None:
                require(bool(re.fullmatch("[!-~]+", returned_session)), "Invalid session identifier")
                require(self.session is None or self.session == returned_session, "Server changed session unexpectedly")
                self.session = returned_session
            safe = {
                "http_status": response.status,
                "content_type": content_type,
                "content_length": response.getheader("Content-Length"),
                "date": response.getheader("Date"),
                "session_header_present": returned_session is not None,
                "protocol_header": response.getheader("MCP-Protocol-Version"),
                "redirect_refused": 300 <= response.status < 400,
            }
            self.save(prefix + ".headers.json", json.dumps(safe, indent=2).encode())
            require(not 300 <= response.status < 400, "MCP redirect refused")
            if response.status == 404 and self.session is not None:
                raise ValueError("MCP session expired; automatic session restart refused")
            while True:
                if response.isclosed():
                    break
                remaining = min(deadline - time.monotonic(), self.remaining(cleanup))
                require(remaining > 0, "MCP request deadline exceeded")
                sock = connection.sock or getattr(getattr(getattr(response, "fp", None), "raw", None), "_sock", None)
                require(sock is not None, "Cannot enforce response socket deadline")
                sock.settimeout(remaining)
                chunk = response.read1(min(16384, self.limits["response_bytes"] + 1 - len(raw)))
                if not chunk:
                    break
                raw.extend(chunk)
                require(len(raw) <= self.limits["response_bytes"], "MCP response cap exceeded")
                if content_type == "text/event-stream" and identifier is not None:
                    pending += chunk
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        line = line.rstrip(b"\r")
                        if line.startswith(b"data:"):
                            event.append(line[5:].lstrip(b" "))
                        elif not line:
                            data = b"\n".join(event)
                            event = []
                            if not data:
                                continue
                            item = json.loads(data)
                            if item.get("id") == identifier and ("result" in item or "error" in item):
                                answer = item
                                break
                            require(
                                "method" not in item or "id" not in item,
                                "Unexpected server request; client has no advertised capabilities",
                            )
                    if answer is not None:
                        break
            require(
                time.monotonic() <= deadline and self.remaining(cleanup) >= 0, "MCP request/global deadline crossed"
            )
            if cleanup:
                require(response.status in [200, 202, 204, 405], "MCP session DELETE failed")
                return {
                    "attempted": True,
                    "http_status": response.status,
                    "terminated": response.status != 405,
                    "server_disallows_delete": response.status == 405,
                }
            if identifier is None:
                require(response.status == 202 and (not raw), "Initialized notification not accepted as empty202")
                return None
            require(response.status == 200, "MCP request HTTP status refused: " + str(response.status))
            if content_type == "application/json":
                answer = json.loads(raw)
            elif content_type != "text/event-stream":
                raise ValueError("MCP response must be JSON or SSE")
            require(
                isinstance(answer, dict) and answer.get("jsonrpc") == "2.0" and (answer.get("id") == identifier),
                "Exact JSON-RPC reply missing",
            )
            require("error" not in answer and "result" in answer, "MCP JSON-RPC error; see retained raw reply")
            return answer["result"]
        finally:
            connection.close()
            available = max(0, self.limits["output_bytes"] - 65536 - self.retained)
            retained = bytes(raw[:available])
            self.save(prefix + ".response.raw", retained)
            self.records.append(
                {
                    "sequence": self.sequence,
                    "status": None if response is None else response.status,
                    "response_bytes": len(raw),
                    "retained_response_bytes": len(retained),
                    "response_truncated_by_total_cap": len(retained) != len(raw),
                    "cleanup": cleanup,
                }
            )

    def request(self, method, params):
        return self.exchange({"jsonrpc": "2.0", "id": self.sequence + 1, "method": method, "params": params})

    def initialize(self):
        result = self.request(
            "initialize",
            {
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "spicy-regs-local-delivery", "version": "1"},
            },
        )
        require(result.get("protocolVersion") == PROTOCOL, "Negotiated protocol differs from supported2025-11-25")
        self.protocol = result["protocolVersion"]
        self.exchange({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def tool(self, name, arguments):
        result = self.request("tools/call", {"name": name, "arguments": arguments})
        require(result.get("isError") is not True, "MCP tool refused; see retained reply")
        if isinstance(result.get("structuredContent"), dict):
            return result["structuredContent"]
        blocks = [b["text"] for b in result.get("content", []) if b.get("type") == "text"]
        require(len(blocks) == 1, "Expected single structured JSON tool reply")
        return json.loads(blocks[0])
