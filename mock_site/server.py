"""本地模拟采购站点：游标分页列表 / 详情 / 附件下载 / 管理接口。

游标为签名令牌（offset + 过期时间 + nonce），nonce 变化或超时即返回 410。
时间线推进（/admin/advance）时会向采集系统回调地址推送变更通知，
且同一 callback_id 故意投递两次，模拟重复回调。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .dataset import SiteState

SECRET = b"mock-site-secret"


def make_cursor(state: SiteState, offset: int) -> str:
    payload = {"offset": offset, "exp": time.time() + state.cursor_ttl, "nonce": state.cursor_nonce}
    raw = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    sig = hmac.new(SECRET, raw.encode(), hashlib.sha256).hexdigest()[:16]
    return f"{raw}.{sig}"


def parse_cursor(state: SiteState, cursor: str) -> int:
    raw, sig = cursor.rsplit(".", 1)
    expect = hmac.new(SECRET, raw.encode(), hashlib.sha256).hexdigest()[:16]
    if not hmac.compare_digest(sig, expect):
        raise ValueError("游标签名无效")
    payload = json.loads(base64.urlsafe_b64decode(raw.encode()))
    if payload.get("nonce") != state.cursor_nonce:
        raise ValueError("游标已失效（站点数据已变更）")
    if payload.get("exp", 0) < time.time():
        raise ValueError("游标已过期")
    return int(payload["offset"])


class MockSiteHandler(BaseHTTPRequestHandler):
    server_version = "MockProcurementSite/1.0"

    @property
    def state(self) -> SiteState:
        return self.server.state

    def log_message(self, *args):  # 静默访问日志
        pass

    # ---------- 工具 ----------
    def _json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    # ---------- GET ----------
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        with self.state.lock:
            try:
                if path == "/api/announcements":
                    self._handle_list(qs)
                elif path.startswith("/api/announcements/"):
                    self._handle_detail(path.rsplit("/", 1)[1])
                elif path.startswith("/attachments/"):
                    self._handle_attachment(path.rsplit("/", 1)[1])
                elif path == "/admin/state":
                    self._json(self.state.stats())
                else:
                    self._json({"error": "not_found"}, 404)
            except BrokenPipeError:
                pass

    def _handle_list(self, qs):
        self.state.list_requests += 1
        cursor = qs.get("cursor", [None])[0]
        if cursor:
            try:
                offset = parse_cursor(self.state, cursor)
            except ValueError as e:
                self._json({"error": "cursor_expired", "message": str(e)}, 410)
                return
        else:
            offset = 0
        items, next_offset, has_more = self.state.list_page(offset)
        self._json({
            "items": items,
            "next_cursor": make_cursor(self.state, next_offset) if has_more else None,
            "has_more": has_more,
        })

    def _handle_detail(self, source_no):
        self.state.detail_requests += 1
        ann = self.state.announcements.get(source_no)
        if not ann:
            self._json({"error": "not_found"}, 404)
            return
        self._json(ann.detail())

    def _handle_attachment(self, att_id):
        att = self.state.find_attachment(att_id)
        if not att:
            self._json({"error": "not_found"}, 404)
            return
        att.downloads += 1
        if att.downloads <= att.fail_times:
            self._json({"error": "internal", "message": f"附件服务器故障（第{att.downloads}次）"}, 500)
            return
        body = att.body
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---------- POST ----------
    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/admin/advance":
            events = self.state.advance()
            self._send_callbacks(events)  # 锁外发送，避免与采集端相互等待
            self._json({"step": self.state.step, "events": events})
        elif path == "/admin/reset":
            callback_url = self.state.callback_url
            new_state = SiteState()
            new_state.callback_url = callback_url
            self.server.state = new_state
            self._json({"ok": True})
        elif path == "/admin/expire_cursors":
            self.state.expire_cursors()
            self._json({"ok": True, "nonce": self.state.cursor_nonce})
        elif path == "/admin/config":
            cfg = self._read_json()
            if "callback_url" in cfg:
                self.state.callback_url = cfg["callback_url"]
            if "page_size" in cfg:
                self.state.page_size = int(cfg["page_size"])
            if "duplicate_boundary" in cfg:
                self.state.duplicate_boundary = bool(cfg["duplicate_boundary"])
            if "cursor_ttl" in cfg:
                self.state.cursor_ttl = float(cfg["cursor_ttl"])
            self._json({"ok": True})
        else:
            self._json({"error": "not_found"}, 404)

    def _send_callbacks(self, events):
        """推送变更通知；同一 callback_id 故意投递两次（模拟重复回调）。"""
        url = self.state.callback_url
        if not url:
            return
        for i, ev in enumerate(events):
            payload = {
                "callback_id": f"cb-{self.state.step}-{i}-{ev['source_no']}",
                "event": ev["event"],
                "source_no": ev["source_no"],
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            self.state.callbacks_sent.append(payload)
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            for _ in range(2):  # 重复投递
                try:
                    req = urllib.request.Request(
                        url, data=body, headers={"Content-Type": "application/json"})
                    urllib.request.urlopen(req, timeout=10).read()
                except Exception:
                    pass  # 回调不可达不影响站点自身


def create_server(host: str = "127.0.0.1", port: int = 8901, state: SiteState | None = None) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), MockSiteHandler)
    server.state = state or SiteState()
    server.daemon_threads = True
    return server
