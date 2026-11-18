"""采集端 HTTP 客户端：封装列表翻页、详情与附件下载，所有请求经过限速器。"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from urllib.parse import quote


class SiteError(Exception):
    pass


class CursorExpired(SiteError):
    """站点返回 410：游标过期或失效。"""


class DownloadError(SiteError):
    pass


class ListPage:
    def __init__(self, items, next_cursor, has_more):
        self.items = items
        self.next_cursor = next_cursor
        self.has_more = has_more


def _err_msg(body: bytes) -> str:
    try:
        data = json.loads(body.decode("utf-8"))
        return data.get("message") or data.get("error", "")
    except Exception:
        return body[:200].decode("utf-8", "replace")


class SiteClient:
    def __init__(self, base_url: str, rate_limiter=None, timeout: float = 10.0):
        self.base = base_url.rstrip("/")
        self.rate_limiter = rate_limiter
        self.timeout = timeout

    def _get(self, path: str) -> tuple[int, bytes]:
        if self.rate_limiter is not None:
            self.rate_limiter.acquire()
        req = urllib.request.Request(self.base + path)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()
        except urllib.error.URLError as e:
            raise SiteError(f"站点不可达: {e.reason}") from e

    def list_page(self, cursor: str | None) -> ListPage:
        path = "/api/announcements"
        if cursor:
            path += "?cursor=" + quote(cursor, safe="")
        status, body = self._get(path)
        if status == 410:
            raise CursorExpired(_err_msg(body))
        if status != 200:
            raise SiteError(f"列表请求失败 HTTP {status}: {_err_msg(body)}")
        data = json.loads(body.decode("utf-8"))
        return ListPage(data["items"], data.get("next_cursor"), bool(data.get("has_more")))

    def get_detail(self, source_no: str) -> dict:
        status, body = self._get(f"/api/announcements/{quote(source_no)}")
        if status != 200:
            raise SiteError(f"详情请求失败 {source_no} HTTP {status}: {_err_msg(body)}")
        return json.loads(body.decode("utf-8"))

    def download(self, url_path: str) -> bytes:
        status, body = self._get(url_path)
        if status != 200:
            raise DownloadError(f"附件下载失败 HTTP {status}: {_err_msg(body)}")
        return body
