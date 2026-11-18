"""采集系统管理界面（Python 标准库实现，服务端渲染）。

页面：仪表盘 / 采集批次 / 公告（含版本历史）/ 附件状态 / 隔离区 / 搜索 / 重跑结果 / 回调记录
操作：立即采集、重试失败附件、重新解析隔离区、推进站点时间线（演示）
接口：POST /callbacks 接收站点变更回调（重复回调幂等去重）
"""
from __future__ import annotations

import html
import json
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from collector.client import SiteClient
from collector.db import DB
from collector.pipeline import Collector
from collector.ratelimit import RateLimiter

CSS = """
body{font-family:system-ui,"PingFang SC","Microsoft YaHei",sans-serif;margin:0;background:#f5f6f8;color:#222}
header{background:#1f3a5f;color:#fff;padding:12px 24px}
header h1{font-size:18px;margin:0 0 8px}
nav a{color:#cfe0f5;margin-right:16px;text-decoration:none;font-size:14px}
nav a.active{color:#fff;font-weight:600;border-bottom:2px solid #fff;padding-bottom:2px}
main{padding:20px 24px;max-width:1280px;margin:0 auto}
h2{font-size:17px;margin:18px 0 8px}
table{border-collapse:collapse;width:100%;background:#fff;margin:10px 0;font-size:13px}
th,td{border:1px solid #e2e5ea;padding:6px 10px;text-align:left;vertical-align:top}
th{background:#eef1f5;white-space:nowrap}
.badge{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;color:#fff;white-space:nowrap}
.badge.ok{background:#2e7d32}.badge.bad{background:#c62828}.badge.warn{background:#ef6c00}
.badge.quar{background:#6a1b9a}.badge.info{background:#546e7a}
.cards{display:flex;gap:12px;flex-wrap:wrap;margin:10px 0}
.card{background:#fff;border:1px solid #e2e5ea;border-radius:8px;padding:12px 18px;min-width:120px}
.card .num{font-size:24px;font-weight:700}
.card .label{font-size:12px;color:#666;margin-top:2px}
form.inline{display:inline-block;margin:4px 8px 4px 0}
button{background:#1f3a5f;color:#fff;border:0;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:13px}
button:hover{background:#2a4f7f}
pre{background:#f0f2f5;padding:12px;border-radius:6px;white-space:pre-wrap;word-break:break-all}
input,select{padding:5px 8px;border:1px solid #ccc;border-radius:4px;font-size:13px}
.flash{background:#fff8e1;border:1px solid #ffe082;padding:8px 12px;border-radius:6px;margin:10px 0}
.toolbar{margin:12px 0;padding:12px;background:#fff;border:1px solid #e2e5ea;border-radius:8px}
.muted{color:#888;font-size:12px}
"""

NAV = [
    ("/", "仪表盘"),
    ("/batches", "采集批次"),
    ("/announcements", "公告"),
    ("/attachments", "附件"),
    ("/quarantine", "隔离区"),
    ("/search", "搜索"),
    ("/reruns", "重跑结果"),
    ("/callbacks", "回调记录"),
]

BADGE_CLASS = {
    "ok": "ok", "success": "ok", "active": "ok", "processed": "ok", "resolved": "ok",
    "failed": "bad", "withdrawn": "bad",
    "partial_failed": "warn", "duplicate": "warn",
    "quarantined": "quar", "open": "quar",
    "running": "info", "pending": "info", "received": "info",
}

CHANGE_TYPE_LABEL = {
    "initial": "首次采集", "revised": "正文修订", "withdrawn": "撤回", "restored": "恢复发布",
}


def esc(s) -> str:
    return html.escape("" if s is None else str(s))


def badge(status) -> str:
    cls = BADGE_CLASS.get(status, "info")
    return f'<span class="badge {cls}">{esc(status)}</span>'


def change_type(t) -> str:
    return esc(CHANGE_TYPE_LABEL.get(t, t))


def render_page(title: str, body: str, active: str = "") -> bytes:
    links = ""
    for href, label in NAV:
        cls = ' class="active"' if href == active else ""
        links += f'<a href="{href}"{cls}>{label}</a>'
    doc = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>{esc(title)} · 采购公告采集系统</title>
<style>{CSS}</style></head>
<body><header><h1>采购公告采集系统</h1><nav>{links}</nav></header>
<main>{body}</main></body></html>"""
    return doc.encode("utf-8")


def table(headers, rows) -> str:
    head = "".join(f"<th>{esc(h)}</th>" for h in headers)
    if rows:
        body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in rows)
    else:
        body = f'<tr><td colspan="{len(headers)}" class="muted">暂无数据</td></tr>'
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def batch_row(b: dict) -> list:
    return [
        f'<a href="/batches/{b["id"]}">#{b["id"]}</a>',
        esc(b["kind"]), badge(b["status"]),
        esc(b["started_at"]), esc(b["finished_at"] or "-"),
        b["listed"], b["new_count"], b["updated_count"], b["withdrawn_count"],
        b["duplicate_count"], b["cursor_rebuilds"],
        b["att_ok"], b["att_failed"], b["att_skipped"], b["quarantined"],
        esc(b["error"] or ""),
    ]


BATCH_HEADERS = ["批次", "类型", "状态", "开始", "结束", "列表条数", "新增", "修订",
                 "撤回", "重复合并", "游标重建", "附件成功", "附件失败", "附件跳过", "隔离", "错误"]


class AppCtx:
    def __init__(self, db: DB, collector: Collector, site_url: str):
        self.db = db
        self.collector = collector
        self.site_url = site_url


class AdminHandler(BaseHTTPRequestHandler):
    server_version = "CollectorAdmin/1.0"

    @property
    def ctx(self) -> AppCtx:
        return self.server.ctx

    def log_message(self, *args):
        pass

    # ---------- 响应工具 ----------
    def _send_html(self, title: str, body: str, active: str = "", status: int = 200):
        data = render_page(title, body, active)
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json(self, obj, status: int = 200):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str):
        self.send_response(303)
        self.send_header("Location", location)
        self.end_headers()

    # ---------- GET 路由 ----------
    def do_GET(self):
        path = urlparse(self.path).path
        qs = parse_qs(urlparse(self.path).query)
        try:
            if path == "/":
                self._dashboard()
            elif path == "/batches":
                self._batches()
            elif path.startswith("/batches/"):
                self._batch_detail(int(path.split("/")[2]))
            elif path == "/announcements":
                self._announcements(qs)
            elif path.startswith("/announcements/"):
                self._ann_detail(int(path.split("/")[2]))
            elif path.startswith("/versions/"):
                self._version_detail(int(path.split("/")[2]))
            elif path == "/attachments":
                self._attachments(qs)
            elif path == "/quarantine":
                self._quarantine()
            elif path == "/search":
                self._search(qs)
            elif path == "/reruns":
                self._reruns()
            elif path == "/callbacks":
                self._callbacks_page()
            else:
                self._send_html("未找到", "<p>404 页面不存在</p>", status=404)
        except Exception as e:
            self._send_html("错误", f'<div class="flash">页面渲染失败：{esc(e)}</div>', status=500)

    # ---------- POST 路由 ----------
    def do_POST(self):
        path = urlparse(self.path).path
        try:
            if path == "/callbacks":
                self._callback_api()
            elif path == "/actions/collect":
                self._run_action(lambda: self.ctx.collector.run_batch("incremental"), "/batches")
            elif path == "/actions/retry":
                self._run_action(lambda: self.ctx.collector.retry_failed_attachments(), "/reruns")
            elif path == "/actions/reprocess":
                self._run_action(lambda: self.ctx.collector.reprocess_quarantine(), "/quarantine")
            elif path == "/actions/advance":
                self._advance_site()
            else:
                self._send_html("未找到", "<p>404</p>", status=404)
        except Exception as e:
            self._send_html("操作失败",
                            f'<div class="flash">操作失败：{esc(e)}</div>'
                            '<p><a href="/">返回仪表盘</a></p>', status=500)

    def _run_action(self, fn, redirect: str):
        fn()
        self._redirect(redirect)

    def _advance_site(self):
        req = urllib.request.Request(self.ctx.site_url + "/admin/advance", data=b"{}")
        urllib.request.urlopen(req, timeout=60).read()
        self._redirect("/batches")

    def _callback_api(self):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            self._json({"error": "invalid json"}, 400)
            return
        if "callback_id" not in payload or "source_no" not in payload:
            self._json({"error": "缺少 callback_id 或 source_no"}, 400)
            return
        result = self.ctx.collector.handle_callback(payload)
        self._json({"result": result})

    # ---------- 页面 ----------
    def _dashboard(self):
        s = self.ctx.db.stats()
        cards = [
            ("公告总数", s["announcements_total"]),
            ("有效公告", s["announcements"].get("active", 0)),
            ("已撤回", s["announcements"].get("withdrawn", 0)),
            ("版本总数", s["versions"]),
            ("附件正常", s["attachments"].get("ok", 0)),
            ("附件失败", s["attachments"].get("failed", 0)),
            ("隔离区待处理", s["quarantine_open"]),
            ("回调记录", s["callbacks"]),
        ]
        cards_html = '<div class="cards">' + "".join(
            f'<div class="card"><div class="num">{n}</div><div class="label">{esc(label)}</div></div>'
            for label, n in cards
        ) + "</div>"
        toolbar = (
            '<div class="toolbar">'
            '<form class="inline" method="post" action="/actions/collect"><button>立即采集（增量）</button></form>'
            '<form class="inline" method="post" action="/actions/retry"><button>重试失败附件</button></form>'
            '<form class="inline" method="post" action="/actions/reprocess"><button>重新解析隔离区</button></form>'
            '<form class="inline" method="post" action="/actions/advance"><button>推进站点时间线（演示）</button></form>'
            f'<div class="muted">站点：{esc(self.ctx.site_url)}</div>'
            "</div>"
        )
        batches = self.ctx.db.list_batches(limit=5)
        recent = "<h2>最近批次</h2>" + table(BATCH_HEADERS, [batch_row(b) for b in batches])
        self._send_html("仪表盘", f"<h2>概览</h2>{cards_html}{toolbar}{recent}", "/")

    def _batches(self):
        batches = self.ctx.db.list_batches(limit=200)
        body = ("<h2>采集批次</h2>"
                '<form class="inline" method="post" action="/actions/collect"><button>立即采集（增量）</button></form>'
                + table(BATCH_HEADERS, [batch_row(b) for b in batches]))
        self._send_html("采集批次", body, "/batches")

    def _batch_detail(self, batch_id: int):
        b = self.ctx.db.get_batch(batch_id)
        if not b:
            self._send_html("批次不存在", "<p>批次不存在</p>", "/batches", 404)
            return
        info = table(["字段", "值"], [
            ["批次", f'#{b["id"]}'], ["类型", esc(b["kind"])], ["状态", badge(b["status"])],
            ["开始", esc(b["started_at"])], ["结束", esc(b["finished_at"] or "-")],
            ["列表条数", b["listed"]], ["新增", b["new_count"]], ["修订", b["updated_count"]],
            ["撤回", b["withdrawn_count"]], ["重复合并", b["duplicate_count"]],
            ["游标重建", b["cursor_rebuilds"]],
            ["附件成功/失败/跳过", f'{b["att_ok"]} / {b["att_failed"]} / {b["att_skipped"]}'],
            ["隔离", b["quarantined"]], ["错误", esc(b["error"] or "-")],
        ])
        events = self.ctx.db.list_attachment_events(batch_id=batch_id)
        ev_rows = [[e["ts"], esc(e["source_no"]), esc(e["att_key"]), esc(e["att_name"]),
                    badge(e["event"]), esc(e["detail"] or "")] for e in events]
        body = (f'<h2>批次 #{batch_id}</h2>{info}'
                f"<h2>附件事件（{len(events)}）</h2>"
                + table(["时间", "公告", "附件", "名称", "事件", "详情"], ev_rows)
                + '<p><a href="/batches">返回批次列表</a></p>')
        self._send_html(f"批次 #{batch_id}", body, "/batches")

    def _announcements(self, qs):
        status = qs.get("status", [None])[0]
        category = qs.get("category", [None])[0]
        anns = self.ctx.db.list_announcements(status=status or None, category=category or None)
        filt = (
            '<form method="get" action="/announcements" class="toolbar">'
            '状态 <select name="status"><option value="">全部</option>'
            f'<option value="active"{" selected" if status == "active" else ""}>active</option>'
            f'<option value="withdrawn"{" selected" if status == "withdrawn" else ""}>withdrawn</option></select> '
            '分类 <select name="category"><option value="">全部</option>'
            + "".join(f'<option{" selected" if category == c else ""}>{c}</option>'
                      for c in ["工程类", "货物类", "服务类", "其他"])
            + '</select> <button type="submit">过滤</button></form>'
        )
        rows = [[
            f'<a href="/announcements/{a["id"]}">{esc(a["source_no"])}</a>',
            esc(a.get("current_title") or "-"),
            badge(a["status"]), esc(a.get("category") or "-"),
            a["version_count"], a["seen_count"],
            f'<a href="/batches/{a["last_seen_batch"]}">#{a["last_seen_batch"]}</a>',
        ] for a in anns]
        body = (f"<h2>公告（{len(anns)}）</h2>" + filt
                + table(["编号", "当前标题", "状态", "分类", "版本数", "出现次数", "最近批次"], rows))
        self._send_html("公告", body, "/announcements")

    def _ann_detail(self, ann_id: int):
        db = self.ctx.db
        ann = db.get_announcement(ann_id)
        if not ann:
            self._send_html("公告不存在", "<p>公告不存在</p>", "/announcements", 404)
            return
        info = table(["字段", "值"], [
            ["编号", esc(ann["source_no"])], ["状态", badge(ann["status"])],
            ["当前分类", esc(ann.get("category") or "-")],
            ["出现次数（重复合并）", ann["seen_count"]],
            ["首次/最近批次", f'#{ann["first_seen_batch"]} / #{ann["last_seen_batch"]}'],
        ])
        versions = db.list_versions(ann_id)
        v_rows = [[
            f'<a href="/versions/{v["id"]}">v{v["version_no"]}</a>',
            change_type(v["change_type"]),
            esc(v["title"]),
            esc(v.get("category") or "-"),
            esc(v.get("published_at") or "-"),
            f'<a href="/batches/{v["batch_id"]}">#{v["batch_id"]}</a>' if v["batch_id"] else "-",
            "✔ 当前" if v["is_current"] else "",
            esc((v["content_hash"] or "")[:12] + "…"),
        ] for v in versions]
        atts = db.attachments_of(ann_id)
        a_rows = [[
            esc(t["att_key"]), esc(t["name"]), badge(t["status"]),
            t["attempts"], esc(t["last_error"] or "-"),
            esc((t["sha256_actual"] or "-")[:12]),
        ] for t in atts]
        body = (f'<h2>公告 {esc(ann["source_no"])}</h2>{info}'
                f"<h2>版本历史（{len(versions)}，旧版本可追溯）</h2>"
                + table(["版本", "变更类型", "标题", "分类", "发布时间", "采集批次", "当前", "内容哈希"], v_rows)
                + f"<h2>附件（{len(atts)}）</h2>"
                + table(["附件", "名称", "状态", "尝试次数", "最近错误", "内容哈希"], a_rows)
                + '<p><a href="/announcements">返回公告列表</a></p>')
        self._send_html(f"公告 {ann['source_no']}", body, "/announcements")

    def _version_detail(self, version_id: int):
        v = self.ctx.db.get_version(version_id)
        if not v:
            self._send_html("版本不存在", "<p>版本不存在</p>", "/announcements", 404)
            return
        ann = self.ctx.db.get_announcement(v["announcement_id"])
        body = (
            f'<h2>版本 v{v["version_no"]}（{esc(ann["source_no"])}）'
            f'{" · 当前版本" if v["is_current"] else " · 历史版本"}</h2>'
            + table(["字段", "值"], [
                ["变更类型", change_type(v["change_type"])],
                ["站点版本号", esc(v.get("source_version_no"))],
                ["分类", esc(v.get("category") or "-")],
                ["发布时间", esc(v.get("published_at") or "-")],
                ["采集批次", f'#{v["batch_id"]}' if v["batch_id"] else "-"],
                ["采集时间", esc(v["created_at"])],
                ["内容哈希", esc(v["content_hash"])],
            ])
            + f'<h2>标题</h2><p>{esc(v["title"])}</p>'
            + f'<h2>正文</h2><pre>{esc(v["content"])}</pre>'
            + f'<p><a href="/announcements/{v["announcement_id"]}">返回公告详情</a></p>'
        )
        self._send_html(f"版本 v{v['version_no']}", body, "/announcements")

    def _attachments(self, qs):
        status = qs.get("status", [None])[0]
        atts = self.ctx.db.list_attachments(status=status or None)
        filt = (
            '<form method="get" action="/attachments" class="toolbar">状态 '
            '<select name="status"><option value="">全部</option>'
            + "".join(f'<option{" selected" if status == s else ""}>{s}</option>'
                      for s in ["ok", "failed", "quarantined", "pending"])
            + '</select> <button type="submit">过滤</button></form>'
        )
        rows = [[
            f'<a href="/announcements/{t["announcement_id"]}">{esc(t["source_no"])}</a>',
            esc(t["att_key"]), esc(t["name"]), badge(t["status"]),
            t["attempts"], esc(t["last_error"] or "-"),
            esc(t["local_path"] or "-"),
        ] for t in atts]
        body = (f"<h2>附件状态（{len(atts)}）</h2>" + filt
                + table(["公告", "附件", "名称", "状态", "尝试次数", "最近错误", "本地路径"], rows))
        self._send_html("附件状态", body, "/attachments")

    def _quarantine(self):
        db = self.ctx.db
        open_items = db.quarantine_list(status="open")
        resolved = db.quarantine_list(status="resolved")
        o_rows = [[
            q["id"], f'<a href="/announcements/{q["announcement_id"]}">{esc(q["source_no"])}</a>',
            esc(q["att_key"]), esc(q["att_name"]), esc(q["reason"]),
            esc(q["created_at"]), esc(q["raw_path"] or "-"),
        ] for q in open_items]
        r_rows = [[
            q["id"], esc(q["source_no"]), esc(q["att_key"]), esc(q["reason"]),
            esc(q["resolved_at"] or "-"), esc(q["resolution"] or "-"),
        ] for q in resolved]
        body = (
            f"<h2>隔离区 · 待处理（{len(open_items)}）</h2>"
            '<form class="inline" method="post" action="/actions/reprocess"><button>全部重新解析</button></form>'
            '<div class="muted">解析失败的附件在此隔离，不会伪装成下载成功；修复后可重新解析。</div>'
            + table(["ID", "公告", "附件", "名称", "隔离原因", "隔离时间", "原始文件"], o_rows)
            + f"<h2>已解决（{len(resolved)}）</h2>"
            + table(["ID", "公告", "附件", "原因", "解决时间", "解决方式"], r_rows)
        )
        self._send_html("隔离区", body, "/quarantine")

    def _search(self, qs):
        q = qs.get("q", [""])[0]
        results = self.ctx.db.search(q) if q.strip() else []
        form = (
            '<form method="get" action="/search" class="toolbar">'
            f'<input name="q" value="{esc(q)}" size="40" placeholder="搜索当前版本标题/正文（撤回公告不可搜）"> '
            '<button type="submit">搜索</button></form>'
        )
        rows = [[
            f'<a href="/announcements/{r["ann_id"]}">{esc(r["source_no"])}</a>',
            esc(r["title"]), esc(r.get("snip") or ""),
        ] for r in results]
        body = f"<h2>搜索</h2>{form}<h2>结果（{len(results)}）</h2>" + table(["编号", "标题", "片段"], rows)
        self._send_html("搜索", body, "/search")

    def _reruns(self):
        batches = self.ctx.db.list_batches(limit=200, kinds=["retry_attachments", "reprocess", "callback"])
        body = ("<h2>重跑结果</h2>"
                '<form class="inline" method="post" action="/actions/retry"><button>重试失败附件</button></form>'
                '<form class="inline" method="post" action="/actions/reprocess"><button>重新解析隔离区</button></form>'
                '<div class="muted">附件重试（retry_attachments）、隔离区重解析（reprocess）与回调补采（callback）批次</div>'
                + table(BATCH_HEADERS, [batch_row(b) for b in batches]))
        self._send_html("重跑结果", body, "/reruns")

    def _callbacks_page(self):
        cbs = self.ctx.db.list_callbacks(limit=200)
        rows = [[
            c["id"], esc(c["callback_id"]), esc(c["event"]), esc(c["source_no"]),
            badge(c["result"] or "received"), esc(c["received_at"]),
        ] for c in cbs]
        body = (f"<h2>回调记录（{len(cbs)}）</h2>"
                '<div class="muted">同一 callback_id 重复投递只会处理一次，重复记录不会出现在表中</div>'
                + table(["ID", "回调ID", "事件", "公告", "处理结果", "接收时间"], rows))
        self._send_html("回调记录", body, "/callbacks")


def create_app(site_url: str, port: int, db_path: str, storage_dir: str,
               rate: float = 20.0, host: str = "127.0.0.1",
               configure_callback: bool = True) -> ThreadingHTTPServer:
    db = DB(db_path)
    db.init_schema()
    client = SiteClient(site_url, RateLimiter(rate))
    collector = Collector(db, client, storage_dir)
    server = ThreadingHTTPServer((host, port), AdminHandler)
    server.ctx = AppCtx(db, collector, site_url)
    server.daemon_threads = True
    if configure_callback:
        # 向站点登记回调地址（站点不可达时忽略，稍后可手动配置）
        try:
            cb_url = f"http://{host}:{server.server_address[1]}/callbacks"
            req = urllib.request.Request(
                site_url.rstrip("/") + "/admin/config",
                data=json.dumps({"callback_url": cb_url}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=3).read()
        except Exception:
            pass
    return server
