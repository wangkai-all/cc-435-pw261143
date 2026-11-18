"""SQLite 存储层：批次、断点、公告版本、附件、隔离区、回调与搜索索引。

设计要点：
- announcements.source_no 唯一：重复公告在此合并，seen_count 记录出现次数
- announcement_versions 全量保留历史版本，is_current 标记当前版本
- search_index（FTS5 trigram）只索引当前版本，版本切换/撤回时同步增删
- attachments 状态机：pending → ok / failed / quarantined，隔离区绝不伪装成功
- callbacks.callback_id 唯一：重复回调幂等去重
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,              -- full / incremental / retry_attachments / reprocess / callback
    status TEXT NOT NULL DEFAULT 'running',  -- running / success / partial_failed / failed
    started_at TEXT NOT NULL,
    finished_at TEXT,
    listed INTEGER NOT NULL DEFAULT 0,
    new_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    withdrawn_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    cursor_rebuilds INTEGER NOT NULL DEFAULT 0,
    att_ok INTEGER NOT NULL DEFAULT 0,
    att_failed INTEGER NOT NULL DEFAULT 0,
    att_skipped INTEGER NOT NULL DEFAULT 0,
    quarantined INTEGER NOT NULL DEFAULT 0,
    error TEXT
);
CREATE TABLE IF NOT EXISTS checkpoints (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS announcements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_no TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'active',   -- active / withdrawn
    category TEXT,
    seen_count INTEGER NOT NULL DEFAULT 0,
    first_seen_batch INTEGER,
    last_seen_batch INTEGER,
    current_version_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS announcement_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    announcement_id INTEGER NOT NULL REFERENCES announcements(id),
    version_no INTEGER NOT NULL,             -- 本地递增序号
    source_version_no INTEGER,               -- 站点侧版本号
    change_type TEXT NOT NULL,               -- initial / revised / withdrawn / restored
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    category TEXT,
    published_at TEXT,
    batch_id INTEGER,
    is_current INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE (announcement_id, version_no)
);
CREATE TABLE IF NOT EXISTS attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    announcement_id INTEGER NOT NULL REFERENCES announcements(id),
    att_key TEXT NOT NULL,
    name TEXT,
    url TEXT,
    sha256_expected TEXT,
    size_expected INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending / ok / failed / quarantined
    local_path TEXT,
    sha256_actual TEXT,
    extracted_text TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT NOT NULL,
    UNIQUE (announcement_id, att_key)
);
CREATE TABLE IF NOT EXISTS attachment_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attachment_id INTEGER NOT NULL,
    batch_id INTEGER,
    ts TEXT NOT NULL,
    event TEXT NOT NULL,                     -- attempt / ok / failed / quarantined
    detail TEXT
);
CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attachment_id INTEGER NOT NULL REFERENCES attachments(id),
    batch_id INTEGER,
    reason TEXT NOT NULL,
    raw_path TEXT,
    status TEXT NOT NULL DEFAULT 'open',     -- open / resolved
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution TEXT
);
CREATE TABLE IF NOT EXISTS callbacks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    callback_id TEXT NOT NULL UNIQUE,
    event TEXT,
    source_no TEXT,
    payload TEXT,
    result TEXT,
    received_at TEXT NOT NULL
);
CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
    source_no UNINDEXED,
    version_id UNINDEXED,
    title,
    content,
    tokenize = 'trigram'
);
"""

BATCH_STAT_FIELDS = {
    "listed", "new_count", "updated_count", "withdrawn_count", "duplicate_count",
    "cursor_rebuilds", "att_ok", "att_failed", "att_skipped", "quarantined",
}

ATT_UPDATABLE = {"status", "local_path", "sha256_actual", "extracted_text", "last_error"}


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


class DB:
    def __init__(self, path: str):
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        # WAL 下 NORMAL 仍保证崩溃一致性，只在断电时可能丢失最后几个事务；
        # 换来提交延迟从 ~5ms 降到 ~0.1ms（采集系统可接受）
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def init_schema(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(parent, exist_ok=True)
        with self._connect() as con:
            con.executescript(SCHEMA)

    # ---------- 批次 ----------
    def start_batch(self, kind: str) -> int:
        with self._connect() as con:
            cur = con.execute(
                "INSERT INTO batches(kind, status, started_at) VALUES (?, 'running', ?)",
                (kind, now_iso()),
            )
            return cur.lastrowid

    def bump_batch(self, batch_id: int, field: str, n: int = 1) -> None:
        if field not in BATCH_STAT_FIELDS:
            raise ValueError(f"未知批次统计字段: {field}")
        with self._connect() as con:
            con.execute(f"UPDATE batches SET {field} = {field} + ? WHERE id = ?", (n, batch_id))

    def finish_batch(self, batch_id: int, status: str, error: str | None = None) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE batches SET status = ?, finished_at = ?, error = ? WHERE id = ?",
                (status, now_iso(), error, batch_id),
            )

    def get_batch(self, batch_id: int) -> dict | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
            return dict(row) if row else None

    def list_batches(self, limit: int = 100, kinds: list[str] | None = None) -> list[dict]:
        sql = "SELECT * FROM batches"
        args: list = []
        if kinds:
            sql += " WHERE kind IN (%s)" % ",".join("?" * len(kinds))
            args.extend(kinds)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        with self._connect() as con:
            return [dict(r) for r in con.execute(sql, args).fetchall()]

    # ---------- 断点 ----------
    def get_checkpoint(self, key: str) -> str | None:
        with self._connect() as con:
            row = con.execute("SELECT value FROM checkpoints WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None

    def set_checkpoint(self, key: str, value: str | None) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT INTO checkpoints(key, value, updated_at) VALUES (?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (key, value, now_iso()),
            )

    def clear_checkpoint(self, key: str) -> None:
        with self._connect() as con:
            con.execute("DELETE FROM checkpoints WHERE key = ?", (key,))

    # ---------- 公告 ----------
    def upsert_announcement(self, item: dict, batch_id: int):
        """按 source_no 合并重复公告。返回 (row_dict, created)。"""
        now = now_iso()
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM announcements WHERE source_no = ?", (item["source_no"],)
            ).fetchone()
            if row:
                con.execute(
                    "UPDATE announcements SET seen_count = seen_count + 1,"
                    " last_seen_batch = ?, updated_at = ? WHERE id = ?",
                    (batch_id, now, row["id"]),
                )
                return dict(row), False
            cur = con.execute(
                "INSERT INTO announcements(source_no, status, seen_count,"
                " first_seen_batch, last_seen_batch, created_at, updated_at)"
                " VALUES (?, 'active', 1, ?, ?, ?, ?)",
                (item["source_no"], batch_id, batch_id, now, now),
            )
            row = con.execute("SELECT * FROM announcements WHERE id = ?", (cur.lastrowid,)).fetchone()
            return dict(row), True

    def get_announcement(self, ann_id: int) -> dict | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM announcements WHERE id = ?", (ann_id,)).fetchone()
            return dict(row) if row else None

    def get_announcement_by_no(self, source_no: str) -> dict | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM announcements WHERE source_no = ?", (source_no,)).fetchone()
            return dict(row) if row else None

    def list_announcements(self, status: str | None = None, category: str | None = None) -> list[dict]:
        sql = (
            "SELECT a.*, v.title AS current_title,"
            " (SELECT COUNT(*) FROM announcement_versions x WHERE x.announcement_id = a.id) AS version_count"
            " FROM announcements a LEFT JOIN announcement_versions v ON v.id = a.current_version_id"
        )
        conds, args = [], []
        if status:
            conds.append("a.status = ?")
            args.append(status)
        if category:
            conds.append("a.category = ?")
            args.append(category)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY a.source_no"
        with self._connect() as con:
            return [dict(r) for r in con.execute(sql, args).fetchall()]

    # ---------- 版本 ----------
    def current_version(self, ann_id: int) -> dict | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM announcement_versions WHERE announcement_id = ? AND is_current = 1",
                (ann_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_versions(self, ann_id: int) -> list[dict]:
        with self._connect() as con:
            return [dict(r) for r in con.execute(
                "SELECT * FROM announcement_versions WHERE announcement_id = ? ORDER BY version_no DESC",
                (ann_id,),
            ).fetchall()]

    def get_version(self, version_id: int) -> dict | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM announcement_versions WHERE id = ?", (version_id,)).fetchone()
            return dict(row) if row else None

    def add_version(self, ann_id: int, *, change_type: str, title: str, content: str,
                    content_hash: str, category: str | None, source_version_no=None,
                    published_at=None, batch_id=None, withdrawn: bool = False) -> int:
        """追加新版本并切换 is_current；同步更新公告状态/分类与搜索索引。"""
        now = now_iso()
        with self._connect() as con:
            row = con.execute(
                "SELECT COALESCE(MAX(version_no), 0) AS m FROM announcement_versions WHERE announcement_id = ?",
                (ann_id,),
            ).fetchone()
            version_no = row["m"] + 1
            con.execute(
                "UPDATE announcement_versions SET is_current = 0 WHERE announcement_id = ?", (ann_id,))
            cur = con.execute(
                "INSERT INTO announcement_versions(announcement_id, version_no, source_version_no,"
                " change_type, title, content, content_hash, category, published_at, batch_id,"
                " is_current, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,1,?)",
                (ann_id, version_no, source_version_no, change_type, title, content, content_hash,
                 category, published_at, batch_id, now),
            )
            version_id = cur.lastrowid
            new_status = "withdrawn" if withdrawn else "active"
            con.execute(
                "UPDATE announcements SET current_version_id = ?, category = ?, status = ?,"
                " updated_at = ? WHERE id = ?",
                (version_id, category, new_status, now, ann_id),
            )
            # 搜索索引跟随当前版本：先删旧行；撤回的公告不再入索引
            source_no = con.execute(
                "SELECT source_no FROM announcements WHERE id = ?", (ann_id,)).fetchone()["source_no"]
            con.execute("DELETE FROM search_index WHERE source_no = ?", (source_no,))
            if not withdrawn:
                con.execute(
                    "INSERT INTO search_index(source_no, version_id, title, content) VALUES (?,?,?,?)",
                    (source_no, version_id, title, content),
                )
            return version_id

    # ---------- 附件 ----------
    def upsert_attachment(self, ann_id: int, att: dict) -> dict:
        now = now_iso()
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM attachments WHERE announcement_id = ? AND att_key = ?",
                (ann_id, att["att_id"]),
            ).fetchone()
            if row:
                con.execute(
                    "UPDATE attachments SET name = ?, url = ?, sha256_expected = ?,"
                    " size_expected = ?, updated_at = ? WHERE id = ?",
                    (att["name"], att["url"], att["sha256"], att["size"], now, row["id"]),
                )
                att_id = row["id"]
            else:
                cur = con.execute(
                    "INSERT INTO attachments(announcement_id, att_key, name, url,"
                    " sha256_expected, size_expected, status, updated_at)"
                    " VALUES (?,?,?,?,?,?, 'pending', ?)",
                    (ann_id, att["att_id"], att["name"], att["url"], att["sha256"], att["size"], now),
                )
                att_id = cur.lastrowid
            return dict(con.execute("SELECT * FROM attachments WHERE id = ?", (att_id,)).fetchone())

    def get_attachment(self, att_id: int) -> dict | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM attachments WHERE id = ?", (att_id,)).fetchone()
            return dict(row) if row else None

    def list_attachments(self, status: str | None = None) -> list[dict]:
        sql = (
            "SELECT t.*, a.source_no FROM attachments t"
            " JOIN announcements a ON a.id = t.announcement_id"
        )
        args: list = []
        if status:
            sql += " WHERE t.status = ?"
            args.append(status)
        sql += " ORDER BY t.id"
        with self._connect() as con:
            return [dict(r) for r in con.execute(sql, args).fetchall()]

    def attachments_of(self, ann_id: int) -> list[dict]:
        with self._connect() as con:
            return [dict(r) for r in con.execute(
                "SELECT * FROM attachments WHERE announcement_id = ? ORDER BY id", (ann_id,),
            ).fetchall()]

    def att_update(self, att_id: int, **fields) -> None:
        unknown = set(fields) - ATT_UPDATABLE
        if unknown:
            raise ValueError(f"不可更新的附件字段: {unknown}")
        sets = ", ".join(f"{k} = ?" for k in fields)
        args = list(fields.values()) + [now_iso(), att_id]
        with self._connect() as con:
            con.execute(f"UPDATE attachments SET {sets}, updated_at = ? WHERE id = ?", args)

    def att_bump_attempts(self, att_id: int) -> int:
        with self._connect() as con:
            con.execute("UPDATE attachments SET attempts = attempts + 1 WHERE id = ?", (att_id,))
            row = con.execute("SELECT attempts FROM attachments WHERE id = ?", (att_id,)).fetchone()
            return row["attempts"]

    def att_event(self, att_id: int, batch_id: int | None, event: str, detail: str | None = None) -> None:
        with self._connect() as con:
            con.execute(
                "INSERT INTO attachment_events(attachment_id, batch_id, ts, event, detail)"
                " VALUES (?,?,?,?,?)",
                (att_id, batch_id, now_iso(), event, detail),
            )

    def list_attachment_events(self, batch_id: int | None = None,
                               att_id: int | None = None, limit: int = 300) -> list[dict]:
        sql = (
            "SELECT e.*, t.att_key, t.name AS att_name, a.source_no"
            " FROM attachment_events e"
            " JOIN attachments t ON t.id = e.attachment_id"
            " JOIN announcements a ON a.id = t.announcement_id"
        )
        conds, args = [], []
        if batch_id is not None:
            conds.append("e.batch_id = ?")
            args.append(batch_id)
        if att_id is not None:
            conds.append("e.attachment_id = ?")
            args.append(att_id)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY e.id LIMIT ?"
        args.append(limit)
        with self._connect() as con:
            return [dict(r) for r in con.execute(sql, args).fetchall()]

    # ---------- 隔离区 ----------
    def quarantine_add(self, att_id: int, batch_id: int | None, reason: str, raw_path: str) -> int:
        with self._connect() as con:
            cur = con.execute(
                "INSERT INTO quarantine(attachment_id, batch_id, reason, raw_path, status, created_at)"
                " VALUES (?,?,?,?, 'open', ?)",
                (att_id, batch_id, reason, raw_path, now_iso()),
            )
            return cur.lastrowid

    def quarantine_list(self, status: str | None = None) -> list[dict]:
        sql = (
            "SELECT q.*, t.att_key, t.name AS att_name, t.announcement_id, a.source_no"
            " FROM quarantine q"
            " JOIN attachments t ON t.id = q.attachment_id"
            " JOIN announcements a ON a.id = t.announcement_id"
        )
        args: list = []
        if status:
            sql += " WHERE q.status = ?"
            args.append(status)
        sql += " ORDER BY q.id DESC"
        with self._connect() as con:
            return [dict(r) for r in con.execute(sql, args).fetchall()]

    def quarantine_resolve(self, qid: int, resolution: str) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE quarantine SET status = 'resolved', resolved_at = ?, resolution = ? WHERE id = ?",
                (now_iso(), resolution, qid),
            )

    # ---------- 回调 ----------
    def insert_callback(self, payload: dict) -> bool:
        """写入回调记录；callback_id 重复时返回 False（幂等去重）。"""
        try:
            with self._connect() as con:
                con.execute(
                    "INSERT INTO callbacks(callback_id, event, source_no, payload, result, received_at)"
                    " VALUES (?,?,?,?, 'received', ?)",
                    (payload["callback_id"], payload.get("event"), payload.get("source_no"),
                     json.dumps(payload, ensure_ascii=False), now_iso()),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def mark_callback(self, callback_id: str, result: str) -> None:
        with self._connect() as con:
            con.execute("UPDATE callbacks SET result = ? WHERE callback_id = ?", (result, callback_id))

    def list_callbacks(self, limit: int = 100) -> list[dict]:
        with self._connect() as con:
            return [dict(r) for r in con.execute(
                "SELECT * FROM callbacks ORDER BY id DESC LIMIT ?", (limit,),
            ).fetchall()]

    # ---------- 搜索 ----------
    def search(self, query: str) -> list[dict]:
        """只命中当前版本；撤回公告不可搜。短词（<3 字符）退化为 LIKE。"""
        q = query.strip()
        if not q:
            return []
        with self._connect() as con:
            if len(q) >= 3:
                safe = re.sub(r'["()*:^]', " ", q).strip()
                if not safe:
                    return []
                try:
                    rows = con.execute(
                        "SELECT a.id AS ann_id, s.source_no, s.title,"
                        " snippet(search_index, 3, '[', ']', '…', 12) AS snip"
                        " FROM search_index s JOIN announcements a ON a.source_no = s.source_no"
                        " WHERE search_index MATCH ? ORDER BY rank LIMIT 50",
                        (f'"{safe}"',),
                    ).fetchall()
                    return [dict(r) for r in rows]
                except sqlite3.OperationalError:
                    return []
            like = f"%{q}%"
            rows = con.execute(
                "SELECT a.id AS ann_id, a.source_no, v.title, substr(v.content, 1, 80) AS snip"
                " FROM announcements a JOIN announcement_versions v ON v.id = a.current_version_id"
                " WHERE a.status != 'withdrawn' AND (v.title LIKE ? OR v.content LIKE ?) LIMIT 50",
                (like, like),
            ).fetchall()
            return [dict(r) for r in rows]

    # ---------- 统计 ----------
    def stats(self) -> dict:
        with self._connect() as con:
            ann = {r["status"]: r["n"] for r in con.execute(
                "SELECT status, COUNT(*) n FROM announcements GROUP BY status")}
            att = {r["status"]: r["n"] for r in con.execute(
                "SELECT status, COUNT(*) n FROM attachments GROUP BY status")}
            quar = con.execute("SELECT COUNT(*) n FROM quarantine WHERE status = 'open'").fetchone()["n"]
            versions = con.execute("SELECT COUNT(*) n FROM announcement_versions").fetchone()["n"]
            batches = con.execute("SELECT COUNT(*) n FROM batches").fetchone()["n"]
            cbs = con.execute("SELECT COUNT(*) n FROM callbacks").fetchone()["n"]
        return {
            "announcements": ann,
            "announcements_total": sum(ann.values()),
            "attachments": att,
            "attachments_total": sum(att.values()),
            "quarantine_open": quar,
            "versions": versions,
            "batches": batches,
            "callbacks": cbs,
        }
