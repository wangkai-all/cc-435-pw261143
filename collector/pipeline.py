"""采集主流程：列表翻页 → 详情 → 版本同步 → 附件下载/解析 → 索引与分类。

可靠性设计：
- 断点：每页处理完写入 checkpoints，崩溃后下一批从游标继续
- 游标过期：收到 410 时从头重建游标，已采集数据靠 source_no 幂等合并
- 附件幂等：sha256 校验 + 临时文件原子落盘；已 ok 且校验匹配的附件跳过
- 解析失败：进入隔离区（quarantined），绝不标记为成功
"""
from __future__ import annotations

import hashlib
import os

from .classify import classify
from .client import CursorExpired, SiteClient
from .db import DB
from .parse import ParseError, parse_attachment


class SimulatedCrash(Exception):
    """测试注入：模拟采集器在翻页中途崩溃。"""


class Collector:
    def __init__(self, db: DB, client: SiteClient, storage_dir: str, parser=parse_attachment):
        self.db = db
        self.client = client
        self.storage_dir = storage_dir
        self.parser = parser
        self.fail_after_pages: int | None = None  # 测试钩子：第 N 页后模拟崩溃

    # ---------- 批次入口 ----------
    def run_batch(self, kind: str = "incremental", source_no: str | None = None) -> int:
        """跑一个采集批次；source_no 非空时只采单条（回调触发）。"""
        batch_id = self.db.start_batch(kind)
        try:
            if source_no:
                detail = self.client.get_detail(source_no)
                ann, _ = self.db.upsert_announcement(detail, batch_id)
                self.db.bump_batch(batch_id, "listed")
                self._sync(ann, detail, batch_id)
            else:
                self._crawl_list(batch_id)
            self.db.finish_batch(batch_id, self._final_status(batch_id))
        except Exception as e:
            self.db.finish_batch(batch_id, "failed", error=str(e))
            raise
        return batch_id

    def retry_failed_attachments(self) -> int:
        """重跑：只处理下载失败的附件（幂等，已 ok 的不动）。"""
        batch_id = self.db.start_batch("retry_attachments")
        try:
            for att in self.db.list_attachments(status="failed"):
                self._download_and_parse(dict(att), batch_id)
            self.db.finish_batch(batch_id, self._final_status(batch_id))
        except Exception as e:
            self.db.finish_batch(batch_id, "failed", error=str(e))
            raise
        return batch_id

    def reprocess_quarantine(self) -> int:
        """重跑：重新解析隔离区附件（例如解析器修复或文件被人工修复后）。"""
        batch_id = self.db.start_batch("reprocess")
        try:
            for q in self.db.quarantine_list(status="open"):
                att = self.db.get_attachment(q["attachment_id"])
                try:
                    with open(q["raw_path"], "rb") as f:
                        data = f.read()
                    text = self.parser(data)
                except (ParseError, OSError) as e:
                    self.db.att_event(att["id"], batch_id, "failed", f"重新解析仍失败: {e}")
                    self.db.bump_batch(batch_id, "quarantined")
                    continue
                self.db.att_update(att["id"], status="ok", extracted_text=text, last_error=None)
                self.db.quarantine_resolve(q["id"], "重新解析成功")
                self.db.att_event(att["id"], batch_id, "ok", "隔离区重新解析成功")
                self.db.bump_batch(batch_id, "att_ok")
            self.db.finish_batch(batch_id, self._final_status(batch_id))
        except Exception as e:
            self.db.finish_batch(batch_id, "failed", error=str(e))
            raise
        return batch_id

    def handle_callback(self, payload: dict) -> str:
        """处理站点回调；同一 callback_id 重复投递时幂等返回 duplicate。"""
        if not self.db.insert_callback(payload):
            return "duplicate"
        try:
            self.run_batch(kind="callback", source_no=payload["source_no"])
            self.db.mark_callback(payload["callback_id"], "processed")
        except Exception as e:
            self.db.mark_callback(payload["callback_id"], f"error: {e}")
            raise
        return "processed"

    # ---------- 列表翻页 ----------
    def _crawl_list(self, batch_id: int) -> None:
        cursor = self.db.get_checkpoint("list_cursor")
        pages = 0
        while True:
            try:
                page = self.client.list_page(cursor)
            except CursorExpired:
                # 游标过期：从头重建；已采集数据靠 upsert 幂等合并，不产生重复
                self.db.bump_batch(batch_id, "cursor_rebuilds")
                cursor = None
                page = self.client.list_page(None)
            for item in page.items:
                self.db.bump_batch(batch_id, "listed")
                ann, created = self.db.upsert_announcement(item, batch_id)
                if not created:
                    self.db.bump_batch(batch_id, "duplicate_count")
                detail = self.client.get_detail(item["source_no"])
                self._sync(ann, detail, batch_id)
            # 断点：一页完整处理完才推进游标
            self.db.set_checkpoint("list_cursor", page.next_cursor)
            pages += 1
            if self.fail_after_pages and pages >= self.fail_after_pages:
                raise SimulatedCrash(f"模拟崩溃：第 {pages} 页处理后采集器中断")
            if not page.has_more:
                break
            cursor = page.next_cursor
        self.db.clear_checkpoint("list_cursor")

    # ---------- 单条同步 ----------
    def _sync(self, ann: dict, detail: dict, batch_id: int) -> None:
        self._sync_version(ann, detail, batch_id)
        if not detail.get("withdrawn"):
            self._sync_attachments(ann, detail, batch_id)

    def _sync_version(self, ann: dict, detail: dict, batch_id: int) -> None:
        current = self.db.current_version(ann["id"])
        if detail.get("withdrawn"):
            if ann["status"] != "withdrawn":
                # 撤回：内容存档为 withdrawn 版本，公告移出搜索索引，历史版本仍可追溯
                self.db.add_version(
                    ann["id"], change_type="withdrawn",
                    title=detail["title"], content=detail["content"],
                    content_hash=detail["content_hash"], category=None,
                    source_version_no=detail.get("version_no"),
                    published_at=detail.get("published_at"), batch_id=batch_id,
                    withdrawn=True,
                )
                self.db.bump_batch(batch_id, "withdrawn_count")
            return
        if current is None:
            category = classify(detail["title"], detail["content"])
            self.db.add_version(
                ann["id"], change_type="initial",
                title=detail["title"], content=detail["content"],
                content_hash=detail["content_hash"], category=category,
                source_version_no=detail.get("version_no"),
                published_at=detail.get("published_at"), batch_id=batch_id,
            )
            self.db.bump_batch(batch_id, "new_count")
        elif current["content_hash"] != detail["content_hash"]:
            change_type = "restored" if ann["status"] == "withdrawn" else "revised"
            category = classify(detail["title"], detail["content"])
            self.db.add_version(
                ann["id"], change_type=change_type,
                title=detail["title"], content=detail["content"],
                content_hash=detail["content_hash"], category=category,
                source_version_no=detail.get("version_no"),
                published_at=detail.get("published_at"), batch_id=batch_id,
            )
            self.db.bump_batch(batch_id, "updated_count")
        # 内容未变：无需新版本（upsert_announcement 已更新 seen_count / last_seen_batch）

    def _sync_attachments(self, ann: dict, detail: dict, batch_id: int) -> None:
        for att in detail.get("attachments", []):
            row = self.db.upsert_attachment(ann["id"], att)
            self._ensure_attachment(row, batch_id)

    # ---------- 附件 ----------
    def _ensure_attachment(self, att: dict, batch_id: int) -> None:
        # 幂等：已下载且校验匹配的附件不重复下载
        if (att["status"] == "ok"
                and att["sha256_actual"] == att["sha256_expected"]
                and att["local_path"] and os.path.exists(att["local_path"])):
            self.db.bump_batch(batch_id, "att_skipped")
            return
        # 隔离区附件：内容未变化时不重复下载，由 reprocess 处理；站点换了新内容则重下
        if (att["status"] == "quarantined"
                and att["sha256_actual"] == att["sha256_expected"]):
            self.db.bump_batch(batch_id, "att_skipped")
            return
        self._download_and_parse(att, batch_id)

    def _download_and_parse(self, att: dict, batch_id: int) -> None:
        att_id = att["id"]
        attempt = self.db.att_bump_attempts(att_id)
        self.db.att_event(att_id, batch_id, "attempt", f"第{attempt}次下载 {att['name']}")
        try:
            data = self.client.download(att["url"])
        except Exception as e:
            self.db.att_update(att_id, status="failed", last_error=str(e))
            self.db.att_event(att_id, batch_id, "failed", str(e))
            self.db.bump_batch(batch_id, "att_failed")
            return
        digest = hashlib.sha256(data).hexdigest()
        if digest != att["sha256_expected"]:
            msg = f"校验和不匹配（期望 {att['sha256_expected'][:12]}… 实际 {digest[:12]}…）"
            self.db.att_update(att_id, status="failed", last_error=msg)
            self.db.att_event(att_id, batch_id, "failed", msg)
            self.db.bump_batch(batch_id, "att_failed")
            return
        # 原子落盘：先写临时文件再 rename，重试不会产生半截文件
        path = self._dest_path(att)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp.{batch_id}"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
        try:
            text = self.parser(data)
        except ParseError as e:
            # 解析失败 → 隔离区，绝不标记成功
            self.db.att_update(att_id, status="quarantined", local_path=path,
                               sha256_actual=digest, last_error=str(e))
            self.db.quarantine_add(att_id, batch_id, str(e), path)
            self.db.att_event(att_id, batch_id, "quarantined", str(e))
            self.db.bump_batch(batch_id, "quarantined")
            return
        self.db.att_update(att_id, status="ok", local_path=path, sha256_actual=digest,
                           extracted_text=text, last_error=None)
        self.db.att_event(att_id, batch_id, "ok", f"{len(data)} 字节，解析成功")
        self.db.bump_batch(batch_id, "att_ok")

    def _dest_path(self, att: dict) -> str:
        return os.path.join(self.storage_dir, "attachments",
                            str(att["announcement_id"]), att["att_key"])

    def _final_status(self, batch_id: int) -> str:
        batch = self.db.get_batch(batch_id)
        if batch["att_failed"] or batch["quarantined"]:
            return "partial_failed"
        return "success"
