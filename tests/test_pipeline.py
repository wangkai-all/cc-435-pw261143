"""采集流程场景测试：

- 全量采集 + 重复公告合并
- 断点续采（崩溃后从游标恢复）
- 附件部分下载失败 + 幂等重试
- 修订/撤回：版本可追溯，搜索索引与分类跟随当前版本
- 解析失败进隔离区（不伪装成功）+ 修复后重解析
- 游标过期自动重建
- 重复回调幂等
- 限速器行为
"""
import json
import os
import tempfile
import threading
import unittest
import urllib.request

from collector.db import DB
from collector.client import SiteClient
from collector.parse import MAGIC
from collector.pipeline import Collector, SimulatedCrash
from collector.ratelimit import RateLimiter
from mock_site.server import create_server


class PipelineTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.site = create_server(port=0)
        self.port = self.site.server_address[1]
        threading.Thread(target=self.site.serve_forever, daemon=True).start()
        self.db = DB(os.path.join(self.tmp.name, "collector.db"))
        self.db.init_schema()
        self.client = SiteClient(f"http://127.0.0.1:{self.port}", RateLimiter(100000))
        self.collector = Collector(self.db, self.client,
                                   os.path.join(self.tmp.name, "storage"))

    def tearDown(self):
        self.site.shutdown()
        self.site.server_close()
        self.tmp.cleanup()

    # ---------- 辅助 ----------
    def site_stats(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/admin/state") as r:
            return json.loads(r.read())

    def post_site(self, path, payload=None):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}",
                                     data=json.dumps(payload or {}).encode())
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())

    def sum_seen(self):
        with self.db._connect() as con:
            return con.execute("SELECT SUM(seen_count) n FROM announcements").fetchone()["n"]

    def ann_count(self):
        with self.db._connect() as con:
            return con.execute("SELECT COUNT(*) n FROM announcements").fetchone()["n"]


class TestFullCollection(PipelineTestBase):
    def test_full_collection_dedup_and_batch_stats(self):
        bid = self.collector.run_batch("full")
        batch = self.db.get_batch(bid)

        # 24 条公告；列表分页边界重复 → 28 条记录，4 条被合并
        self.assertEqual(self.ann_count(), 24)
        self.assertEqual(batch["listed"], 28)
        self.assertEqual(batch["duplicate_count"], 4)
        self.assertEqual(batch["new_count"], 24)
        self.assertEqual(self.sum_seen(), 28)

        # 附件 24 个：22 成功，1 下载失败（att-0007-a），1 解析失败进隔离区（att-0008-a）
        self.assertEqual(batch["att_ok"], 22)
        self.assertEqual(batch["att_failed"], 1)
        self.assertEqual(batch["quarantined"], 1)
        self.assertEqual(batch["status"], "partial_failed")

        stats = self.db.stats()
        self.assertEqual(stats["attachments"]["ok"], 22)
        self.assertEqual(stats["attachments"]["failed"], 1)
        self.assertEqual(stats["attachments"]["quarantined"], 1)

    def test_second_run_is_idempotent(self):
        self.collector.run_batch("full")
        bid = self.collector.run_batch("incremental")
        batch = self.db.get_batch(bid)
        # 内容未变：无新版本；已 ok 的附件全部跳过，不重复下载
        self.assertEqual(batch["new_count"], 0)
        self.assertEqual(batch["updated_count"], 0)
        self.assertEqual(batch["att_ok"], 0)
        # 跳过 23 个唯一附件（22 ok + 1 隔离）+ 5 个翻页边界重复公告的附件
        self.assertEqual(batch["att_skipped"], 28)
        # 失败的 att-0007-a 在普通批次中也会自动重试（本次仍失败）
        self.assertEqual(batch["att_failed"], 1)
        self.assertEqual(self.ann_count(), 24)
        with self.db._connect() as con:
            n = con.execute("SELECT COUNT(*) n FROM announcement_versions").fetchone()["n"]
        self.assertEqual(n, 24)  # 每公告仍只有 1 个版本


class TestResumeAfterCrash(PipelineTestBase):
    def test_resume_from_checkpoint(self):
        self.collector.fail_after_pages = 2
        with self.assertRaises(SimulatedCrash):
            self.collector.run_batch("full")

        # 崩溃批次标记 failed，断点已保存
        batch1 = self.db.list_batches()[0]
        self.assertEqual(batch1["status"], "failed")
        cursor = self.db.get_checkpoint("list_cursor")
        self.assertIsNotNone(cursor)
        self.assertLess(self.ann_count(), 24)

        # 续跑：从断点继续，最终完整且无重复
        self.collector.fail_after_pages = None
        self.collector.run_batch("incremental")
        self.assertEqual(self.ann_count(), 24)
        self.assertIsNone(self.db.get_checkpoint("list_cursor"))
        # source_no 唯一约束保证无重复公告
        with self.db._connect() as con:
            n = con.execute(
                "SELECT COUNT(*) n FROM (SELECT source_no FROM announcements"
                " GROUP BY source_no HAVING COUNT(*) > 1)").fetchone()["n"]
        self.assertEqual(n, 0)


class TestAttachmentRetry(PipelineTestBase):
    def test_partial_failure_then_idempotent_retry(self):
        self.collector.run_batch("full")
        stats = self.site_stats()
        self.assertEqual(stats["attachments"]["att-0007-a"]["downloads"], 1)

        # 第一次重试：站点第 2 次仍返回 500
        self.collector.retry_failed_attachments()
        att = [a for a in self.db.list_attachments(status="failed")]
        self.assertEqual(len(att), 1)
        self.assertEqual(att[0]["att_key"], "att-0007-a")
        self.assertEqual(att[0]["attempts"], 2)

        # 第二次重试：成功
        bid = self.collector.retry_failed_attachments()
        batch = self.db.get_batch(bid)
        self.assertEqual(batch["att_ok"], 1)
        self.assertEqual(batch["status"], "success")
        self.assertEqual(self.db.list_attachments(status="failed"), [])

        # 幂等：站点侧该附件恰好被下载 3 次；正常附件只下载过 1 次
        stats = self.site_stats()
        self.assertEqual(stats["attachments"]["att-0007-a"]["downloads"], 3)
        self.assertEqual(stats["attachments"]["att-0002-a"]["downloads"], 1)

        # 再次重试：没有 failed 附件，空批次，不触发任何下载
        self.collector.retry_failed_attachments()
        stats = self.site_stats()
        self.assertEqual(stats["attachments"]["att-0007-a"]["downloads"], 3)

    def test_retry_batch_visible_in_reruns(self):
        self.collector.run_batch("full")
        self.collector.retry_failed_attachments()
        reruns = self.db.list_batches(kinds=["retry_attachments", "reprocess", "callback"])
        self.assertEqual(len(reruns), 1)
        self.assertEqual(reruns[0]["kind"], "retry_attachments")


class TestQuarantine(PipelineTestBase):
    def test_parse_failure_goes_to_quarantine_not_success(self):
        self.collector.run_batch("full")
        att = [a for a in self.db.list_attachments() if a["att_key"] == "att-0008-a"][0]
        # 解析失败：状态是 quarantined，绝不伪装成 ok
        self.assertEqual(att["status"], "quarantined")
        self.assertIsNotNone(att["last_error"])
        open_q = self.db.quarantine_list(status="open")
        self.assertEqual(len(open_q), 1)
        self.assertIn("PKCDOC", open_q[0]["reason"])
        # 批次统计里它计入 quarantined 而非 att_ok
        batch = self.db.list_batches()[0]
        self.assertEqual(batch["quarantined"], 1)
        self.assertEqual(batch["att_ok"], 22)

    def test_reprocess_after_fix(self):
        self.collector.run_batch("full")
        # 文件确实损坏：重新解析仍失败，保持 open
        self.collector.reprocess_quarantine()
        self.assertEqual(len(self.db.quarantine_list(status="open")), 1)

        # 人工修复隔离文件后重跑：解析成功，隔离单关闭
        q = self.db.quarantine_list(status="open")[0]
        with open(q["raw_path"], "wb") as f:
            f.write(MAGIC + "修复后的附件内容".encode("utf-8"))
        self.collector.reprocess_quarantine()
        self.assertEqual(len(self.db.quarantine_list(status="open")), 0)
        resolved = self.db.quarantine_list(status="resolved")
        self.assertEqual(len(resolved), 1)
        att = self.db.get_attachment(q["attachment_id"])
        self.assertEqual(att["status"], "ok")
        self.assertIn("修复后的附件内容", att["extracted_text"])


class TestRevisionAndWithdrawal(PipelineTestBase):
    def test_revision_withdrawal_and_search_index(self):
        self.collector.run_batch("full")
        # 修订前：旧关键词可搜，分类为工程类
        self.assertEqual(len(self.db.search("沥青路面")), 1)
        self.assertEqual(self.db.search("智慧城市"), [])
        ann3 = self.db.get_announcement_by_no("PN-0003")
        self.assertEqual(ann3["category"], "工程类")

        # 推进时间线：修订 PN-0003、撤回 PN-0005、新增 PN-0025
        self.post_site("/admin/advance")
        self.collector.run_batch("incremental")

        # PN-0003：两个版本，旧版本可追溯；分类跟随新版本变为服务类
        ann3 = self.db.get_announcement_by_no("PN-0003")
        versions = self.db.list_versions(ann3["id"])
        self.assertEqual(len(versions), 2)
        self.assertEqual({v["change_type"] for v in versions}, {"initial", "revised"})
        v1 = [v for v in versions if v["version_no"] == 1][0]
        self.assertEqual(v1["is_current"], 0)
        self.assertIn("沥青路面", v1["content"])  # 旧版本内容仍可查
        self.assertEqual(v1["category"], "工程类")
        self.assertEqual(ann3["category"], "服务类")  # 分类跟随当前版本

        # 搜索索引跟随版本：新词命中，旧词不再命中
        hits = self.db.search("智慧城市")
        self.assertEqual([h["source_no"] for h in hits], ["PN-0003"])
        self.assertEqual(self.db.search("沥青路面"), [])

        # PN-0005：撤回，搜索不到，但历史版本完整保留
        ann5 = self.db.get_announcement_by_no("PN-0005")
        self.assertEqual(ann5["status"], "withdrawn")
        v5 = self.db.list_versions(ann5["id"])
        self.assertEqual({v["change_type"] for v in v5}, {"initial", "withdrawn"})
        self.assertEqual(self.db.search("绿化养护"), [])

        # PN-0025：新增公告被采到
        ann25 = self.db.get_announcement_by_no("PN-0025")
        self.assertIsNotNone(ann25)
        self.assertEqual(ann25["category"], "货物类")
        self.assertEqual(self.ann_count(), 25)

    def test_withdrawn_version_keeps_content_hash(self):
        self.collector.run_batch("full")
        self.post_site("/admin/advance")
        self.collector.run_batch("incremental")
        ann5 = self.db.get_announcement_by_no("PN-0005")
        withdrawn_v = [v for v in self.db.list_versions(ann5["id"])
                       if v["change_type"] == "withdrawn"][0]
        self.assertTrue(withdrawn_v["is_current"])
        self.assertIn("绿化养护", withdrawn_v["content"])  # 撤回版本存档了内容


class TestCursorExpiry(PipelineTestBase):
    def test_cursor_expired_recovers_automatically(self):
        # 采 1 页后崩溃，断点保存了游标
        self.collector.fail_after_pages = 1
        with self.assertRaises(SimulatedCrash):
            self.collector.run_batch("full")
        self.assertIsNotNone(self.db.get_checkpoint("list_cursor"))

        # 站点侧游标失效（nonce 轮换），续跑时旧游标返回 410
        self.post_site("/admin/expire_cursors")
        self.collector.fail_after_pages = None
        bid = self.collector.run_batch("incremental")
        batch = self.db.get_batch(bid)

        # 采集器自动从头重建游标并完成全量，数据无重复
        self.assertGreaterEqual(batch["cursor_rebuilds"], 1)
        self.assertEqual(self.ann_count(), 24)
        self.assertIsNone(self.db.get_checkpoint("list_cursor"))


class TestDuplicateCallback(PipelineTestBase):
    def test_duplicate_callback_processed_once(self):
        self.collector.run_batch("full")
        self.post_site("/admin/advance")

        payload = {"callback_id": "cb-test-1", "event": "announcement.changed",
                   "source_no": "PN-0003", "ts": "2026-11-10T10:00:00"}
        r1 = self.collector.handle_callback(payload)
        self.assertEqual(r1, "processed")
        ann3 = self.db.get_announcement_by_no("PN-0003")
        self.assertEqual(len(self.db.list_versions(ann3["id"])), 2)

        # 同一 callback_id 重复投递：幂等返回 duplicate，不重复处理
        r2 = self.collector.handle_callback(payload)
        self.assertEqual(r2, "duplicate")
        self.assertEqual(len(self.db.list_versions(ann3["id"])), 2)
        cbs = self.db.list_callbacks()
        self.assertEqual(len(cbs), 1)
        self.assertEqual(cbs[0]["result"], "processed")


class TestRateLimiter(unittest.TestCase):
    def test_token_bucket_paces_requests(self):
        now = [0.0]
        slept = []

        def fake_sleep(s):
            slept.append(round(s, 3))
            now[0] += s

        limiter = RateLimiter(rate_per_sec=2.0, burst=1,
                              time_func=lambda: now[0], sleep_func=fake_sleep)
        self.assertEqual(limiter.acquire(), 0.0)  # 初始令牌立即获得
        limiter.acquire()
        limiter.acquire()
        self.assertEqual(slept, [0.5, 0.5])  # 2 个/秒 → 每个后续请求等 0.5s
        self.assertAlmostEqual(now[0], 1.0)

    def test_real_limiter_roughly_paces(self):
        import time
        limiter = RateLimiter(rate_per_sec=50.0)
        start = time.monotonic()
        for _ in range(6):
            limiter.acquire()
        elapsed = time.monotonic() - start
        self.assertGreater(elapsed, 0.08)  # 6 个请求 @50/s ≈ 0.1s


if __name__ == "__main__":
    unittest.main()
