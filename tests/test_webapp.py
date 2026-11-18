"""管理界面测试：页面可用性 + 回调端到端（站点重复投递 → 幂等去重）。"""
import json
import os
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from urllib.parse import quote

from mock_site.server import create_server
from webapp.admin import create_app


class WebappTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.site = create_server(port=0)
        self.site_port = self.site.server_address[1]
        threading.Thread(target=self.site.serve_forever, daemon=True).start()
        self.app = create_app(
            site_url=f"http://127.0.0.1:{self.site_port}", port=0,
            db_path=os.path.join(self.tmp.name, "collector.db"),
            storage_dir=os.path.join(self.tmp.name, "storage"),
            rate=100000,
        )
        self.app_port = self.app.server_address[1]
        threading.Thread(target=self.app.serve_forever, daemon=True).start()

    def tearDown(self):
        self.app.shutdown()
        self.app.server_close()
        self.site.shutdown()
        self.site.server_close()
        self.tmp.cleanup()

    def get(self, path):
        url = f"http://127.0.0.1:{self.app_port}{quote(path, safe='/?=&')}"
        with urllib.request.urlopen(url) as r:
            return r.status, r.read().decode("utf-8")

    def post(self, path, payload=None):
        data = json.dumps(payload or {}).encode("utf-8")
        req = urllib.request.Request(f"http://127.0.0.1:{self.app_port}{path}", data=data,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.status, r.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            if e.code == 303:
                return e.code, ""
            raise


class TestPages(WebappTestBase):
    def test_all_pages_render(self):
        self.post("/actions/collect")
        for path in ["/", "/batches", "/announcements", "/attachments",
                     "/quarantine", "/search?q=采购", "/reruns", "/callbacks"]:
            status, body = self.get(path)
            self.assertEqual(status, 200, path)
            self.assertIn("采购公告采集系统", body)

    def test_announcement_detail_and_version_pages(self):
        self.post("/actions/collect")
        db = self.app.ctx.db
        ann = db.get_announcement_by_no("PN-0003")
        status, body = self.get(f"/announcements/{ann['id']}")
        self.assertEqual(status, 200)
        self.assertIn("版本历史", body)
        v = db.list_versions(ann["id"])[0]
        status, body = self.get(f"/versions/{v['id']}")
        self.assertEqual(status, 200)
        self.assertIn("正文", body)

    def test_batch_detail_page(self):
        self.post("/actions/collect")
        db = self.app.ctx.db
        batch = db.list_batches()[0]
        status, body = self.get(f"/batches/{batch['id']}")
        self.assertEqual(status, 200)
        self.assertIn("附件事件", body)


class TestCallbackEndToEnd(WebappTestBase):
    def test_site_advance_triggers_duplicate_callbacks_deduped(self):
        self.post("/actions/collect")
        # 推进时间线：站点对每个变更事件把同一 callback_id 投递两次
        self.post("/actions/advance")

        db = self.app.ctx.db
        # 3 个变更事件 → 3 条回调记录；重复投递被幂等去重
        cbs = db.list_callbacks()
        self.assertEqual(len(cbs), 3)
        self.assertTrue(all(c["result"] == "processed" for c in cbs))

        # 回调触发的单条采集已生效
        ann3 = db.get_announcement_by_no("PN-0003")
        self.assertEqual(len(db.list_versions(ann3["id"])), 2)
        ann5 = db.get_announcement_by_no("PN-0005")
        self.assertEqual(ann5["status"], "withdrawn")
        self.assertIsNotNone(db.get_announcement_by_no("PN-0025"))

        # 回调批次出现在重跑结果中
        reruns = db.list_batches(kinds=["callback"])
        self.assertEqual(len(reruns), 3)

    def test_callback_api_rejects_bad_payload(self):
        req = urllib.request.Request(f"http://127.0.0.1:{self.app_port}/callbacks",
                                     data=b"{}", headers={"Content-Type": "application/json"})
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req)
        self.assertEqual(cm.exception.code, 400)


class TestSearchPage(WebappTestBase):
    def test_search_follows_current_version(self):
        self.post("/actions/collect")
        status, body = self.get("/search?q=沥青路面")
        self.assertIn("PN-0003", body)
        self.post("/actions/advance")
        status, body = self.get("/search?q=沥青路面")
        self.assertNotIn("PN-0003", body)  # 旧版本内容不再命中
        status, body = self.get("/search?q=智慧城市")
        self.assertIn("PN-0003", body)


if __name__ == "__main__":
    unittest.main()
