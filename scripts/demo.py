"""端到端演示：启动模拟站点 + 管理界面，自动跑完整采集流程。

流程：首次全量采集（含部分附件失败）→ 重试失败附件 → 推进站点时间线
（修订 PN-0003、撤回 PN-0005、新增 PN-0025，站点重复推送回调）→ 再次采集。

用法: python3 scripts/demo.py
"""
import json
import os
import shutil
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mock_site.server import create_server
from webapp.admin import create_app

SITE_PORT = 8901
APP_PORT = 8902
SITE = f"http://127.0.0.1:{SITE_PORT}"
APP = f"http://127.0.0.1:{APP_PORT}"


def post(url):
    req = urllib.request.Request(url, data=b"{}")
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.status


def get_json(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read())


def step(msg):
    print(f"\n=== {msg} ===")


def main():
    # 清理旧的演示数据，保证可重复运行
    shutil.rmtree("data/demo", ignore_errors=True)
    os.makedirs("data/demo", exist_ok=True)

    site = create_server(port=SITE_PORT)
    threading.Thread(target=site.serve_forever, daemon=True).start()
    app = create_app(site_url=SITE, port=APP_PORT,
                     db_path="data/demo/collector.db",
                     storage_dir="data/demo/storage", rate=30.0)
    threading.Thread(target=app.serve_forever, daemon=True).start()
    time.sleep(0.5)

    step("1. 首次全量采集（att-0007-a 前 2 次下载失败，att-0008-a 损坏进隔离区）")
    post(f"{APP}/actions/collect")
    time.sleep(0.2)

    step("2. 重试失败附件（第 1 次仍失败，第 2 次成功 —— 幂等重试）")
    post(f"{APP}/actions/retry")
    post(f"{APP}/actions/retry")

    step("3. 推进站点时间线：修订 PN-0003、撤回 PN-0005、新增 PN-0025")
    print("    （站点会向采集系统重复推送同一 callback_id 的回调，验证幂等去重）")
    post(f"{APP}/actions/advance")
    time.sleep(0.2)

    step("4. 再次增量采集（版本切换，搜索索引与分类跟随当前版本）")
    post(f"{APP}/actions/collect")

    step("5. 站点侧统计（附件下载次数，验证幂等：成功附件只下载 1 次）")
    stats = get_json(f"{SITE}/admin/state")
    interesting = {k: v for k, v in stats["attachments"].items()
                   if k in ("att-0007-a", "att-0008-a", "att-0002-a")}
    print(json.dumps(interesting, ensure_ascii=False, indent=2))

    print(f"\n演示数据就绪，打开管理界面查看：{APP}")
    print("  /batches        采集批次（含重跑与回调批次）")
    print("  /announcements  公告列表 → 详情看版本历史")
    print("  /attachments    附件状态")
    print("  /quarantine     隔离区")
    print("  /search?q=智慧城市   搜索（只命中当前版本）")
    print("  /reruns         重跑结果")
    print("  /callbacks      回调记录（重复回调已去重）")
    print("\nCtrl+C 退出。")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
