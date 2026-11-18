"""启动本地模拟采购站点。用法: python3 scripts/run_mock_site.py [端口]"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mock_site.server import create_server

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8901
    srv = create_server(port=port)
    print(f"模拟采购站点: http://127.0.0.1:{port}")
    print("  GET  /api/announcements          公告列表（游标分页）")
    print("  GET  /api/announcements/{编号}   公告详情")
    print("  GET  /attachments/{附件ID}       附件下载（含故障注入）")
    print("  POST /admin/advance              推进时间线（修订/撤回/新增）")
    print("  POST /admin/expire_cursors       使游标过期")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
