"""启动采集系统管理界面。用法: python3 scripts/run_webapp.py [站点URL] [端口]"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from webapp.admin import create_app

if __name__ == "__main__":
    site_url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8901"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8902
    srv = create_app(
        site_url=site_url, port=port,
        db_path="data/collector.db", storage_dir="data/storage",
        rate=20.0,  # 限速：每秒最多 20 个请求
    )
    print(f"管理界面: http://127.0.0.1:{port}  (采集站点: {site_url})")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
