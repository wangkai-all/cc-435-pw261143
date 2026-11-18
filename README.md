# 采购公告采集系统

从本地模拟站点采集采购公告（分页列表、附件、修订版本），支持断点续采、限速、
重复公告合并、附件幂等重试；公告撤回/修订时历史版本可追溯，搜索索引与分类
跟随当前版本；附件解析失败进入隔离区而非伪装成功。纯 Python 标准库实现，无第三方依赖。

## 快速开始

```bash
# 一键端到端演示（启动站点+界面，自动跑完整流程）
python3 scripts/demo.py
# 然后打开 http://127.0.0.1:8902

# 或分开启动
python3 scripts/run_mock_site.py 8901          # 模拟采购站点
python3 scripts/run_webapp.py http://127.0.0.1:8901 8902   # 采集系统管理界面

# 运行测试（19 个用例）
python3 -m unittest discover -s tests -v
```

## 目录结构

```
mock_site/          本地模拟采购站点（被采集方）
  dataset.py          确定性数据集 + 故障注入 + 时间线脚本
  server.py           游标分页列表 / 详情 / 附件下载 / 管理接口
collector/          采集核心
  db.py               SQLite schema + 数据访问（批次/断点/版本/附件/隔离区/回调/FTS）
  pipeline.py         采集主流程：翻页→详情→版本同步→附件下载解析
  client.py           HTTP 客户端（所有请求经过限速器）
  ratelimit.py        令牌桶限速器
  classify.py         关键词规则分类器（跟随当前版本）
  parse.py            附件解析器（PKCDOC 格式；失败抛 ParseError）
webapp/admin.py     管理界面（页面 + 回调接收 + 操作入口）
scripts/            启动脚本与端到端 demo
tests/              场景测试
```

## 核心机制

### 断点续采
每页处理完整后把 `next_cursor` 写入 `checkpoints` 表。采集中途崩溃（批次标记
`failed`）后，下一批从断点游标继续；已采数据靠 `source_no` 唯一约束幂等合并，
不会产生重复公告。

### 游标过期
站点游标是签名令牌（offset + 过期时间 + nonce）。游标失效时站点返回 410，
采集器自动从头重建游标继续（批次统计 `cursor_rebuilds` 计数），数据靠 upsert 幂等。

### 限速
`RateLimiter` 令牌桶套在 HTTP 客户端上，列表/详情/附件所有请求统一限速
（默认 20 req/s，可配置）。

### 重复公告合并
站点在分页边界重复上页最后一条（模拟真实站点翻页漂移）。采集端按
`source_no` 合并：重复条目只增加 `seen_count`，批次统计 `duplicate_count`。

### 附件幂等重试
- 下载后校验 sha256，写 `.tmp` 临时文件再 `os.replace` 原子落盘
- 状态为 `ok` 且校验匹配的附件直接跳过，不重复下载（站点侧下载计数可证）
- 下载失败标记 `failed`，可通过「重试失败附件」批次（`retry_attachments`）重跑，
  也可在普通批次中自动重试

### 版本追溯 / 撤回
- 每次内容变化追加 `announcement_versions` 行（initial/revised/withdrawn/restored），
  旧版本 `is_current=0` 但完整保留，页面可查历史版本正文
- 撤回时追加 `withdrawn` 存档版本，公告移出搜索索引，历史版本仍可追溯
- 搜索索引（FTS5 trigram）与分类结果只跟随当前版本：修订后旧内容搜不到、
  新内容可搜，分类按新正文重算

### 隔离区
附件下载成功但解析失败（如格式损坏）时进入 `quarantine`，状态为
`quarantined` 而非 `ok`——绝不伪装成成功。修复文件或解析器后可通过
「重新解析」批次（`reprocess`）恢复为 `ok` 并关闭隔离单。

### 重复回调
站点推送变更通知时会把同一 `callback_id` 投递两次。`callbacks.callback_id`
唯一约束保证只处理一次，重复投递返回 `duplicate`，不重复触发采集。

## 页面

| 路径 | 内容 |
|---|---|
| `/` | 仪表盘：统计概览 + 操作入口 + 最近批次 |
| `/batches` | 采集批次（含统计、游标重建次数、错误） |
| `/batches/{id}` | 批次详情：附件事件流 |
| `/announcements` | 公告列表（状态/分类过滤） |
| `/announcements/{id}` | 公告详情：版本历史 + 附件状态 |
| `/versions/{id}` | 单个版本正文（含历史版本） |
| `/attachments` | 附件状态总览（按状态过滤） |
| `/quarantine` | 隔离区：待处理/已解决 + 重新解析 |
| `/search` | 搜索（只命中当前版本，撤回不可搜） |
| `/reruns` | 重跑结果（附件重试/隔离重解析/回调补采批次） |
| `/callbacks` | 回调记录（重复回调已去重） |

## 模拟站点内置场景

| 场景 | 注入方式 |
|---|---|
| 部分下载失败 | `att-0007-a` 前 2 次下载返回 500，第 3 次成功 |
| 解析失败 | `att-0008-a` 内容损坏（无 PKCDOC 头），下载正常但无法解析 |
| 重复公告 | 列表翻页边界重复上页最后一条 |
| 游标过期 | `POST /admin/expire_cursors` 使已签发游标失效（410） |
| 修订/撤回/新增 | `POST /admin/advance`：PN-0003 修订、PN-0005 撤回、新增 PN-0025 |
| 重复回调 | 时间线推进时同一 `callback_id` 投递两次 |

## 测试覆盖

`tests/test_pipeline.py`：全量采集与合并、二次采集幂等、崩溃断点续采、
附件部分失败与幂等重试（站点侧下载次数断言）、隔离区（不伪装成功 + 修复后重解析）、
修订/撤回后版本追溯与索引/分类跟随、游标过期自动重建、重复回调幂等、限速器行为。

`tests/test_webapp.py`：全部页面渲染、公告/版本/批次详情页、回调端到端
（站点重复投递 → 只处理一次）、搜索跟随当前版本。
