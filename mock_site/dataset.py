"""模拟采购站点的数据集与时间线脚本。

确定性生成 24 条公告，并预置以下故障/变更场景：
- 列表分页边界重复上页最后一条（验证重复公告合并）
- att-0007-a 前 2 次下载返回 500（验证部分下载失败 + 幂等重试）
- att-0008-a 内容损坏，下载正常但无法解析（验证隔离区）
- 时间线推进后：PN-0003 修订正文、PN-0005 撤回、新增 PN-0025
"""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field

# 模拟的附件文档格式：PKCDOC 魔数 + UTF-8 文本
MAGIC = b"PKCDOC\n"

CATEGORIES = {
    "工程类": ["工程", "施工", "建设", "改造", "市政"],
    "货物类": ["设备", "物资", "供货", "采购"],
    "服务类": ["服务", "咨询", "监理", "运维"],
}
CAT_NAMES = list(CATEGORIES.keys())


@dataclass
class SiteAttachment:
    att_id: str
    name: str
    body: bytes
    fail_times: int = 0   # 前 N 次下载请求返回 500
    downloads: int = 0    # 站点侧收到的下载请求总数（供幂等性断言）

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()

    @property
    def size(self) -> int:
        return len(self.body)


@dataclass
class SiteAnnouncement:
    source_no: str
    title: str
    content: str
    version_no: int = 1
    withdrawn: bool = False
    published_at: str = "2026-10-01T09:00:00"
    updated_at: str = "2026-10-01T09:00:00"
    attachments: list[SiteAttachment] = field(default_factory=list)

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(f"{self.title}\n{self.content}".encode("utf-8")).hexdigest()

    def summary(self) -> dict:
        return {
            "source_no": self.source_no,
            "title": self.title,
            "version_no": self.version_no,
            "withdrawn": self.withdrawn,
            "published_at": self.published_at,
            "updated_at": self.updated_at,
        }

    def detail(self) -> dict:
        d = self.summary()
        d.update({
            "content": self.content,
            "content_hash": self.content_hash,
            "attachments": [
                {
                    "att_id": a.att_id,
                    "name": a.name,
                    "url": f"/attachments/{a.att_id}",
                    "sha256": a.sha256,
                    "size": a.size,
                }
                for a in self.attachments
            ],
        })
        return d


def _make_attachment(att_id: str, label: str, corrupt: bool = False, fail_times: int = 0) -> SiteAttachment:
    if corrupt:
        body = b"\x89PNG\r\n\x1a\n" + f"binary-scan-{att_id}".encode("ascii")
        name = f"{att_id}-扫描件.png"
    else:
        body = MAGIC + f"附件{att_id}：{label}。具体内容详见采购文件。".encode("utf-8")
        name = f"{att_id}-{label}.pcd"
    return SiteAttachment(att_id=att_id, name=name, body=body, fail_times=fail_times)


class SiteState:
    """站点全局状态：公告数据、时间线步进、游标配置、回调配置与统计。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.step = 0
        self.page_size = 5
        self.duplicate_boundary = True   # 翻页时重复上页最后一条
        self.cursor_ttl = 3600.0
        self._nonce_seq = 1
        self.cursor_nonce = "nonce-1"
        self.callback_url: str | None = None
        self.callbacks_sent: list[dict] = []
        self.list_requests = 0
        self.detail_requests = 0
        self.announcements: dict[str, SiteAnnouncement] = {}
        self._build_initial()

    def _build_initial(self) -> None:
        for i in range(1, 25):
            no = f"PN-{i:04d}"
            cat = CAT_NAMES[i % 3]
            kws = CATEGORIES[cat]
            kw = kws[i % len(kws)]
            title = f"{kw}项目采购公告（第{i:02d}号）"
            content = (
                f"本单位就{kw}项目进行公开采购，编号{no}，欢迎符合条件的供应商参加。"
                f"项目类别：{cat}。"
            )
            if no == "PN-0003":
                content += "施工范围含沥青路面摊铺与路基整修。"
            if no == "PN-0005":
                content += "服务内容为城区绿化养护。"
            atts = []
            for j in range(i % 3):
                att_id = f"att-{i:04d}-{chr(97 + j)}"
                atts.append(_make_attachment(att_id, f"{kw}项目文件{j + 1}"))
            day = (i % 28) + 1
            self.announcements[no] = SiteAnnouncement(
                source_no=no, title=title, content=content,
                published_at=f"2026-10-{day:02d}T09:00:00",
                updated_at=f"2026-10-{day:02d}T09:00:00",
                attachments=atts,
            )
        # 故障注入：PN-0007 的附件前 2 次下载失败
        self.announcements["PN-0007"].attachments[0].fail_times = 2
        # 故障注入：PN-0008 的第一个附件内容损坏（下载正常但无法解析）
        a8 = self.announcements["PN-0008"]
        a8.attachments[0] = _make_attachment("att-0008-a", "货物项目扫描件", corrupt=True)

    # ---------- 列表与详情 ----------
    def ordered(self) -> list[SiteAnnouncement]:
        return sorted(self.announcements.values(), key=lambda a: a.source_no)

    def list_page(self, offset: int):
        """返回 (items, next_offset, has_more)。分页边界故意重复上页最后一条。"""
        items = self.ordered()
        page = items[offset:offset + self.page_size]
        out = []
        if self.duplicate_boundary and 0 < offset < len(items):
            out.append(items[offset - 1].summary())
        out.extend(a.summary() for a in page)
        next_offset = offset + self.page_size
        has_more = next_offset < len(items)
        return out, (next_offset if has_more else None), has_more

    def find_attachment(self, att_id: str) -> SiteAttachment | None:
        for ann in self.announcements.values():
            for att in ann.attachments:
                if att.att_id == att_id:
                    return att
        return None

    # ---------- 时间线 ----------
    def advance(self) -> list[dict]:
        """推进时间线，返回变更事件列表（用于触发回调）。"""
        with self.lock:
            self.step += 1
            events: list[dict] = []
            if self.step == 1:
                a3 = self.announcements["PN-0003"]
                a3.title = "智慧城市运行咨询服务项目采购公告（第03号）"
                a3.content = (
                    "本单位就智慧城市运行咨询服务项目进行公开采购，编号PN-0003，"
                    "服务内容包括平台运维与数据咨询。项目类别：服务类。"
                )
                a3.version_no = 2
                a3.updated_at = "2026-11-10T10:00:00"
                events.append({"event": "announcement.changed", "source_no": "PN-0003"})

                a5 = self.announcements["PN-0005"]
                a5.withdrawn = True
                a5.updated_at = "2026-11-10T10:05:00"
                events.append({"event": "announcement.withdrawn", "source_no": "PN-0005"})

                self.announcements["PN-0025"] = SiteAnnouncement(
                    source_no="PN-0025",
                    title="办公设备供货项目采购公告（第25号）",
                    content="本单位就办公设备供货项目进行公开采购，编号PN-0025。项目类别：货物类。",
                    published_at="2026-11-10T11:00:00",
                    updated_at="2026-11-10T11:00:00",
                    attachments=[_make_attachment("att-0025-a", "办公设备清单")],
                )
                events.append({"event": "announcement.changed", "source_no": "PN-0025"})
            return events

    def expire_cursors(self) -> None:
        """使所有已签发游标失效（模拟站点侧游标过期）。"""
        with self.lock:
            self._nonce_seq += 1
            self.cursor_nonce = f"nonce-{self._nonce_seq}"

    def stats(self) -> dict:
        with self.lock:
            return {
                "step": self.step,
                "announcements": len(self.announcements),
                "list_requests": self.list_requests,
                "detail_requests": self.detail_requests,
                "attachments": {
                    a.att_id: {"downloads": a.downloads, "fail_times": a.fail_times}
                    for ann in self.announcements.values()
                    for a in ann.attachments
                },
                "callbacks_sent": list(self.callbacks_sent),
            }
