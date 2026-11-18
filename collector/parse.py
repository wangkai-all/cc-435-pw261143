"""附件解析器：模拟的 .pcd 文档格式（PKCDOC 魔数 + UTF-8 文本）。

解析失败抛 ParseError，由采集流程将附件送入隔离区，
绝不把解析失败伪装成下载成功。
"""

MAGIC = b"PKCDOC\n"


class ParseError(Exception):
    """附件无法解析（格式损坏或不支持）。"""


def parse_attachment(data: bytes) -> str:
    if not data.startswith(MAGIC):
        raise ParseError("无法识别的附件格式：缺少 PKCDOC 文件头")
    try:
        return data[len(MAGIC):].decode("utf-8")
    except UnicodeDecodeError as e:
        raise ParseError(f"附件文本解码失败: {e}") from e
