"""基于关键词规则的公告分类器。分类结果跟随公告当前版本。"""

CATEGORIES = {
    "工程类": ["工程", "施工", "建设", "改造", "市政", "道路"],
    "货物类": ["设备", "物资", "供货", "货物", "采购"],
    "服务类": ["服务", "咨询", "监理", "运维", "设计"],
}


def classify(title: str, content: str) -> str:
    text = f"{title}\n{content}"
    scores = {cat: sum(text.count(kw) for kw in kws) for cat, kws in CATEGORIES.items()}
    best = max(scores.values())
    if best == 0:
        return "其他"
    # 平分时按固定顺序取先者，保证结果确定性
    for cat in CATEGORIES:
        if scores[cat] == best:
            return cat
    return "其他"
