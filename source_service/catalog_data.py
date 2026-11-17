"""博物馆藏品示例目录数据。

共三个版本，每版 3 页，每页 2 条藏品（编号、名称、年代、材质）。
版本之间刻意保留/修改/删除部分编号，用于演示发布时移除
"源中已不存在的藏品"。
"""

from __future__ import annotations

# 藏品字段：编号 / 名称 / 年代 / 材质
VERSIONS: dict[str, list[dict[str, str]]] = {
    "v1": [
        {"id": "A001", "name": "青铜饕餮纹鼎", "era": "商代晚期", "material": "青铜"},
        {"id": "A002", "name": "玉琮", "era": "良渚文化", "material": "透闪石玉"},
        {"id": "A003", "name": "唐三彩马", "era": "唐代", "material": "陶胎三彩釉"},
        {"id": "A004", "name": "青瓷莲花尊", "era": "南朝", "material": "青瓷"},
        {"id": "A005", "name": "定窑白瓷碗", "era": "北宋", "material": "白瓷"},
        {"id": "A006", "name": "朱漆戗金奁", "era": "南宋", "material": "木胎漆器"},
    ],
    "v2": [
        # A001 更名（模拟著录修订），A004 被移除，新增 B001/B002
        {"id": "A001", "name": "青铜兽面纹鼎", "era": "商代晚期", "material": "青铜"},
        {"id": "A002", "name": "玉琮", "era": "良渚文化", "material": "透闪石玉"},
        {"id": "A003", "name": "唐三彩马", "era": "唐代", "material": "陶胎三彩釉"},
        {"id": "A005", "name": "定窑白瓷碗（修订）", "era": "北宋", "material": "白瓷"},
        {"id": "A006", "name": "朱漆戗金奁", "era": "南宋", "material": "木胎漆器"},
        {"id": "B001", "name": "青花缠枝莲瓶", "era": "元代", "material": "青花瓷"},
    ],
    "v3": [
        # 再精简：只保留两条并新增 C001，验证旧数据被清除
        {"id": "A002", "name": "玉琮", "era": "良渚文化", "material": "透闪石玉"},
        {"id": "B001", "name": "青花缠枝莲瓶", "era": "元代", "material": "青花瓷"},
        {"id": "C001", "name": "珐琅彩开光瓶", "era": "清乾隆", "material": "铜胎画珐琅"},
    ],
}

PAGE_SIZE = 2


def paginate(version: str) -> list[list[dict[str, str]]]:
    """把某一版本的藏品切成固定的三页（最后一页可能不足）。"""
    items = VERSIONS[version]
    pages: list[list[dict[str, str]]] = []
    for start in range(0, len(items), PAGE_SIZE):
        pages.append(items[start : start + PAGE_SIZE])
    # 演示要求"每版三页"：数据不足时以空页补齐，保证 total_pages 恒为 3
    while len(pages) < 3:
        pages.append([])
    return pages


ALL_VERSIONS = list(VERSIONS.keys())
