"""
analysis_engine.zones — 运动区间定义
=====================================
心率5区间（ACSM）、速度5区间、踏频4区间、坡度5区间。

所有区间均可通过环境变量覆盖（如 ANALYSIS_MAX_HR）。
"""

import os

# ─── 心率区间 (ACSM 5-Zone 模型) ──────────────
DEFAULT_MAX_HR = int(os.environ.get("ANALYSIS_MAX_HR", "194"))

HR_ZONES = {
    "Z1_恢复":      (0.50, 0.60, "恢复/热身"),
    "Z2_有氧基础":   (0.60, 0.70, "有氧基础训练"),
    "Z3_有氧进阶":   (0.70, 0.80, "有氧进阶训练"),
    "Z4_乳酸阈值":   (0.80, 0.90, "乳酸阈值训练"),
    "Z5_无氧极限":   (0.90, 1.00, "无氧极限训练"),
}

HR_ZONE_ORDER = ["Z1_恢复", "Z2_有氧基础", "Z3_有氧进阶", "Z4_乳酸阈值", "Z5_无氧极限"]

# ─── 速度区间 (km/h) ─────────────────────────
SPEED_ZONES = {
    "极慢(<10)":      (0, 10),
    "慢速(10-18)":    (10, 18),
    "中速(18-25)":    (18, 25),
    "快速(25-32)":    (25, 32),
    "高速(>32)":      (32, 999),
}

# ─── 踏频区间 (rpm) ──────────────────────────
CADENCE_ZONES = {
    "低踏频(<60)":    (0, 60),
    "中踏频(60-80)":  (60, 80),
    "高踏频(80-100)": (80, 100),
    "极高踏频(>100)": (100, 999),
}

# ─── 坡度区间 (%) ────────────────────────────
GRADE_ZONES = {
    "下坡(< -3%)":     (-999, -3),
    "平路/缓坡(-3~3%)": (-3, 3),
    "爬坡(3~6%)":      (3, 6),
    "陡坡(6~10%)":     (6, 10),
    "极陡坡(>10%)":    (10, 999),
}


def classify_zone(value, zones):
    """将数值归类到区间，返回 (zone_name, pct_of_range)。

    Args:
        value: 数值（心率bpm/速度kmh/踏频rpm/坡度%）
        zones: 区间定义 dict {name: (lo, hi)}

    Returns:
        (zone_name, pct) — pct 表示在该区间内的相对位置 (0~1)
    """
    if value is None:
        return None, 0
    for name, (lo, hi) in zones.items():
        if lo <= value < hi:
            mid = (lo + hi) / 2
            pct = (value - lo) / (hi - lo) if hi != lo else 0.5
            return name, pct
    # 超出范围，归到最后一个区间
    last_name = list(zones.keys())[-1]
    return last_name, 1.0
