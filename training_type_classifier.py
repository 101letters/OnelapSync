"""
训练类型分类器 — 规则决策树

基于心率区间分布（+ 可选功率区间）的规则分类器。

优先级（从高到低）:
  1. Z5 > 5%          OR  功率 Z5+Z6 > 3%          → anaerobic  (无氧)
  2. Z4 > 20%         OR  功率 Z4 > 15%            → threshold  (阈值)
  3. Z3 > 25%                                      → tempo      (节奏)
  4. Z4 > 10% AND Z5 > 2%                          → vo2max     (最大摄氧量)
  5. Z2 > 40%                                      → endurance  (耐力)
  6. Z1+Z2 > 70% AND avg_hr < 130                  → recovery   (恢复)
  7. 默认                                          → endurance

返回英文训练类型字符串，可配合 get_training_type_label() 获取中文标签。
"""

import json
from typing import Dict, Any


def classify_training_type(analysis_row: dict) -> str:
    """
    基于心率区间分布 + 功率区间分布的规则分类器。

    参数:
        analysis_row: 包含 hr_zones / power_zones / basic_stats 的字典

    返回:
        训练类型字符串: recovery | endurance | tempo | threshold | vo2max | anaerobic
    """
    # ── 解包数据 ──────────────────────────────────────────
    hr_zones: Dict[str, Any] = {}
    raw_hr = analysis_row.get("hr_zones")
    if raw_hr:
        hr_zones = json.loads(raw_hr) if isinstance(raw_hr, str) else raw_hr

    power_zones: Dict[str, Any] = {}
    raw_pw = analysis_row.get("power_zones")
    if raw_pw:
        power_zones = json.loads(raw_pw) if isinstance(raw_pw, str) else raw_pw

    basic: Dict[str, Any] = {}
    raw_basic = analysis_row.get("basic_stats")
    if raw_basic:
        basic = json.loads(raw_basic) if isinstance(raw_basic, str) else raw_basic

    # ── 提取各区间百分比 ──────────────────────────────────

    def get_zone_pct(zones_dict: dict, zone_prefix: str) -> float:
        """从 zones dict 提取某区百分比。"""
        for k, v in zones_dict.items():
            if k.startswith(zone_prefix) and isinstance(v, dict):
                return v.get("pct", 0) or 0
        return 0.0

    z1_pct = get_zone_pct(hr_zones, "Z1")
    z2_pct = get_zone_pct(hr_zones, "Z2")
    z3_pct = get_zone_pct(hr_zones, "Z3")
    z4_pct = get_zone_pct(hr_zones, "Z4")
    z5_pct = get_zone_pct(hr_zones, "Z5")

    pz4_pct = get_zone_pct(power_zones, "Z4")
    pz5_pct = get_zone_pct(power_zones, "Z5")
    pz6_pct = get_zone_pct(power_zones, "Z6")

    avg_hr = basic.get("avg_heart_rate", 0) or 0

    # ── 决策树（按优先级） ────────────────────────────────

    # 1. 无氧：心率 Z5 占比极高，或功率 Z5+Z6 有明显占比
    if z5_pct > 5 or (pz5_pct + pz6_pct) > 3:
        return "anaerobic"

    # 2. 阈值：心率 Z4 占比极高，或功率 Z4 有明显占比
    if z4_pct > 20 or pz4_pct > 15:
        return "threshold"

    # 3. 节奏：心率 Z3 为主
    if z3_pct > 25:
        return "tempo"

    # 4. 最大摄氧量：Z4 明显升高（10-20%）且 Z5 有一定占比（2-5%），
    #    但未达到阈值或无氧门槛。需要两个区同时满足，避免误判节奏训练。
    if z4_pct > 10 and z5_pct > 2:
        return "vo2max"

    # 5. 耐力：Z2 为主
    if z2_pct > 40:
        return "endurance"

    # 6. 恢复：低强度、低心率
    if (z1_pct + z2_pct) > 70 and avg_hr < 130:
        return "recovery"

    # 7. 兜底
    return "endurance"


def get_training_type_label(ttype: str) -> str:
    """返回训练类型的中文标签 + emoji。"""
    labels = {
        "recovery":   "🔋 恢复骑行",
        "endurance":  "🚴 耐力训练",
        "tempo":      "⚡ 节奏训练",
        "threshold":  "🔥 阈值训练",
        "vo2max":     "💨 最大摄氧量训练",
        "anaerobic":  "💥 无氧训练",
    }
    return labels.get(ttype, "🚴 骑行")
