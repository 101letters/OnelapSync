"""
训练负荷计算引擎 — Bannister EWMA (CTL/ATL/TSB)

核心算法：
  - CTL (Chronic Training Load): τ=42 天, 代表长期体能
  - ATL (Acute Training Load):  τ=7 天,  代表短期疲劳
  - TSB (Training Stress Balance): TSB = CTL - ATL
  - Edwards TRIMP: 心率区间加权求和

公式：
  new = old + λ × (trimp − old)
  λ = 1 − exp(−1/τ)
"""

import math
from typing import List, Tuple, Optional, Dict, Any

# ── 默认半衰期 ──────────────────────────────────────────────
TAU_CTL = 42   # Fitness 半衰期 42 天
TAU_ATL = 7    # Fatigue 半衰期  7 天


# ── TRIMP 计算 ──────────────────────────────────────────────

def calc_edwards_trimp_from_zones(hr_zones: dict) -> float:
    """
    从心率区间数据计算 Edwards TRIMP。

    hr_zones: {
        "Z1_恢复":   {"count": 1200, "pct": 20.0},
        "Z2_有氧基础": {"count": 2400, "pct": 40.0},
        ...
    }
    返回 TRIMP-min（分钟）。
    """
    weights = {
        "Z1": 1, "Z2": 2, "Z3": 3, "Z4": 4, "Z5": 5,
    }
    total_seconds = 0.0
    for zone_key, zdata in hr_zones.items():
        if not isinstance(zdata, dict):
            continue
        # 支持 "Z1_恢复" 和 "Z1" 两种 key
        base_key = zone_key.split("_")[0] if "_" in zone_key else zone_key
        weight = weights.get(base_key, 1)
        count = zdata.get("count", 0) or 0
        total_seconds += weight * count
    return total_seconds / 60.0


def calc_trimp_from_basic(basic: dict, hr_zones: dict) -> float:
    """
    从 basic_stats 和 hr_zones 计算 TRIMP。

    优先用区间数据（Edwards），回退用心率区间时间 × 心率百分比。
    """
    # 优先：Edwards TRIMP
    if hr_zones and any(
        (z.get("count", 0) or 0) > 0
        for z in hr_zones.values()
        if isinstance(z, dict)
    ):
        return calc_edwards_trimp_from_zones(hr_zones)

    # 回退：时长 × 心率比例
    total_time_s = basic.get("total_moving_time_s", 0) or basic.get("total_time_s", 0) or 0
    avg_hr = basic.get("avg_heart_rate", 0) or 0
    if total_time_s > 0 and avg_hr > 0:
        rest_hr = 45
        max_hr = 194
        ratio = (avg_hr - rest_hr) / (max_hr - rest_hr)
        return ratio * (total_time_s / 60.0)
    return 0.0


# ── EWMA 核心 ───────────────────────────────────────────────

def calc_ewma(
    trimp_values: List[float],
    tau_days: int,
    initial: float = 0.0,
) -> List[float]:
    """
    指数加权移动平均。

    trimp_values: 按日期排序的每日 TRIMP 列表
    tau_days:    半衰期天数（CTL=42, ATL=7）
    initial:     初始值（第 0 天的基线）

    公式：new = old + λ × (trimp − old)
          λ = 1 − exp(−1/τ)
    """
    if not trimp_values:
        return []
    lam = 1.0 - math.exp(-1.0 / tau_days)
    result: List[float] = []
    current = initial
    for trimp in trimp_values:
        new = current + lam * (trimp - current)
        result.append(new)
        current = new
    return result


def compute_ctl_atl_tsb(
    trimp_values: List[float],
    tau_ctl: int = TAU_CTL,
    tau_atl: int = TAU_ATL,
) -> List[Tuple[float, float, float]]:
    """
    从每日 TRIMP 序列递推 CTL/ATL/TSB。

    返回 [(ctl, atl, tsb), ...] 与输入一一对应。
    """
    ctl_values = calc_ewma(trimp_values, tau_ctl)
    atl_values = calc_ewma(trimp_values, tau_atl)
    return [(c, a, c - a) for c, a in zip(ctl_values, atl_values)]


# ── 工具函数 ────────────────────────────────────────────────

def estimate_initial_ctl(
    trimp_values: List[float],
    lookback: int = 42,
) -> float:
    """
    估算初始 CTL（历史数据不足时用）。

    取前 lookback 天的 TRIMP 均值作为初始值。
    """
    if not trimp_values:
        return 0.0
    usable = trimp_values[:lookback]
    return sum(usable) / len(usable)


# ── 数据库写入接口 ──────────────────────────────────────────

def update_training_load_from_db(
    db,
    date_str: str,
    trimp: float,
    force_recalc: bool = False,
) -> dict:
    """
    写入或更新 training_load 表，全链重算 CTL/ATL/TSB。

    参数:
        db:           数据库访问对象（需提供 get_training_load /
                       get_training_load_range / save_training_load）
        date_str:     日期字符串 "YYYY-MM-DD"
        trimp:        当日 TRIMP（会累加到已有值上）
        force_recalc: 是否强制重算（覆盖已有值）

    返回:
        {"ctl": float, "atl": float, "tsb": float}
    """
    # 1. 累加当天 TRIMP
    existing = db.get_training_load(date_str)
    if existing and not force_recalc:
        new_trimp = existing["trimp"] + trimp
        new_count = existing["activity_count"] + 1
    else:
        new_trimp = trimp
        new_count = 1

    # 2. 获取所有历史 TRIMP 数据（含当天）
    all_loads = db.get_training_load_range("2000-01-01", "2099-12-31")

    # 3. 构建有序 TRIMP 序列
    date_trimp_map = {row["date"]: row["trimp"] for row in all_loads}
    date_trimp_map[date_str] = new_trimp
    sorted_dates = sorted(date_trimp_map.keys())
    trimp_sequence = [date_trimp_map[d] for d in sorted_dates]

    # 4. 估算初始值
    ctl = estimate_initial_ctl(trimp_sequence)
    atl = estimate_initial_ctl(trimp_sequence[:7]) if len(trimp_sequence) >= 7 else ctl

    # 5. EWMA 递推
    lam_ctl = 1.0 - math.exp(-1.0 / TAU_CTL)
    lam_atl = 1.0 - math.exp(-1.0 / TAU_ATL)

    current_ctl, current_atl = float(ctl), float(atl)
    for tr in trimp_sequence:
        current_ctl += lam_ctl * (tr - current_ctl)
        current_atl += lam_atl * (tr - current_atl)

    result_ctl = current_ctl
    result_atl = current_atl
    result_tsb = result_ctl - result_atl

    # 6. 只写当天
    db.save_training_load(
        date_str,
        new_trimp,
        round(result_ctl, 1),
        round(result_atl, 1),
        round(result_tsb, 1),
        new_count,
    )

    return {
        "ctl": round(result_ctl, 1),
        "atl": round(result_atl, 1),
        "tsb": round(result_tsb, 1),
    }
