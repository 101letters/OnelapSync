"""
统一指标计算引擎（metrics_engine）
===================================
纯函数 + 无状态。输入简单数据结构，输出简单值或 dict。
所有消费者（coach、推送层、报告脚本）从这里取统一上下文。

核心指标：
  - Edwards TRIMP、CTL/ATL/TSB、ACWR
  - 强度因子（IF）、训练质量分类
  - 睡眠利用率、恢复指数
  - 趋势判断

设计原则：
  1. 纯函数 — 不碰 DB、不碰外部
  2. 输入输出为简单数据类型（dict / list / float）
  3. build_training_context() 为唯一聚合入口

数据来源：
  training_load_calculator.py（本模块基于其基础扩展）

时间常数：
  CTL τ=42 天（体能）
  ATL τ=7 天（疲劳）
"""

import math
from typing import Any, Dict, List, Optional, Tuple

# ── 默认参数 ──────────────────────────────────────────────
TAU_CTL = 42      # 慢性训练负荷半衰期（天）
TAU_ATL = 7       # 急性训练负荷半衰期（天）
DEFAULT_REST_HR = 45
DEFAULT_MAX_HR = 194
DEFAULT_THRESHOLD_HR = 170

# ── TSB 区间标签 ──────────────────────────────────────────
TSB_ZONES = [
    ("high_risk",    float("-inf"), -25, "高风险，建议休息"),
    ("fatigued",     -25,           -10, "疲劳，控制强度"),
    ("neutral",      -10,           10,  "平衡，正常训练"),
    ("fresh",        10,            25,  "过度恢复，可高强度"),
    ("over_fresh",   25,            float("inf"), "非常恢复"),
]

# ── 睡眠恢复指数权重 ─────────────────────────────────────
SLEEP_DURATION_WEIGHT = 0.4
SLEEP_DEEP_WEIGHT = 0.35
SLEEP_CONTINUITY_WEIGHT = 0.25

MIN_RECOVERY_DAYS_FOR_STABLE = 7  # 至少 7 天数据才出稳定指标
COLD_START_DAYS = 42              # 冷启动标识阈值


# ===================================================================
#  第一部分：TRIMP 计算
# ===================================================================

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
    weights = {"Z1": 1, "Z2": 2, "Z3": 3, "Z4": 4, "Z5": 5}
    total_seconds = 0.0
    for zone_key, zdata in hr_zones.items():
        if not isinstance(zdata, dict):
            continue
        base_key = zone_key.split("_")[0] if "_" in zone_key else zone_key
        weight = weights.get(base_key, 1)
        count = zdata.get("count", 0) or 0
        total_seconds += weight * count
    return total_seconds / 60.0


def calc_trimp_from_basic(basic: dict, hr_zones: dict) -> float:
    """
    从 basic_stats 和 hr_zones 计算 TRIMP。

    优先用 Edwards 区间法，回退用心率比例法。
    """
    if hr_zones and any(
        (z.get("count", 0) or 0) > 0
        for z in hr_zones.values()
        if isinstance(z, dict)
    ):
        return calc_edwards_trimp_from_zones(hr_zones)

    total_time_s = (
        basic.get("total_moving_time_s", 0)
        or basic.get("total_time_s", 0)
        or 0
    )
    avg_hr = basic.get("avg_heart_rate", 0) or 0
    if total_time_s > 0 and avg_hr > 0:
        ratio = (avg_hr - DEFAULT_REST_HR) / (DEFAULT_MAX_HR - DEFAULT_REST_HR)
        return ratio * (total_time_s / 60.0)
    return 0.0


# ===================================================================
#  第二部分：EWMA — CTL / ATL / TSB
# ===================================================================

def calc_ewma(
    trimp_values: List[float],
    tau_days: int,
    initial: float = 0.0,
) -> List[float]:
    """
    指数加权移动平均。

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
    返回 [(ctl, atl, tsb), ...]。
    """
    ctl_values = calc_ewma(trimp_values, tau_ctl)
    atl_values = calc_ewma(trimp_values, tau_atl)
    return [(c, a, c - a) for c, a in zip(ctl_values, atl_values)]


def estimate_initial_ctl(trimp_values: List[float], lookback: int = 42) -> float:
    """估算初始 CTL（历史数据不足时用）。取前 lookback 天的 TRIMP 均值。"""
    if not trimp_values:
        return 0.0
    usable = trimp_values[:lookback]
    return sum(usable) / len(usable)


# ===================================================================
#  第三部分：ACWR 与 TSB 分类
# ===================================================================

def compute_acwr(atl: float, ctl: float) -> float:
    """
    急性/慢性负荷比。ACWR = ATL / CTL。

    说明：与 TSB 互补而非替代。
      - TSB → 训练准备度（今天能不能高质量训练）
      - ACWR → 负荷变化速率（是不是加量太快了）
    """
    if ctl <= 0:
        return 1.0  # 冷启动默认
    return round(atl / ctl, 2)


def classify_acwr_zone(acwr: float) -> Dict[str, Any]:
    """
    ACWR 区间分类。

    返回:
        {"zone": str, "risk": str, "suggestion": str}
    """
    if acwr < 0.8:
        return {"zone": "under_load", "risk": "low", "suggestion": "可适当加量"}
    elif acwr <= 1.3:
        return {"zone": "optimal", "risk": "low", "suggestion": "负荷平衡"}
    elif acwr <= 1.5:
        return {"zone": "elevated", "risk": "medium", "suggestion": "谨慎加量"}
    else:
        return {"zone": "high", "risk": "high", "suggestion": "建议减量防伤"}


def classify_tsb_zone(tsb: float) -> Dict[str, Any]:
    """
    TSB 区间分类。

    返回:
        {"zone": str, "label": str, "suggestion": str}
    """
    for zone_name, lo, hi, suggestion in TSB_ZONES:
        if lo <= tsb < hi:
            return {"zone": zone_name, "tsb": round(tsb, 1), "suggestion": suggestion}
    return {"zone": "unknown", "tsb": round(tsb, 1), "suggestion": "数据异常"}


def compute_tsb_ramp_rate(tsb_history: List[float]) -> float:
    """
    TSB 变化率（ramp rate）。

    TSB 快速下降（<-5/天）比绝对值高危。
    返回最近几天的平均日变化量。
    """
    if len(tsb_history) < 2:
        return 0.0
    deltas = [tsb_history[i] - tsb_history[i - 1] for i in range(1, len(tsb_history))]
    return round(sum(deltas) / len(deltas), 2)


# ===================================================================
#  第四部分：训练质量指标
# ===================================================================

def compute_intensity_factor(avg_hr: float, threshold_hr: float = DEFAULT_THRESHOLD_HR) -> float:
    """
    强度因子 IF = avg_hr / threshold_hr。

    IF 范围通常 0.5~1.2：
      - < 0.75 恢复
      - 0.75~0.85 有氧基础
      - 0.85~0.95 节奏/阈值
      - > 0.95 高强度
    """
    if threshold_hr <= 0 or avg_hr <= 0:
        return 0.0
    return round(avg_hr / threshold_hr, 2)


def classify_ride_quality(
    if_score: float,
    z4_plus_pct: float,
) -> Dict[str, Any]:
    """
    训练质量分类。

    z4_plus_pct: Z4+ 心率区间时间占比（0~100）

    返回:
        {"quality": str, "label": str, "note": str}
    """
    if z4_plus_pct >= 20 and if_score >= 0.95:
        return {"quality": "high_intensity", "label": "高强度有效刺激",
                "note": "阈值/无氧训练，恢复期需充足"}
    elif z4_plus_pct >= 10 and if_score >= 0.85:
        return {"quality": "tempo", "label": "节奏训练",
                "note": "有氧阈值边缘，有效提升"}
    elif if_score >= 0.75 and z4_plus_pct < 10:
        return {"quality": "aerobic", "label": "有氧基础",
                "note": "稳定有氧，基础耐力训练"}
    elif if_score < 0.75:
        return {"quality": "recovery", "label": "恢复训练",
                "note": "低强度恢复，流动性训练"}
    else:
        return {"quality": "maintenance", "label": "维持训练",
                "note": "常规训练日"}


# ===================================================================
#  第五部分：睡眠与恢复
# ===================================================================

def compute_sleep_utilization(sleep: dict) -> float:
    """
    睡眠利用率 0-100。

    输入格式:
    {
        "total_seconds": 28800,       # 总睡眠时长（秒）
        "deep_sleep_pct": 25.0,       # 深睡占比 %
        "continuity_score": 80.0      # 连续性评分 0-100（可选）
    }

    权重：时长 40%，深睡 35%，连续性 25%。
    """
    total_s = sleep.get("total_seconds", 0) or 0
    deep_pct = sleep.get("deep_sleep_pct", 0) or 0
    continuity = sleep.get("continuity_score", 80) or 80

    # 时长评分：7-9h 最优，线性衰减
    hours = total_s / 3600.0
    if hours >= 8:
        duration_score = 100
    elif hours >= 6:
        duration_score = 60 + (hours - 6) / 2 * 40
    elif hours >= 4:
        duration_score = 20 + (hours - 4) / 2 * 40
    else:
        duration_score = max(0, hours / 4 * 20)

    # 深睡评分：20-30% 最优
    if 20 <= deep_pct <= 30:
        deep_score = 100
    elif deep_pct >= 15:
        deep_score = 70 + (deep_pct - 15) / 5 * 30
    elif deep_pct >= 10:
        deep_score = 40 + (deep_pct - 10) / 5 * 30
    else:
        deep_score = max(0, deep_pct / 10 * 40)

    score = (
        duration_score * SLEEP_DURATION_WEIGHT
        + deep_score * SLEEP_DEEP_WEIGHT
        + continuity * SLEEP_CONTINUITY_WEIGHT
    )
    return round(min(100, max(0, score)), 1)


def compute_recovery_index(
    sleep_score: float,
    tsb: float = 0.0,
    hr_resting_trend: float = 0.0,
) -> float:
    """
    综合恢复指数 0-100。

    权重：
      - 睡眠利用率 60%
      - TSB 归一化 30%（TSB > 10 为 100，< -25 为 0）
      - 静息心率趋势 10%（正 = 疲劳，负 = 恢复）
    """
    # TSB → 恢复贡献
    if tsb >= 10:
        tsb_score = 100
    elif tsb >= -10:
        tsb_score = 70 + (tsb + 10) / 20 * 30
    elif tsb >= -25:
        tsb_score = 30 + (tsb + 25) / 15 * 40
    else:
        tsb_score = max(0, 30 + (tsb + 25) / 25 * 30)

    # 静息心率趋势
    hr_score = max(0, 100 - abs(hr_resting_trend) * 10)

    index = sleep_score * 0.6 + tsb_score * 0.3 + hr_score * 0.1
    return round(min(100, max(0, index)), 1)


def classify_recovery(score: float) -> Dict[str, Any]:
    """
    恢复指数分类。

    返回:
        {"level": str, "action": str}
    """
    if score >= 80:
        return {"level": "excellent", "action": "可照常训练，可挑战高强度"}
    elif score >= 60:
        return {"level": "good", "action": "正常训练"}
    elif score >= 40:
        return {"level": "fair", "action": "降级训练，降低强度或缩短时长"}
    else:
        return {"level": "poor", "action": "建议休息"}


# ===================================================================
#  第六部分：趋势判断
# ===================================================================

def compute_trend(
    values: List[float],
    window: int = 3,
    min_decline_pct: float = -5.0,
) -> Tuple[bool, float]:
    """
    判断 N 天趋势。

    参数:
        values:          按日期排序的数值列表（最近的在最后）
        window:          趋势窗口天数
        min_decline_pct: 被判定为"下降"的阈值百分比（负值）

    返回:
        (is_declining, slope_per_day)
        - is_declining: 是否连续下降
        - slope: 线性回归斜率（每日变化量）
    """
    if len(values) < window:
        return (False, 0.0)

    recent = values[-window:]
    # 简单线性回归求斜率
    n = len(recent)
    x_avg = (n - 1) / 2.0
    y_avg = sum(recent) / n

    numerator = sum(i * v for i, v in enumerate(recent)) - n * x_avg * y_avg
    denominator = sum(i * i for i in range(n)) - n * x_avg * x_avg

    if denominator == 0:
        return (False, 0.0)

    slope = numerator / denominator
    # 相对变化率 %
    if y_avg > 0:
        change_pct = (slope * n) / y_avg * 100
    else:
        change_pct = 0.0

    is_declining = change_pct <= min_decline_pct and slope < 0
    return (is_declining, round(slope, 2))


# ===================================================================
#  第七部分：统一上下文（唯一有业务语义的聚合入口）
# ===================================================================

def build_training_context(
    trimp_42d: List[Dict[str, Any]],
    sleep_7d: Optional[List[Dict[str, Any]]] = None,
    rides_7d: Optional[List[Dict[str, Any]]] = None,
    threshold_hr: int = DEFAULT_THRESHOLD_HR,
) -> Dict[str, Any]:
    """
    聚合所有指标为统一上下文 dict。

    参数:
        trimp_42d: 近 42 天每日 TRIMP 列表
                    [{"date": "2026-04-30", "trimp": 85.0}, ...]
        sleep_7d:  近 7 天睡眠数据列表（可选）
                    [{"date": "2026-05-04", "total_seconds": 28800,
                      "deep_sleep_pct": 25.0, "continuity_score": 80}, ...]
        rides_7d:  近 7 天骑行数据列表（可选）
                    [{"date": "2026-05-04", "trimp": 85.0,
                      "avg_hr": 145, "z4_plus_pct": 5.0,
                      "duration_min": 60}, ...]
        threshold_hr: 阈值心率（用于 IF 计算）

    返回:
        统一上下文 dict
    """
    context: Dict[str, Any] = {}

    # ── 冷启动检测 ──
    days_available = len(trimp_42d)
    if days_available < MIN_RECOVERY_DAYS_FOR_STABLE:
        context["cold_start"] = True
        context["days_available"] = days_available
        context["note"] = "数据不足 7 天，指标仅供参考"
    elif days_available < COLD_START_DAYS:
        context["cold_start"] = True
        context["days_available"] = days_available
        context["note"] = f"CTL 历史不足 42 天（当前 {days_available} 天），TSB 值偏大属正常"
    else:
        context["cold_start"] = False
        context["days_available"] = days_available

    # ── CTL / ATL / TSB ──
    trimp_values = [d["trimp"] for d in trimp_42d]

    initial_ctl = estimate_initial_ctl(trimp_values)
    initial_atl = estimate_initial_ctl(trimp_values[:7]) if len(trimp_values) >= 7 else initial_ctl

    lam_ctl = 1.0 - math.exp(-1.0 / TAU_CTL)
    lam_atl = 1.0 - math.exp(-1.0 / TAU_ATL)

    current_ctl = initial_ctl
    current_atl = initial_atl
    for t in trimp_values:
        current_ctl = current_ctl + lam_ctl * (t - current_ctl)
        current_atl = current_atl + lam_atl * (t - current_atl)

    tsb = current_ctl - current_atl

    context["ctl"] = round(current_ctl, 1)
    context["atl"] = round(current_atl, 1)
    context["tsb"] = round(tsb, 1)
    context["tsb_zone"] = classify_tsb_zone(tsb)
    context["acwr"] = compute_acwr(current_atl, current_ctl)
    context["acwr_zone"] = classify_acwr_zone(context["acwr"])

    # ── TSB 变化率 ──
    if len(trimp_values) >= 3:
        # 用最近几天的 TSB 历史估算 ramp rate
        cat_results = compute_ctl_atl_tsb(trimp_values)
        tsb_history = [r[2] for r in cat_results]
        context["tsb_ramp_rate"] = compute_tsb_ramp_rate(tsb_history[-5:])
    else:
        context["tsb_ramp_rate"] = 0.0

    # ── 睡眠 ──
    if sleep_7d:
        latest_sleep = sleep_7d[-1] if sleep_7d else {}
        context["sleep_score"] = compute_sleep_utilization(latest_sleep)

        sleep_scores = [
            compute_sleep_utilization(s) for s in sleep_7d
        ]
        sleep_trend = compute_trend(sleep_scores)
        context["sleep_trend"] = {
            "is_declining": sleep_trend[0],
            "slope": sleep_trend[1],
        }

        # 睡眠评分和连续天数
        low_sleep_days = sum(1 for s in sleep_scores if s < 60)
        context["low_sleep_days"] = low_sleep_days
        context["consecutive_low_sleep"] = _count_consecutive_low(sleep_scores, 60)
    else:
        context["sleep_score"] = 75.0  # 无数据时默认
        context["sleep_trend"] = {"is_declining": False, "slope": 0.0}
        context["low_sleep_days"] = 0
        context["consecutive_low_sleep"] = 0

    # ── 负荷趋势 ──
    if len(trimp_values) >= 3:
        load_trend = compute_trend(trimp_values)
        context["load_trend"] = {
            "is_declining": load_trend[0],
            "slope": load_trend[1],
        }
    else:
        context["load_trend"] = {"is_declining": False, "slope": 0.0}

    # ── 恢复指数 ──
    context["recovery_index"] = compute_recovery_index(
        sleep_score=context["sleep_score"],
        tsb=tsb,
    )
    context["recovery_level"] = classify_recovery(context["recovery_index"])

    # ── 最近一次骑行 ──
    if rides_7d:
        last_ride = rides_7d[-1]
        context["last_ride"] = {
            "date": last_ride.get("date", ""),
            "trimp": last_ride.get("trimp", 0),
            "duration_min": last_ride.get("duration_min", 0),
            "avg_hr": last_ride.get("avg_hr", 0),
        }
        avg_hr = last_ride.get("avg_hr", 0)
        if avg_hr > 0:
            if_score = compute_intensity_factor(avg_hr, threshold_hr)
            z4_plus = last_ride.get("z4_plus_pct", 0)
            context["last_ride"]["if"] = if_score
            context["last_ride"]["quality"] = classify_ride_quality(if_score, z4_plus)
    else:
        context["last_ride"] = None

    # ── 训练建议优先级 ──
    context["priority_flags"] = _build_priority_flags(context)

    return context


def _count_consecutive_low(scores: List[float], threshold: float = 60) -> int:
    """统计末尾连续低于阈值的天数。"""
    count = 0
    for s in reversed(scores):
        if s < threshold:
            count += 1
        else:
            break
    return count


def _build_priority_flags(context: Dict[str, Any]) -> Dict[str, bool]:
    """
    根据上下文生成优先级标记（用于推送 delta gate）。

    返回:
        {
            "overtraining_risk": bool,    # 过度训练风险
            "sleep_deficit": bool,        # 睡眠严重不足
            "tsb_critical": bool,         # TSB 高危
            "recovery_critical": bool,    # 恢复极差
        }
    """
    flags: Dict[str, bool] = {
        "overtraining_risk": False,
        "sleep_deficit": False,
        "tsb_critical": False,
        "recovery_critical": False,
    }

    # 过度训练：ACWR > 1.5 AND TSB < -15
    acwr = context.get("acwr", 1.0)
    tsb = context.get("tsb", 0)
    if acwr > 1.5 and tsb < -15:
        flags["overtraining_risk"] = True

    # 睡眠严重不足：连续 2+ 天 < 60
    if context.get("consecutive_low_sleep", 0) >= 2:
        flags["sleep_deficit"] = True

    # TSB 高危
    tsb_zone = context.get("tsb_zone", {})
    if isinstance(tsb_zone, dict) and tsb_zone.get("zone") == "high_risk":
        flags["tsb_critical"] = True

    # 恢复极差
    rec_level = context.get("recovery_level", {})
    if isinstance(rec_level, dict) and rec_level.get("level") == "poor":
        flags["recovery_critical"] = True

    return flags


# ===================================================================
#  数据库写入接口（唯一有副作用的函数，供同步脚本调用）
# ===================================================================

def update_training_load_from_db(
    db,
    date_str: str,
    trimp: float,
    force_recalc: bool = False,
) -> Dict[str, float]:
    """
    写入或更新 training_load 表，全链重算 CTL/ATL/TSB。

    参数:
        db:           数据库访问对象（需提供 get_training_load /
                       get_training_load_range / save_training_load）
        date_str:     日期字符串 "YYYY-MM-DD"
        trimp:        当日 TRIMP（会累加到已有值上）
        force_recalc: 是否强制重算

    返回:
        {"ctl": float, "atl": float, "tsb": float}
    """
    existing = db.get_training_load(date_str)
    if existing and not force_recalc:
        new_trimp = existing["trimp"] + trimp
        new_count = existing["activity_count"] + 1
    else:
        new_trimp = trimp
        new_count = 1

    all_loads = db.get_training_load_range("2000-01-01", "2099-12-31")
    date_trimp_map = {row["date"]: row["trimp"] for row in all_loads}
    date_trimp_map[date_str] = new_trimp
    sorted_dates = sorted(date_trimp_map.keys())
    trimp_sequence = [date_trimp_map[d] for d in sorted_dates]

    ctl = estimate_initial_ctl(trimp_sequence)
    atl = estimate_initial_ctl(trimp_sequence[:7]) if len(trimp_sequence) >= 7 else ctl

    lam_ctl = 1.0 - math.exp(-1.0 / TAU_CTL)
    lam_atl = 1.0 - math.exp(-1.0 / TAU_ATL)

    current_ctl, current_atl = ctl, atl
    for t in trimp_sequence:
        current_ctl += lam_ctl * (t - current_ctl)
        current_atl += lam_atl * (t - current_atl)

    db.save_training_load(
        date_str, new_trimp,
        round(current_ctl, 1), round(current_atl, 1), round(current_ctl - current_atl, 1),
        new_count,
    )

    return {
        "ctl": round(current_ctl, 1),
        "atl": round(current_atl, 1),
        "tsb": round(current_ctl - current_atl, 1),
    }
