"""
analysis_engine.fallback — AI 降级模板
=========================================
当 coach agent 不可用/超时/失败时，用规则引擎
从结构化分析数据生成合理的 Strava 活动描述。

确保 Strava 活动描述永远不会为空。
"""

from typing import Optional

from .models import AnalysisResult
from .zones import HR_ZONE_ORDER, DEFAULT_MAX_HR


def generate_fallback_description(analysis: dict, activity_name: str = "") -> str:
    """从结构化分析数据生成降级描述。

    当 coach agent 调用失败时使用此函数兜底。

    Args:
        analysis: 分析结果 dict（来自 FitAnalyzer.to_dict）
        activity_name: 活动名称

    Returns:
        自然语言描述，约 80-150 字
    """
    basic = analysis.get("basic_stats", {}) or analysis.get("basic", {})
    hr_zones = analysis.get("hr_zones", {}) or analysis.get("distributions", {}).get("heart_rate_zones", {})
    elevation = analysis.get("elevation", {}) or {}
    temp = analysis.get("temperature", {}) or {}
    caps = analysis.get("capabilities", {}) or {}

    lines = []

    # 活动概要
    dist = basic.get("total_distance_km", 0)
    duration_s = basic.get("total_time_s", 0)
    duration_min = duration_s / 60 if duration_s else 0
    avg_hr = basic.get("avg_heart_rate")
    avg_spd = basic.get("avg_speed_kmh")

    parts = []
    if dist:
        parts.append(f"{dist:.1f}km")
    if duration_min:
        parts.append(f"{duration_min:.0f}分钟")
    if avg_spd:
        parts.append(f"均速{avg_spd:.1f}km/h")
    if avg_hr:
        parts.append(f"均心{avg_hr:.0f}bpm")

    if parts:
        lines.append("🚴 " + " · ".join(parts))

    # 心率区间
    if hr_zones and caps.get("has_heart_rate"):
        z2_val = hr_zones.get("Z2_有氧基础", {}).get("pct", 0)
        z1_val = hr_zones.get("Z1_恢复", {}).get("pct", 0)
        z4_val = hr_zones.get("Z4_乳酸阈值", {}).get("pct", 0)
        z5_val = hr_zones.get("Z5_无氧极限", {}).get("pct", 0)

        aerobic = z2_val + z1_val
        intensity = z4_val + z5_val

        if aerobic > 70:
            lines.append(f"❤️ 有氧基础训练为主（Z1+Z2={aerobic:.0f}%），心率控制稳定")
        elif intensity > 30:
            lines.append(f"🔥 高强度训练（Z4+Z5={intensity:.0f}%），无氧占比偏高")
        elif aerobic > 50:
            lines.append(f"❤️ 混氧训练，有氧占比{aerobic:.0f}%")

        if avg_hr:
            pct = (avg_hr / DEFAULT_MAX_HR) * 100
            if pct < 65:
                lines.append(f"   强度偏低（{pct:.0f}%HRmax），恢复骑")
            elif pct < 78:
                lines.append(f"   强度适中（{pct:.0f}%HRmax）")
            else:
                lines.append(f"   强度较高（{pct:.0f}%HRmax），注意恢复")

    # 踏频
    avg_cad = basic.get("avg_cadence")
    if avg_cad and caps.get("has_cadence"):
        if avg_cad >= 80:
            lines.append(f"🔄 踏频优秀（{avg_cad:.0f}rpm）")
        elif avg_cad >= 60:
            lines.append(f"🔄 踏频适中（{avg_cad:.0f}rpm）")
        else:
            lines.append(f"🔄 踏频偏低（{avg_cad:.0f}rpm），建议提高")

    # 爬升
    ascent = elevation.get("total_ascent_m", 0)
    if ascent and caps.get("has_altitude"):
        if ascent > 200:
            lines.append(f"⛰️ 爬升{ascent:.0f}m，路线起伏较大")
        elif ascent > 50:
            lines.append(f"⛰️ 爬升{ascent:.0f}m，有一定起伏")

    # 温度
    avg_temp = temp.get("avg")
    if avg_temp is not None:
        if avg_temp > 30:
            lines.append(f"🌡️ 高温骑行（{avg_temp:.0f}°C），注意补水")
        elif avg_temp < 10:
            lines.append(f"🌡️ 低温骑行（{avg_temp:.0f}°C），注意保暖")

    # 训练建议（简单规则）
    if dist and duration_min:
        pace = duration_min / dist if dist > 0 else 0
        if pace > 5 and intensity < 10:
            lines.append("💡 建议：下次可适当增加强度或间歇训练")
        elif pace < 2.5:
            lines.append("💡 建议：速度不错，注意保持有氧基础训练")

    return "\n".join(lines) if lines else "🚴 骑行训练完成"


def generate_fallback_coach_output(analysis: dict) -> str:
    """生成完整的降级教练输出（描述 + 明天建议），供 coach_worker._fallback_output 调用。

    当 coach agent 调用失败时，用规则引擎生成结构化的 Strava 活动描述。
    输出格式与 coach agent 保持一致：👍 亮点 / ⚠️ 待改进 / 💡 建议 / 📅 明天建议。
    """
    desc = generate_fallback_description(analysis)
    tip = generate_fallback_tip(analysis)
    if desc and tip:
        return f"{desc}\n\n{tip}"
    return desc or tip or "🚴 骑行训练完成"


def generate_fallback_tip(analysis: dict) -> str:
    """生成明日的训练建议（降级用）。"""
    basic = analysis.get("basic_stats", {}) or analysis.get("basic", {})
    hr_zones = analysis.get("hr_zones", {}) or analysis.get("distributions", {}).get("heart_rate_zones", {})
    dist = basic.get("total_distance_km", 0)

    intensity = sum(
        hr_zones.get(z, {}).get("pct", 0)
        for z in ["Z4_乳酸阈值", "Z5_无氧极限"]
    )

    if intensity > 30 or dist > 40:
        return "💡 明日建议休息或放松骑，高强度训练后需要充分恢复。"
    elif dist > 20:
        return "💡 明日建议 Z2 有氧骑行 45-60 分钟，保持训练节奏。"
    else:
        return "💡 明日可进行中等强度训练，建议 Z3 节奏骑 30-45 分钟。"


def generate_simple_template_fallback(analysis: dict, activity_name: str = "") -> str:
    """生成简洁模板文案（基于 activity_analysis 数据）。

    格式示例：
        今日骑行 XXkm，用时 XXmin，平均心率 XXbpm
        心率区间分布：Z1 X% / Z2 X% / Z3 X% / Z4 X% / Z5 X%
        继续保持，加油💪

    当 coach agent 调用失败时，用此函数生成兜底 Strava 描述。
    """
    basic = analysis.get("basic_stats", {}) or analysis.get("basic", {})
    hr_zones = analysis.get("hr_zones", {}) or analysis.get("distributions", {}).get("heart_rate_zones", {})
    caps = analysis.get("capabilities", {})

    # ── 活动基本信息 ──
    dist = basic.get("total_distance_km", 0)
    duration_s = basic.get("total_time_s", 0)
    duration_min = duration_s / 60 if duration_s else 0
    avg_hr = basic.get("avg_heart_rate")

    parts = []
    if dist:
        parts.append(f"{dist:.1f}km")
    if duration_min:
        parts.append(f"{duration_min:.0f}min")
    if avg_hr and caps.get("has_heart_rate"):
        parts.append(f"均心{avg_hr:.0f}bpm")
    elif avg_hr:
        parts.append(f"均心{avg_hr:.0f}bpm")

    title = activity_name or f"{dist:.1f}km 骑行"
    line1 = f"🚴 {title}"
    if parts:
        line1 += f"（{' · '.join(parts)}）"
    lines = [line1]

    # ── 心率区间分布 ──
    if hr_zones and caps.get("has_heart_rate"):
        zone_order = [("Z1_恢复", "Z1"), ("Z2_有氧基础", "Z2"),
                       ("Z3_有氧进阶", "Z3"), ("Z4_乳酸阈值", "Z4"),
                       ("Z5_无氧极限", "Z5")]
        zone_strs = []
        for full_key, short_key in zone_order:
            zdata = hr_zones.get(full_key, {})
            pct = zdata.get("pct", 0)
            if pct > 0:
                zone_strs.append(f"{short_key} {pct:.0f}%")
        if zone_strs:
            lines.append("❤️ 心率区间：" + " / ".join(zone_strs))

    # ── 踏频 ──
    avg_cad = basic.get("avg_cadence")
    if avg_cad and caps.get("has_cadence"):
        lines.append(f"🔄 均踏频：{avg_cad:.0f}rpm")

    # ── 爬升 ──
    elevation = analysis.get("elevation", {})
    ascent = elevation.get("total_ascent_m", 0)
    if ascent and caps.get("has_altitude"):
        lines.append(f"⛰️ 爬升：{ascent:.0f}m")

    # ── 温度 ──
    temp = analysis.get("temperature", {})
    avg_temp = temp.get("avg")
    if avg_temp is not None:
        lines.append(f"🌡️ 均温：{avg_temp:.0f}°C")

    # ── 建议 ──
    if dist > 0 and duration_min > 0 and avg_hr:
        aero = sum(hr_zones.get(z, {}).get("pct", 0) for z in ["Z1_恢复", "Z2_有氧基础"])
        if aero > 70:
            lines.append("💡 以有氧基础训练为主，继续保持，加油💪")
        else:
            lines.append("💡 继续保持，加油💪")
    else:
        lines.append("💡 继续保持，加油💪")

    return "\n".join(lines)
