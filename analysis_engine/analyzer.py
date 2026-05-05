"""
analysis_engine.analyzer — 核心分析引擎
=========================================
将 RideRecord 列表转化为 AnalysisResult。
负责：心率/速度/踏频/坡度区间统计、爬升计算、温度汇总。
"""

import logging
from typing import Optional

from .models import AnalysisResult, RideRecord
from .zones import (
    HR_ZONES, HR_ZONE_ORDER,
    SPEED_ZONES, CADENCE_ZONES, GRADE_ZONES,
    DEFAULT_MAX_HR,
    classify_zone,
)

logger = logging.getLogger(__name__)

# 最小有效心率（低于此值视为传感器未连接）
MIN_VALID_HR = 30
# 最大有效速度 km/h（超过此值视为异常）
MAX_VALID_SPEED_KPH = 120


class FitAnalyzer:
    """FIT 数据分析器。

    用法：
        analyzer = FitAnalyzer(records, max_hr=194)
        result = analyzer.analyze()

    也支持直接传 bytes（自动解析）：
        analyzer = FitAnalyzer(fit_bytes, max_hr=194)
        result = analyzer.analyze()
    """

    def __init__(self, records_or_data, max_hr: int = DEFAULT_MAX_HR, verbose: bool = False):
        self.max_hr = max_hr
        self.verbose = verbose

        # 兼容：如果传的是 bytes，自动解析
        if isinstance(records_or_data, bytes):
            from .parser import parse_fit
            self.records = parse_fit(records_or_data)
        elif isinstance(records_or_data, list):
            self.records = records_or_data
        else:
            raise TypeError(f"expected bytes or list[RideRecord], got {type(records_or_data)}")

        if not self.records:
            raise ValueError("无记录点可分析")

    def analyze(self) -> AnalysisResult:
        """执行全部分析，返回 AnalysisResult。"""
        records = self.records
        result = AnalysisResult(record_count=len(records))

        # ── 基础统计 ──────────────────────────
        hr_vals = [r.heart_rate for r in records if r.heart_rate and r.heart_rate >= MIN_VALID_HR]
        cad_vals = [r.cadence for r in records if r.cadence]
        spd_vals = [r.speed for r in records if r.speed is not None]
        alt_vals = [r.altitude for r in records if r.altitude is not None]
        temp_vals = [r.temperature for r in records if r.temperature is not None]
        pwr_vals = [r.power for r in records if r.power]

        if hr_vals:
            result.has_heart_rate = True
            result.avg_heart_rate = sum(hr_vals) / len(hr_vals)
            result.max_heart_rate = max(hr_vals)

        if cad_vals:
            result.has_cadence = True
            result.avg_cadence = sum(cad_vals) / len(cad_vals)
            result.max_cadence = max(cad_vals)

        if pwr_vals:
            result.has_power = True
            result.avg_power = sum(pwr_vals) / len(pwr_vals)
            result.max_power = max(pwr_vals)

        if temp_vals:
            result.has_temperature = True
            result.avg_temperature = sum(temp_vals) / len(temp_vals)
            result.min_temperature = min(temp_vals)
            result.max_temperature = max(temp_vals)

        if alt_vals:
            result.has_altitude = True
            result.min_altitude_m = min(alt_vals)
            result.max_altitude_m = max(alt_vals)
            # 爬升/下降
            ascent = 0
            descent = 0
            for i in range(1, len(alt_vals)):
                diff = alt_vals[i] - alt_vals[i - 1]
                if diff > 1:
                    ascent += diff
                elif diff < -1:
                    descent += abs(diff)
            result.total_ascent_m = ascent
            result.total_descent_m = descent

        # ── 速度和距离 ────────────────────────
        if spd_vals:
            # 先尝试 m/s * 3.6 → km/h
            spd_kph = [s * 3.6 for s in spd_vals if s * 3.6 < MAX_VALID_SPEED_KPH]
            if spd_kph:
                avg1 = sum(spd_kph) / len(spd_kph)
                # 如果 *3.6 后平均 > 50 km/h，说明 speed 可能已是 km/h
                if avg1 > 50:
                    spd_kph2 = [s for s in spd_vals if s < MAX_VALID_SPEED_KPH]
                    if spd_kph2:
                        avg2 = sum(spd_kph2) / len(spd_kph2)
                        # 取更合理的值（< 50 的版本优先）
                        if avg2 < avg1:
                            spd_kph = spd_kph2
                result.max_speed_kmh = max(spd_kph)
                result.avg_speed_kmh = sum(spd_kph) / len(spd_kph)

        # 总时间（按记录点首尾时间差）
        if len(records) > 1 and records[-1].timestamp and records[0].timestamp:
            result.total_time_s = records[-1].timestamp - records[0].timestamp
            if result.total_time_s < 0:
                result.total_time_s = 0

        # 总距离（从最后一个有效距离值）
        dists = [r.distance for r in records if r.distance is not None]
        if dists:
            result.total_distance_km = (dists[-1] - dists[0]) / 1000 if len(dists) > 1 else dists[-1] / 1000

        # ── 区间分布（秒级统计）───────────────
        # 心率区间
        if hr_vals:
            dist = _build_zone_distribution(
                counts=_count_zone_seconds(records, HR_ZONES, self.max_hr, key="heart_rate"),
                total=len(records),
                zone_order=HR_ZONE_ORDER,
            )
            result.hr_zone_distribution = dist

        # 速度区间（复用前面的 spd_kph 逻辑）
        if spd_vals:
            spd_kph_list = [s * 3.6 for s in spd_vals if s * 3.6 < MAX_VALID_SPEED_KPH]
            if spd_kph_list:
                avg = sum(spd_kph_list) / len(spd_kph_list)
                # 如果 *3.6 后平均 > 50 km/h，尝试不带 *3.6
                if avg > 50:
                    spd_kph2 = [s for s in spd_vals if s < MAX_VALID_SPEED_KPH]
                    if spd_kph2 and sum(spd_kph2)/len(spd_kph2) < avg:
                        spd_kph_list = spd_kph2
                zones = {}
                for name, (lo, hi) in SPEED_ZONES.items():
                    cnt = sum(1 for s in spd_kph_list if lo <= s < hi)
                    if cnt:
                        zones[name] = {"count": cnt, "pct": round(cnt / len(spd_kph_list) * 100, 1)}
                result.speed_zone_distribution = zones

        # 踏频区间
        if cad_vals:
            zones = {}
            for name, (lo, hi) in CADENCE_ZONES.items():
                cnt = sum(1 for c in cad_vals if lo <= c < hi)
                if cnt:
                    zones[name] = {"count": cnt, "pct": round(cnt / len(cad_vals) * 100, 1)}
            result.cadence_zone_distribution = zones

        # 坡度区间
        if alt_vals and len(alt_vals) > 1:
            grades = []
            for i in range(1, len(alt_vals)):
                d_alt = alt_vals[i] - alt_vals[i - 1]
                d_dist = (dists[i] - dists[i - 1]) if i < len(dists) and dists[i] and dists[i - 1] else 1
                if d_dist > 0:
                    grades.append((d_alt / d_dist) * 100)
            if grades:
                zones = {}
                for name, (lo, hi) in GRADE_ZONES.items():
                    cnt = sum(1 for g in grades if lo <= g < hi)
                    if cnt:
                        zones[name] = {"count": cnt, "pct": round(cnt / len(grades) * 100, 1)}
                result.grade_zone_distribution = zones

        if self.verbose:
            logger.info(f"分析完成: {result.record_count} 记录点, "
                        f"HR={result.has_heart_rate}, "
                        f"CAD={result.has_cadence}, "
                        f"PWR={result.has_power}, "
                        f"TEMP={result.has_temperature}, "
                        f"ALT={result.has_altitude}")

        return result

    def to_dict(self, result: Optional[AnalysisResult] = None) -> dict:
        """将 AnalysisResult 转为 JSON 可序列化 dict。"""
        if result is None:
            result = self.analyze()

        return {
            "basic": {
                "total_time_s": result.total_time_s,
                "total_distance_km": result.total_distance_km,
                "avg_speed_kmh": result.avg_speed_kmh,
                "max_speed_kmh": result.max_speed_kmh,
                "avg_heart_rate": result.avg_heart_rate,
                "max_heart_rate": result.max_heart_rate,
                "avg_cadence": result.avg_cadence,
                "max_cadence": result.max_cadence,
                "avg_power": result.avg_power,
                "max_power": result.max_power,
            },
            "elevation": {
                "total_ascent_m": result.total_ascent_m,
                "total_descent_m": result.total_descent_m,
                "min_altitude_m": result.min_altitude_m,
                "max_altitude_m": result.max_altitude_m,
            },
            "temperature": {
                "avg": result.avg_temperature,
                "min": result.min_temperature,
                "max": result.max_temperature,
            },
            "distributions": {
                "heart_rate_zones": result.hr_zone_distribution,
                "speed_zones": result.speed_zone_distribution,
                "cadence_zones": result.cadence_zone_distribution,
                "grade_zones": result.grade_zone_distribution,
            },
            "capabilities": {
                "record_count": result.record_count,
                "has_heart_rate": result.has_heart_rate,
                "has_cadence": result.has_cadence,
                "has_power": result.has_power,
                "has_temperature": result.has_temperature,
                "has_altitude": result.has_altitude,
            },
        }


# ─── 内部辅助 ──────────────────────────────────


def _count_zone_seconds(records, zones, max_val, key="hr"):
    """按秒统计各区间占比。"""
    counts = {name: 0 for name in zones}
    for rec in records:
        val = getattr(rec, key, None)
        if val is None:
            continue
        if key in ("hr", "heart_rate"):
            if val < MIN_VALID_HR:
                continue
            pct = val / max_val
            for name, (lo, hi, _) in zones.items():
                if lo <= pct < hi:
                    counts[name] += 1
                    break
        else:
            for name, (lo, hi) in zones.items():
                if lo <= val < hi:
                    counts[name] += 1
                    break
    return counts


def _build_zone_distribution(counts: dict, total: int, zone_order: list) -> dict:
    """将原始计数转为带百分比的分布 dict，按 zone_order 排序。"""
    result = {}
    for name in zone_order:
        cnt = counts.get(name, 0)
        pct = round(cnt / total * 100, 1) if total > 0 else 0
        result[name] = {"count": cnt, "pct": pct}
    return result
