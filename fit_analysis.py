#!/usr/bin/env python3
"""
FIT 文件解析与骑行数据分析模块
=================================
从 Magene/顽鹿 下载的 FIT 文件中提取结构化骑行数据，
包括心率区间分布、爬升/下降、速度分布、踏频分布、温度等。

依赖：fitparse (已安装)
输出：结构化 JSON + 自然语言分析报告

使用方式：
    # 作为模块导入
    from fit_analysis import FitAnalyzer
    analyzer = FitAnalyzer(fit_bytes)
    data = analyzer.analyze()
    report = analyzer.generate_report(data)

    # 命令行
    python3 fit_analysis.py activity.fit --json
    python3 fit_analysis.py activity.fit --report
"""

import argparse
import json
import math
import os
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import fitparse


# ─── 心率区间定义 ──────────────────────────────
# 基于最大心率百分比（默认 220-age，可由用户覆盖）
DEFAULT_MAX_HR = int(os.environ.get("ANALYSIS_MAX_HR", "194"))  # 默认最大心率（ACSM公式: 220-age 近似），可通过配置覆盖

HR_ZONES = {
    "Z1_恢复":     (0.50, 0.60, "恢复/热身"),
    "Z2_有氧基础":  (0.60, 0.70, "有氧基础训练"),
    "Z3_有氧进阶":  (0.70, 0.80, "有氧进阶训练"),
    "Z4_乳酸阈值":  (0.80, 0.90, "乳酸阈值训练"),
    "Z5_无氧极限":  (0.90, 1.00, "无氧极限训练"),
}

# ─── 速度区间定义 (km/h) ─────────────────────
SPEED_ZONES = {
    "极慢(<10)":     (0, 10),
    "慢速(10-18)":   (10, 18),
    "中速(18-25)":   (18, 25),
    "快速(25-32)":   (25, 32),
    "高速(>32)":     (32, 999),
}

# ─── 踏频区间定义 (rpm) ─────────────────────
CADENCE_ZONES = {
    "低踏频(<60)":   (0, 60),
    "中踏频(60-80)": (60, 80),
    "高踏频(80-100)":(80, 100),
    "极高踏频(>100)":(100, 999),
}

# ─── 坡度区间定义 (%) ───────────────────────
GRADE_ZONES = {
    "下坡(< -3%)":    (-999, -3),
    "平路/缓坡(-3~3%)": (-3, 3),
    "爬坡(3~6%)":     (3, 6),
    "陡坡(6~10%)":    (6, 10),
    "极陡坡(>10%)":   (10, 999),
}


@dataclass
class RideRecord:
    """单条记录点"""
    timestamp: float = 0
    heart_rate: Optional[int] = None
    cadence: Optional[int] = None
    speed: Optional[float] = None  # m/s
    altitude: Optional[float] = None  # meters
    temperature: Optional[float] = None  # °C
    power: Optional[int] = None  # watts
    distance: Optional[float] = None  # meters
    grade: Optional[float] = None  # percent


@dataclass
class AnalysisResult:
    """结构化分析结果"""
    # 基本信息
    total_time_s: float = 0
    total_distance_km: float = 0
    avg_speed_kmh: float = 0
    max_speed_kmh: float = 0
    avg_heart_rate: Optional[float] = None
    max_heart_rate: Optional[int] = None
    avg_cadence: Optional[float] = None
    max_cadence: Optional[int] = None
    avg_power: Optional[float] = None
    max_power: Optional[int] = None

    # 海拔
    total_ascent_m: float = 0
    total_descent_m: float = 0
    min_altitude_m: Optional[float] = None
    max_altitude_m: Optional[float] = None

    # 温度
    avg_temperature: Optional[float] = None
    min_temperature: Optional[float] = None
    max_temperature: Optional[float] = None

    # 分布数据
    hr_zone_distribution: dict = field(default_factory=dict)
    speed_zone_distribution: dict = field(default_factory=dict)
    cadence_zone_distribution: dict = field(default_factory=dict)
    grade_zone_distribution: dict = field(default_factory=dict)

    # 原始统计
    record_count: int = 0
    has_heart_rate: bool = False
    has_cadence: bool = False
    has_power: bool = False
    has_temperature: bool = False
    has_altitude: bool = False


class FitAnalyzer:
    """FIT 文件分析器"""

    def __init__(self, fit_data: bytes, max_hr: int = DEFAULT_MAX_HR, verbose: bool = False):
        """
        Args:
            fit_data: FIT 文件二进制数据
            max_hr: 最大心率（用于区间计算）
            verbose: 是否输出调试信息
        """
        self.fit_data = fit_data
        self.max_hr = max_hr
        self.verbose = verbose
        self.records: list[RideRecord] = []
        self._parse()

    def _parse(self):
        """解析 FIT 文件，提取 record 消息"""
        # 写入临时文件（fitparse 需要文件对象）
        with tempfile.NamedTemporaryFile(suffix=".fit", delete=False) as tmp:
            tmp.write(self.fit_data)
            tmp_path = tmp.name

        try:
            fitfile = fitparse.FitFile(tmp_path, data_processor=fitparse.StandardUnitsDataProcessor())
            for record in fitfile.get_messages("record"):
                self._process_record(record)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        if self.verbose:
            print(f"[fit_analysis] 解析完成: {len(self.records)} 条记录点", file=sys.stderr)

    def _safe_get(self, record, field_name, default=None, convert=None):
        """安全获取字段值"""
        try:
            for field in record:
                if field.name == field_name:
                    val = field.value
                    if val is None:
                        return default
                    if convert:
                        try:
                            return convert(val)
                        except (ValueError, TypeError):
                            return default
                    return val
        except Exception:
            pass
        return default

    def _process_record(self, record):
        """处理单条 record 消息"""
        hr = self._safe_get(record, "heart_rate", convert=int)
        cadence = self._safe_get(record, "cadence", convert=int)
        speed = self._safe_get(record, "speed", convert=float)  # m/s (StandardUnits)
        altitude = self._safe_get(record, "altitude", convert=float)
        temperature = self._safe_get(record, "temperature", convert=float)
        power = self._safe_get(record, "power", convert=int)
        distance = self._safe_get(record, "distance", convert=float)
        grade = self._safe_get(record, "grade", convert=float)
        timestamp = self._safe_get(record, "timestamp", convert=lambda x: x.timestamp() if hasattr(x, 'timestamp') else 0)

        r = RideRecord(
            timestamp=timestamp,
            heart_rate=hr,
            cadence=cadence,
            speed=speed,
            altitude=altitude,
            temperature=temperature,
            power=power,
            distance=distance,
            grade=grade,
        )
        self.records.append(r)

    def analyze(self) -> AnalysisResult:
        """分析所有记录点，返回结构化数据"""
        result = AnalysisResult(record_count=len(self.records))

        if not self.records:
            return result

        # ── 收集有效数据 ──
        hr_values = [r.heart_rate for r in self.records if r.heart_rate and r.heart_rate > 0]
        cadence_values = [r.cadence for r in self.records if r.cadence and r.cadence > 0]
        speed_values = [r.speed for r in self.records if r.speed is not None and r.speed >= 0]
        altitude_values = [(i, r.altitude) for i, r in enumerate(self.records) if r.altitude is not None]
        temp_values = [r.temperature for r in self.records if r.temperature is not None]
        power_values = [r.power for r in self.records if r.power and r.power > 0]

        # ── 心率 ──
        result.has_heart_rate = len(hr_values) > 0
        if result.has_heart_rate:
            result.avg_heart_rate = round(sum(hr_values) / len(hr_values))
            result.max_heart_rate = max(hr_values)
            result.hr_zone_distribution = self._calc_zone_distribution(
                hr_values, HR_ZONES, self.max_hr, is_relative=True
            )

        # ── 踏频 ──
        result.has_cadence = len(cadence_values) > 0
        if result.has_cadence:
            result.avg_cadence = round(sum(cadence_values) / len(cadence_values), 1)
            result.max_cadence = max(cadence_values)
            result.cadence_zone_distribution = self._calc_zone_distribution(
                cadence_values, CADENCE_ZONES, absolute=True
            )

        # ── 速度 (StandardUnits 已转为 km/h) ──
        if speed_values:
            result.avg_speed_kmh = round(sum(speed_values) / len(speed_values), 1)
            result.max_speed_kmh = round(max(speed_values), 1)
            result.speed_zone_distribution = self._calc_zone_distribution(
                speed_values, SPEED_ZONES, absolute=True
            )

        # ── 距离 (StandardUnits 已转为 km) ──
        distances = [r.distance for r in self.records if r.distance is not None]
        if distances:
            result.total_distance_km = round(distances[-1], 2)

        # ── 海拔 ──
        result.has_altitude = len(altitude_values) > 0
        if result.has_altitude:
            alts = [a for _, a in altitude_values]
            result.min_altitude_m = round(min(alts), 1)
            result.max_altitude_m = round(max(alts), 1)

            # 计算累计爬升/下降（忽略 <2m 的微小波动）
            prev_alt = altitude_values[0][1]
            for _, alt in altitude_values[1:]:
                diff = alt - prev_alt
                if abs(diff) >= 2:
                    if diff > 0:
                        result.total_ascent_m += diff
                    else:
                        result.total_descent_m += abs(diff)
                    prev_alt = alt
            result.total_ascent_m = round(result.total_ascent_m)
            result.total_descent_m = round(result.total_descent_m)

            # 坡度分布
            grade_values = [r.grade for r in self.records if r.grade is not None]
            if grade_values:
                result.grade_zone_distribution = self._calc_zone_distribution(
                    grade_values, GRADE_ZONES, absolute=True
                )

        # ── 温度 ──
        result.has_temperature = len(temp_values) > 0
        if result.has_temperature:
            result.avg_temperature = round(sum(temp_values) / len(temp_values), 1)
            result.min_temperature = round(min(temp_values), 1)
            result.max_temperature = round(max(temp_values), 1)

        # ── 功率 ──
        result.has_power = len(power_values) > 0
        if result.has_power:
            result.avg_power = round(sum(power_values) / len(power_values))
            result.max_power = max(power_values)

        # ── 总时间（基于记录点时间跨度） ──
        timestamps = [r.timestamp for r in self.records if r.timestamp > 0]
        if len(timestamps) >= 2:
            result.total_time_s = round(timestamps[-1] - timestamps[0])

        return result

    def _calc_zone_distribution(self, values, zones, max_val=None, is_relative=False, absolute=False):
        """
        计算区间分布

        Args:
            values: 数值列表
            zones: 区间定义 dict
            max_val: 最大值（用于相对区间计算）
            is_relative: True 表示 zones 的边界是相对值 (0-1)
            absolute: True 表示 zones 的边界是绝对值
        """
        total = len(values)
        distribution = {}

        for zone_name, zone_def in zones.items():
            lo, hi = zone_def[0], zone_def[1]
            count = 0
            if is_relative:
                lo_abs = lo * max_val
                hi_abs = hi * max_val
            else:
                lo_abs = lo
                hi_abs = hi

            for v in values:
                if lo_abs <= v < hi_abs:
                    count += 1
            # 上边界包含等于的情况
            if is_relative and hi >= 1.0:
                for v in values:
                    if v >= max_val:
                        count += 1

            distribution[zone_name] = {
                "count": count,
                "pct": round(count / total * 100, 1) if total > 0 else 0,
            }

        return distribution

    def to_dict(self, result: AnalysisResult) -> dict:
        """将 AnalysisResult 转为可 JSON 序列化的字典"""
        d = {
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
        return d

    def generate_report(self, result: AnalysisResult, activity_name: str = "") -> str:
        """
        基于结构化数据生成自然语言分析报告。
        当 LLM 不可用时，使用规则引擎生成。
        """
        lines = []
        
        # 标题
        if activity_name:
            lines.append(f"📊 {activity_name} 数据分析")
        else:
            lines.append(f"📊 骑行数据分析")
        lines.append("")

        # ── 亮点 ──
        lines.append("✨ 亮点：")

        # 爬坡分析
        if result.has_altitude and result.grade_zone_distribution:
            climb_pct = sum(
                v["pct"] for k, v in result.grade_zone_distribution.items()
                if "爬坡" in k or "陡坡" in k
            )
            if climb_pct > 20:
                lines.append(f"  🏔️ 爬坡能力突出！{climb_pct:.1f}% 时间在爬坡，累计爬升 {result.total_ascent_m} 米")
            elif climb_pct > 10:
                lines.append(f"  ⛰️ 包含适量爬坡，累计爬升 {result.total_ascent_m} 米（{climb_pct:.1f}% 时间）")

        # 速度分析
        if result.speed_zone_distribution:
            fast_pct = sum(
                v["pct"] for k, v in result.speed_zone_distribution.items()
                if "快速" in k or "高速" in k
            )
            medium_pct = sum(
                v["pct"] for k, v in result.speed_zone_distribution.items()
                if "中速" in k
            )
            if fast_pct > 15:
                lines.append(f"  ⚡ 高速巡航表现出色！{fast_pct:.1f}% 时间在 25km/h 以上")
            elif medium_pct > 30:
                lines.append(f"  🚴 平路巡航稳定，中速(18-25km/h)占 {medium_pct:.1f}%，均速 {result.avg_speed_kmh} km/h")
            else:
                lines.append(f"  🚴 均速 {result.avg_speed_kmh} km/h，最高速度 {result.max_speed_kmh} km/h")

        # 踏频分析
        if result.has_cadence and result.cadence_zone_distribution:
            optimal_pct = sum(
                v["pct"] for k, v in result.cadence_zone_distribution.items()
                if "高踏频" in k or "中踏频" in k
            )
            if optimal_pct > 60:
                lines.append(f"  🔄 踏频控制优秀！中高踏频(60-100rpm)占 {optimal_pct:.1f}%，平均 {result.avg_cadence} rpm")
            else:
                lines.append(f"  🔄 平均踏频 {result.avg_cadence} rpm，最高 {result.max_cadence} rpm")

        # 距离
        if result.total_distance_km > 0:
            hours = result.total_time_s / 3600
            if result.total_distance_km >= 40:
                lines.append(f"  🎯 长距离骑行！{result.total_distance_km} km，用时约 {hours:.1f}h")
            else:
                lines.append(f"  📏 总里程 {result.total_distance_km} km，用时约 {hours:.1f}h")

        # ── 待改进 ──
        lines.append("")
        lines.append("⚠️ 待改进：")

        improvement_count = 0

        # 心率区间分析
        if result.has_heart_rate and result.hr_zone_distribution:
            z2_pct = result.hr_zone_distribution.get("Z2_有氧基础", {}).get("pct", 0)
            z3_pct = result.hr_zone_distribution.get("Z3_有氧进阶", {}).get("pct", 0)
            z4_pct = result.hr_zone_distribution.get("Z4_乳酸阈值", {}).get("pct", 0)
            z5_pct = result.hr_zone_distribution.get("Z5_无氧极限", {}).get("pct", 0)

            if z5_pct > 20:
                lines.append(f"  🔴 无氧区间(Z5)占比 {z5_pct:.1f}% 偏高，注意控制强度")
                improvement_count += 1
            if z2_pct + z3_pct < 40:
                lines.append(f"  🟡 有氧基础训练可能不足（Z2+Z3 仅 {z2_pct+z3_pct:.1f}%），建议增加 Zone2 长距离骑行")
                improvement_count += 1
            if z4_pct > 40:
                lines.append(f"  🟠 乳酸阈值区间(Z4)占比 {z4_pct:.1f}%，训练强度偏高，注意恢复")
                improvement_count += 1

        # 踏频
        if result.has_cadence and result.cadence_zone_distribution:
            low_cadence_pct = result.cadence_zone_distribution.get("低踏频(<60)", {}).get("pct", 0)
            if low_cadence_pct > 30:
                lines.append(f"  🦵 低踏频(<60rpm)占比 {low_cadence_pct:.1f}%，可能加重膝盖负担")
                improvement_count += 1

        # 功率缺失
        if not result.has_power:
            lines.append(f"  ⚡ 缺少功率数据（无功率计），训练负荷无法量化评估")
            improvement_count += 1

        if improvement_count == 0:
            lines.append(f"  ✅ 本次骑行数据表现均衡，暂无显著改进项")

        # ── 建议 ──
        lines.append("")
        lines.append("💡 建议：")

        if result.has_heart_rate:
            z2 = result.hr_zone_distribution.get("Z2_有氧基础", {})
            z2_pct = z2.get("pct", 0) if z2 else 0
            if z2_pct < 25:
                lines.append(f"  1. 每周增加 1-2 次 Zone2 长距离骑行（心率 {int(self.max_hr*0.6)}-{int(self.max_hr*0.7)} bpm），提升有氧基础")
            if result.hr_zone_distribution.get("Z5_无氧极限", {}).get("pct", 0) > 15:
                lines.append(f"  2. 高强度占比偏高，注意安排充分的恢复时间（至少 48h）")

        if result.has_cadence:
            low_cadence_pct = result.cadence_zone_distribution.get("低踏频(<60)", {}).get("pct", 0)
            if low_cadence_pct > 20:
                lines.append(f"  3. 尝试提高踏频至 80-95 rpm，减少膝关节压力")

        if result.total_distance_km < 15 and result.total_distance_km > 0:
            lines.append(f"  📌 当前为短距离骑行（{result.total_distance_km}km），建议每周安排 1-2 次 30km+ 中长距离")

        if not result.has_power:
            lines.append(f"  📌 如需更精准的训练评估，可考虑添加功率计，以获取 TSS/IF 等量化指标")

        lines.append("")
        lines.append("---")
        lines.append(f"📈 数据点数: {result.record_count} | 传感器: "
                     f"{'❤️' if result.has_heart_rate else ''}"
                     f"{'🔄' if result.has_cadence else ''}"
                     f"{'⚡' if result.has_power else ''}"
                     f"{'🌡️' if result.has_temperature else ''}"
                     f"{'⛰️' if result.has_altitude else ''}")
        lines.append("")

        return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="FIT 文件骑行数据分析")
    parser.add_argument("fit_file", help="FIT 文件路径")
    parser.add_argument("--max-hr", type=int, default=DEFAULT_MAX_HR, help=f"最大心率（默认 {DEFAULT_MAX_HR}）")
    parser.add_argument("--json", action="store_true", help="输出结构化 JSON")
    parser.add_argument("--report", action="store_true", help="输出自然语言分析报告")
    parser.add_argument("--activity-name", default="", help="活动名称（用于报告标题）")
    parser.add_argument("--verbose", action="store_true", help="详细输出")
    args = parser.parse_args()

    fit_path = Path(args.fit_file)
    if not fit_path.exists():
        print(f"错误: 文件不存在: {args.fit_file}", file=sys.stderr)
        sys.exit(1)

    fit_data = fit_path.read_bytes()

    analyzer = FitAnalyzer(fit_data, max_hr=args.max_hr, verbose=args.verbose)
    result = analyzer.analyze()

    if args.json or not args.report:
        print(json.dumps(analyzer.to_dict(result), ensure_ascii=False, indent=2))

    if args.report:
        print(analyzer.generate_report(result, args.activity_name))


if __name__ == "__main__":
    main()
