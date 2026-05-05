"""
analysis_engine.models — 数据模型
===================================
RideRecord：单条记录点（时间戳/心率/踏频/速度/海拔/温度/功率）
AnalysisResult：完整分析结果（区间分布/爬升/统计摘要）
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RideRecord:
    """FIT 文件中的单条记录点"""
    timestamp: float = 0
    heart_rate: Optional[int] = None
    cadence: Optional[int] = None
    speed: Optional[float] = None       # m/s
    altitude: Optional[float] = None     # meters
    temperature: Optional[float] = None  # °C
    power: Optional[int] = None          # watts
    distance: Optional[float] = None     # meters
    grade: Optional[float] = None        # percent


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

    # 爬升数据
    total_ascent_m: float = 0
    total_descent_m: float = 0
    min_altitude_m: float = 0
    max_altitude_m: float = 0

    # 温度
    avg_temperature: Optional[float] = None
    min_temperature: Optional[float] = None
    max_temperature: Optional[float] = None

    # 区间分布（秒级统计）
    hr_zone_distribution: dict = field(default_factory=dict)
    speed_zone_distribution: dict = field(default_factory=dict)
    cadence_zone_distribution: dict = field(default_factory=dict)
    grade_zone_distribution: dict = field(default_factory=dict)

    # 实际记录点数
    record_count: int = 0

    # 能力标记（哪些数据可用）
    has_heart_rate: bool = False
    has_cadence: bool = False
    has_power: bool = False
    has_temperature: bool = False
    has_altitude: bool = False
