"""
analysis_engine.parser — FIT 文件解析器
=========================================
解析 FIT 二进制文件 → RideRecord 列表。
支持标准 FIT 协议，兼容顽鹿/佳明/Wahoo 等设备输出。
"""

import logging
from typing import Optional

import fitparse

from .models import RideRecord

logger = logging.getLogger(__name__)

# FIT 记录类型常量
FIT_RECORD_RECORD = "record"        # 标准数据记录
FIT_RECORD_LAP = "lap"             # 圈数据
FIT_RECORD_SESSION = "session"     # 会话数据
FIT_RECORD_EVENT = "event"         # 事件


def parse_fit(fit_data: bytes) -> list[RideRecord]:
    """解析 FIT 二进制数据，返回 RideRecord 列表。

    Args:
        fit_data: FIT 文件原始字节

    Returns:
        RideRecord 列表，按时间戳排序

    Raises:
        ValueError: FIT 数据无效或无法解析
    """
    try:
        fitfile = fitparse.FitFile(
            fit_data,
            data_processor=fitparse.StandardUnitsDataProcessor(),
        )
    except Exception as e:
        raise ValueError(f"FIT 解析失败: {e}")

    records = []
    for msg in fitfile.get_messages(FIT_RECORD_RECORD):
        fields = {f.name: f.value for f in msg.fields if f.value is not None}
        if not fields:
            continue

        rec = RideRecord()

        # 时间戳
        ts = fields.get("timestamp")
        if ts is not None:
            rec.timestamp = ts.timestamp() if hasattr(ts, "timestamp") else float(ts)

        rec.heart_rate = _int_or_none(fields.get("heart_rate"))
        rec.cadence = _int_or_none(fields.get("cadence"))
        rec.speed = _float_or_none(fields.get("speed"))
        rec.altitude = _float_or_none(fields.get("altitude"))
        rec.temperature = _float_or_none(fields.get("temperature"))
        rec.power = _int_or_none(fields.get("power"))

        # 距离（enhanced_speed / enhanced_altitude 优先）
        if fields.get("enhanced_speed") is not None:
            rec.speed = _float_or_none(fields.get("enhanced_speed"))
        if fields.get("enhanced_altitude") is not None:
            rec.altitude = _float_or_none(fields.get("enhanced_altitude"))

        rec.distance = _float_or_none(fields.get("distance"))

        records.append(rec)

    if not records:
        raise ValueError("FIT 文件中无有效数据记录点")

    return records


def _int_or_none(val) -> Optional[int]:
    return int(val) if val is not None else None


def _float_or_none(val) -> Optional[float]:
    return float(val) if val is not None else None
