"""
analysis_engine — FIT 文件解析与骑行数据分析引擎
=================================================
模块化拆分为：
  zones.py     区间定义（心率/速度/踏频/坡度）
  models.py    数据模型（RideRecord, AnalysisResult）
  parser.py    FIT 文件解析器
  analyzer.py  核心分析引擎（FitAnalyzer）

向后兼容：
  from analysis_engine import FitAnalyzer
  from analysis_engine import AnalysisResult, RideRecord
"""

from .analyzer import FitAnalyzer
from .models import AnalysisResult, RideRecord
from .parser import parse_fit
from .zones import HR_ZONES, SPEED_ZONES, CADENCE_ZONES, GRADE_ZONES, DEFAULT_MAX_HR
from .fallback import generate_fallback_description, generate_fallback_tip

__all__ = [
    "FitAnalyzer",
    "AnalysisResult",
    "RideRecord",
    "parse_fit",
    "HR_ZONES",
    "SPEED_ZONES",
    "CADENCE_ZONES",
    "GRADE_ZONES",
    "DEFAULT_MAX_HR",
    "generate_fallback_description",
    "generate_fallback_tip",
]
