"""
QwenPaw OneLap Sync — 数据库模块
===================================

表结构：
  - activities        活动主表（取代 sync_state_v3.json）
  - analysis          分析结果表（取代 activity_analysis_results.json）
  - coach_outputs     AI 教练输出
  - coach_cache       教练输出缓存（替代 coach_cache.json）
  - writeback_log     Strava 写回幂等日志
  - pending_uploads   Strava 上传轮询队列
  - meta              系统元数据
  - tasks             任务队列
  - training_load     训练负荷表（CTL/ATL/TSB）
  - training_plans    训练计划表（每周教练计划）
  - training_errors   执行偏差记录表

用法：
    from db import Database
    db = Database()
    db.upsert_activity(onelap_id="xxx", strava_id=123, ...)
    db.claim_activities("uploaded", "analyzing", "analyze_worker", limit=5)
    db.get_coach_cache(content_hash="...")
"""

from .database import Database
from .activity_repo import ActivityRepo
from .analysis_repo import AnalysisRepo
from .coach_repo import CoachRepo
from .task_repo import TaskRepo
from .writeback_repo import WritebackRepo
from .training_load_repo import TrainingLoadRepo
from .training_plans_repo import TrainingPlansRepo

__all__ = [
    "Database",
    "ActivityRepo",
    "AnalysisRepo",
    "CoachRepo",
    "TaskRepo",
    "WritebackRepo",
    "TrainingLoadRepo",
    "TrainingPlansRepo",
]
