"""
Database — 组合类
====================
通过多继承组合所有 mixin，对外暴露统一的 Database 接口。

用法：
    from db import Database
    db = Database()
    db.upsert_activity(onelap_id="xxx", ...)
    db.get_recent_activities(days=7)
"""

from __future__ import annotations

from .connection import ConnMixin
from .activity_repo import ActivityRepo
from .analysis_repo import AnalysisRepo
from .coach_repo import CoachRepo
from .task_repo import TaskRepo
from .writeback_repo import WritebackRepo
from .training_load_repo import TrainingLoadRepo
from .training_plans_repo import TrainingPlansRepo


class Database(ConnMixin, ActivityRepo, AnalysisRepo, CoachRepo, TaskRepo, WritebackRepo, TrainingLoadRepo, TrainingPlansRepo):
    """
    统一的数据库接口。

    MRO（方法解析顺序）：
        Database → ConnMixin → ActivityRepo → AnalysisRepo → CoachRepo
                 → TaskRepo → WritebackRepo → TrainingLoadRepo → TrainingPlansRepo

    所有 mixin 通过 self.conn 访问数据库连接（由 ConnMixin 提供）。
    """
    pass
