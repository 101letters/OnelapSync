"""
任务/元数据/状态 数据访问层 — TaskRepo
=========================================
meta / pending_uploads / tasks 表 + 旧 JSON 状态兼容。

使用方式：通过 Database 多继承混入，self.conn 由 ConnMixin 提供。
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class TaskRepo:
    """任务与元数据操作。需要 self.conn（由 ConnMixin 提供）。"""

    # ─── Meta（系统元数据）───────────────────────

    def get_meta(self, key: str, default: Any = None) -> Any:
        cur = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = cur.fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except (json.JSONDecodeError, TypeError):
            return row["value"]

    def set_meta(self, key: str, value: Any):
        self.conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            (key, json.dumps(value, ensure_ascii=False)),
        )
        self.conn.commit()

    # ─── Pending Uploads ─────────────────────────

    def save_pending_upload(self, onelap_record_id: str, upload_id: str, activity_name: str):
        self.conn.execute(
            "INSERT OR REPLACE INTO pending_uploads (onelap_record_id, upload_id, activity_name, status, updated_at) "
            "VALUES (?, ?, ?, 'pending', datetime('now'))",
            (onelap_record_id, upload_id, activity_name),
        )
        self.conn.commit()

    def mark_pending_completed(self, onelap_record_id: str):
        self.conn.execute(
            "UPDATE pending_uploads SET status='completed', updated_at=datetime('now') WHERE onelap_record_id=?",
            (onelap_record_id,),
        )
        self.conn.commit()

    def mark_pending_failed(self, onelap_record_id: str, error: str):
        self.conn.execute(
            "UPDATE pending_uploads SET status='failed', error=?, updated_at=datetime('now') WHERE onelap_record_id=?",
            (error, onelap_record_id),
        )
        self.conn.commit()

    def get_pending_uploads(self) -> dict[str, dict]:
        cur = self.conn.execute("SELECT * FROM pending_uploads ORDER BY updated_at DESC")
        result = {}
        for row in cur.fetchall():
            d = dict(row)
            result[d.pop("onelap_record_id")] = d
        return result

    def clear_pending_upload(self, onelap_record_id: str):
        self.conn.execute("DELETE FROM pending_uploads WHERE onelap_record_id=?", (onelap_record_id,))
        self.conn.commit()

    # ─── Tasks ──────────────────────────────────

    def create_task(self, onelap_id: str, task_type: str):
        self.conn.execute(
            "INSERT INTO tasks (onelap_id, task_type, status, created_at) VALUES (?, ?, 'pending', datetime('now'))",
            (onelap_id, task_type),
        )
        self.conn.commit()

    def get_pending_tasks(self, task_type: str = None, limit: int = 10) -> list[dict]:
        if task_type:
            cur = self.conn.execute(
                "SELECT * FROM tasks WHERE status='pending' AND task_type=? ORDER BY created_at ASC LIMIT ?",
                (task_type, limit),
            )
        else:
            cur = self.conn.execute(
                "SELECT * FROM tasks WHERE status='pending' ORDER BY created_at ASC LIMIT ?",
                (limit,),
            )
        return [dict(row) for row in cur.fetchall()]

    def update_task_status(self, task_id: int, status: str, error_msg: str = None):
        self.conn.execute(
            "UPDATE tasks SET status=?, error_msg=?, updated_at=datetime('now') WHERE id=?",
            (status, error_msg, task_id),
        )
        self.conn.commit()

    # ─── 旧 JSON 状态兼容 ────────────────────────

    def load_state_dict(self) -> dict:
        """从 DB 构建兼容旧 sync_state_v3.json 格式的字典。"""
        state = {
            "activities": self.get_all_activities(),
            "last_run": self.get_meta("last_run", ""),
            "last_check_time": self.get_meta("last_check_time", ""),
            "consecutive_failures": self.get_meta("consecutive_failures", 0),
            "total_synced": self.get_meta("total_synced", 0),
            "version": "3.4-db",
            "updated_at": datetime.now().isoformat(),
        }
        analysis_list = self.get_all_analysis()
        state["analysis_results"] = {a["onelap_id"]: a for a in analysis_list}
        return state

    def save_state_dict(self, state: dict):
        """将兼容旧格式的状态字典持久化到 meta 表。"""
        for key in ("last_run", "last_check_time", "consecutive_failures", "total_synced"):
            if key in state:
                self.set_meta(key, state[key])
