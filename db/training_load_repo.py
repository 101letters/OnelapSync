"""
Training Load 数据访问层 — TrainingLoadRepo
==============================================
提供训练负荷（CTL/ATL/TSB）的 CRUD 操作。
"""
from __future__ import annotations

from datetime import datetime, date


class TrainingLoadRepo:
    """训练负荷（CTL/ATL/TSB）数据库操作。需要 self.conn（由 ConnMixin 提供）。"""

    def save_training_load(
        self,
        date_str: str,
        trimp: float,
        ctl: float,
        atl: float,
        tsb: float,
        activity_count: int = 1,
    ):
        self.conn.execute(
            """INSERT OR REPLACE INTO training_load
               (date, trimp, ctl, atl, tsb, activity_count, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
            (date_str, trimp, ctl, atl, tsb, activity_count),
        )
        self.conn.commit()

    def get_training_load(self, date_str: str) -> dict | None:
        cur = self.conn.execute(
            "SELECT * FROM training_load WHERE date=?", (date_str,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_training_load_range(self, start_date: str, end_date: str) -> list[dict]:
        cur = self.conn.execute(
            "SELECT * FROM training_load WHERE date >= ? AND date <= ? ORDER BY date",
            (start_date, end_date),
        )
        return [dict(row) for row in cur.fetchall()]

    def get_latest_training_load(self) -> dict | None:
        cur = self.conn.execute(
            "SELECT * FROM training_load ORDER BY date DESC LIMIT 1"
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def delete_training_load(self, date_str: str):
        self.conn.execute("DELETE FROM training_load WHERE date=?", (date_str,))
        self.conn.commit()
