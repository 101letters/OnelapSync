"""
Writeback 日志数据访问层 — WritebackRepo
=========================================
记录已成功写回 Strava 的活动，用于保证 writeback_worker 幂等：
Strava API 写入成功后，先落库 writeback_log，再释放 activities 状态。
"""

from __future__ import annotations


class WritebackRepo:
    """写回日志相关数据库操作。需要 self.conn（由 ConnMixin 提供）。"""

    def log_writeback(self, activity_id: str, strava_id: int, content_hash: str, success: bool = True):
        """记录一次 Strava 写回结果。activity_id 唯一，重复记录时更新最新内容。"""
        self.conn.execute(
            """
            INSERT INTO writeback_log (activity_id, strava_id, content_hash, success, created_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(activity_id) DO UPDATE SET
                strava_id=excluded.strava_id,
                content_hash=excluded.content_hash,
                success=excluded.success,
                created_at=excluded.created_at
            """,
            (activity_id, strava_id, content_hash, 1 if success else 0),
        )
        self.conn.commit()

    def has_writeback(self, activity_id: str) -> bool:
        """判断该活动是否已有成功写回记录。"""
        cur = self.conn.execute(
            "SELECT 1 FROM writeback_log WHERE activity_id=? AND success=1 LIMIT 1",
            (activity_id,),
        )
        return cur.fetchone() is not None

    def count_writebacks_since(self, days: int = 7) -> int:
        """统计最近 N 天成功写回次数。"""
        cur = self.conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM writeback_log
            WHERE success=1
              AND datetime(created_at) >= datetime('now', ?)
            """,
            (f"-{int(days)} days",),
        )
        return cur.fetchone()["cnt"]
