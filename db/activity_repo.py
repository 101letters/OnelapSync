"""
Activity 数据访问层 — ActivityRepo
====================================
活动表的 CRUD 操作（任务领取、状态管理、FIT缓存路径）。

使用方式：通过 Database 多继承混入，self.conn 由 ConnMixin 提供。
"""

from __future__ import annotations

import json
import logging
import os
import time as ttime
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .connection import CODE_AGENT_DIR

logger = logging.getLogger(__name__)


class ActivityRepo:
    """活动相关数据库操作。需要 self.conn（由 ConnMixin 提供）。"""

    # ─── 活动 CRUD ──────────────────────────────

    def upsert_activity(self, onelap_id: str, **kwargs):
        """插入或更新活动记录。"""
        now = datetime.now(timezone.utc).isoformat()
        fields = {
            "onelap_id": onelap_id,
            "strava_id": kwargs.get("strava_id"),
            "fit_sha256": kwargs.get("fit_sha256"),
            "file_key": kwargs.get("file_key", ""),
            "name": kwargs.get("name"),
            "start_time": kwargs.get("start_time"),
            "distance_km": kwargs.get("distance_km"),
            "avg_heart_rate": kwargs.get("avg_heart_rate"),
            "status": kwargs.get("status", "uploaded"),
            "error_msg": kwargs.get("error_msg"),
            "priority": kwargs.get("priority", 0),
            "sub_status": kwargs.get("sub_status", ""),
            "updated_at": now,
        }
        cur = self.conn.execute("SELECT created_at FROM activities WHERE onelap_id=?", (onelap_id,))
        existing = cur.fetchone()
        fields["created_at"] = existing["created_at"] if existing else now

        cols = ", ".join(fields.keys())
        placeholders = ", ".join("?" for _ in fields)
        updates = ", ".join(f"{k}=excluded.{k}" for k in fields if k != "onelap_id")

        self.conn.execute(
            f"INSERT INTO activities ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT(onelap_id) DO UPDATE SET {updates}",
            list(fields.values()),
        )
        self.conn.commit()

    def get_activity(self, onelap_id: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM activities WHERE onelap_id=?", (onelap_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_activity_by_strava_id(self, strava_id: int) -> dict | None:
        cur = self.conn.execute("SELECT * FROM activities WHERE strava_id=?", (strava_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_activity_by_fit_hash(self, fit_sha256: str) -> dict | None:
        if not fit_sha256:
            return None
        cur = self.conn.execute("SELECT * FROM activities WHERE fit_sha256=?", (fit_sha256,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_all_activities(self) -> dict[str, dict]:
        """返回 {onelap_id: {...}} 格式，兼容旧 state['activities']。"""
        cur = self.conn.execute("SELECT * FROM activities ORDER BY start_time")
        result = {}
        for row in cur.fetchall():
            d = dict(row)
            result[d.pop("onelap_id")] = {
                "name": d["name"] or "",
                "start_time": d["start_time"] or "",
                "distance_km": d["distance_km"] or 0,
                "strava_id": d["strava_id"],
                "fit_sha256": d["fit_sha256"] or "",
                "synced_at": d["updated_at"] or d["created_at"] or "",
                "status": d["status"] or "synced",
            }
        return result

    def get_recent_activities(self, days: int = 7) -> list[dict]:
        """获取最近 N 天的活动列表（按时间倒序）。"""
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cur = self.conn.execute(
            "SELECT * FROM activities WHERE start_time >= ? ORDER BY start_time DESC",
            (cutoff,),
        )
        return [dict(row) for row in cur.fetchall()]

    def count_activities_since(self, days: int = 7) -> int:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        cur = self.conn.execute(
            "SELECT COUNT(*) as cnt FROM activities WHERE start_time >= ?", (cutoff,)
        )
        return cur.fetchone()["cnt"]

    def update_activity_status(self, onelap_id: str, status: str, error_msg: str = None):
        self.conn.execute(
            "UPDATE activities SET status=?, error_msg=?, updated_at=datetime('now') WHERE onelap_id=?",
            (status, error_msg, onelap_id),
        )
        self.conn.commit()

    def update_activity_sub_status(self, onelap_id: str, sub_status: str):
        self.conn.execute(
            "UPDATE activities SET sub_status=? WHERE onelap_id=?",
            (sub_status, onelap_id),
        )
        self.conn.commit()

    def activity_exists(self, onelap_id: str) -> bool:
        cur = self.conn.execute("SELECT 1 FROM activities WHERE onelap_id=?", (onelap_id,))
        return cur.fetchone() is not None

    def get_activities_by_status(self, status: str, limit: int = 10) -> list[dict]:
        """按状态获取活动列表（按优先级降序，旧活动优先处理）。"""
        cur = self.conn.execute(
            "SELECT * FROM activities WHERE status=? ORDER BY priority DESC, start_time ASC LIMIT ?",
            (status, limit),
        )
        return [dict(row) for row in cur.fetchall()]

    def count_by_status(self, status: str) -> int:
        cur = self.conn.execute("SELECT COUNT(*) as cnt FROM activities WHERE status=?", (status,))
        return cur.fetchone()["cnt"]

    # ─── 任务领取机制 ──────────────────────────

    def claim_activities(
        self,
        from_status: str,
        to_status: str,
        worker_id: str,
        limit: int = 5,
        max_retries: int = 3,
        backoff_minutes: int = 5,
    ) -> list[dict]:
        """
        原子领取任务：将 from_status 或可重试失败态记录标记为 to_status + 加锁。
        保证一条任务只被一个 worker 处理。

        兼容旧调用：例如 claim_activities('uploaded', 'analyzing', ...) 会领取
        uploaded，同时领取未超重试次数且到达 next_retry_at 的 analyze_failed。
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        lock_timeout = 300  # 5分钟锁超时
        retry_status_map = {
            "uploaded": ("analyze_failed",),
            "analyzed": ("coach_failed",),
            "coached": ("write_failed",),
        }
        retry_statuses = retry_status_map.get(from_status, ())

        # 保留参数以兼容签名；实际退避时间由 set_activity_error 写入 next_retry_at。
        _ = backoff_minutes

        # V4.5：领取只认未加锁任务。领取成功时递增 lock_version，
        # 后续 release 使用 claim 时拿到的版本做乐观锁校验。
        _ = lock_timeout
        lock_clause = "locked_by IS NULL"

        with self.conn:
            params: list[Any] = [from_status]
            if retry_statuses:
                retry_placeholders = ",".join("?" for _ in retry_statuses)
                status_clause = f"""(status=?
                         OR (status IN ({retry_placeholders})
                             AND COALESCE(retry_count, 0) < ?
                             AND (next_retry_at IS NULL OR datetime(next_retry_at) < datetime('now'))))"""
                params.extend(retry_statuses)
                params.append(max_retries)
            else:
                status_clause = "status=?"
            params.append(limit)

            cur = self.conn.execute(
                f"""SELECT onelap_id FROM activities
                   WHERE {lock_clause}
                     AND {status_clause}
                   ORDER BY priority DESC, start_time ASC
                   LIMIT ?""",
                params,
            )
            candidate_ids = [row["onelap_id"] for row in cur.fetchall()]
            if not candidate_ids:
                return []

            ids: list[str] = []
            for onelap_id in candidate_ids:
                update_cur = self.conn.execute(
                    """UPDATE activities
                       SET status=?,
                           locked_by=?,
                           locked_at=?,
                           lock_version=COALESCE(lock_version, 0) + 1,
                           updated_at=?
                       WHERE onelap_id=?
                         AND locked_by IS NULL""",
                    (to_status, worker_id, now_iso, now_iso, onelap_id),
                )
                if update_cur.rowcount:
                    ids.append(onelap_id)

        if not ids:
            return []

        cur = self.conn.execute(
            f"SELECT * FROM activities WHERE onelap_id IN ({','.join('?' for _ in ids)})",
            ids,
        )
        return [dict(row) for row in cur.fetchall()]

    def release_activity(
        self,
        onelap_id: str,
        new_status: str,
        error_msg: str = None,
        expected_version: int = None,
    ) -> bool:
        """释放任务锁，同时更新状态；expected_version 非空时启用乐观锁校验。"""
        if expected_version is None:
            cur = self.conn.execute(
                "UPDATE activities SET status=?, locked_by=NULL, locked_at=NULL, error_msg=?, last_error=NULL, next_retry_at=NULL, retry_count=0, updated_at=datetime('now') WHERE onelap_id=?",
                (new_status, error_msg, onelap_id),
            )
        else:
            cur = self.conn.execute(
                "UPDATE activities SET status=?, locked_by=NULL, locked_at=NULL, error_msg=?, last_error=NULL, next_retry_at=NULL, retry_count=0, updated_at=datetime('now') WHERE onelap_id=? AND lock_version=?",
                (new_status, error_msg, onelap_id, expected_version),
            )
        self.conn.commit()

        if expected_version is not None and cur.rowcount == 0:
            logger.warning(
                "⚠️ release_activity 乐观锁校验失败，跳过释放: onelap_id=%s expected_version=%s new_status=%s",
                onelap_id,
                expected_version,
                new_status,
            )
            return False
        return True

    def set_activity_error(self, onelap_id: str, error_msg: str, retry_delay_minutes: int = 5):
        """设置失败态 + 错误信息 + 指数退避时间。retry_count 自增。"""
        act = self.get_activity(onelap_id)
        if not act:
            return

        failed_status_map = {
            "analyzing": "analyze_failed",
            "uploaded": "analyze_failed",
            "coaching": "coach_failed",
            "analyzed": "coach_failed",
            "writing": "write_failed",
            "coached": "write_failed",
        }
        current_status = act.get("status")
        failed_status = failed_status_map.get(
            current_status,
            current_status if str(current_status).endswith("_failed") else "failed",
        )

        retry_count = int(act.get("retry_count") or 0) + 1
        delay_minutes = min(retry_delay_minutes * (2 ** (retry_count - 1)), 30)
        next_retry_at = (datetime.now() + timedelta(minutes=delay_minutes)).strftime("%Y-%m-%d %H:%M:%S")
        msg = (error_msg or "")[:1000]

        self.conn.execute(
            """UPDATE activities
               SET status=?,
                   locked_by=NULL,
                   locked_at=NULL,
                   error_msg=?,
                   last_error=?,
                   retry_count=?,
                   next_retry_at=?,
                   updated_at=datetime('now')
               WHERE onelap_id=?""",
            (failed_status, msg, msg, retry_count, next_retry_at, onelap_id),
        )
        self.conn.commit()

    def count_by_status_group(self) -> dict[str, int]:
        """按状态分组统计活动数。"""
        cur = self.conn.execute("SELECT status, COUNT(*) as cnt FROM activities GROUP BY status")
        return {row["status"]: row["cnt"] for row in cur.fetchall()}

    # ─── FIT 缓存路径 ──────────────────────────

    def fit_cache_dir(self) -> Path:
        """FIT 文件缓存目录。"""
        d = CODE_AGENT_DIR / "fit_cache"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def fit_cache_path(self, onelap_id: str) -> Path:
        """FIT 文件缓存路径。"""
        return self.fit_cache_dir() / f"{onelap_id}.fit"

    def clean_fit_cache(self, keep_days: int = 14):
        """清理超过 keep_days 的 FIT 缓存文件。"""
        import time as ttime
        cutoff = ttime.time() - keep_days * 86400
        removed = 0
        for f in self.fit_cache_dir().iterdir():
            if f.suffix == ".fit" and f.stat().st_mtime < cutoff:
                f.unlink(missing_ok=True)
                removed += 1
        if removed:
            print(f"  清理了 {removed} 个过期 FIT 缓存文件")

    def delete_fit_cache(self, onelap_id: str) -> bool:
        """删除指定活动的 FIT 缓存文件。"""
        path = self.fit_cache_path(onelap_id)
        if path.exists():
            path.unlink()
            return True
        return False

    # ─── P1-6 去重前置 ─────────────────────────

    def check_activity_dedup(self, file_key: str) -> dict | None:
        """按 file_key 查重（跨主键，避免同 FIT 重复下载）。

        返回匹配的已处理活动 dict，或 None。

        匹配条件：file_key 相同，且 status 为已处理终态
        （已上传/分析中/已分析/教练中/已教练/写回中/已完成/已去重）。
        """
        if not file_key:
            return None
        cur = self.conn.execute(
            "SELECT * FROM activities WHERE file_key=? "
            "AND status IN ('uploaded','analyzing','analyzed',"
            "'coaching','coached','writing','completed',"
            "'dedup_by_fit_sha256')",
            (file_key,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
