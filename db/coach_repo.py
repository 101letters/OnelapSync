"""
Coach 数据访问层 — CoachRepo
===============================
coach_outputs 表 + coach_cache 表的 CRUD 操作。

使用方式：通过 Database 多继承混入，self.conn 由 ConnMixin 提供。
"""

from __future__ import annotations

import json


class CoachRepo:
    """教练分析相关数据库操作。需要 self.conn（由 ConnMixin 提供）。"""

    # ─── Coach Outputs ──────────────────────────

    def save_coach_output(self, onelap_id: str, raw: str, cleaned: str, model: str = "", source: str = "ai_coach"):
        self.conn.execute(
            "INSERT OR REPLACE INTO coach_outputs (onelap_id, raw_output, cleaned_output, model, source, created_at) "
            "VALUES (?, ?, ?, ?, ?, datetime('now'))",
            (onelap_id, raw, cleaned, model, source),
        )
        self.conn.commit()

    def get_coach_output(self, onelap_id: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM coach_outputs WHERE onelap_id=?", (onelap_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    # ─── Coach Cache（DB级，替代 coach_cache.json）──

    def get_coach_cache(self, content_hash: str) -> str | None:
        """读取 coach 缓存，返回 cleaned_output 或 None。"""
        cur = self.conn.execute("SELECT output FROM coach_cache WHERE content_hash=?", (content_hash,))
        row = cur.fetchone()
        return row["output"] if row else None

    def set_coach_cache(self, content_hash: str, output: str):
        """写入 coach 缓存（原子操作）。"""
        self.conn.execute(
            "INSERT OR REPLACE INTO coach_cache (content_hash, output, created_at) VALUES (?, ?, datetime('now'))",
            (content_hash, output),
        )
        self.conn.commit()

    def get_coach_cache_stats(self) -> dict:
        """缓存命中统计。"""
        cur = self.conn.execute("SELECT COUNT(*) as total FROM coach_cache")
        total = cur.fetchone()["total"]
        return {"total_entries": total}
