"""
数据库连接管理 — ConnMixin
===========================
提供：
- 线程安全的 SQLite 连接管理（WAL 模式）
- 初始化建表（DDL）
- 旧表自动迁移（v1 → v2）

被 Database 类通过多继承使用（MRO 提供 self.conn）。
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from pathlib import Path

_PKG_DIR = Path(__file__).resolve().parent
CODE_AGENT_DIR = _PKG_DIR.parent
DB_PATH = CODE_AGENT_DIR / "onelap_sync.db"
DEFAULT_MAX_HR = int(os.environ.get("ANALYSIS_MAX_HR", "194"))


class ConnMixin:
    """SQLite 连接管理 mixin。子类通过 self.conn 访问数据库。"""

    def __init__(self, db_path: str | Path = DB_PATH):
        self.db_path = Path(db_path)
        self._local = threading.local()
        self._init_db()

    # ─── 连接 ─────────────────────────────────

    @property
    def conn(self) -> sqlite3.Connection:
        """每个线程独立的数据库连接。"""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(self.db_path))
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA foreign_keys=ON")
        return self._local.conn

    def close(self):
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ─── 初始化建表 ───────────────────────────

    def _init_db(self):
        conn = self.conn
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS activities (
                onelap_id       TEXT PRIMARY KEY,
                strava_id       BIGINT,
                fit_sha256      TEXT,
                name            TEXT,
                start_time      TEXT,
                distance_km     REAL,
                avg_heart_rate  INTEGER,
                status          TEXT DEFAULT 'uploaded'
                                CHECK(status IN (
                                    'uploaded','analyzing','analyzed',
                                    'coaching','coached',
                                    'writing','completed',
                                    'analyze_failed','coach_failed','write_failed',
                                    'failed','dedup_by_fit_sha256','skipped'
                                )),
                locked_by       TEXT,
                locked_at       TEXT,
                lock_version    INTEGER DEFAULT 0,
                error_msg       TEXT,
                retry_count     INTEGER DEFAULT 0,
                last_error      TEXT,
                next_retry_at   TEXT,
                created_at      TEXT DEFAULT (datetime('now')),
                updated_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS analysis (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                onelap_id       TEXT NOT NULL REFERENCES activities(onelap_id),
                basic_stats     TEXT,
                hr_zones        TEXT,
                cadence_zones   TEXT,
                speed_zones     TEXT,
                grade_zones     TEXT,
                elevation       TEXT,
                temperature     TEXT,
                capabilities    TEXT,
                power_zones     TEXT,
                training_type   TEXT,
                created_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS coach_outputs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                onelap_id       TEXT NOT NULL REFERENCES activities(onelap_id),
                raw_output      TEXT,
                cleaned_output  TEXT,
                model           TEXT,
                source          TEXT DEFAULT 'ai_coach'
                                CHECK(source IN ('ai_coach','template_fallback')),
                created_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS coach_cache (
                content_hash    TEXT PRIMARY KEY,
                output          TEXT NOT NULL,
                created_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS writeback_log (
                activity_id     TEXT PRIMARY KEY,
                strava_id       BIGINT,
                content_hash    TEXT,
                success         INTEGER DEFAULT 1,
                created_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS pending_uploads (
                onelap_record_id TEXT PRIMARY KEY,
                upload_id       TEXT,
                activity_name   TEXT,
                status          TEXT DEFAULT 'pending'
                                CHECK(status IN ('pending','completed','failed')),
                error           TEXT,
                updated_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS meta (
                key             TEXT PRIMARY KEY,
                value           TEXT
            );

            CREATE TABLE IF NOT EXISTS tasks (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                onelap_id       TEXT NOT NULL REFERENCES activities(onelap_id),
                task_type       TEXT NOT NULL
                                CHECK(task_type IN ('sync','analyze','coach','writeback')),
                status          TEXT DEFAULT 'pending'
                                CHECK(status IN ('pending','running','done','failed','skipped')),
                retry_count     INTEGER DEFAULT 0,
                error_msg       TEXT,
                created_at      TEXT DEFAULT (datetime('now')),
                updated_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS training_load (
                date            TEXT PRIMARY KEY,
                trimp           REAL DEFAULT 0,
                ctl             REAL DEFAULT 0,
                atl             REAL DEFAULT 0,
                tsb             REAL DEFAULT 0,
                activity_count  INTEGER DEFAULT 0,
                updated_at      TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS training_plans (
                plan_id             TEXT PRIMARY KEY,
                week_id             TEXT NOT NULL,
                date                TEXT NOT NULL,
                planned_type        TEXT DEFAULT '',
                planned_duration_min REAL,
                planned_trimp       REAL,
                planned_zones       TEXT DEFAULT '{}',
                description         TEXT DEFAULT '',
                plan_context        TEXT DEFAULT '{}',
                created_at          TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS training_errors (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                plan_id             TEXT NOT NULL REFERENCES training_plans(plan_id),
                date                TEXT NOT NULL,
                strava_id           BIGINT,
                actual_duration_min REAL,
                actual_trimp        REAL,
                actual_type         TEXT DEFAULT '',
                intensity_factor    REAL,
                deviation_type      TEXT DEFAULT 'unknown'
                                    CHECK(deviation_type IN ('completed','partial','skipped','over','unknown')),
                deviation_pct       REAL DEFAULT 0,
                note                TEXT DEFAULT '',
                created_at          TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_training_plans_date
                ON training_plans(date);
            CREATE INDEX IF NOT EXISTS idx_training_plans_week
                ON training_plans(week_id);
            CREATE INDEX IF NOT EXISTS idx_training_errors_date
                ON training_errors(date);
            CREATE INDEX IF NOT EXISTS idx_training_errors_plan
                ON training_errors(plan_id);

            CREATE INDEX IF NOT EXISTS idx_activities_status
                ON activities(status);
            CREATE INDEX IF NOT EXISTS idx_activities_start_time
                ON activities(start_time);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_activities_fit_sha256
                ON activities(fit_sha256) WHERE fit_sha256 IS NOT NULL AND fit_sha256 != '';
            CREATE INDEX IF NOT EXISTS idx_analysis_onelap_id
                ON analysis(onelap_id);
            CREATE INDEX IF NOT EXISTS idx_coach_outputs_onelap_id
                ON coach_outputs(onelap_id);
            CREATE INDEX IF NOT EXISTS idx_tasks_status
                ON tasks(status);
            CREATE INDEX IF NOT EXISTS idx_tasks_onelap_id
                ON tasks(onelap_id);
        """)

        # ── 旧表迁移 ──
        self._migrate_activities_v2(conn)
        self._migrate_activities_retry_columns(conn)
        self._migrate_activities_lock_version(conn)
        self._migrate_fit_sha256_unique(conn)
        self._migrate_coach_outputs_source(conn)
        self._migrate_analysis_columns(conn)
        self._migrate_activities_sub_status(conn)
        self._migrate_activities_priority(conn)
        self._migrate_activities_file_key(conn)

        # ── 索引（迁移后建，确保列存在）──
        conn.execute("CREATE INDEX IF NOT EXISTS idx_activities_locked_by ON activities(locked_by)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_activities_locked_at ON activities(locked_at)")

    # ─── 旧表迁移 ────────────────────────────────

    def _migrate_fit_sha256_unique(self, conn: sqlite3.Connection):
        """清理重复 fit_sha256，保障 UNIQUE 索引不冲突。"""
        dups = conn.execute("""
            SELECT fit_sha256, COUNT(*) as cnt
            FROM activities
            WHERE fit_sha256 IS NOT NULL AND fit_sha256 != ''
            GROUP BY fit_sha256
            HAVING cnt > 1
        """).fetchall()
        if not dups:
            return
        _log = logging.getLogger("db.migrate")
        _log.warning(f"🔄 发现 {len(dups)} 组重复 fit_sha256，保留最新记录，其余置 NULL...")
        for row in dups:
            h = row["fit_sha256"]
            conn.execute("""
                UPDATE activities
                SET fit_sha256 = NULL
                WHERE fit_sha256 = ?
                  AND rowid NOT IN (
                      SELECT rowid FROM activities
                      WHERE fit_sha256 = ?
                      ORDER BY rowid DESC
                      LIMIT 1
                  )
            """, (h, h))
        conn.commit()

    def _migrate_coach_outputs_source(self, conn: sqlite3.Connection):
        """为已有 coach_outputs 表补齐 source 列（ai_coach / template_fallback）。"""
        cursor = conn.execute("PRAGMA table_info(coach_outputs)")
        cols = {row[1] for row in cursor.fetchall()}
        if "source" not in cols:
            conn.execute("ALTER TABLE coach_outputs ADD COLUMN source TEXT DEFAULT 'ai_coach'")
            conn.commit()

    def _migrate_analysis_columns(self, conn: sqlite3.Connection):
        """v2→v3: 为 analysis 表补齐 power_zones 和 training_type 列。"""
        cursor = conn.execute("PRAGMA table_info(analysis)")
        cols = {row[1] for row in cursor.fetchall()}
        migrations = [
            ("power_zones", "ALTER TABLE analysis ADD COLUMN power_zones TEXT"),
            ("training_type", "ALTER TABLE analysis ADD COLUMN training_type TEXT"),
        ]
        for col, sql in migrations:
            if col not in cols:
                conn.execute(sql)
        conn.commit()

    def _migrate_activities_sub_status(self, conn: sqlite3.Connection):
        """为已有 activities 表补齐 sub_status 列（细化状态）。"""
        cursor = conn.execute("PRAGMA table_info(activities)")
        cols = {row[1] for row in cursor.fetchall()}
        if "sub_status" not in cols:
            try:
                conn.execute("ALTER TABLE activities ADD COLUMN sub_status TEXT DEFAULT ''")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            conn.commit()

    def _migrate_activities_priority(self, conn: sqlite3.Connection):
        """为已有 activities 表补齐 priority 列（队列优先级）。"""
        cursor = conn.execute("PRAGMA table_info(activities)")
        cols = {row[1] for row in cursor.fetchall()}
        if "priority" not in cols:
            try:
                conn.execute("ALTER TABLE activities ADD COLUMN priority INTEGER DEFAULT 0")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            conn.commit()

    def _migrate_activities_lock_version(self, conn: sqlite3.Connection):
        """为已有 activities 表补齐乐观锁版本列。"""
        cursor = conn.execute("PRAGMA table_info(activities)")
        cols = {row[1] for row in cursor.fetchall()}
        if "lock_version" not in cols:
            conn.execute("ALTER TABLE activities ADD COLUMN lock_version INTEGER DEFAULT 0")
            conn.commit()

    def _migrate_activities_retry_columns(self, conn: sqlite3.Connection):
        """为已有 activities 表补齐 retry/backoff 相关列。"""
        cursor = conn.execute("PRAGMA table_info(activities)")
        cols = {row[1] for row in cursor.fetchall()}
        migrations = [
            ("retry_count", "ALTER TABLE activities ADD COLUMN retry_count INTEGER DEFAULT 0"),
            ("last_error", "ALTER TABLE activities ADD COLUMN last_error TEXT"),
            ("next_retry_at", "ALTER TABLE activities ADD COLUMN next_retry_at TEXT"),
        ]
        for col, sql in migrations:
            if col not in cols:
                conn.execute(sql)
        conn.commit()

    def _migrate_activities_v2(self, conn: sqlite3.Connection):
        """检测旧版 activities 表（缺 locked_by 列），在线迁移到新版。"""
        cursor = conn.execute("PRAGMA table_info(activities)")
        cols = {row[1] for row in cursor.fetchall()}
        if 'locked_by' in cols:
            return

        _log = logging.getLogger("db.migrate")
        _log.warning("🔄 检测到旧版 activities 表，正在迁移到 v2...")

        conn.executescript("""
            CREATE TABLE activities_v2 (
                onelap_id       TEXT PRIMARY KEY,
                strava_id       BIGINT,
                fit_sha256      TEXT,
                name            TEXT,
                start_time      TEXT,
                distance_km     REAL,
                avg_heart_rate  INTEGER,
                status          TEXT DEFAULT 'uploaded'
                                CHECK(status IN (
                                    'uploaded','analyzing','analyzed',
                                    'coaching','coached',
                                    'writing','completed',
                                    'analyze_failed','coach_failed','write_failed',
                                    'failed','dedup_by_fit_sha256','skipped'
                                )),
                locked_by       TEXT,
                locked_at       TEXT,
                lock_version    INTEGER DEFAULT 0,
                error_msg       TEXT,
                retry_count     INTEGER DEFAULT 0,
                last_error      TEXT,
                next_retry_at   TEXT,
                created_at      TEXT DEFAULT (datetime('now')),
                updated_at      TEXT DEFAULT (datetime('now'))
            );

            INSERT INTO activities_v2 (
                onelap_id, strava_id, fit_sha256, name, start_time,
                distance_km, avg_heart_rate, status, error_msg, created_at, updated_at
            ) SELECT
                onelap_id, strava_id, fit_sha256, name, start_time,
                distance_km, avg_heart_rate,
                CASE WHEN status = 'synced' THEN 'uploaded'
                     WHEN status = 'failed' THEN 'failed'
                     ELSE status
                END,
                error_msg, created_at, updated_at
            FROM activities;

            DROP TABLE activities;
            ALTER TABLE activities_v2 RENAME TO activities;

            CREATE INDEX IF NOT EXISTS idx_activities_status
                ON activities(status);
            CREATE INDEX IF NOT EXISTS idx_activities_start_time
                ON activities(start_time);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_activities_fit_sha256
                ON activities(fit_sha256) WHERE fit_sha256 IS NOT NULL AND fit_sha256 != '';
            CREATE INDEX IF NOT EXISTS idx_activities_locked_by
                ON activities(locked_by);
            CREATE INDEX IF NOT EXISTS idx_activities_locked_at
                ON activities(locked_at);
        """)

        _log.info("✅ activities 表迁移完成（locked_by + 展开状态机）")

    def _migrate_activities_file_key(self, conn: sqlite3.Connection):
        """P1-6 去重前置：添加 file_key 列 + 索引（按 file_key 单列索引，用于跨主键去重）。"""
        cursor = conn.execute("PRAGMA table_info(activities)")
        cols = {row[1] for row in cursor.fetchall()}
        _log = logging.getLogger("db.migrate")
        if "file_key" not in cols:
            _log.info("🔄 添加 activities.file_key 列 (P1-6 去重前置)")
            conn.execute("ALTER TABLE activities ADD COLUMN file_key TEXT")
        else:
            _log.info("✅ activities.file_key 列已存在")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_activities_file_key "
            "ON activities(file_key)"
        )
        conn.commit()
        _log.info("✅ activities.file_key 列 + 索引就绪")
