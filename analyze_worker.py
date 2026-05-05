#!/usr/bin/env python3
"""
analyze_worker — FIT 分析 Worker (v2)
=======================================
职责：任务领取 → 读 FIT 缓存 → 解析心率/速度/踏频等 → 写 analysis 表 → 按保留策略清理 FIT

调度：QwenPaw cron 每5分钟
依赖：db.py, fit_analysis.py

改进：
- 任务领取机制（locked_by/locked_at），保证并发安全
- 扩展状态机：uploaded → analyzing → analyzed / analyze_failed
- FIT 保留策略：保留最近 30 天 FIT；异常任务（*_failed）永久保留
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
FIT_RETENTION_DAYS = 30

LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("analyze_worker")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TimedRotatingFileHandler(LOG_DIR / "analyze_worker.log", when="midnight", backupCount=14, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console)

sys.path.insert(0, str(BASE_DIR))
from db import Database
from fit_analysis import FitAnalyzer
from idle_backoff import should_skip, update_state
from log_utils import log_event, log_json
from training_load_calculator import calc_trimp_from_basic, update_training_load_from_db
from training_type_classifier import classify_training_type


def _parse_activity_start_time(value):
    """解析 activity.start_time，兼容多种历史格式。"""
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        # 兜底支持 Python ISO 变体（例如带微秒或 +00:00）。
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _fit_retention_decision(activity, retention_days=FIT_RETENTION_DAYS):
    """返回 (是否删除 FIT, 原因, 活动年龄天数)。

    策略：
    - status 以 ``_failed`` 结尾的异常任务永久保留 FIT；
    - 根据 activity["start_time"] 判断活动年龄；
    - 最近 retention_days 天内的 FIT 保留，超过才允许删除。
    """
    status = str(activity.get("status") or "")
    if status.endswith("_failed"):
        return False, "failed_status_permanent_keep", None

    start_time = _parse_activity_start_time(activity.get("start_time"))
    if start_time is None:
        return False, "invalid_start_time_keep", None

    now = datetime.now(timezone.utc)
    age_days = max(0, int((now - start_time).total_seconds() // 86400))
    if now - start_time >= timedelta(days=retention_days):
        return True, "older_than_retention", age_days
    return False, "within_retention", age_days


class AnalyzeWorker:
    def __init__(self, db: Database):
        self.db = db
        self.max_hr = int(os.environ.get("ANALYSIS_MAX_HR", "194"))
        self.max_per_run = 5
        self.worker_id = "analyze_worker"

    def run(self) -> dict:
        """任务领取 → 读 FIT → 解析 → 存分析结果 → 释放锁"""
        run_t0 = time.time()
        activities = self.db.claim_activities(
            from_status="uploaded",
            to_status="analyzing",
            worker_id=self.worker_id,
            limit=self.max_per_run,
        )
        if not activities:
            return {"analyzed": 0, "analyze_failed": 0, "skipped": 0, "message": "无待分析活动"}

        log_event(logger, "analyze_start", count=len(activities))
        log_json(logger, "analyze", "INFO", "claim_activities", f"领取分析任务: {len(activities)} 个",
                 count=len(activities))

        ok = 0
        fail = 0
        skipped = 0
        logger.info(f"📊 领取分析任务: {len(activities)} 个")

        for act in activities:
            onelap_id = act["onelap_id"]
            version = act.get("lock_version", 0)
            name = act.get("name", "")
            t0 = time.time()

            # 幂等：已有分析结果则快速跳过
            existing = self.db.get_analysis(onelap_id)
            if existing:
                logger.info(f"  ⏭️ {name}: 已有分析结果，跳过")
                self.db.release_activity(onelap_id, "analyzed", expected_version=version)
                skipped += 1
                elapsed = int((time.time() - t0) * 1000)
                log_event(logger, "analyze_done", trace_id=onelap_id, activity_id=onelap_id, status="success", duration_ms=elapsed)
                continue

            # 读 FIT 缓存
            fit_path = self.db.fit_cache_path(onelap_id)
            if not fit_path.exists():
                logger.warning(f"  ⚠️ {name}: FIT 缓存不存在 ({fit_path})")
                self.db.set_activity_error(onelap_id, "FIT缓存不存在", retry_delay_minutes=5)
                fail += 1
                elapsed = int((time.time() - t0) * 1000)
                log_event(logger, "analyze_done", trace_id=onelap_id, activity_id=onelap_id, status="failed", duration_ms=elapsed)
                continue

            try:
                fit_data = fit_path.read_bytes()
                logger.info(f"  📊 分析: {name} ({fit_path.stat().st_size / 1024:.0f}KB)")
                log_json(logger, "analyze", "INFO", "parse_start", f"开始解析 FIT: {name}",
                         trace_id=onelap_id, activity_id=onelap_id, fit_size_kb=int(fit_path.stat().st_size / 1024))

                analyzer = FitAnalyzer(fit_data, max_hr=self.max_hr, verbose=False)
                result = analyzer.analyze()

                if result.record_count == 0:
                    logger.warning(f"  ⚠️ {name}: FIT 无有效记录")
                    self.db.set_activity_error(onelap_id, "FIT无有效记录", retry_delay_minutes=5)
                    fail += 1
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "analyze_done", trace_id=onelap_id, activity_id=onelap_id, status="failed", duration_ms=elapsed)
                    continue

                structured = analyzer.to_dict(result)
                structured["activity_name"] = name
                structured["onelap_act_id"] = onelap_id
                structured["strava_aid"] = act.get("strava_id")
                structured["max_hr_configured"] = self.max_hr

                # 存 DB
                self.db.save_analysis(onelap_id, structured)

                # ── Post-analysis: 训练负荷 + 训练类型 ──────────
                try:
                    # 1. 计算 TRIMP
                    hr_zones = structured.get("distributions", {}).get("heart_rate_zones", {})
                    basic = structured.get("basic", {})
                    trimp = calc_trimp_from_basic(basic, hr_zones)

                    # 2. 提取活动日期
                    start_time = act.get("start_time", "")
                    activity_date = str(start_time)[:10] if start_time else ""

                    # 3. 计算 CTL/ATL/TSB 并写入 training_load 表
                    if trimp > 0 and activity_date:
                        load_result = update_training_load_from_db(self.db, activity_date, trimp)
                        log_json(logger, "analyze", "INFO", "training_load_updated",
                                 f"TRIMP={trimp:.1f} CTL={load_result['ctl']} ATL={load_result['atl']} TSB={load_result['tsb']}",
                                 trace_id=onelap_id)

                    # 4. 分类训练类型并写入 analysis 表
                    analysis_row = self.db.get_analysis(onelap_id)
                    if analysis_row:
                        ttype = classify_training_type(analysis_row)
                        self.db.update_analysis_type(onelap_id, ttype)
                        log_json(logger, "analyze", "INFO", "training_type_classified",
                                 f"type={ttype}", trace_id=onelap_id)

                except Exception as e:
                    log_json(logger, "analyze", "WARNING", "post_analysis_skipped",
                             f"训练负荷/分类失败: {e}", trace_id=onelap_id)
                    # 不抛出异常，不阻塞主流程

                self.db.release_activity(onelap_id, "analyzed", expected_version=version)
                logger.info(f"  ✅ {name}: 分析完成")

                # FIT 保留策略：最近 30 天保留；异常任务（*_failed）永久保留。
                delete_fit, fit_reason, fit_age_days = _fit_retention_decision(act)
                if delete_fit:
                    if self.db.delete_fit_cache(onelap_id):
                        logger.info(f"    🗑️ FIT 缓存已删除（{fit_age_days}天前，超过 {FIT_RETENTION_DAYS} 天保留期）")
                        log_event(
                            logger,
                            "fit_cache_deleted",
                            trace_id=onelap_id,
                            activity_id=onelap_id,
                            reason=fit_reason,
                            age_days=fit_age_days,
                            retention_days=FIT_RETENTION_DAYS,
                        )
                else:
                    logger.info(f"    📦 FIT 缓存保留: {fit_reason}")
                    log_event(
                        logger,
                        "fit_cache_retained",
                        trace_id=onelap_id,
                        activity_id=onelap_id,
                        reason=fit_reason,
                        age_days=fit_age_days,
                        retention_days=FIT_RETENTION_DAYS,
                    )

                ok += 1
                elapsed = int((time.time() - t0) * 1000)
                log_event(logger, "analyze_done", trace_id=onelap_id, activity_id=onelap_id, status="success", duration_ms=elapsed)
                log_json(logger, "analyze", "INFO", "parse_done", f"解析完成: {name}",
                         trace_id=onelap_id, activity_id=onelap_id, duration_ms=elapsed)

            except Exception as e:
                logger.error(f"  ❌ {name}: 分析失败: {e}")
                self.db.set_activity_error(onelap_id, str(e), retry_delay_minutes=5)
                fail += 1
                elapsed = int((time.time() - t0) * 1000)
                log_event(logger, "analyze_done", trace_id=onelap_id, activity_id=onelap_id, status="failed", duration_ms=elapsed)
                log_json(logger, "analyze", "ERROR", "parse_skip", f"解析失败: {name}",
                         trace_id=onelap_id, activity_id=onelap_id, duration_ms=elapsed, error=str(e)[:200])

            time.sleep(0.3)

        elapsed = int((time.time() - run_t0) * 1000)
        log_json(logger, "analyze", "INFO", "analyze_cycle_done", f"分析周期完成: {ok} 成功 / {fail} 失败 / {skipped} 跳过",
                 analyzed=ok, analyze_failed=fail, skipped=skipped, duration_ms=elapsed)
        return {
            "analyzed": ok,
            "analyze_failed": fail,
            "skipped": skipped,
            "total": len(activities),
        }


def main():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    try:
        # ── 空闲退避检查 ──
        if should_skip("analyze_worker", BASE_DIR):
            return

        db = Database()
        worker = AnalyzeWorker(db)
        result = worker.run()
        had_work = "message" not in result
        update_state("analyze_worker", had_work, BASE_DIR)

        if result["analyzed"] or result["analyze_failed"]:
            logger.info(f"✅ analyze_worker: {result['analyzed']} 成功 / {result['analyze_failed']} 失败 / {result['skipped']} 跳过")
    except Exception as e:
        logger.error(f"analyze_worker 异常: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
