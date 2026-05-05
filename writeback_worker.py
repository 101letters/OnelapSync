#!/usr/bin/env python3
"""
writeback_worker — Strava 写回 Worker (v3)
=============================================
职责：任务领取 → 读 coach_outputs + analysis 心率区间 → 训练类型标签
      → 写 Strava 活动描述 + 标题 → Bark 通知 → 释放锁

调度：QwenPaw cron 每5分钟
依赖：db.py

v3 新增：
- 基于心率区间分布自动打训练类型标签（恢复骑/耐力/节奏/阈值/高强度）
- 标签写入 Strava 活动标题（name 字段）
- 判定优先级：高强度 > 阈值 > 节奏 > 耐力 > 恢复骑
"""

import hashlib
import logging
import os
import re
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("writeback_worker")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TimedRotatingFileHandler(LOG_DIR / "writeback_worker.log", when="midnight", backupCount=14, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console)

sys.path.insert(0, str(BASE_DIR))
from db import Database
from idle_backoff import should_skip, update_state
from log_utils import log_event, log_json
from strava_api import strava_request, StravaRateLimitError

STRAVA_API_BASE = os.environ.get("STRAVA_API_BASE", "https://www.strava.com/api/v3")

# ─── 训练类型标签 ───────────────────────────────

# 标签判定规则（按优先级从高到低排列）
TRAINING_LABELS = [
    ("高强度", lambda z: _zone_pct(z, "Z5_无氧极限") > 10),
    ("阈值",   lambda z: _zone_pct(z, "Z4_乳酸阈值") > 20),
    ("节奏",   lambda z: _zone_pct(z, "Z3_有氧进阶") > 30),
    ("耐力",   lambda z: _zone_pct(z, "Z2_有氧基础") > 40),
    ("恢复骑", lambda z: _zone_pct(z, "Z1_恢复") > 40 and _zone_pct(z, "Z4_乳酸阈值") + _zone_pct(z, "Z5_无氧极限") < 10),
]

# 动态构造标签前缀正则（从 TRAINING_LABELS 生成，新增标签自动生效）
_LABEL_NAMES = "|".join(label for label, _ in TRAINING_LABELS)
LABEL_PREFIX_RE = re.compile(rf'^\[({_LABEL_NAMES})\]\s{{1,2}}')


# ─── 写回错误分类 ──────────────────────────────

class StravaWritebackError(Exception):
    """Strava 写回 HTTP 错误，携带 HTTP 状态码供分类使用。"""
    def __init__(self, status_code: int, message: str = ""):
        self.status_code = status_code
        super().__init__(message or f"HTTP {status_code}")


def _classify_writeback_error(e: Exception) -> str:
    """分类写回错误：返回 'retryable' 或 'permanent'。

    可重试：401 凭证过期（token 可能因临时网络/API 抖动刷新失败）/
           timeout / connection reset / 429 / 5xx / token 刷新失败
    不可重试：400 / 403 / 404 / 数据校验失败
    """
    # ── StravaRateLimitError（429）→ 可重试 ──
    if isinstance(e, StravaRateLimitError):
        return "retryable"

    # ── StravaWritebackError → 按 HTTP 状态码 ──
    if isinstance(e, StravaWritebackError):
        code = e.status_code
        if 500 <= code < 600:
            return "retryable"
        if code in (400, 403, 404):
            return "permanent"
        # 401 不在此处处理（token 可能因临时抖动刷新失败），走兜底 retryable
        return "retryable"  # 未知状态码保守重试

    # ── requests 超时 / 连接异常 → 可重试 ──
    if isinstance(e, requests.exceptions.Timeout):
        return "retryable"
    if isinstance(e, requests.exceptions.ConnectionError):
        return "retryable"

    # ── 字符串兜底匹配 ──
    msg = str(e).lower()
    permanent_keywords = ["400", "403", "404",
                          "validation", "invalid request",
                          "bad request", "forbidden", "not found"]
    retryable_keywords = ["timeout", "connection reset", "connection refused",
                          "too many requests", "rate limit", "token refresh",
                          "5xx", "500", "502", "503", "504"]

    for kw in permanent_keywords:
        if kw in msg:
            return "permanent"
    for kw in retryable_keywords:
        if kw in msg:
            return "retryable"

    return "retryable"  # 默认保守：可重试


def send_bark(title: str, body: str):
    url = os.environ.get("BARK_URL")
    if not url:
        return
    try:
        requests.post(url, json={"title": title, "body": body}, timeout=10)
    except Exception as e:
        logger.warning(f"Bark 通知失败: {e}")


def _zone_pct(hr_zones: dict, zone_key: str) -> float:
    """安全获取心率区间百分比，缺失返回 0。"""
    if not hr_zones:
        return 0.0
    zone = hr_zones.get(zone_key, {})
    if not zone:
        return 0.0
    return float(zone.get("pct", 0.0))


def _classify_training_type(hr_zones: dict) -> str | None:
    """根据心率区间分布判定训练类型标签。

    按优先级从高到低匹配：高强度 > 阈值 > 节奏 > 耐力 > 恢复骑。
    返回标签名称（如 '恢复骑'），不满足任何条件返回 None。

    要求至少 4/5 区间有有效数据（pct > 0），否则视为数据不完整。
    """
    if not hr_zones:
        return None
    # 数据完整性检查：至少 4 个区间有 pct > 0
    zone_keys = ("Z1_恢复", "Z2_有氧基础", "Z3_有氧进阶", "Z4_乳酸阈值", "Z5_无氧极限")
    populated = sum(1 for z in zone_keys if _zone_pct(hr_zones, z) > 0.0)
    if populated < 4:
        return None
    for label, rule in TRAINING_LABELS:
        if rule(hr_zones):
            return label
    return None


def _build_training_title(current_name: str, label: str | None) -> str | None:
    """在活动名前面加 [标签] 前缀，如果已有相同标签则跳过。

    Args:
        current_name: 当前活动名称（如 "05月04日午间骑行（35.5km·145bpm）"）
        label: 训练类型标签（如 "恢复骑"），为 None 则不修改

    Returns:
        新标题，或 None（无需修改时）
    """
    if not label or not current_name:
        return None

    label_prefix = f"[{label}]  "

    # 检查是否已有标签前缀
    existing_match = LABEL_PREFIX_RE.match(current_name)
    if existing_match:
        existing_label = existing_match.group(1)
        if existing_label == label:
            return None  # 已有相同标签，不重复加
        # 标签不同，替换旧标签
        return label_prefix + current_name[existing_match.end():]

    # 无标签前缀，添加新标签
    return label_prefix + current_name


class WritebackWorker:
    def __init__(self, db: Database):
        self.db = db
        self.max_per_run = 5
        self.strava_token = os.environ.get("STRAVA_ACCESS_TOKEN", "")
        self.worker_id = "writeback_worker"

    def _update_strava_activity(self, strava_id: int, name: str | None, description: str) -> bool:
        """写入 Strava 活动描述 + 标题。

        Args:
            strava_id: Strava 活动 ID
            name: 新标题（含训练标签），为 None 则仅更新描述
            description: Strava 活动描述（coach 输出）

        Returns:
            True 表示写入成功
            False 表示 strava_id 缺失（活动数据问题）

        Raises:
            StravaWritebackError: 非 200 HTTP 响应（携带状态码）或 token 缺失
            StravaRateLimitError: 429 重试耗尽
            requests.exceptions.*: 网络层异常（timeout / connection 等）
        """
        if not strava_id:
            return False  # 活动数据问题 → permanent
        if not self.strava_token:
            # Token 缺失不要封死活动：抛异常进入 retryable 分类
            raise StravaWritebackError(401, "Strava token 缺失，等待下轮重试")
        payload = {"description": description}
        if name:
            payload["name"] = name
        resp = strava_request("PUT",
            f"{STRAVA_API_BASE}/activities/{strava_id}",
            headers={"Authorization": f"Bearer {self.strava_token}"},
            json=payload,
            timeout=30,
        )
        if resp.status_code == 200:
            return True
        raise StravaWritebackError(resp.status_code, f"HTTP {resp.status_code}: {resp.text[:200]}")

    def _refresh_token_if_needed(self):
        """如果 Strava token 过期，尝试刷新。"""
        from strava_api import refresh_strava_token  # 延迟导入
        try:
            test = strava_request("GET",
                f"{STRAVA_API_BASE}/athlete",
                headers={"Authorization": f"Bearer {self.strava_token}"},
                timeout=10,
            )
            if test.status_code == 401:
                logger.info("Strava token 过期，尝试刷新...")
                new_token = refresh_strava_token()
                if new_token:
                    self.strava_token = new_token
                    logger.info("Strava token 刷新成功")
                    return True
                else:
                    logger.error("Strava token 刷新失败")
            return False
        except StravaRateLimitError as e:
            logger.error(f"Strava token 检查时遭遇限流: {e}")
            # 不阻断，假设 token 仍有效
            return True
        except Exception as e:
            logger.warning(f"Strava 连通性检查异常: {e}")
            return False

    def _mark_permanent_error(self, onelap_id: str, error_msg: str, expected_version: int = 0):
        """标记活动为永久写回失败 —— retry_count 直接设为最大值，防止再被领取。

        与 set_activity_error 的区别：
        - 不走指数退避，直接设 retry_count=3（max_retries）
        - 状态设为 write_failed，释放锁
        - claim_activities 因 retry_count >= max_retries 不会再领取此活动

        expected_version: claim 时拿到的 lock_version，更新时乐观锁校验，
                          防止并发修改导致覆盖。
        """
        act = self.db.get_activity(onelap_id)
        if not act:
            return

        failed_status_map = {
            "writing": "write_failed",
            "coached": "write_failed",
        }
        current_status = act.get("status", "writing")
        failed_status = failed_status_map.get(current_status, "write_failed")
        msg = (error_msg or "")[:1000]

        cursor = self.db.conn.execute(
            """UPDATE activities
               SET status=?,
                   locked_by=NULL,
                   locked_at=NULL,
                   error_msg=?,
                   last_error=?,
                   retry_count=3,
                   next_retry_at=NULL,
                   updated_at=datetime('now')
               WHERE onelap_id=? AND lock_version=?""",
            (failed_status, msg, msg, onelap_id, expected_version),
        )
        self.db.conn.commit()
        if cursor.rowcount == 0:
            logger.warning(
                f"  ⚠️ {onelap_id}: 永久失败标记未写入（lock_version 不匹配: "
                f"expected={expected_version}, actual={act.get('lock_version')}），可能已被并发修改"
            )
        else:
            logger.error(f"  🔒 {onelap_id}: 永久失败标记 → {failed_status} (retry_count=3)")

    def run(self) -> dict:
        """任务领取 → 写 Strava 描述 → Bark 通知 → 释放锁"""
        run_t0 = time.time()
        activities = self.db.claim_activities(
            from_status="coached",
            to_status="writing",
            worker_id=self.worker_id,
            limit=self.max_per_run,
        )
        if not activities:
            return {"completed": 0, "write_failed": 0, "message": "无待写回活动"}

        log_event(logger, "writeback_start", count=len(activities))
        log_json(logger, "writeback", "INFO", "claim_activities", f"领取写回任务: {len(activities)} 个",
                 count=len(activities))

        # 先检查 token
        self._refresh_token_if_needed()

        ok = 0
        fail = 0
        newly_completed = 0
        logger.info(f"✍️ 领取写回任务: {len(activities)} 个")

        for act in activities:
            onelap_id = act["onelap_id"]
            version = act.get("lock_version", 0)
            strava_id = act.get("strava_id")
            name = act.get("name", "")
            t0 = time.time()

            try:
                # 幂等保护：Strava 写入成功后会先记录 writeback_log。
                # 即使后续 release_activity('completed') 失败，下次领取到任务也不会重复写 Strava。
                if self.db.has_writeback(onelap_id):
                    logger.info(f"  ⏭️ {name}: 已存在 writeback_log，跳过 Strava 写入")
                    self.db.release_activity(onelap_id, "completed", expected_version=version)
                    ok += 1
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "writeback_done", trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, status="skipped", duration_ms=elapsed)
                    log_json(logger, "writeback", "INFO", "write_skip", f"已存在跳过: {name}",
                             trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, reason="already_completed")
                    continue

                if not strava_id:
                    logger.warning(f"  ⚠️ {name}: 无 strava_id")
                    self._mark_permanent_error(onelap_id, "无 strava_id", expected_version=version)
                    fail += 1
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "writeback_done", trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, status="failed", duration_ms=elapsed)
                    continue

                # 读 coach output
                coach = self.db.get_coach_output(onelap_id)
                if not coach:
                    logger.warning(f"  ⚠️ {name}: 无 coach 输出")
                    self._mark_permanent_error(onelap_id, "无 coach 输出", expected_version=version)
                    fail += 1
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "writeback_done", trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, status="failed", duration_ms=elapsed)
                    continue

                description = (coach.get("cleaned_output") or coach.get("raw_output") or "").strip()
                if not description:
                    logger.warning(f"  ⚠️ {name}: coach 输出为空")
                    self._mark_permanent_error(onelap_id, "coach 输出为空", expected_version=version)
                    fail += 1
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "writeback_done", trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, status="failed", duration_ms=elapsed)
                    continue

                # ── 训练类型标签判定 ──────────────────
                new_name = None
                try:
                    analysis = self.db.get_analysis(onelap_id)
                    if analysis:
                        hr_zones = analysis.get("hr_zones", {})
                        label = _classify_training_type(hr_zones)
                        if label:
                            new_name = _build_training_title(name, label)
                            if new_name:
                                logger.info(f"  🏷️ {name} → 训练类型: [{label}] → {new_name}")
                                log_json(logger, "writeback", "INFO", "training_label",
                                         f"训练标签判定: [{label}]",
                                         trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id,
                                         label=label, current_name=name, new_name=new_name)
                            else:
                                logger.info(f"  ⏭️ {name}: 已有相同标签 [{label}]，跳过")
                        else:
                            logger.info(f"  ℹ️ {name}: 心率区间不符合任何训练类型标签条件")
                    else:
                        logger.info(f"  ℹ️ {name}: 无分析数据，跳过训练标签判定")
                except Exception as e:
                    logger.warning(f"  ⚠️ {name}: 训练标签判定异常: {e}")

                # ── 写入 Strava（描述 + 标题）──────────
                log_json(logger, "writeback", "INFO", "write_start", f"开始写回 Strava: {name}",
                         trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id)
                content_hash = hashlib.sha256(description.encode("utf-8")).hexdigest()
                success = self._update_strava_activity(strava_id, new_name, description)
                if success:
                    # 关键顺序：先记录 writeback_log，再释放 completed，保证下次重试幂等。
                    self.db.log_writeback(onelap_id, int(strava_id), content_hash, success=True)
                    self.db.release_activity(onelap_id, "completed", expected_version=version)
                    title_suffix = f" (标签: 已更新)" if new_name else ""
                    logger.info(f"  ✅ {name}: Strava 已更新{title_suffix}")
                    ok += 1
                    newly_completed += 1
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "writeback_done", trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, status="success", duration_ms=elapsed)
                    log_json(logger, "writeback", "INFO", "write_done", f"写回完成: {name}",
                             trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, duration_ms=elapsed,
                             training_label=(new_name is not None))
                else:
                    # strava_id 缺失（token 缺失已在 _update_strava_activity 内通过异常处理）
                    self._mark_permanent_error(onelap_id, "Strava 凭证缺失", expected_version=version)
                    fail += 1
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "writeback_done", trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, status="failed", duration_ms=elapsed)

                time.sleep(0.5)

            except StravaRateLimitError as e:
                # 429 限流 → 可重试，利用 StravaRateLimitError.retry_after 作为退避时间
                error_type = _classify_writeback_error(e)
                logger.error(f"  ⚠️ Strava 限流 ({error_type}): {e} (建议等待 {e.retry_after}s)")
                log_json(logger, "writeback", "ERROR", "rate_limit", f"Strava 写回限流: {e}",
                         trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, retry_after=e.retry_after, error_type=error_type)
                send_bark("Strava 限流告警", f"写回任务限流，建议 {e.retry_after} 秒后重试")
                self.db.set_activity_error(onelap_id, f"Strava限流: {e}", retry_delay_minutes=max(1, e.retry_after // 60))
                fail += 1
                elapsed = int((time.time() - t0) * 1000)
                log_event(logger, "writeback_done", trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, status="rate_limited", duration_ms=elapsed)
                break  # 限流后停止后续写回

            except Exception as e:
                error_type = _classify_writeback_error(e)
                logger.error(f"  ❌ {name}: {'不可重试' if error_type == 'permanent' else '可重试'}错误: {e}",
                             exc_info=(error_type == "permanent"))
                if error_type == "permanent":
                    self._mark_permanent_error(onelap_id, str(e)[:500], expected_version=version)
                else:
                    self.db.set_activity_error(onelap_id, str(e)[:500], retry_delay_minutes=5)
                fail += 1
                elapsed = int((time.time() - t0) * 1000)
                log_event(logger, "writeback_done", trace_id=onelap_id, activity_id=onelap_id, strava_id=strava_id, status="failed", duration_ms=elapsed, error_type=error_type)

        # Bark 通知
        if newly_completed > 0:
            log_json(logger, "writeback", "INFO", "bark_notify", "Bark 通知: writeback_success",
                     reason="writeback_success", newly_completed=newly_completed, total_ok=ok)
            notify_lines = []
            for act in activities:
                a = self.db.get_activity(act["onelap_id"])
                if a and a.get("status") == "completed":
                    nm = a.get("name", "")
                    aid = a.get("strava_id", "")
                    notify_lines.append(f"  ✅ {nm} (Strava: {aid})")
            if notify_lines:
                send_bark("🚴 OneLap 同步完毕", "\n".join(notify_lines))

        elapsed = int((time.time() - run_t0) * 1000)
        log_json(logger, "writeback", "INFO", "writeback_cycle_done", f"写回周期完成: {ok} 成功 / {fail} 失败",
                 completed=ok, write_failed=fail, duration_ms=elapsed)
        return {
            "completed": ok,
            "write_failed": fail,
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
        if should_skip("writeback_worker", BASE_DIR):
            return

        db = Database()
        worker = WritebackWorker(db)
        result = worker.run()
        had_work = "message" not in result
        update_state("writeback_worker", had_work, BASE_DIR)

        if result["completed"] or result["write_failed"]:
            logger.info(f"✅ writeback_worker: {result['completed']} 成功 / {result['write_failed']} 失败")
    except Exception as e:
        logger.error(f"writeback_worker 异常: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
