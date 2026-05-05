#!/usr/bin/env python3
"""
coach_worker — AI 教练分析 Worker (v3)
========================================
职责：任务领取 → 读 analysis 表 → 调 coach agent（三级策略）→ 存 coach_outputs → 释放锁

三级策略：
  1. 内容哈希缓存（DB）— 相同 FIT 数据复用输出
  2. Coach Agent（超时30s）
  3. 降级模板（agent 不可用时兜底，source='template_fallback'）

调度：QwenPaw cron 每5分钟
依赖：db.py, analysis_engine.fallback

改进：
- 任务领取机制（locked_by/locked_at）
- 扩展状态机：analyzed → coaching → coached / coach_failed
- coach_cache 移入 SQLite（原子写入、可统计）
"""

import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("coach_worker")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TimedRotatingFileHandler(LOG_DIR / "coach_worker.log", when="midnight", backupCount=14, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console)

sys.path.insert(0, str(BASE_DIR))
from db import Database
from idle_backoff import should_skip, update_state
from log_utils import log_event, log_json


# ─── 输出清洗 ──────────────────────────────────

def clean_coach_output(raw: str) -> str:
    """清洗 coach 输出：去 [SESSION:]、markdown、标题、元描述前缀、多余空行"""
    if not raw:
        return raw
    lines = raw.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[SESSION:"):
            continue
        stripped = stripped.replace("**", "")
        if stripped in ("---", "***", "___"):
            continue
        if re.match(r"^#{1,3}\s*.{0,40}$", stripped) and len(stripped) <= 30:
            continue
        if re.match(r"^[📋📊📝🔍📈📉📌🛑☕🚴]\s*.*(?:报告|分析报告).{0,20}$", stripped):
            continue
        # 过滤元描述行：coach agent 有时会加"以下是分析结果，直接返回给 code-agent"之类的引导语
        if re.search(r"(?:以下|以上).*(?:分析结果|分析报告|Strava|返回给)", stripped):
            continue
        if re.search(r"已完成分析|已更新.*笔记|以上是.*描述|直接返回给", stripped):
            continue
        cleaned.append(stripped)
    text = "\n".join(cleaned)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ─── 缓存 key 生成 ─────────────────────────────

def _feature_hash(analysis: dict) -> str:
    """
    特征级缓存 key 生成。
    基于：心率区间分布（归一化）、时长区间、强度级别。
    相同特征分布的训练共享同一个缓存。
    """
    distributions = analysis.get("distributions", {})
    hr_zones = distributions.get("heart_rate_zones", {})
    basic = analysis.get("basic", {})

    # 1. 心率区间占比（归一化到5%精度，减少抖动）
    zone_pcts = {}
    zone_order = ["Z1_恢复", "Z2_有氧基础", "Z3_有氧进阶", "Z4_乳酸阈值", "Z5_无氧极限"]
    for z in zone_order:
        zdata = hr_zones.get(z, {})
        pct = zdata.get("pct", 0) if zdata else 0
        # 归一到5%粒度
        zone_pcts[z] = round(pct / 5) * 5

    # 2. 时长区间（分钟）
    duration_sec = basic.get("total_moving_time_s", 0) or basic.get("total_time_s", 0) or 0
    duration_min = duration_sec / 60
    if duration_min < 30:
        duration_bucket = "0-30min"
    elif duration_min < 60:
        duration_bucket = "30-60min"
    elif duration_min < 90:
        duration_bucket = "60-90min"
    elif duration_min < 120:
        duration_bucket = "90-120min"
    else:
        duration_bucket = "120+min"

    # 3. 强度级别（基于平均心率占最大心率比例）
    avg_hr = basic.get("avg_heart_rate", 0)
    max_hr = 194
    if avg_hr and max_hr:
        hr_pct = avg_hr / max_hr
        if hr_pct < 0.6:
            intensity = "recovery"
        elif hr_pct < 0.75:
            intensity = "endurance"
        elif hr_pct < 0.85:
            intensity = "tempo"
        elif hr_pct < 0.92:
            intensity = "threshold"
        else:
            intensity = "high"
    else:
        intensity = "unknown"

    # 组合特征
    feature = {
        "zone_pcts": zone_pcts,
        "duration_bucket": duration_bucket,
        "intensity": intensity,
    }

    raw = json.dumps(feature, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _analysis_hash(analysis: dict) -> str:
    """[已废弃] 根据分析结果生成缓存 key。
    保留作为向后兼容别名，实际委托给 _feature_hash。
    """
    return _feature_hash(analysis)


class CoachWorker:
    def __init__(self, db: Database):
        self.db = db
        self.max_per_run = 3
        self.agent_timeout = 30
        self.worker_id = "coach_worker"

    # ── Prompt 构造 ──────────────────────────

    def _build_coach_prompt(self, analysis: dict, weekly_overview: str) -> str:
        """构造 coach agent 提示词。"""
        basic = analysis.get("basic", {})
        hr_zones = analysis.get("distributions", {}).get("heart_rate_zones", {})
        dist = basic.get("total_distance_km", 0)
        avg_hr = basic.get("avg_heart_rate", 0)
        avg_speed = basic.get("avg_speed_kmh", 0)
        avg_cad = basic.get("avg_cadence", 0)
        ascent = analysis.get("elevation", {}).get("total_ascent_m", 0)
        temp = analysis.get("temperature", {}).get("avg")

        lines = ["===== 当前活动 ====="]
        name = analysis.get("activity_name", "未知活动")
        lines.append(f"活动：{name}")
        lines.append(f"距离：{dist:.1f}km")
        if avg_hr:
            lines.append(f"均心：{avg_hr:.0f}bpm")
        if hr_zones:
            zone_order = ["Z1_恢复", "Z2_有氧基础", "Z3_有氧进阶", "Z4_乳酸阈值", "Z5_无氧极限"]
            zone_strs = []
            for z in zone_order:
                zdata = hr_zones.get(z, {})
                if zdata:
                    zone_strs.append(f"{z.split('_')[0]} {zdata.get('pct', 0):.1f}%")
            if zone_strs:
                lines.append(f"心率区间：{', '.join(zone_strs)}")
        if avg_speed:
            lines.append(f"均速：{avg_speed:.1f}km/h")
        if avg_cad:
            lines.append(f"均踏频：{avg_cad:.0f}rpm")
        if ascent:
            lines.append(f"爬升：{ascent:.0f}m")
        if temp is not None:
            lines.append(f"温度：{temp:.1f}°C")
        if weekly_overview:
            lines.append("")
            lines.append(weekly_overview)
        lines.append("")
        lines.append("===== 分析建议 =====")
        lines.append("请根据以上当前活动 + 近7天活动概况，分析本次骑行质量，并给出基于数据的训练参考。")
        lines.append("注意：你只负责分析解释，不决定明天练不练——训练决策由规则引擎独立处理。")
        lines.append("")
        lines.append("【重要：输出格式要求】")
        lines.append("直接输出 Strava 活动描述，不要附加任何说明文字（不要写\"以下是分析结果\"\"直接返回给xxx\"等引导语）。")
        lines.append("使用 👍 亮点 / ⚠️ 待改进 / 💡 建议 格式，纯文本+emoji，不要 markdown。")
        lines.append("不要出现「📅 明天建议」区块——训练安排由系统规则决定。")
        return "\n".join(lines)

    def _build_7day_overview(self, current_act_id: str = "") -> str:
        """从 DB 构建近7天概况。"""
        try:
            recent = self.db.get_recent_activities(7)
        except Exception:
            return ""
        recent = [a for a in recent if a.get("onelap_id") != current_act_id]

        if not recent:
            return ""

        total_rides = len(recent)
        total_dist = sum(a.get("distance_km", 0) or 0 for a in recent)
        total_hr = [a.get("avg_heart_rate") for a in recent if a.get("avg_heart_rate")]
        avg_hr = sum(total_hr) / len(total_hr) if total_hr else 0

        lines = ["===== 近7天概况 ====="]
        lines.append(f"骑行次数：{total_rides}")
        lines.append(f"总距离：{total_dist:.1f}km")
        if avg_hr:
            lines.append(f"平均心率：{avg_hr:.0f}bpm")
        # 尝试加入训练负荷指标（如有 training_load 表）
        try:
            load = self.db.get_training_load_range("2000-01-01", "2099-12-31")
            if load:
                latest = load[-1]
                ctl, atl, tsb = latest.get("ctl"), latest.get("atl"), latest.get("tsb")
                if ctl is not None:
                    lines.append(f"CTL={ctl:.0f} ATL={atl:.0f} TSB={tsb:.0f}")
        except Exception:
            pass
        for a in recent[:5]:
            d = a.get("distance_km", 0) or 0
            nm = a.get("name", "未知")
            lines.append(f"  · {nm} ({d:.1f}km)")
        return "\n".join(lines)

    # ── Coach Agent 调用 ─────────────────────

    class CoachCallError(Exception):
        """AI coach 调用失败异常，携带失败原因供日志使用。"""
        def __init__(self, reason: str, original_error: Exception = None):
            super().__init__(reason)
            self.reason = reason
            self.original_error = original_error

    def _call_coach_agent(self, prompt: str) -> str:
        """通过 qwenpaw agents chat 调用 coach agent。
        
        Returns:
            清洗后的 coach 输出文本。
        
        Raises:
            CoachCallError: 调用超时、返回非零、或发生异常时抛出，
                           携带详细失败原因。
        """
        try:
            agent_script = os.environ.get("QWENPAW_AGENT_SCRIPT", "qwenpaw")
            cmd = [
                agent_script, "agents", "chat",
                "--from-agent", "code-agent",
                "--to-agent", "coach",
                "--text", prompt,
                "--timeout", str(self.agent_timeout),
            ]
            logger.debug(f"  调用 coach agent: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=self.agent_timeout + 10)

            if result.returncode != 0:
                stderr_msg = result.stderr[:200] if result.stderr else "无错误输出"
                raise self.CoachCallError(f"返回码 {result.returncode}，stderr: {stderr_msg}")

            output = result.stdout.strip()
            if not output:
                raise self.CoachCallError("agent 返回空输出")

            # 去除非输出行
            lines = output.split("\n")
            body_lines = [l for l in lines if not l.startswith("[") or l.startswith("[分析建议]")]
            return "\n".join(body_lines).strip()

        except subprocess.TimeoutExpired as e:
            raise self.CoachCallError("调用超时（30s 无响应）", original_error=e)
        except self.CoachCallError:
            raise
        except Exception as e:
            raise self.CoachCallError(f"调用异常: {type(e).__name__}: {e}", original_error=e)

    # ── 降级模板 ─────────────────────────────

    def _fallback_output(self, analysis: dict) -> str:
        """当 coach agent 不可用时，生成降级模板内容。
        
        优先使用简洁模板 generate_simple_template_fallback，
        降级到详细模板 generate_fallback_coach_output，
        最终兜底为最小化描述。
        """
        try:
            from analysis_engine.fallback import generate_simple_template_fallback
            name = analysis.get("activity_name", "")
            return generate_simple_template_fallback(analysis, activity_name=name)
        except (ImportError, Exception):
            try:
                from analysis_engine.fallback import generate_fallback_coach_output
                return generate_fallback_coach_output(analysis)
            except ImportError:
                pass

        # 最终兜底
        basic = analysis.get("basic", {})
        dist = basic.get("total_distance_km", 0)
        total_time_s = basic.get("total_time_s", 0) or basic.get("total_duration_min", 0) * 60 or 0
        duration_m = total_time_s / 60 if total_time_s else 0
        avg_hr = basic.get("avg_heart_rate", 0)
        return (
            f"🚴 {analysis.get('activity_name', '骑行')}\n"
            f"距离 {dist:.1f}km · 时长 {duration_m:.0f}min"
            + (f" · 均心 {avg_hr:.0f}bpm" if avg_hr else "")
            + "\n\n💪 继续保持！明天建议适度恢复。"
        )

    # ── 主流程 ─────────────────────────────

    def run(self) -> dict:
        """任务领取 → 读 analysis → 三级策略生成输出 → 存 coach_outputs → 释放锁"""
        run_t0 = time.time()
        activities = self.db.claim_activities(
            from_status="analyzed",
            to_status="coaching",
            worker_id=self.worker_id,
            limit=self.max_per_run,
        )
        if not activities:
            return {"coached": 0, "coach_failed": 0, "cached": 0, "message": "无待教练分析活动"}

        log_event(logger, "coach_start", count=len(activities))
        log_json(logger, "coach", "INFO", "claim_activities", f"领取教练任务: {len(activities)} 个",
                 count=len(activities))

        ok = 0
        fail = 0
        cached = 0
        logger.info(f"🤖 领取教练分析任务: {len(activities)} 个")

        for act in activities:
            onelap_id = act["onelap_id"]
            version = act.get("lock_version", 0)
            name = act.get("name", "")
            t0 = time.time()

            try:
                # 读 analysis 表
                analysis = self.db.get_analysis(onelap_id)
                if not analysis:
                    logger.warning(f"  ⚠️ {name}: 无分析数据")
                    self.db.set_activity_error(onelap_id, "无 analysis 数据", retry_delay_minutes=5)
                    fail += 1
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "coach_done", trace_id=onelap_id, activity_id=onelap_id, status="failed", duration_ms=elapsed)
                    continue

                # 解析 JSON 字段
                parsed = {}
                for key in ("basic_stats", "hr_zones", "cadence_zones", "speed_zones",
                             "grade_zones", "elevation", "temperature", "capabilities"):
                    val = analysis.get(key)
                    if isinstance(val, str):
                        try:
                            parsed[key] = json.loads(val)
                        except json.JSONDecodeError:
                            parsed[key] = val
                    else:
                        parsed[key] = val or {}

                # 构造标准格式（兼容 fit_analysis 的 to_dict 输出）
                analysis_data = {
                    "activity_name": name,
                    "basic": parsed.get("basic_stats", {}),
                    "distributions": {
                        "heart_rate_zones": parsed.get("hr_zones", {}),
                        "cadence_zones": parsed.get("cadence_zones", {}),
                        "speed_zones": parsed.get("speed_zones", {}),
                        "grade_zones": parsed.get("grade_zones", {}),
                    },
                    "elevation": parsed.get("elevation", {}),
                    "temperature": parsed.get("temperature", {}),
                    "capabilities": parsed.get("capabilities", {}),
                }

                # ── 三级策略 ──

                # 第一级：内容哈希缓存（DB）
                cache_key = _feature_hash(analysis_data)
                cached_output = self.db.get_coach_cache(cache_key)
                if cached_output:
                    logger.info(f"  💾 {name}: 缓存命中 (hash={cache_key})")
                    self.db.save_coach_output(
                        onelap_id=onelap_id,
                        raw=cached_output,
                        cleaned=cached_output,
                        model="cache",
                        source="ai_coach",
                    )
                    self.db.release_activity(onelap_id, "coached", expected_version=version)
                    cached += 1
                    # 记录 sub_status
                    try:
                        self.db.update_activity_sub_status(onelap_id, "cache_hit")
                    except Exception:
                        pass
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "coach_done", trace_id=onelap_id, activity_id=onelap_id, status="cache_hit", duration_ms=elapsed)
                    log_json(logger, "coach", "INFO", "cache_hit", f"缓存命中: {name}",
                             trace_id=onelap_id, activity_id=onelap_id, cache_key=cache_key, duration_ms=elapsed)
                    continue

                # 第二级：AI coach agent（超时30s）
                weekly = self._build_7day_overview(current_act_id=onelap_id)
                prompt = self._build_coach_prompt(analysis_data, weekly)

                raw_output = ""
                cleaned = ""
                source = "ai_coach"
                coach_failed = False

                log_json(logger, "coach", "INFO", "ai_coach_start", f"AI 教练分析开始: {name}",
                         trace_id=onelap_id, activity_id=onelap_id)
                try:
                    raw_output = self._call_coach_agent(prompt)
                    cleaned = clean_coach_output(raw_output) if raw_output else ""
                    if not cleaned:
                        logger.info("  coach agent 返回空输出，触发降级")
                        coach_failed = True
                except self.CoachCallError as e:
                    logger.warning(
                        f"  [FALLBACK] AI coach 调用失败（{e.reason}），使用模板降级"
                    )
                    if e.original_error:
                        logger.warning(f"    原始异常: {type(e.original_error).__name__}: {e.original_error}")
                    coach_failed = True

                # 第三级：降级模板
                if coach_failed:
                    cleaned = self._fallback_output(analysis_data)
                    source = "template_fallback"
                    log_json(logger, "coach", "WARNING", "fallback", f"降级模板: {name}",
                             trace_id=onelap_id, activity_id=onelap_id)
                else:
                    log_json(logger, "coach", "INFO", "ai_coach_done", f"AI 教练分析完成: {name}",
                             trace_id=onelap_id, activity_id=onelap_id)

                # 写 coach_outputs
                self.db.save_coach_output(
                    onelap_id=onelap_id,
                    raw=raw_output or cleaned,
                    cleaned=cleaned,
                    model=("ai_coach" if source == "ai_coach" else "template_fallback"),
                    source=source,
                )

                # 写入 coach_cache（DB）— 只缓存 AI coach 正常输出
                if source == "ai_coach":
                    self.db.set_coach_cache(cache_key, cleaned)

                # 释放锁
                self.db.release_activity(onelap_id, "coached", expected_version=version)
                logger.info(f"  ✅ {name}: 教练分析完成 (source={source})")
                ok += 1
                # 记录 sub_status
                sub_status_map = {"ai_coach": "ai_generated", "template_fallback": "fallback"}
                sub_status = sub_status_map.get(source, "")
                if sub_status:
                    try:
                        self.db.update_activity_sub_status(onelap_id, sub_status)
                    except Exception:
                        pass
                elapsed = int((time.time() - t0) * 1000)
                log_event(logger, "coach_done", trace_id=onelap_id, activity_id=onelap_id, status=("template_fallback" if source == "template_fallback" else "success"), duration_ms=elapsed)
                time.sleep(0.5)


            except Exception as e:
                logger.error(f"  ❌ {name}: 处理异常: {e}", exc_info=True)
                self.db.set_activity_error(onelap_id, str(e)[:500], retry_delay_minutes=5)
                fail += 1
                elapsed = int((time.time() - t0) * 1000)
                log_event(logger, "coach_done", trace_id=onelap_id, activity_id=onelap_id, status="failed", duration_ms=elapsed)

        elapsed = int((time.time() - run_t0) * 1000)
        log_json(logger, "coach", "INFO", "coach_cycle_done", f"教练周期完成: {ok} 新 / {cached} 缓存 / {fail} 失败",
                 coached=ok, cached=cached, coach_failed=fail, duration_ms=elapsed)
        return {
            "coached": ok,
            "coach_failed": fail,
            "cached": cached,
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
        if should_skip("coach_worker", BASE_DIR):
            return

        db = Database()
        worker = CoachWorker(db)
        result = worker.run()
        had_work = "message" not in result
        update_state("coach_worker", had_work, BASE_DIR)

        if result["coached"] or result["coach_failed"] or result["cached"]:
            logger.info(f"✅ coach_worker: {result['coached']} 新分析 / {result['cached']} 缓存 / {result['coach_failed']} 失败")
    except Exception as e:
        logger.error(f"coach_worker 异常: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
