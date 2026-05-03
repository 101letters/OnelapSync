#!/usr/bin/env python3
"""
OneLap -> Strava 同步脚本 (V3.4)
使用顽鹿 OneLap OTM API 获取活动 + 下载 FIT，直连 Strava 上传。

流程：
  1. 顽鹿签名登录获取 token
  2. OTM API 获取活动列表（ride_record/list）
  3. OTM API 获取活动详情（analysis/{id}）→ 拿到 fileKey
  4. OTM API 下载 FIT 文件（fit_content/{b64(fileKey)}）
  5. Strava API 比对已有活动（按时间+距离）
  6. 直连 Strava uploads API 上传 FIT 文件
  7. 上传成功后纠正 name + sport_type 为 Ride
  8. FIT 解析提取结构化数据（心率区间/爬升/速度/踏频等）
  9. 提取近7天活动概况（从 sync_state_v3.json）
 10. 调运动顾问(coach) agent 生成分析+明天建议
 11. clean_coach_output() 清洗输出（去 [SESSION:]/markdown/标题/多余空行）
 12. 分析文案写入 Strava 活动描述
 13. 仅推送同步结果到 Bark（不含分析内容）
"""

import argparse
import fcntl
import base64
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import requests

# [V3.2] FIT 数据分析 + 运动顾问
from fit_analysis import FitAnalyzer

BASE_DIR = Path(__file__).resolve().parent
CONFIG_FILE = BASE_DIR / "sync_config_updated.json"
STATE_FILE = BASE_DIR / "sync_state_v3.json"
LOCK_FILE = BASE_DIR / "onelap_sync_v3.lock"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "sync_v3.log"
ENV_FILE = BASE_DIR / ".env"

ONELAP_SIGN_KEY = "fe9f8382418fcdeb136461cac6acae7b"
ONELAP_LOGIN_URL = "https://www.onelap.cn/api/login"
ONELAP_OTM_BASE = "https://otm.onelap.cn"
STRAVA_API_BASE = "https://www.strava.com/api/v3"
STRAVA_OAUTH_URL = "https://www.strava.com/oauth/token"

VERSION = "V3.4"
STATE_VERSION = 3
STATE_TMP_SUFFIX = ".tmp"
STATE_BAK_SUFFIX = ".bak"
TOKEN_LOCK_FILE = BASE_DIR / "strava_token_refresh.lock"
FIT_MIN_BYTES = 500
FIT_MAGIC = b".FIT"
ONELAP_MAX_PAGES = 10
STRAVA_MAX_PAGES = 10
UPLOAD_POLL_INTERVALS = [2] * 10 + [5] * 10 + [10] * 10
ENV_KEYS = {
    "onelap_username": "ONELAP_USERNAME",
    "onelap_password": "ONELAP_PASSWORD",
    "bark_url": "BARK_URL",
    "strava_access_token": "STRAVA_ACCESS_TOKEN",
    "strava_refresh_token": "STRAVA_REFRESH_TOKEN",
    "strava_client_id": "STRAVA_CLIENT_ID",
    "strava_client_secret": "STRAVA_CLIENT_SECRET",
    "analysis_max_hr": "ANALYSIS_MAX_HR",
}


class RateLimitError(RuntimeError):
    """Strava rate limit hit; stop current run immediately."""


class UploadPendingError(RuntimeError):
    """Strava upload accepted but processing did not finish this run."""
    def __init__(self, upload_id: int | str, onelap_record_id: str, activity_name: str):
        self.upload_id = str(upload_id)
        self.onelap_record_id = str(onelap_record_id)
        self.activity_name = activity_name
        super().__init__(f"Strava 上传仍在处理中 (upload_id={upload_id})")


class RetryableActivityError(RuntimeError):
    """Temporary OneLap/Strava condition; retry next cron run."""


def env_value(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_text(path: Path, content: str, encoding: str = "utf-8", backup: bool = False):
    tmp_path = path.with_name(path.name + STATE_TMP_SUFFIX)
    with open(tmp_path, "w", encoding=encoding) as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    if backup and path.exists():
        bak_path = path.with_name(path.name + STATE_BAK_SUFFIX)
        with open(path, "rb") as src, open(bak_path, "wb") as dst:
            dst.write(src.read())
            dst.flush()
            os.fsync(dst.fileno())
    os.replace(tmp_path, path)


def fit_sha256(fit_data: bytes) -> str:
    return hashlib.sha256(fit_data).hexdigest()


def is_valid_fit_content(content: bytes) -> bool:
    # FIT header contains ASCII '.FIT' at bytes 8..11 for normal 12/14 byte headers.
    return len(content) >= FIT_MIN_BYTES and FIT_MAGIC in content[:16]


def load_dotenv(env_path: Path):
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def ensure_dirs():
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger(verbose: bool = False):
    ensure_dirs()
    logger = logging.getLogger("onelap_sync_v3")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = TimedRotatingFileHandler(LOG_FILE, when="midnight", backupCount=14, encoding="utf-8")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger


def format_activity_name(start_riding_time: str, distance_km: float, avg_heart: int) -> str:
    """根据时间、心率、距离生成可读活动名：04月26日晚间骑行（28.0km·156bpm）"""
    try:
        dt = datetime.strptime(start_riding_time[:19], "%Y-%m-%d %H:%M:%S")
        month_day = dt.strftime("%m月%d日")
        hour = dt.hour

        if 5 <= hour < 11:
            period = "早间骑行"
        elif 11 <= hour < 14:
            period = "午间骑行"
        elif 14 <= hour < 18:
            period = "下午骑行"
        else:
            period = "夜间骑行"

        suffix = ""
        if distance_km < 5:
            suffix = "（短活动）"
        elif distance_km >= 40:
            suffix = "（长活动）"

        heart_part = f"·{int(avg_heart)}bpm" if avg_heart else ""
        dist_part = f"（{distance_km:.1f}km{heart_part}）"

        return f"{month_day}{period}{suffix}{dist_part}"
    except (ValueError, TypeError, KeyError):
        return f"骑行活动（{distance_km:.1f}km）"


def clean_coach_output(raw: str) -> str:
    """
    清洗运动顾问输出：
      1. 去掉 [SESSION: ...] 开头的行
      2. 去掉 ** markdown 加粗标记
      3. 去掉标题行（📋 训练分析报告 等）和分隔线（---）
      4. 合并多余空行（最多保留一个空行）
      5. 去掉首尾空白

    Args:
        raw: 运动顾问原始输出文本

    Returns:
        清洗后的干净文本
    """
    if not raw:
        return raw

    lines = raw.split("\n")
    cleaned = []

    for line in lines:
        stripped = line.strip()

        # 1. 去除 [SESSION: ...] 行
        if stripped.startswith("[SESSION:"):
            continue

        # 2. 去除 ** markdown 加粗标记
        stripped = stripped.replace("**", "")

        # 3. 去除标题行和分隔线
        if stripped in ("---", "***", "___"):
            continue
        # 去除 markdown 标题（##/### 开头 + 报告/分析类关键词）
        if re.match(r"^#{1,3}\s*.{0,40}$", stripped) and len(stripped) <= 30:
            continue
        # 去除报告主标题（如 📋 训练分析报告、📊 骑行数据分析报告）
        if re.match(r"^[📋📊📝🔍📈📉📌🛑☕🚴]\s*.*(?:报告|分析报告).{0,20}$", stripped):
            continue

        cleaned.append(stripped)

    # 合并回文本，先处理空行
    text = "\n".join(cleaned)

    # 4. 合并多余空行（2个以上连续换行 → 1个空行）
    text = re.sub(r"\n{3,}", "\n\n", text)

    # 5. 去掉首尾空白
    text = text.strip()

    return text


class MageneStravaSyncV3:
    def __init__(self, config_path: Path, state_path: Path, logger: logging.Logger):
        self.logger = logger
        self.config = self._load_json(config_path, required=True)
        self.state_path = state_path
        self.state = self._load_json(state_path, required=False) or {
            "version": STATE_VERSION,
            "last_run": None,
            "consecutive_failures": 0,
            "activities": {},
        }

        self.account = env_value(ENV_KEYS["onelap_username"])
        self.password = env_value(ENV_KEYS["onelap_password"])
        if not self.account or not self.password:
            raise ValueError("环境变量 ONELAP_USERNAME 或 ONELAP_PASSWORD 未设置")

        self.bark_url = env_value(ENV_KEYS["bark_url"])
        self.strava_access_token = env_value(ENV_KEYS["strava_access_token"])

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        sync_settings = self.config.get("sync_settings", {})
        self.timeout = sync_settings.get("request_timeout", 60)
        self.max_uploads_per_run = sync_settings.get("max_uploads_per_run", 20)
        self.force_sport_type = "Ride"
        self._onelap_token = None

        # [V3.1] 数据分析配置
        analysis_cfg = self.config.get("analysis", {})
        self.analysis_enabled = analysis_cfg.get("enabled", True)
        self.analysis_max_hr = int(env_value(ENV_KEYS["analysis_max_hr"], analysis_cfg.get("max_hr", 190)))

    def _load_json(self, path: Path, required: bool):
        if not path.exists():
            if required:
                raise FileNotFoundError(f"配置文件不存在: {path}")
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_state(self):
        self.state["version"] = STATE_VERSION
        atomic_write_text(
            self.state_path,
            json.dumps(self.state, ensure_ascii=False, indent=2),
            encoding="utf-8",
            backup=True,
        )
        self.logger.debug(f"状态已原子写入: {self.state_path} (bak={self.state_path}.bak)")


    def _log_rate_limit(self, resp: requests.Response, context: str):
        limit = resp.headers.get("X-RateLimit-Limit", "")
        usage = resp.headers.get("X-RateLimit-Usage", "")
        self.logger.error(f"Strava 429 rate limit ({context}); X-RateLimit-Limit={limit}, X-RateLimit-Usage={usage}")

    def _raise_if_rate_limited(self, resp: requests.Response, context: str):
        if resp.status_code == 429:
            self._log_rate_limit(resp, context)
            raise RateLimitError(f"Strava rate limit: {context}")

    def _pending_uploads(self) -> dict:
        return self.state.setdefault("pending_uploads", {})

    def _record_pending_upload(self, upload_id: int | str, onelap_record_id: str, activity_name: str):
        pending = self._pending_uploads()
        pending[str(onelap_record_id)] = {
            "status": "pending",
            "upload_id": str(upload_id),
            "onelap_record_id": str(onelap_record_id),
            "activity_name": activity_name,
            "updated_at": utc_now_iso(),
        }
        self._save_state()
        self.logger.warning(f"Upload pending 已持久化: onelap={onelap_record_id}, upload_id={upload_id}")

    def _clear_pending_upload(self, onelap_record_id: str):
        pending = self._pending_uploads()
        if str(onelap_record_id) in pending:
            pending.pop(str(onelap_record_id), None)
            self._save_state()

    def _mark_pending_failed(self, onelap_record_id: str, error: str):
        pending = self._pending_uploads()
        if str(onelap_record_id) in pending:
            pending[str(onelap_record_id)]["status"] = "failed"
            pending[str(onelap_record_id)]["error"] = error[:300]
            pending[str(onelap_record_id)]["updated_at"] = utc_now_iso()
            self._save_state()

    def _poll_strava_upload(self, upload_id: int | str, intervals: list[int] | None = None) -> int | None:
        intervals = intervals or UPLOAD_POLL_INTERVALS
        for idx, interval in enumerate(intervals, start=1):
            time.sleep(interval)
            pr = requests.get(
                f"{STRAVA_API_BASE}/uploads/{upload_id}",
                headers={"Authorization": f"Bearer {self.strava_access_token}"},
                timeout=30,
            )
            self._raise_if_rate_limited(pr, f"upload poll {upload_id}")
            if pr.status_code != 200:
                self.logger.warning(f"轮询出错: upload_id={upload_id}, status={pr.status_code}")
                continue

            status_info = pr.json()
            status = status_info.get("status", "")
            error = status_info.get("error", "")

            if status == "Your activity is ready.":
                activity_id = status_info.get("activity_id")
                self.logger.info(f"Strava 上传完成: activity_id={activity_id}")
                return activity_id
            if error and "duplicate" in error.lower():
                m = re.search(r"/activities/(\d+)", error or "")
                dup_id = int(m.group(1)) if m else None
                self.logger.info(f"Strava 重复活动 → 已有 id={dup_id}")
                return dup_id
            if status and "error" in status.lower():
                raise RuntimeError(f"Strava 处理失败: {error}")
            if error:
                self.logger.warning(f"上传状态[{idx}/{len(intervals)}]: {status}, 错误: {error}")
            else:
                self.logger.debug(f"上传状态[{idx}/{len(intervals)}]: {status}")
        return None

    def _process_pending_uploads(self, dry_run: bool = False):
        pending = {k: v for k, v in self._pending_uploads().items() if v.get("status") == "pending"}
        self.logger.info(f"Pending upload 检查: {len(pending)} 个")
        if dry_run:
            self.logger.info("DRY RUN: 仅检查 pending upload 数量，不轮询/不写状态")
            return
        activities_state = self.state.setdefault("activities", {})
        for onelap_id, item in pending.items():
            upload_id = item.get("upload_id")
            if not upload_id:
                continue
            try:
                activity_id = self._poll_strava_upload(upload_id, intervals=[0, 2, 5, 10])
                if activity_id:
                    activities_state[onelap_id] = {
                        "name": item.get("activity_name", ""),
                        "start_time": item.get("start_time", ""),
                        "distance_km": item.get("distance_km", 0),
                        "strava_id": activity_id,
                        "synced_at": utc_now_iso(),
                        "status": "synced_from_pending",
                    }
                    self._clear_pending_upload(onelap_id)
                    self.logger.info(f"Pending upload 已恢复: onelap={onelap_id}, strava_id={activity_id}")
                else:
                    self.logger.info(f"Pending upload 仍未完成: onelap={onelap_id}, upload_id={upload_id}")
            except RateLimitError:
                raise
            except (requests.RequestException, ValueError, RuntimeError) as e:
                self._mark_pending_failed(onelap_id, str(e))
                self.logger.warning(f"Pending upload 查询失败，标记 failed: onelap={onelap_id}, {e}")

    def _send_bark(self, title: str, body: str):
        if not self.bark_url:
            return
        try:
            requests.post(self.bark_url, json={"title": title, "body": body}, timeout=10)
        except requests.RequestException as e:
            self.logger.warning(f"Bark通知失败: {e}")

    # [V3.1] ─── FIT 数据分析与报告 ─────────────

    def _run_analysis(self, fit_data: bytes, strava_aid: int, display_name: str, onelap_act_id: str):
        """
        对已同步的 FIT 文件执行数据分析，提取结构化数据供运动顾问(coach) agent 使用。

        Returns:
            (structured_data: dict | None, analysis_summary: str | None)
            structured_data 包含完整结构化骑行数据（心率区间/爬升/速度/踏频等）。
            analysis_summary 为规则引擎生成的简短摘要（V3.2 中不再用于 Bark 推送）。
        """
        if not self.analysis_enabled:
            self.logger.info(f"  数据分析已禁用，跳过")
            return None, None

        try:
            self.logger.info(f"  📊 FIT 数据分析中...")
            analyzer = FitAnalyzer(fit_data, max_hr=self.analysis_max_hr, verbose=False)
            result = analyzer.analyze()

            if result.record_count == 0:
                self.logger.warning(f"  FIT 文件无有效记录点，跳过分析")
                return None, None

            # 提取结构化数据（供 LLM agent 生成报告，不再使用规则引擎）
            structured = analyzer.to_dict(result)
            structured["activity_name"] = display_name
            structured["onelap_act_id"] = onelap_act_id
            structured["strava_aid"] = strava_aid
            structured["max_hr_configured"] = self.analysis_max_hr

            # 生成简短摘要 (≤200字符) 用于 Bark 推送
            summary_parts = []
            if result.has_heart_rate:
                summary_parts.append(f"❤️均心{result.avg_heart_rate:.0f}bpm")
            if result.has_cadence:
                summary_parts.append(f"🔄均踏频{result.avg_cadence:.0f}rpm")
            if result.has_altitude and result.total_ascent_m > 0:
                summary_parts.append(f"⛰️爬升{result.total_ascent_m}m")
            summary_parts.append(f"📏{result.total_distance_km:.1f}km·均速{result.avg_speed_kmh:.1f}km/h")

            # 心率区间简短摘要
            if result.has_heart_rate and result.hr_zone_distribution:
                z4 = result.hr_zone_distribution.get("Z4_乳酸阈值", {}).get("pct", 0)
                z5 = result.hr_zone_distribution.get("Z5_无氧极限", {}).get("pct", 0)
                if z4 + z5 > 50:
                    summary_parts.append(f"🔥高强度{z4+z5:.0f}%")
                z2 = result.hr_zone_distribution.get("Z2_有氧基础", {}).get("pct", 0)
                z3 = result.hr_zone_distribution.get("Z3_有氧进阶", {}).get("pct", 0)
                if z2 + z3 < 35:
                    summary_parts.append(f"⚠️有氧不足")

            analysis_summary = " | ".join(summary_parts)

            self.logger.info(f"  📊 FIT 数据已结构化（{result.record_count} 条记录），待运动顾问生成分析")

            return structured, analysis_summary

        except (RuntimeError, ValueError, OSError) as e:
            self.logger.error(f"  数据分析失败: {e}", exc_info=True)
            return None, None

    def _update_activity_description(self, strava_aid: int, description: str) -> bool:
        """更新 Strava 活动的描述字段"""
        try:
            resp = requests.put(
                f"{STRAVA_API_BASE}/activities/{strava_aid}",
                headers={"Authorization": f"Bearer {self.strava_access_token}"},
                json={"description": description},
                timeout=30,
            )
            if resp.status_code == 401:
                self.refresh_strava_token()
                resp = requests.put(
                    f"{STRAVA_API_BASE}/activities/{strava_aid}",
                    headers={"Authorization": f"Bearer {self.strava_access_token}"},
                    json={"description": description},
                    timeout=30,
                )
            return resp.status_code == 200
        except requests.RequestException as e:
            self.logger.warning(f"更新活动描述异常: {e}")
            return False

    # [V3.2] ─── 运动顾问集成 ───────────────────

    def _build_7day_overview(self, current_act_id: str = "") -> str:
        """
        从 sync_state_v3.json 提取近7天活动概况。

        Args:
            current_act_id: 当前处理的活动ID，在列表中加注标记

        Returns:
            格式化的近7天概况文本，为空则返回空字符串
        """
        try:
            state_data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            return ""

        activities = state_data.get("activities", {})
        if not activities:
            return ""

        cutoff = datetime.now() - timedelta(days=7)
        recent = []

        for act_id, info in activities.items():
            start_str = info.get("start_time", "")
            if not start_str:
                continue
            try:
                start_dt = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if start_dt < cutoff:
                continue

            dist = info.get("distance_km", 0)

            # 过滤极短活动（对训练分析无意义）
            if dist < 0.5:
                continue

            name = info.get("name", "")

            # 从活动名中解析平均心率
            hr = 0
            hr_match = re.search(r"(\d+)bpm", name)
            if hr_match:
                hr = int(hr_match.group(1))

            # 强度评级（基于平均心率）
            if hr >= 165:
                intensity = "高强度"
            elif hr >= 152:
                intensity = "中高强度"
            elif hr >= 140:
                intensity = "中强度"
            elif hr > 0:
                intensity = "低强度"
            else:
                intensity = "—"

            recent.append({
                "id": act_id,
                "date": start_dt.strftime("%m/%d"),
                "distance": dist,
                "hr": hr,
                "intensity": intensity,
            })

        if not recent:
            return ""

        # 按日期排序
        recent.sort(key=lambda x: x["date"])

        total_count = len(recent)
        total_dist = sum(r["distance"] for r in recent)

        lines = []
        lines.append("===== 近7天活动概况 =====")
        lines.append(f"总次数：{total_count}次")
        lines.append(f"总距离：{total_dist:.1f}km")

        lines.append("活动列表：")
        for r in recent:
            marker = "（当前）" if r["id"] == current_act_id else ""
            hr_str = f"{r['hr']}bpm" if r["hr"] > 0 else "—"
            lines.append(f"- {r['date']} {r['distance']:.0f}km {hr_str} {r['intensity']}{marker}")

        return "\n".join(lines)

    def _format_coach_input(self, structured_data: dict, weekly_overview: str = "") -> str:
        """格式化结构化数据为运动顾问输入（含近7天概况）"""
        lines = []

        lines.append("===== 当前活动 =====")

        activity_name = structured_data.get("activity_name", "未知活动")
        lines.append(f"活动：{activity_name}")

        basic = structured_data.get("basic", {})
        dist = basic.get("total_distance_km", 0)
        lines.append(f"距离：{dist:.1f}km")

        avg_hr = basic.get("avg_heart_rate")
        if avg_hr:
            lines.append(f"均心：{avg_hr:.0f}bpm")

        # 心率区间
        hr_zones = structured_data.get("distributions", {}).get("heart_rate_zones", {})
        if hr_zones:
            zone_order = ["Z1_恢复", "Z2_有氧基础", "Z3_有氧进阶", "Z4_乳酸阈值", "Z5_无氧极限"]
            zone_strs = []
            for zname in zone_order:
                zdata = hr_zones.get(zname, {})
                if zname in hr_zones:
                    zone_strs.append(f"{zname.split('_')[0]} {zdata.get('pct', 0):.1f}%")
            if zone_strs:
                lines.append(f"心率区间：{', '.join(zone_strs)}")

        avg_speed = basic.get("avg_speed_kmh")
        if avg_speed:
            lines.append(f"均速：{avg_speed:.1f}km/h")

        avg_cad = basic.get("avg_cadence")
        if avg_cad:
            lines.append(f"均踏频：{avg_cad:.0f}rpm")

        elevation = structured_data.get("elevation", {})
        ascent = elevation.get("total_ascent_m", 0)
        if ascent > 0:
            lines.append(f"爬升：{ascent:.0f}m")

        temp = structured_data.get("temperature", {}).get("avg")
        if temp is not None:
            lines.append(f"温度：{temp:.1f}°C")

        # 附上近7天概况
        if weekly_overview:
            lines.append("")
            lines.append(weekly_overview)

        lines.append("")
        lines.append("===== 明天建议 =====")
        lines.append("请根据以上当前活动 + 近7天活动概况，给出明天是骑行还是休息的建议，以及如果骑的话建议什么强度。")

        return "\n".join(lines)

    def _call_coach_agent(self, structured_data: dict, weekly_overview: str = "") -> str | None:
        """
        调用运动顾问(coach) agent 生成分析文案。
        超时/失败不抛异常，返回 None。
        """
        try:
            coach_input = self._format_coach_input(structured_data, weekly_overview)
            prompt = (
                f"[Agent code-agent requesting] 请分析以下骑行数据，生成简洁分析报告"
                f"（亮点/待改进/建议+明天建议，带emoji），控制在250字以内：\n\n{coach_input}"
            )
            self.logger.info("  🤖 调用运动顾问(coach)分析...")

            result = subprocess.run(
                [
                    "qwenpaw", "agents", "chat",
                    "--from-agent", "code-agent",
                    "--to-agent", "coach",
                    "--text", prompt,
                    "--timeout", "90",
                ],
                capture_output=True, text=True,
                timeout=120,
                cwd=str(BASE_DIR),
            )

            if result.returncode != 0:
                self.logger.warning(
                    f"  运动顾问调用失败 (rc={result.returncode}): "
                    f"{result.stderr[:200] if result.stderr else '(no stderr)'}"
                )
                return None

            output = result.stdout.strip()
            if output:
                self.logger.info(f"  ✅ 运动顾问返回 ({len(output)} chars)")
                return output
            else:
                self.logger.warning("  运动顾问返回空内容")
                return None

        except subprocess.TimeoutExpired:
            self.logger.warning("  运动顾问调用超时 (120s)")
            return None
        except FileNotFoundError:
            self.logger.warning("  qwenpaw CLI 不可用，跳过运动顾问")
            return None
        except (subprocess.SubprocessError, OSError, ValueError) as e:
            self.logger.warning(f"  运动顾问调用异常: {e}")
            return None

    def _process_coach_analyses(self, pending_analyses: list[dict]) -> tuple[int, int]:
        """
        处理所有待分析活动：调运动顾问 → 写入 Strava 描述。
        单个失败不阻塞整体流程。

        Returns:
            (success_count, fail_count)
        """
        if not pending_analyses:
            return 0, 0

        # 近7天活动概况（同一批次共享，只算一次）
        # 取第一个活动的 onelap_act_id 作为"当前"标记
        first_act_id = pending_analyses[0].get("onelap_act_id", "")
        weekly_overview = self._build_7day_overview(current_act_id=first_act_id)
        if weekly_overview:
            self.logger.info(f"  📅 近7天活动概况已生成")
        else:
            self.logger.info(f"  📅 近7天无历史活动数据")

        success = 0
        fail = 0
        total = len(pending_analyses)
        self.logger.info(f"🤖 开始运动顾问分析 ({total} 个活动)...")

        for i, sa in enumerate(pending_analyses):
            strava_aid = sa.get("strava_aid")
            display_name = sa.get("activity_name", f"活动{i+1}")

            if not strava_aid:
                self.logger.warning(f"  [{i+1}/{total}] 跳过 {display_name}: 无 strava_aid")
                fail += 1
                continue

            self.logger.info(f"  [{i+1}/{total}] 分析: {display_name}")

            # 调运动顾问
            coach_text = self._call_coach_agent(sa, weekly_overview)
            if not coach_text:
                self.logger.warning(f"  运动顾问未返回结果，跳过 Strava 描述更新")
                fail += 1
                continue

            # [V3.3.1] 清洗 coach 输出（去 [SESSION:]/markdown/标题/多余空行）
            coach_text = clean_coach_output(coach_text)
            if not coach_text:
                self.logger.warning(f"  运动顾问输出清洗后为空，跳过 Strava 描述更新")
                fail += 1
                continue

            # 写入 Strava 活动描述
            self.logger.info(f"  📝 写入 Strava 描述 (activity_id={strava_aid})")
            if self._update_activity_description(strava_aid, coach_text):
                self.logger.info(f"  ✅ {display_name} 分析已写入 Strava")
                success += 1
            else:
                self.logger.warning(f"  ⚠️ {display_name} 描述写入失败")
                fail += 1

            time.sleep(1)  # 避免频繁 API 调用

        self.logger.info(f"🤖 运动顾问完成: {success} 成功 / {fail} 失败")
        return success, fail

    # ─── 顽鹿登录 ──────────────────────────────

    def login_onelap(self) -> str:
        """顽鹿签名登录，返回 token"""
        nonce = uuid.uuid4().hex[:16]
        ts = str(int(time.time()))
        pwd_md5 = hashlib.md5(self.password.encode()).hexdigest()
        sign_str = (
            f"account={self.account}&nonce={nonce}&password={pwd_md5}"
            f"&timestamp={ts}&key={ONELAP_SIGN_KEY}"
        )
        sign = hashlib.md5(sign_str.encode()).hexdigest()

        resp = self.session.post(
            ONELAP_LOGIN_URL,
            json={"account": self.account, "password": pwd_md5},
            headers={"nonce": nonce, "timestamp": ts, "sign": sign},
            timeout=self.timeout,
        )
        data = resp.json()
        if data.get("code") != 200:
            raise RuntimeError(f"顽鹿登录失败: {data}")

        token = data["data"][0]["token"]
        nickname = data["data"][0]["userinfo"].get("nickname", "")
        uid = data["data"][0]["userinfo"]["uid"]
        self.logger.info(f"顽鹿登录成功: {nickname} (uid={uid})")
        self._onelap_token = token
        return token

    # ─── OTM 活动列表 ──────────────────────────

    def fetch_onelap_activities(self, days: int) -> list[dict]:
        """通过顽鹿 OTM API 获取活动列表"""
        if not self._onelap_token:
            raise RuntimeError("先调用 login_onelap()")

        s = requests.Session()
        s.headers.update({
            "Authorization": self._onelap_token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        all_activities = []
        page = 1
        cutoff = datetime.now() - timedelta(days=days)
        cutoff_str = cutoff.strftime("%Y-%m-%d")

        while True:
            resp = s.post(
                f"{ONELAP_OTM_BASE}/api/otm/ride_record/list",
                json={"limit": 50, "page": page},
                timeout=self.timeout,
            )
            data = resp.json()
            if data.get("code") != 200:
                self.logger.warning(f"OTM 列表异常: {data}")
                break

            records = data.get("data", {}).get("list", [])
            if not records:
                break

            for rec in records:
                start_time = rec.get("start_riding_time", "")
                if not start_time:
                    continue
                if start_time[:10] < cutoff_str:
                    # 后续页只会更早，直接返回
                    self.logger.info(
                        f"OTM: 已获取 {len(all_activities)} 个活动 "
                        f"(截止日期 {cutoff_str}，当前页最早 {start_time[:10]})"
                    )
                    return all_activities
                all_activities.append(rec)

            page += 1
            if page > ONELAP_MAX_PAGES:
                self.logger.warning(f"OTM: 达到最大页数保护 max_pages={ONELAP_MAX_PAGES}")
                break

        earliest = min((a.get("start_riding_time", "") for a in all_activities if a.get("start_riding_time")), default="")
        self.logger.info(f"OTM: 拉取页数={min(page, ONELAP_MAX_PAGES)}, 活动数={len(all_activities)}, 最早活动={earliest}")
        self.logger.info(f"OTM: 获取到 {len(all_activities)} 个 {days}天内活动")
        return all_activities

    # ─── 活动详情 + FIT 下载 ───────────────────

    def get_activity_detail(self, record_id: str) -> dict:
        """获取活动详情（含 fileKey）"""
        s = requests.Session()
        s.headers.update({
            "Authorization": self._onelap_token,
            "Accept": "application/json",
        })
        resp = s.get(
            f"{ONELAP_OTM_BASE}/api/otm/ride_record/analysis/{record_id}",
            timeout=self.timeout,
        )
        data = resp.json()
        if data.get("code") != 200:
            raise RuntimeError(f"获取详情失败: {data}")
        return data.get("data", {}).get("ridingRecord", {})

    def download_fit(self, record_id: str) -> bytes:
        """下载活动的 FIT 文件。先获取详情拿 fileKey，再下载。"""
        # 1. 获取详情拿 fileKey
        detail = self.get_activity_detail(record_id)
        file_key = detail.get("fileKey", "")
        if not file_key:
            raise RetryableActivityError(f"活动 {record_id} 暂无 fileKey，跳过本轮等待下轮重试")

        # 2. base64 编码 fileKey
        encoded = base64.b64encode(file_key.encode()).decode()

        # 3. 下载 FIT
        s = requests.Session()
        s.headers.update({
            "Authorization": self._onelap_token,
            "Accept": "application/octet-stream",
        })
        resp = s.get(
            f"{ONELAP_OTM_BASE}/api/otm/ride_record/analysis/fit_content/{encoded}",
            timeout=120,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"FIT 下载失败: {resp.status_code} {resp.text[:200]}")
        if len(resp.content) < FIT_MIN_BYTES:
            raise RuntimeError(f"FIT 文件过小 ({len(resp.content)} bytes)，可能无效")
        if not is_valid_fit_content(resp.content):
            sample = resp.content[:80].decode("utf-8", errors="replace")
            raise RuntimeError(f"FIT header 校验失败，可能是错误页/JSON: {sample}")

        self.logger.debug(f"FIT 下载成功: {len(resp.content)} bytes")
        return resp.content

    # ─── Strava Token 管理 ─────────────────────

    def refresh_strava_token(self) -> str:
        """刷新 Strava token。
        策略：Strava OAuth 原生刷新 → .env 存量降级。"""
        with open(TOKEN_LOCK_FILE, "w") as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            self.logger.debug("Strava token refresh lock acquired")
            refresh_token = env_value(ENV_KEYS["strava_refresh_token"])

            # ── 方式 1：直连 Strava OAuth ──
            if refresh_token:
                client_id = env_value(ENV_KEYS["strava_client_id"])
                client_secret = env_value(ENV_KEYS["strava_client_secret"])
                if client_id and client_secret:
                    try:
                        resp = requests.post(
                            STRAVA_OAUTH_URL,
                            data={
                                "client_id": client_id,
                                "client_secret": client_secret,
                                "grant_type": "refresh_token",
                                "refresh_token": refresh_token,
                            },
                            timeout=30,
                        )
                        self._raise_if_rate_limited(resp, "oauth token refresh")
                        data = resp.json()
                        access_token = data.get("access_token", "")
                        new_refresh = data.get("refresh_token", "")
                        if access_token:
                            self._update_strava_tokens(access_token, new_refresh, refresh_token)
                            self.logger.info("Strava token 刷新成功 (Strava直连)")
                            return access_token
                        self.logger.warning(f"Strava OAuth 直连失败: {data}")
                    except (requests.RequestException, ValueError) as e:
                        self.logger.warning(f"Strava OAuth 直连异常: {e}")

            # ── 方式 2：降级用 .env 里的存量 access token ──
            old_token = env_value(ENV_KEYS["strava_access_token"])
            if old_token:
                self.logger.warning("刷新失败，使用 .env 中现有 access token")
                self.strava_access_token = old_token
                return old_token

            raise RuntimeError("Strava token 不可用：刷新失败且无存量 access token")

    def _update_strava_tokens(self, access_token: str, new_refresh: str, old_refresh: str):
        """更新 access token，原子写回 .env。refresh token 变化时同步更新。"""
        self.strava_access_token = access_token

        env_content = ENV_FILE.read_text(encoding="utf-8")
        old_access = env_value(ENV_KEYS["strava_access_token"])
        if old_access and old_access != access_token:
            env_content = env_content.replace(old_access, access_token)
            os.environ[ENV_KEYS["strava_access_token"]] = access_token

        if new_refresh and new_refresh != old_refresh:
            self.logger.info("Strava refreshToken 已滚动，更新 .env")
            env_content = env_content.replace(old_refresh, new_refresh)
            os.environ[ENV_KEYS["strava_refresh_token"]] = new_refresh

        atomic_write_text(ENV_FILE, env_content, encoding="utf-8", backup=True)
        self.logger.debug(".env token 已原子写入")

    # ─── Strava 上传 ───────────────────────────

    def upload_to_strava(self, fit_data: bytes, activity_name: str, onelap_record_id: str = "") -> int | None:
        """
        上传 FIT 到 Strava，弹性退避轮询等待完成，返回 activity_id。
        超时则把 upload_id 持久化为 pending，下一轮优先恢复。
        """
        with tempfile.NamedTemporaryFile(suffix=".fit", delete=False) as tmp:
            tmp.write(fit_data)
            tmp_path = tmp.name

        try:
            safe_name = f"activity_{hashlib.md5(activity_name.encode()).hexdigest()[:8]}"
            with open(tmp_path, "rb") as f:
                resp = requests.post(
                    f"{STRAVA_API_BASE}/uploads",
                    headers={"Authorization": f"Bearer {self.strava_access_token}"},
                    files={
                        "file": (f"{safe_name}.fit", f, "application/octet-stream"),
                        "name": (None, activity_name),
                        "data_type": (None, "fit"),
                        "activity_type": (None, "ride"),
                    },
                    timeout=120,
                )
            self._raise_if_rate_limited(resp, "upload")

            if resp.status_code == 401:
                self.logger.warning("Strava 上传 401，刷新 token 后重试")
                self.refresh_strava_token()
                with open(tmp_path, "rb") as f:
                    resp = requests.post(
                        f"{STRAVA_API_BASE}/uploads",
                        headers={"Authorization": f"Bearer {self.strava_access_token}"},
                        files={
                            "file": (f"{safe_name}.fit", f, "application/octet-stream"),
                            "name": (None, activity_name),
                            "data_type": (None, "fit"),
                            "activity_type": (None, "ride"),
                        },
                        timeout=120,
                    )
                self._raise_if_rate_limited(resp, "upload retry")

            if resp.status_code != 201:
                body = resp.text[:500]
                if "duplicate" in body.lower():
                    m = re.search(r"/activities/(\d+)", body)
                    dup_id = int(m.group(1)) if m else None
                    self.logger.info(f"Strava 重复上传 → 已有活动 id={dup_id}")
                    return dup_id
                raise RuntimeError(f"Strava 上传失败: {resp.status_code} {body}")

            upload_info = resp.json()
            upload_id = upload_info.get("id")
            if not upload_id:
                raise RuntimeError(f"Strava 上传响应无 id: {upload_info}")

            self.logger.debug(f"Strava 上传已提交: upload_id={upload_id}")
            activity_id = self._poll_strava_upload(upload_id)
            if activity_id:
                return activity_id

            if onelap_record_id:
                self._record_pending_upload(upload_id, onelap_record_id, activity_name)
                raise UploadPendingError(upload_id, onelap_record_id, activity_name)
            raise RuntimeError(f"Strava 上传超时 (upload_id={upload_id})")

        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ─── Strava 活动比对 ──────────────────────

    def fetch_strava_activities(self, days: int) -> list[dict] | None:
        """从 Strava API 获取最近 N 天的活动，用于比对。权限不足/网络异常返回 None"""
        after_ts = int((datetime.now(timezone.utc) - timedelta(days=days + 1)).timestamp())

        s = requests.Session()
        s.headers.update({"Authorization": f"Bearer {self.strava_access_token}"})

        strava_acts = []
        page = 1
        try:
            while True:
                resp = s.get(
                    f"{STRAVA_API_BASE}/athlete/activities",
                    params={"after": after_ts, "page": page, "per_page": 50},
                    timeout=30,
                )
                if resp.status_code == 401:
                    body = resp.json()
                    err_msg = body.get("message", "") if isinstance(body, dict) else ""
                    if "read_permission" in err_msg or "AccessToken" in err_msg:
                        self.logger.info("Strava token 缺少读取权限，跳过比对")
                        return None
                    self.logger.warning(f"Strava API 错误: {resp.status_code} {resp.text[:200]}")
                    break
                self._raise_if_rate_limited(resp, "athlete activities")
                if resp.status_code != 200:
                    break

                batch = resp.json()
                if not batch:
                    break
                strava_acts.extend(batch)
                page += 1
                if page > STRAVA_MAX_PAGES:
                    self.logger.warning(f"Strava: 达到最大页数保护 max_pages={STRAVA_MAX_PAGES}")
                    break
        except RateLimitError:
            raise
        except (requests.RequestException, ValueError) as e:
            self.logger.info(f"Strava API 请求异常，跳过比对: {e}")
            return None

        result = []
        for a in strava_acts:
            result.append({
                "strava_id": a["id"],
                "name": a.get("name", ""),
                "start_date": a.get("start_date", ""),
                "distance": a.get("distance", 0),
            })
        self.logger.info(f"Strava: 最近{days}天 {len(result)} 个活动")
        return result

    def _is_on_strava(self, onelap_act: dict, strava_acts: list[dict]) -> bool:
        """判断顽鹿活动在 Strava 上是否已存在（按时间±5分钟 + 距离±20%）"""
        start_time = onelap_act.get("start_riding_time", "")
        dist_km = onelap_act.get("distance_km", 0)
        if not start_time:
            return False

        try:
            onelap_ts = datetime.strptime(start_time[:19], "%Y-%m-%d %H:%M:%S")
            onelap_ts = onelap_ts.replace(tzinfo=timezone.utc)  # OneLap 已按 UTC 等效时间与 Strava 比对
            onelap_ts_epoch = onelap_ts.timestamp()
        except (ValueError, TypeError):
            return False

        onelap_dist_m = dist_km * 1000

        for sa in strava_acts:
            try:
                strava_ts = datetime.fromisoformat(sa["start_date"].replace("Z", "+00:00")).timestamp()
            except (ValueError, TypeError, KeyError):
                continue
            if abs(onelap_ts_epoch - strava_ts) > 300:
                continue
            strava_dist = sa["distance"]
            if strava_dist == 0:
                continue
            if abs(onelap_dist_m - strava_dist) / max(strava_dist, onelap_dist_m) < 0.2:
                return True
        return False

    # ─── Strava 活动纠正 ──────────────────────

    def correct_strava_activity(self, strava_activity_id: int, activity_name: str) -> bool:
        """纠正 Strava 活动的 name + sport_type 为 Ride"""
        if not strava_activity_id or not self.strava_access_token:
            return False

        payload = {"name": activity_name, "sport_type": self.force_sport_type}
        try:
            resp = requests.put(
                f"{STRAVA_API_BASE}/activities/{strava_activity_id}",
                headers={"Authorization": f"Bearer {self.strava_access_token}"},
                data=payload,
                timeout=30,
            )
            if resp.status_code == 401:
                self.refresh_strava_token()
                resp = requests.put(
                    f"{STRAVA_API_BASE}/activities/{strava_activity_id}",
                    headers={"Authorization": f"Bearer {self.strava_access_token}"},
                    data=payload,
                    timeout=30,
                )
            if resp.status_code == 200:
                self.logger.info(f"Strava 活动已纠正: {activity_name} (id={strava_activity_id})")
                return True
            self.logger.warning(f"纠正失败: {resp.status_code} {resp.text[:200]}")
        except (requests.RequestException, RuntimeError) as e:
            self.logger.warning(f"纠正异常: {e}")
        return False

    # ─── 主流程 ────────────────────────────────

    def run(self, days: int = 1, dry_run: bool = False, force: bool = False, compensate: bool = False):
        effective_days = max(days, 3) if compensate else days
        self.logger.info(
            f"开始同步 {VERSION} (days={effective_days}, dry_run={dry_run}, force={force}, compensate={compensate})"
        )

        self._process_pending_uploads(dry_run=dry_run)
        if dry_run:
            self.logger.info("DRY RUN: 已完成 pending upload 检查，不写入新上传")

        # 1. 顽鹿登录
        try:
            self.login_onelap()
        except (requests.RequestException, ValueError, RuntimeError) as e:
            self.state["consecutive_failures"] = self.state.get("consecutive_failures", 0) + 1
            self._save_state()
            raise RuntimeError(f"顽鹿登录失败: {e}")

        # 2. 获取顽鹿活动列表
        try:
            onelap_activities = self.fetch_onelap_activities(effective_days)
        except (requests.RequestException, ValueError, RuntimeError) as e:
            self.state["consecutive_failures"] = self.state.get("consecutive_failures", 0) + 1
            self._save_state()
            raise RuntimeError(f"获取活动列表失败: {e}")

        if not onelap_activities:
            self.logger.info("顽鹿无新增活动")
            if not dry_run:
                self.state["last_run"] = utc_now_iso()
                self.state["consecutive_failures"] = 0
                self._save_state()
            return {"synced": [], "failed": [], "corrected": [], "message": "无新增活动"}

        # 3. 刷新 Strava token；dry-run 不写 .env，优先使用现有 access token
        try:
            if dry_run and self.strava_access_token:
                self.logger.info("DRY RUN: 使用现有 Strava access token，跳过 token refresh 写回")
            else:
                self.refresh_strava_token()
        except (requests.RequestException, ValueError, RuntimeError) as e:
            self.state["consecutive_failures"] = self.state.get("consecutive_failures", 0) + 1
            if not dry_run:
                self._save_state()
            raise RuntimeError(f"Strava token 刷新失败: {e}")

        # 4. 获取 Strava 活动列表（比对用）
        # compensate 时多查 1 天，避免 UTC 时区截断导致已同步活动被漏掉
        strava_lookup_days = effective_days + 1 if compensate else effective_days
        strava_acts = self.fetch_strava_activities(strava_lookup_days)

        # 5. 过滤出需要同步的活动
        activities_state_for_dedupe = self.state.get("activities", {})
        synced_set = set(activities_state_for_dedupe.keys())
        synced_hashes = {v.get("fit_sha256") for v in activities_state_for_dedupe.values() if isinstance(v, dict) and v.get("fit_sha256")}
        to_sync = []
        skipped_too_short = 0
        min_duration_seconds = 180  # 3分钟
        min_distance_km = 2.0

        for act in onelap_activities:
            act_id = act.get("id", "")
            if not force and act_id in synced_set:
                continue

            # ── 时长/距离过滤：跳过不足3分钟且距离不足2km的短记录 ──
            duration = act.get("time_seconds", 0)
            distance = act.get("distance_km", 0)
            if duration < min_duration_seconds or distance < min_distance_km:
                display = format_activity_name(
                    act["start_riding_time"], distance, act.get("avg_heart_bpm", 0)
                )
                reason_parts = []
                if duration < min_duration_seconds:
                    reason_parts.append(f"时长 {duration}s 不足{min_duration_seconds}s")
                if distance < min_distance_km:
                    reason_parts.append(f"距离 {distance:.1f}km 不足{min_distance_km}km")
                self.logger.info(f"跳过: {display} ({', '.join(reason_parts)})")
                synced_set.add(act_id)
                skipped_too_short += 1
                continue

            if not force and strava_acts is not None and self._is_on_strava(act, strava_acts):
                display = format_activity_name(
                    act["start_riding_time"], act.get("distance_km", 0), act.get("avg_heart_bpm", 0)
                )
                self.logger.info(f"跳过: {display} (Strava 上已存在)")
                synced_set.add(act_id)
                continue
            to_sync.append(act)

        if not to_sync:
            if skipped_too_short:
                self.logger.info(f"所有活动已处理 (跳过 {skipped_too_short} 个不足3分钟或不足2km的活动)")
            else:
                self.logger.info("所有活动已在 Strava 上，无需同步")
            # 将过滤掉的短活动 ID 持久化，避免每轮重复评估
            if synced_set != set(self.state.get("activities", {}).keys()):
                self.state["activities"] = self.state.get("activities", {})
                for act_id in synced_set:
                    if act_id not in self.state["activities"]:
                        self.state["activities"][act_id] = {
                            "name": "",
                            "start_time": "",
                            "distance_km": 0,
                            "skipped_reason": "too_short",
                        }
            if not dry_run:
                self.state["last_run"] = utc_now_iso()
                self.state["consecutive_failures"] = 0
                self._save_state()
            return {"synced": [], "failed": [], "corrected": [], "message": "已在 Strava 上"}

        if dry_run:
            self.logger.info(f"DRY RUN: 将同步 {len(to_sync)} 个活动")
            for a in to_sync:
                name = format_activity_name(a["start_riding_time"], a.get("distance_km", 0), a.get("avg_heart_bpm", 0))
                self.logger.info(f"  - {name}")
            return {"synced": [format_activity_name(a["start_riding_time"], a.get("distance_km", 0), a.get("avg_heart_bpm", 0)) for a in to_sync], "failed": [], "corrected": [], "dry_run": True}

        # 6. 逐个同步：下载 FIT → 上传 Strava → 纠正 → [V3.1] 分析
        synced = []
        failed = []
        duplicated = []
        corrected = []
        pending_analyses = []  # [V3.2] 结构化数据，供运动顾问分析
        activities_state = self.state.setdefault("activities", {})

        for i, activity in enumerate(to_sync[:self.max_uploads_per_run]):
            act_id = activity.get("id", "")
            start_time = activity.get("start_riding_time", "")
            distance_km = activity.get("distance_km", 0)
            avg_heart = activity.get("avg_heart_bpm", 0)
            display_name = format_activity_name(start_time, distance_km, avg_heart)

            self.logger.info(f"[{i+1}/{len(to_sync)}] 处理: {display_name}")

            try:
                # 下载 FIT
                self.logger.info(f"  下载 FIT: {act_id}")
                fit_data = self.download_fit(act_id)

                # 上传到 Strava
                self.logger.info(f"  上传 Strava...")
                fit_hash = fit_sha256(fit_data)
                if not force and fit_hash in synced_hashes:
                    self.logger.info(f"  跳过: FIT sha256 已同步 ({fit_hash[:12]})")
                    activities_state[act_id] = {
                        "name": display_name,
                        "start_time": start_time,
                        "distance_km": distance_km,
                        "fit_sha256": fit_hash,
                        "synced_at": utc_now_iso(),
                        "status": "dedup_by_fit_sha256",
                    }
                    self._save_state()
                    continue
                strava_aid = self.upload_to_strava(fit_data, display_name, act_id)

                if strava_aid:
                    # 上传成功（或检测到重复，拿到了已有 activity_id）
                    previously_synced = act_id in activities_state
                    if previously_synced:
                        duplicated.append(display_name)
                        self.logger.info(f"  已是重复活动 (Strava id={strava_aid})，仅做数据分析")
                    else:
                        time.sleep(3)
                        if self.correct_strava_activity(strava_aid, display_name):
                            corrected.append(display_name)
                        synced.append(display_name)

                    # [V3.2] 提取结构化数据供运动顾问分析
                    time.sleep(1)
                    structured_data, _summary = self._run_analysis(fit_data, strava_aid, display_name, act_id)
                    if structured_data:
                        pending_analyses.append(structured_data)

                    activities_state[act_id] = {
                        "name": display_name,
                        "start_time": start_time,
                        "distance_km": distance_km,
                        "strava_id": strava_aid,
                        "fit_sha256": fit_hash,
                        "synced_at": utc_now_iso(),
                    }
                    synced_hashes.add(fit_hash)
                else:
                    # strava_aid 为 None（上传失败，非重复）
                    failed.append(display_name)
                    self.logger.error(f"  上传失败（非重复）: {display_name}")

            except UploadPendingError as e:
                self.logger.warning(f"  上传进入 pending，等待下轮恢复: {e}")
            except RetryableActivityError as e:
                self.logger.info(f"  可重试跳过: {e}")
            except RateLimitError:
                self.logger.error("  Strava rate limit，本轮停止")
                break
            except (requests.RequestException, ValueError, RuntimeError, OSError) as e:
                self.logger.error(f"  同步失败: {e}")
                failed.append(display_name)

            time.sleep(1)

        self.state["last_run"] = utc_now_iso()
        self.state["consecutive_failures"] = 0
        self._save_state()

        # [V3.2] 调运动顾问(coach)分析 → 写入 Strava 描述
        if pending_analyses:
            coach_ok, coach_fail = self._process_coach_analyses(pending_analyses)

            # 仍写一份到待处理文件做备份
            pending_file = BASE_DIR / "analysis_pending.json"
            existing = []
            if pending_file.exists():
                try:
                    existing = json.loads(pending_file.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    pass
            existing.extend(pending_analyses)
            atomic_write_text(pending_file, json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8", backup=True)
            self.logger.info(f"📋 {len(pending_analyses)} 个活动分析数据已备份到 {pending_file}")

        # ─── 通知（仅同步结果，不含分析内容）───
        if synced:
            lines = [f"- {name} -> ✅ Strava" for name in synced]
            if corrected:
                lines.append(f"  ({len(corrected)} 个已纠正为 Ride)")
            if duplicated:
                lines.append(f"{len(duplicated)} 个已在 Strava 上")
            if failed:
                lines.append(f"{len(failed)} 个失败")
            self._send_bark("🚴 骑行同步成功", "\n".join(lines))
        elif failed and not duplicated:
            lines = [f"- {name} -> ❌ 失败" for name in failed]
            self._send_bark("🚴 骑行同步失败", "\n".join(lines))
        # 全重复 / 无新增 / 已在 Strava 上 → 静默

        self.logger.info(f"同步完成: {len(synced)} 成功 / {len(duplicated)} 重复 / {len(failed)} 失败 / {len(corrected)} 纠正")
        return {"synced": synced, "failed": failed, "duplicated": duplicated, "corrected": corrected}


def main():
    load_dotenv(ENV_FILE)

    parser = argparse.ArgumentParser(description="Magene -> Strava 同步 (V3.4 - 顽鹿OTM + Strava直连)")
    parser.add_argument("--days", type=int, default=1, help="同步最近 N 天的活动")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不执行同步")
    parser.add_argument("--force", action="store_true", help="强制重新同步所有活动")
    parser.add_argument("--compensate", action="store_true", help="补偿同步（最近3天）")
    parser.add_argument("--verbose", action="store_true", help="详细日志")
    parser.add_argument("--no-analysis", action="store_true", help="禁用 FIT 数据分析")
    args = parser.parse_args()

    logger = setup_logger(args.verbose)

    # 全局文件锁：非阻塞，防止 cron 重叠执行
    lock = LOCK_FILE.open("w")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        logger.info(f"Global lock acquired: {LOCK_FILE}")
    except BlockingIOError:
        logger.warning("Another instance is running, exiting")
        lock.close()
        return

    try:
        sync = MageneStravaSyncV3(CONFIG_FILE, STATE_FILE, logger)
        if args.no_analysis:
            sync.analysis_enabled = False
            logger.info("数据分析已通过 --no-analysis 禁用")
        result = sync.run(
            days=args.days,
            dry_run=args.dry_run,
            force=args.force or args.compensate,
            compensate=args.compensate,
        )
    except (requests.RequestException, ValueError, RuntimeError, OSError) as e:
        logger.error("同步执行异常", exc_info=True)
        if "sync" in dir():
            try:
                sync._send_bark("🚴 骑行同步失败", str(e)[:100].replace("\n", " "))
            except (requests.RequestException, RuntimeError):
                pass
        sys.exit(1)
    finally:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        finally:
            lock.close()


if __name__ == "__main__":
    main()
