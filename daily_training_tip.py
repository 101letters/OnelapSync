#!/usr/bin/env python3
"""
Daily training tip: generate tomorrow's cycling/training suggestion and push to Telegram.

Design constraints:
- Strava is read-only: only GET /athlete/activities and OAuth refresh in memory if needed.
- Weather and Strava failures degrade to a usable suggestion instead of failing the run.
- One target_date is sent at most once unless --force is used.
- --dry-run prints the Telegram text and does not send or mark as sent.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
GOAL_FILE = BASE_DIR / "training_goal.yaml"
STATE_FILE = BASE_DIR / "daily_training_tip_state.json"
SYNC_STATE_FILE = BASE_DIR / "sync_state_v3.json"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "daily_training_tip.log"

STRAVA_API_BASE = os.environ.get("STRAVA_API_BASE", "https://www.strava.com/api/v3")
STRAVA_OAUTH_URL = os.environ.get("STRAVA_OAUTH_URL", "https://www.strava.com/oauth/token")
# ─── 彩云天气（主） ────────────────────
CAIYUN_API_BASE = os.environ.get("CAIYUN_API_BASE", "https://api.caiyunapp.com/v2.5")

# ─── 和风天气（后备） ──────────────────

WEEKDAY_CN = "一二三四五六日"


@dataclass
class Goal:
    target_hours: float = 6.0
    target_rides: int = 4
    target_long_ride: bool = True
    target_intensity_sessions: int = 1
    priority: str = "base endurance"


@dataclass
class LoadSummary:
    status: str
    source: str
    activities: list[dict[str, Any]]
    count: int
    hours: float
    distance_km: float
    load_level: str
    intensity_sessions: int
    current_week_hours: float
    current_week_rides: int
    total_kilojoules: float = 0.0
    total_suffer: float = 0.0
    avg_hr: float = 0.0
    avg_watts: float = 0.0


@dataclass
class WeatherSummary:
    status: str
    text: str
    temp_min: str = "—"
    temp_max: str = "—"
    wind: str = "—"
    outdoor_ok: bool = True
    raw: dict[str, Any] | None = None


@dataclass
class Recommendation:
    icon: str
    conclusion: str
    kind: str
    duration: str
    intensity: str
    focus: str
    reminder: str


def load_dotenv(path: Path = ENV_FILE) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def setup_logger(verbose: bool = False) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("daily_training_tip")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    file_handler = TimedRotatingFileHandler(LOG_FILE, when="midnight", backupCount=30, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    if verbose:
        logger.addHandler(console)
    return logger


def parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_simple_yaml(path: Path) -> dict[str, Any]:
    """Small YAML fallback for the simple training_goal.yaml shape."""
    result: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, result)]
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value == "":
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            value = value.strip('"').strip("'")
            if value.lower() in {"true", "false"}:
                parsed: Any = value.lower() == "true"
            else:
                try:
                    parsed = int(value) if re.fullmatch(r"[-+]?\d+", value) else float(value)
                except ValueError:
                    parsed = value
            parent[key] = parsed
    return result


def load_goal(path: Path = GOAL_FILE) -> Goal:
    if not path.exists():
        return Goal()
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        data = parse_simple_yaml(path)
    weekly = data.get("weekly", {}) if isinstance(data, dict) else {}
    return Goal(
        target_hours=float(weekly.get("target_hours", 6)),
        target_rides=int(weekly.get("target_rides", 4)),
        target_long_ride=parse_bool(weekly.get("target_long_ride"), True),
        target_intensity_sessions=int(weekly.get("target_intensity_sessions", 1)),
        priority=str(weekly.get("priority", "base endurance")),
    )


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def atomic_write_json(path: Path, data: Any) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def already_sent(target: date) -> bool:
    state = load_json(STATE_FILE, {})
    return bool(state.get("sent", {}).get(target.isoformat()))


def mark_sent(target: date, message: str, send_status: str) -> None:
    state = load_json(STATE_FILE, {})
    state.setdefault("sent", {})[target.isoformat()] = {
        "sent_at": datetime.now().isoformat(timespec="seconds"),
        "send_status": send_status,
        "message_sha1": __import__("hashlib").sha1(message.encode("utf-8")).hexdigest(),
    }
    atomic_write_json(STATE_FILE, state)


def get_strava_token(logger: logging.Logger) -> tuple[str, str]:
    access = os.environ.get("STRAVA_ACCESS_TOKEN", "")
    refresh = os.environ.get("STRAVA_REFRESH_TOKEN", "")
    client_id = os.environ.get("STRAVA_CLIENT_ID", "")
    client_secret = os.environ.get("STRAVA_CLIENT_SECRET", "")
    if access:
        return access, "env_access_token"
    if not (refresh and client_id and client_secret and requests):
        return "", "missing_token"
    try:
        resp = requests.post(
            STRAVA_OAUTH_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh,
            },
            timeout=20,
        )
        if resp.status_code == 200:
            return resp.json().get("access_token", ""), "oauth_refreshed_memory_only"
        logger.warning("Strava token refresh failed: %s %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("Strava token refresh exception: %s", e)
    return "", "refresh_failed"


def normalize_strava_activity(a: dict[str, Any]) -> dict[str, Any]:
    moving_seconds = float(a.get("moving_time") or a.get("elapsed_time") or 0)
    distance_km = float(a.get("distance") or 0) / 1000.0
    avg_hr = a.get("average_heartrate") or 0
    max_hr = a.get("max_heartrate") or 0
    avg_watts = a.get("average_watts") or 0
    max_watts = a.get("max_watts") or 0
    kilojoules = a.get("kilojoules") or 0
    suffer = a.get("suffer_score") or 0
    elev = a.get("total_elevation_gain") or 0
    trainer = a.get("trainer", False)
    start = a.get("start_date_local") or a.get("start_date") or ""
    return {
        "id": a.get("id"),
        "name": a.get("name", ""),
        "start": start[:19],
        "distance_km": distance_km,
        "hours": moving_seconds / 3600.0,
        "avg_hr": float(avg_hr or 0),
        "max_hr": float(max_hr or 0),
        "avg_watts": float(avg_watts or 0),
        "max_watts": float(max_watts or 0),
        "kilojoules": float(kilojoules or 0),
        "suffer_score": float(suffer or 0),
        "elev_gain": float(elev or 0),
        "trainer": bool(trainer),
        "type": a.get("sport_type") or a.get("type") or "Ride",
    }


def read_strava_activities(target: date, days: int, logger: logging.Logger) -> tuple[list[dict[str, Any]], str]:
    if requests is None:
        return [], "requests_missing"
    token, token_source = get_strava_token(logger)
    if not token:
        return [], token_source
    after_dt = datetime.combine(target - timedelta(days=days), time.min, tzinfo=timezone.utc)
    before_dt = datetime.combine(target, time.min, tzinfo=timezone.utc)
    headers = {"Authorization": f"Bearer {token}"}
    acts: list[dict[str, Any]] = []
    try:
        for page in range(1, 6):
            resp = requests.get(
                f"{STRAVA_API_BASE}/athlete/activities",
                headers=headers,
                params={"after": int(after_dt.timestamp()), "before": int(before_dt.timestamp()), "page": page, "per_page": 50},
                timeout=25,
            )
            if resp.status_code == 401 and token_source == "env_access_token":
                # Refresh in memory only; do not write .env/token files.
                token2, src2 = get_strava_token(logger)
                if token2 and src2 == "oauth_refreshed_memory_only":
                    headers = {"Authorization": f"Bearer {token2}"}
                    resp = requests.get(
                        f"{STRAVA_API_BASE}/athlete/activities",
                        headers=headers,
                        params={"after": int(after_dt.timestamp()), "before": int(before_dt.timestamp()), "page": page, "per_page": 50},
                        timeout=25,
                    )
            if resp.status_code != 200:
                return [], f"api_{resp.status_code}"
            batch = resp.json()
            if not batch:
                break
            acts.extend(normalize_strava_activity(x) for x in batch)
            if len(batch) < 50:
                break
        return acts, "ok"
    except Exception as e:
        logger.warning("Strava read failed: %s", e)
        return [], "exception"


def parse_local_dt(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value[:19], fmt)
        except ValueError:
            continue
    return None


def read_sync_state_activities(target: date, days: int) -> list[dict[str, Any]]:
    state = load_json(SYNC_STATE_FILE, {})
    out: list[dict[str, Any]] = []
    start_day = target - timedelta(days=days)
    for info in (state.get("activities") or {}).values():
        dt = parse_local_dt(str(info.get("start_time", "")))
        if not dt or not (start_day <= dt.date() < target):
            continue
        dist = float(info.get("distance_km") or 0)
        if dist < 0.5:
            continue
        name = str(info.get("name", ""))
        m = re.search(r"(\d+)bpm", name)
        avg_hr = float(m.group(1)) if m else 0
        # Fallback duration estimate, conservative recreational road cycling.
        hours = dist / 25.0 if dist > 0 else 0
        out.append({
            "id": info.get("strava_id"),
            "name": name,
            "start": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "distance_km": dist,
            "hours": hours,
            "avg_hr": avg_hr,
            "suffer_score": 0,
            "type": "Ride",
        })
    return out


def summarize_load(target: date, goal: Goal, logger: logging.Logger) -> LoadSummary:
    acts, status = read_strava_activities(target, 7, logger)
    source = "strava"
    if status != "ok":
        fallback = read_sync_state_activities(target, 7)
        if fallback:
            acts, source = fallback, "sync_state_fallback"
            status = f"degraded_{status}"
        else:
            acts, source = [], "none"
    # Filter rides and tiny records.
    acts = [a for a in acts if float(a.get("distance_km") or 0) >= 0.5 or float(a.get("hours") or 0) >= 0.05]
    hours = sum(float(a.get("hours") or 0) for a in acts)
    distance = sum(float(a.get("distance_km") or 0) for a in acts)
    total_kj = sum(float(a.get("kilojoules") or 0) for a in acts)
    total_suffer = sum(float(a.get("suffer_score") or 0) for a in acts)
    # 心率区间估算强度课次数（HR >= 155 或 suffer >= 80 或 kJ/h >= 350）
    avg_hrs = [float(a.get("avg_hr") or 0) for a in acts if float(a.get("avg_hr") or 0) > 0]
    avg_watts_list = [float(a.get("avg_watts") or 0) for a in acts if float(a.get("avg_watts") or 0) > 0]
    intensity_sessions = sum(
        1 for a in acts
        if float(a.get("avg_hr") or 0) >= 155
        or float(a.get("suffer_score") or 0) >= 80
        or (float(a.get("hours") or 0) > 0 and float(a.get("kilojoules") or 0) / float(a.get("hours")) >= 350)
    )
    # 综合负荷判断：结合时长、能量消耗、疲劳分数
    load_score = hours * 100 + total_kj * 0.3 + total_suffer * 5
    baseline = goal.target_hours * 100  # 周目标对应基准
    if load_score >= baseline * 0.85:
        level = "偏高"
    elif load_score >= baseline * 0.45:
        level = "中等"
    elif hours > 0:
        level = "偏低"
    else:
        level = "未知"

    week_start = target - timedelta(days=target.weekday())
    week_acts = []
    for a in acts:
        dt = parse_local_dt(str(a.get("start", "")))
        if dt and week_start <= dt.date() < target:
            week_acts.append(a)
    return LoadSummary(
        status=status,
        source=source,
        activities=acts,
        count=len(acts),
        hours=hours,
        distance_km=distance,
        load_level=level,
        intensity_sessions=intensity_sessions,
        current_week_hours=sum(float(a.get("hours") or 0) for a in week_acts),
        current_week_rides=len(week_acts),
        total_kilojoules=total_kj,
        total_suffer=total_suffer,
        avg_hr=sum(avg_hrs) / len(avg_hrs) if avg_hrs else 0.0,
        avg_watts=sum(avg_watts_list) / len(avg_watts_list) if avg_watts_list else 0.0,
    )


# ─── 彩云天气 API（主）────────────────────

CAIYUN_SKYCON_MAP = {
    "CLEAR_DAY": "晴", "CLEAR_NIGHT": "晴",
    "PARTLY_CLOUDY_DAY": "多云", "PARTLY_CLOUDY_NIGHT": "多云",
    "CLOUDY": "阴", "LIGHT_HAZE": "轻霾", "MODERATE_HAZE": "霾",
    "HEAVY_HAZE": "重霾", "LIGHT_RAIN": "小雨", "MODERATE_RAIN": "中雨",
    "HEAVY_RAIN": "大雨", "STORM_RAIN": "暴雨", "FOG": "雾",
    "LIGHT_SNOW": "小雪", "MODERATE_SNOW": "中雪", "HEAVY_SNOW": "大雪",
    "STORM_SNOW": "暴雪", "DUST": "浮尘", "SAND": "沙尘",
    "WIND": "大风", "SLEET": "雨夹雪", "HAIL": "冰雹",
    "THUNDER": "雷阵雨", "THUNDER_HAIL": "雷雨冰雹",
}


def fetch_caiyun(target: date, logger: logging.Logger) -> WeatherSummary | None:
    """彩云天气 48h 逐小时预报（主）"""
    token = os.environ.get("CAIYUN_TOKEN")
    location = os.environ.get("CAIYUN_LOCATION")
    if not (requests and token and location):
        return None
    try:
        url = f"{CAIYUN_API_BASE}/{token}/{location}/hourly.json"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            logger.warning("Caiyun HTTP %s", resp.status_code)
            return None
        data = resp.json()
        if data.get("status") != "ok":
            logger.warning("Caiyun status not ok: %s", data.get("status"))
            return None
        hourly = data.get("result", {}).get("hourly", {})
        if not hourly:
            return None
        # 筛选目标日期的逐小时数据 (06:00~22:00 可骑窗口)
        target_str = target.isoformat()
        hours_today = []
        for i in range(len(hourly.get("precipitation", []))):
            dt_str = hourly["precipitation"][i].get("datetime", "")
            if dt_str.startswith(target_str):
                hours_today.append({
                    "time": dt_str,
                    "precip": hourly.get("precipitation", [{}])[i].get("value", 0) if i < len(hourly.get("precipitation", [])) else 0,
                    "temp": hourly.get("temperature", [{}])[i].get("value", 20) if i < len(hourly.get("temperature", [])) else 20,
                    "wind_speed": hourly.get("wind", [{}])[i].get("speed", 0) if i < len(hourly.get("wind", [])) else 0,
                    "wind_dir": hourly.get("wind", [{}])[i].get("direction", 0) if i < len(hourly.get("wind", [])) else 0,
                    "skycon": hourly.get("skycon", [{}])[i].get("value", "CLOUDY") if i < len(hourly.get("skycon", [])) else "CLOUDY",
                    "humidity": hourly.get("humidity", [{}])[i].get("value", 0.5) if i < len(hourly.get("humidity", [])) else 0.5,
                })
        if not hours_today:
            return None
        # 综合判断
        peak_precip = max(h["precip"] for h in hours_today)
        peak_wind = max(h["wind_speed"] for h in hours_today)
        avg_temp = sum(h["temp"] for h in hours_today) / len(hours_today)
        min_temp = min(h["temp"] for h in hours_today)
        max_temp = max(h["temp"] for h in hours_today)
        rainy_hours = sum(1 for h in hours_today if h["precip"] > 0.5)
        # 主要天气描述：取白天最常见 skycon
        day_hours = [h for h in hours_today if 6 <= int(h["time"][11:13]) <= 18]
        if not day_hours:
            day_hours = hours_today
        skycon_counts: dict[str, int] = {}
        for h in day_hours:
            skycon_counts[h["skycon"]] = skycon_counts.get(h["skycon"], 0) + 1
        main_skycon = max(skycon_counts, key=skycon_counts.get)
        weather_text = CAIYUN_SKYCON_MAP.get(main_skycon, main_skycon)
        if rainy_hours >= 3:
            weather_text = f"{weather_text}，多时段有雨"
        outdoor_ok = peak_precip < 2.0 and peak_wind < 8.0 and min_temp >= 0
        wind_desc = f"{peak_wind:.0f}m/s"
        if peak_wind < 3:
            wind_desc = f"{peak_wind:.0f}m/s（微风）"
        elif peak_wind < 6:
            wind_desc = f"{peak_wind:.0f}m/s（和风）"
        elif peak_wind < 9:
            wind_desc = f"{peak_wind:.0f}m/s（较大）"
        else:
            wind_desc = f"{peak_wind:.0f}m/s（大风）"
        return WeatherSummary(
            status="ok",
            text=weather_text,
            temp_min=f"{min_temp:.0f}",
            temp_max=f"{max_temp:.0f}",
            wind=wind_desc,
            outdoor_ok=outdoor_ok,
            raw={"caiyun": data, "hours": hours_today},
        )
    except Exception as e:
        logger.warning("Caiyun exception: %s", e)
        return None


def fetch_qweather(target: date, logger: logging.Logger) -> WeatherSummary | None:
    """和风天气 7天预报（后备）—— 需从环境变量读取 QWEATHER_HOST + QWEATHER_KEY"""
    host = os.environ.get("QWEATHER_HOST")
    key = os.environ.get("QWEATHER_KEY")
    loc = os.environ.get("QWEATHER_LOCATION")
    if not (requests and host and key and loc):
        return None
    try:
        base_url = f"https://{host}/v7"
        daily_resp = requests.get(f"{base_url}/weather/7d", params={"location": loc, "key": key}, timeout=15)
        hourly_resp = requests.get(f"{base_url}/weather/24h", params={"location": loc, "key": key}, timeout=15)
        if daily_resp.status_code != 200:
            return None
        daily_data = daily_resp.json()
        hourly_data = hourly_resp.json() if hourly_resp.status_code == 200 else {}
        if daily_data.get("code") != "200":
            return None
        day_item = None
        for item in daily_data.get("daily", []):
            if item.get("fxDate") == target.isoformat():
                day_item = item
                break
        if not day_item and daily_data.get("daily"):
            day_item = daily_data["daily"][0]
        if not day_item:
            return None
        text = day_item.get("textDay") or day_item.get("textNight") or "—"
        temp_min = str(day_item.get("tempMin", "—"))
        temp_max = str(day_item.get("tempMax", "—"))
        wind_scale = day_item.get("windScaleDay") or day_item.get("windScaleNight") or "—"
        precip = float(day_item.get("precip", 0) or 0)
        wind_num = max([int(x) for x in re.findall(r"\d+", str(wind_scale))] or [0])
        bad_text = any(x in text for x in ["雨", "雪", "雷", "暴", "沙", "霾"])
        outdoor_ok = not (bad_text or precip >= 3 or wind_num >= 5)
        return WeatherSummary(
            status="ok",
            text=text,
            temp_min=temp_min,
            temp_max=temp_max,
            wind=f"{wind_scale}级" if str(wind_scale) != "—" and "级" not in str(wind_scale) else str(wind_scale),
            outdoor_ok=outdoor_ok,
            raw={"daily": daily_data, "hourly": hourly_data},
        )
    except Exception as e:
        logger.warning("QWeather fallback exception: %s", e)
        return None


def fetch_weather(target: date, logger: logging.Logger) -> WeatherSummary:
    """天气获取：彩云（主）→ 和风（后备）"""
    if requests is None:
        return WeatherSummary(status="missing_requests", text="天气获取失败（依赖缺失），按普通天气保守安排", outdoor_ok=True)
    # 彩云主
    result = fetch_caiyun(target, logger)
    if result and result.status == "ok":
        return result
    # 和风后备
    logger.info("Caiyun failed, trying QWeather fallback")
    result = fetch_qweather(target, logger)
    if result and result.status == "ok":
        return result
    return WeatherSummary(status="all_failed", text="天气获取失败，按普通天气保守安排", outdoor_ok=True)


def make_recommendation(target: date, goal: Goal, load: LoadSummary, weather: WeatherSummary) -> Recommendation:
    weekday = target.weekday()
    remaining_hours = max(0.0, goal.target_hours - load.current_week_hours)
    rides_gap = max(0, goal.target_rides - load.current_week_rides)
    high_load = load.load_level == "偏高"
    no_outdoor = not weather.outdoor_ok

    if high_load and load.intensity_sessions >= goal.target_intensity_sessions:
        return Recommendation("😴", "建议休息或主动恢复", "休息 / 恢复拉伸", "20–30 分钟", "很轻松", "放松髋屈肌、小腿和臀腿，早点睡", "如果身体状态很好，也只做30–40分钟恢复骑")
    if no_outdoor:
        if high_load:
            return Recommendation("🧘", "建议室内恢复训练", "室内骑行 / 拉伸", "40–50 分钟", "Z1–Z2", "保持轻齿比高踏频，不追速度", "天气不适合户外，别硬刚风雨")
        return Recommendation("🏠", "建议进行室内 Z2 有氧骑行", "室内骑行", "50–70 分钟", "Z2", "稳定输出，踏频保持85–95rpm", "若天气临时转好，可改户外平路有氧")
    if remaining_hours >= 2.0 and (weekday >= 5 or goal.target_long_ride and rides_gap <= 2):
        return Recommendation("🚴", "建议进行 Z2 耐力骑行", "户外骑行", "75–120 分钟", "Z2", "平路或缓坡，控制心率不过冲", "若睡眠差或腿部酸痛，缩短为60分钟有氧")
    if load.load_level in {"中等", "偏低", "未知"} and load.intensity_sessions < goal.target_intensity_sessions and weekday in {1, 2, 3}:
        return Recommendation("🚴", "建议进行节奏/甜区训练", "户外骑行", "60–75 分钟", "Z2–Z3", "热身充分，加入2组8–12分钟稳定节奏段", "如果腿沉，取消节奏段改全程Z2")
    if load.load_level == "偏低" or rides_gap > 0:
        return Recommendation("🚴", "建议进行 Z2 有氧骑行", "户外骑行", "60–75 分钟", "Z2", "保持踏频稳定，结束后拉伸10分钟", "若睡眠差或腿部酸痛，改为40分钟恢复骑")
    return Recommendation("🧘", "建议进行恢复骑或轻松有氧", "户外骑行 / 恢复", "40–60 分钟", "Z1–Z2", "轻松转腿，不做冲刺和爬坡硬顶", "如果主观疲劳高，直接休息也可以")


def format_message(target: date, goal: Goal, load: LoadSummary, weather: WeatherSummary, rec: Recommendation) -> str:
    weekday = WEEKDAY_CN[target.weekday()]
    weather_line = (
        f"{weather.text}，{weather.temp_min}–{weather.temp_max}℃，风力{weather.wind}，"
        f"{'适合户外' if weather.outdoor_ok else '不太适合户外'}"
    ) if weather.status == "ok" else weather.text

    # 负荷详情行
    load_detail = f"训练 {load.count} 次，约 {load.hours:.1f} 小时，负荷{load.load_level}"
    if load.total_kilojoules > 0:
        load_detail += f"\n- 总消耗：{load.total_kilojoules:.0f} kJ，疲劳评分：{load.total_suffer:.0f}"
    if load.avg_hr > 0:
        load_detail += f"\n- 平均心率：{load.avg_hr:.0f} bpm"
    if load.avg_watts > 0:
        load_detail += f"\n- 平均功率：{load.avg_watts:.0f} W"

    return f"""{rec.icon} 明日训练建议｜{target.isoformat()} 周{weekday}

结论：{rec.conclusion}

安排：
- 类型：{rec.kind}
- 时长：{rec.duration}
- 强度：{rec.intensity}
- 重点：{rec.focus}

依据：
- 近7天：{load_detail}
- 本周目标：{goal.target_hours:g}小时，目前完成{load.current_week_hours:.1f}小时
- 明日天气：{weather_line}

提醒：
- {rec.reminder}"""


# ─── Telegram 推送：走 QwenPaw 渠道 ──────────


def send_telegram(text: str) -> str:
    """通过 qwenpaw channels send 推送消息到当前 Telegram 会话。"""
    from shlex import quote as shq
    channel = "telegram"
    target_user = os.environ.get("QWENPAW_TARGET_USER")
    target_session = os.environ.get("QWENPAW_TARGET_SESSION")
    agent_id = os.environ.get("QWENPAW_AGENT_ID") or "default"
    if not (target_user and target_session):
        return "skipped_missing_qwenpaw_config"
    cmd = (
        f"qwenpaw channels send "
        f"--agent-id {shq(agent_id)} "
        f"--channel {shq(channel)} "
        f"--target-user {shq(target_user)} "
        f"--target-session {shq(target_session)} "
        f"--text {shq(text)}"
    )
    try:
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        if proc.returncode == 0:
            return "sent"
        logger = logging.getLogger("daily_training_tip")
        logger.warning("qwenpaw channels send exit=%s stderr=%s", proc.returncode, proc.stderr[:300])
        return f"failed_exit_{proc.returncode}"
    except subprocess.TimeoutExpired:
        return "failed_timeout"
    except Exception as e:
        return f"failed_{type(e).__name__}"


def run(args: argparse.Namespace) -> int:
    load_dotenv()
    logger = setup_logger(args.verbose)
    target = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else date.today() + timedelta(days=1)

    if already_sent(target) and not args.force and not args.dry_run:
        logger.info(
            "daily_tip skipped duplicate target_date=%s weather_status=skipped strava_status=skipped activity_count=0 send_status=duplicate",
            target.isoformat(),
        )
        return 0

    goal = load_goal()
    load = summarize_load(target, goal, logger)
    weather = fetch_weather(target, logger)
    rec = make_recommendation(target, goal, load, weather)
    message = format_message(target, goal, load, weather, rec)

    if args.dry_run:
        print(message)
        send_status = "dry_run"
    else:
        send_status = send_telegram(message)
        if send_status == "sent":
            mark_sent(target, message, send_status)

    logger.info(
        "daily_tip finished target_date=%s weather_status=%s strava_status=%s strava_source=%s activity_count=%s send_status=%s",
        target.isoformat(), weather.status, load.status, load.source, load.count, send_status,
    )
    if send_status.startswith("failed"):
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate daily training tip and push to Telegram")
    parser.add_argument("--date", help="Target date YYYY-MM-DD; default is tomorrow")
    parser.add_argument("--dry-run", action="store_true", help="Print message only, do not send Telegram or mark sent")
    parser.add_argument("--force", action="store_true", help="Allow re-sending the same target_date")
    parser.add_argument("--verbose", action="store_true", help="Also print logs to stdout")
    return parser


if __name__ == "__main__":
    try:
        raise SystemExit(run(build_parser().parse_args()))
    except KeyboardInterrupt:
        raise SystemExit(130)
