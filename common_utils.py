"""
通用工具模块 — common_utils
============================
提取三个脚本（morning_training_tip / evening_digest / sunday_training_tip）中
重复的公共逻辑：coach 调用、天气获取、DB 连接。

用途：
  1. 消除 call_coach() 在 morning 和 sunday 中的重复
  2. 消除 _qw_get() 在 morning 和 sunday 中的重复
  3. 提供统一的 DB 连接管理
"""

import json
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# ─── 路径 ──────────────────────────────────────
WORKSPACE_DIR = Path("/root/.qwenpaw/workspaces/code-agent")
SCRIPTS_DIR = Path("/root/.qwenpaw/scripts")
_TZ = timezone(timedelta(hours=8))

# ─── Coach 调用 ────────────────────────────────

def call_coach(
    prompt: str,
    agent_id: str = "coach",
    timeout_sec: int = 120,
    from_agent: str = "code-agent",
    to_agent: Optional[str] = None,
) -> Optional[str]:
    """
    调用 coach agent 获取训练建议。

    Args:
        prompt: 请求文本
        agent_id: 目标 agent ID（默认 coach）。保留该参数用于兼容旧调用；
            当未显式传入 to_agent 时，agent_id 会作为目标 agent。
        timeout_sec: subprocess 超时秒数
        from_agent: 发起调用的 agent ID（默认 code-agent）
        to_agent: 目标 agent ID；如未指定则使用 agent_id

    Returns:
        coach 回复文本，失败返回 None
    """
    target_agent = to_agent or agent_id
    try:
        result = subprocess.run(
            [
                "qwenpaw", "agents", "chat",
                "--from-agent", from_agent,
                "--to-agent", target_agent,
                "--text", prompt,
            ],
            capture_output=True, text=True, timeout=timeout_sec,
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            if output:
                return _extract_coach_reply(output)
        print(f"[WARN] coach 调用失败: code={result.returncode}", file=sys.stderr)
        return None
    except subprocess.TimeoutExpired:
        print(f"[WARN] coach 调用超时 ({timeout_sec}s)", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[WARN] coach 调用异常: {e}", file=sys.stderr)
        return None


def _extract_coach_reply(raw: str) -> str:
    """从 coach agent 的 JSON 响应中提取文本。"""
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            # 尝试多种可能的字段名
            for key in ("text", "content", "message", "reply"):
                if key in data:
                    val = data[key]
                    if isinstance(val, str):
                        return val.strip()
                    if isinstance(val, list):
                        texts = [v.get("text", "") for v in val if isinstance(v, dict)]
                        return " ".join(texts).strip()
        return str(data)
    except (json.JSONDecodeError, TypeError):
        # 不是 JSON，直接返回
        return raw.strip()


# ─── 天气 ──────────────────────────────────────

def fetch_weather(city: str = "萧山", api_key_env: str = "QWEATHER_KEY") -> str:
    """
    获取城市天气预报。

    Returns:
        格式化天气文本，失败时包含错误提示
    """
    import os
    key = os.environ.get(api_key_env, "")
    if not key:
        return "⚠️ 天气 API 未配置"

    api_base = "https://devapi.qweather.com/v7/weather"

    def _qw_get(path: str) -> dict:
        """QWeather API GET 请求。"""
        import urllib.request
        url = f"{api_base}{path}?key={key}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                return json.loads(resp.read())
        except Exception as e:
            return {"code": "502", "error": str(e)}

    # 先用城市搜索获取 location ID
    try:
        import urllib.request
        geo_url = f"https://geoapi.qweather.com/v2/city/lookup?location={city}&key={key}"
        with urllib.request.urlopen(geo_url, timeout=10) as resp:
            geo = json.loads(resp.read())
        if geo.get("code") != "200" or not geo.get("location"):
            return f"⚠️ 天气查询失败: 城市'{city}'未找到"
        loc_id = geo["location"][0]["id"]
    except Exception as e:
        return f"⚠️ 城市查询失败: {e}"

    # 今天天气
    today = _qw_get(f"/now?location={loc_id}")
    if today.get("code") != "200":
        return "⚠️ 天气查询失败"

    now = today.get("now", {})
    temp = now.get("temp", "?")
    feels = now.get("feelsLike", "?")
    humidity = now.get("humidity", "?")
    wind_dir = now.get("windDir", "?")
    wind_scale = now.get("windScale", "?")
    text = now.get("text", "?")

    # 未来3天预报
    forecast = _qw_get(f"/3d?location={loc_id}")
    if forecast.get("code") == "200":
        daily = forecast.get("daily", [])
        forecast_lines = []
        for d in daily[:2]:
            day = d.get("fxDate", "")[5:]
            d_text = d.get("textDay", "?")
            d_temp_min = d.get("tempMin", "?")
            d_temp_max = d.get("tempMax", "?")
            d_wind = d.get("windDirDay", "?")
            d_scale = d.get("windScaleDay", "?")
            forecast_lines.append(f"   {day} {d_text} {d_temp_min}~{d_temp_max}°C {d_wind}{d_scale}级")
        forecast_str = "\n".join(forecast_lines) if forecast_lines else "   (预报数据为空)"
    else:
        forecast_str = "   (预报暂不可用)"

    lines = [
        f"🌤 天气",
        f"├── 当前: {text} {temp}°C (体感{feels}°) 💧{humidity}%",
        f"├── 风力: {wind_dir}{wind_scale}级",
        f"└── 未来:",
        forecast_str,
    ]
    return "\n".join(lines)


# ─── DB 连接管理 ──────────────────────────────

@contextmanager
def get_db(db_path: Optional[Path] = None):
    """
    获取 DB 连接（统一连接管理，避免重复建立）。

    用途：调用方 with get_db() as conn: ...
    """
    import sqlite3
    path = db_path or (WORKSPACE_DIR / "onelap_sync.db")
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


# ─── 日期工具 ──────────────────────────────────

def today_str() -> str:
    """返回今天日期 YYYY-MM-DD。"""
    return datetime.now(_TZ).strftime("%Y-%m-%d")


def today_cn() -> str:
    """返回中文星期几。"""
    weekdays = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    return weekdays[datetime.now(_TZ).weekday()]


# ─── 冷启动检测 ───────────────────────────────

def is_cold_start(conn) -> bool:
    """检测是否冷启动（training_load 少于3条）。"""
    import sqlite3
    try:
        cur = conn.execute("SELECT COUNT(*) as c FROM training_load")
        row = cur.fetchone()
        count = row["c"] if hasattr(row, "keys") else row[0]
        return count < 3
    except sqlite3.Error:
        return True  # 降级：冷启动


# ─── 睡眠读取 ──────────────────────────────────

def get_sleep_summary(sleep_file: Optional[Path] = None) -> Optional[dict]:
    """读取 Apple Health 睡眠摘要 JSON。"""
    path = sleep_file or Path("/root/OnelapSync/recoverysync/data/sleep_summary.json")
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        days = data.get("days", {})
        if not days:
            return None
        latest_date = sorted(days.keys())[-1]
        return days[latest_date]
    except Exception as e:
        print(f"[WARN] 读睡眠文件失败: {e}", file=sys.stderr)
        return None
