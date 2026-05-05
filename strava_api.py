#!/usr/bin/env python3
"""
Strava API 共享模块
====================
提供 Strava token 刷新、API 调用等共享功能。
供 sync_worker / writeback_worker 使用。

v2.0: 增加 HTTP 429 指数退避重试机制（RFC 标准 + 随机抖动）。
v2.1: StravaRateLimitError 增加 endpoint 字段，自动从 URL 识别。
"""

import logging
import os
import random
import time
from urllib.parse import urlparse

import requests

STRAVA_API_BASE = os.environ.get("STRAVA_API_BASE", "https://www.strava.com/api/v3")
STRAVA_OAUTH_URL = os.environ.get("STRAVA_OAUTH_URL", "https://www.strava.com/oauth/token")

logger = logging.getLogger(__name__)


# ─── 端点识别 ──────────────────────────────────

def _detect_endpoint(url: str) -> str:
    """从 URL 路径识别 Strava API endpoint，用于 429 日志准确归因。

    映射规则：
        /oauth/token                    → token
        /uploads (POST 创建上传)         → upload
        /uploads/{id} (GET 轮询)         → poll
        /activities/{id} (PUT 纠正)      → correct
        /athlete/activities (GET 列表)   → fetch
        /athlete (GET 当前运动员)         → athlete
    """
    path = urlparse(url).path
    if "/oauth/token" in path:
        return "token"
    if path.endswith("/uploads"):
        return "upload"
    if "/uploads/" in path:
        return "poll"
    if "/activities/" in path and "/athlete/activities" not in path:
        return "correct"
    if "/athlete/activities" in path:
        return "fetch"
    if "/athlete" in path:
        return "athlete"
    return "unknown"


# ─── 限流异常 ──────────────────────────────────

class StravaRateLimitError(Exception):
    """Strava API 限流异常，指数退避重试耗尽后抛出。

    Attributes:
        retry_after: 建议等待秒数（来自 Retry-After 头或默认 60s）。
        endpoint:    触发限流的 API 端点（自动从 URL 识别）。
    """

    def __init__(self, message: str, retry_after: int = 60, endpoint: str = "unknown"):
        super().__init__(message)
        self.retry_after = retry_after
        self.endpoint = endpoint


# ─── 内部工具 ──────────────────────────────────

def _parse_retry_after(resp: requests.Response) -> int | None:
    """从 HTTP 响应头解析 Retry-After（秒）。"""
    raw = resp.headers.get("Retry-After", "")
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


# ─── 核心：带指数退避的 Strava API 请求 ────────

def strava_request(
    method: str,
    url: str,
    max_retries: int = 3,
    backoff_base: float = 2.0,
    initial_wait: float = 1.0,
    **kwargs,
) -> requests.Response:
    """发送 Strava API 请求，自动对 HTTP 429 进行指数退避重试。

    退避策略：指数退避 + 随机抖动（Full Jitter），参考 AWS Architecture Blog
    "Exponential Backoff And Jitter"。

    参数：
        method:      HTTP 方法 (GET/POST/PUT/DELETE)
        url:         Strava API URL
        max_retries: 最大重试次数 (默认 3)
        backoff_base: 退避基数 (默认 2)
        initial_wait: 初始等待秒数 (默认 1)
        **kwargs:    透传给 requests.request()

    返回：
        requests.Response 对象

    抛出：
        StravaRateLimitError: 重试耗尽后仍为 429
    """
    for attempt in range(max_retries):
        resp = requests.request(method, url, **kwargs)

        if resp.status_code != 429:
            return resp

        # ── 429 Rate Limit Exceeded ──
        retry_after = _parse_retry_after(resp)

        if attempt < max_retries - 1:
            # 指数退避 + 随机抖动
            base_sleep = initial_wait * (backoff_base ** attempt)
            jitter = random.uniform(0, base_sleep)
            sleep_time = base_sleep + jitter if retry_after is None else float(retry_after)

            logger.warning(
                f"Strava API 429 Rate Limit (attempt {attempt + 1}/{max_retries}), "
                f"{sleep_time:.1f}s 后重试 | URL: {url[:120]}"
            )
            time.sleep(sleep_time)
        else:
            # 重试耗尽
            retry_after = retry_after or 60
            endpoint = _detect_endpoint(url)
            logger.error(
                f"Strava API 429 重试耗尽 ({max_retries}/{max_retries}) "
                f"| endpoint={endpoint} | URL: {url[:120]}"
            )
            raise StravaRateLimitError(
                f"Strava API 限流，{max_retries}次重试后仍为429",
                retry_after=retry_after,
                endpoint=endpoint,
            )

    # 防御性代码，正常不会走到这里
    raise StravaRateLimitError(
        "Strava API 限流异常（未知路径）",
        endpoint=_detect_endpoint(url),
    )


def get_strava_token() -> str:
    """获取当前 Strava access token（从环境变量）。"""
    return os.environ.get("STRAVA_ACCESS_TOKEN", "")


def refresh_strava_token() -> str | None:
    """刷新 Strava OAuth token，返回新的 access_token。"""
    cid = os.environ.get("STRAVA_CLIENT_ID", "")
    secret = os.environ.get("STRAVA_CLIENT_SECRET", "")
    refresh = os.environ.get("STRAVA_REFRESH_TOKEN", "")

    if not all([cid, secret, refresh]):
        logger.warning("Strava OAuth 凭据不全，无法刷新")
        return None

    try:
        resp = strava_request("POST", STRAVA_OAUTH_URL, data={
            "client_id": cid,
            "client_secret": secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh,
        }, timeout=30)

        if resp.status_code != 200:
            logger.warning(f"Strava token 刷新失败: {resp.status_code} {resp.text[:200]}")
            return None

        data = resp.json()
        new_token = data["access_token"]
        # 更新环境变量
        os.environ["STRAVA_ACCESS_TOKEN"] = new_token
        logger.info("Strava token 已刷新")
        return new_token

    except StravaRateLimitError:
        logger.error("Strava OAuth token 刷新时遭遇限流，重试耗尽")
        return None
    except Exception as e:
        logger.warning(f"Strava token 刷新异常: {e}")
        return None


def test_token(strava_token: str) -> bool:
    """测试 token 是否有效。"""
    try:
        resp = strava_request(
            "GET",
            f"{STRAVA_API_BASE}/athlete",
            headers={"Authorization": f"Bearer {strava_token}"},
            timeout=10,
        )
        return resp.status_code == 200
    except StravaRateLimitError:
        logger.warning("Strava token 测试时遭遇限流")
        return True  # 限流不阻断，假设 token 有效
    except Exception:
        return True  # 网络问题不阻断
