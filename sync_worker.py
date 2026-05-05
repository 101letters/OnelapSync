#!/usr/bin/env python3
"""
sync_worker — OneLap 同步上传 Worker
======================================
职责：登录顽鹿 → 拉新活动 → 上传 Strava → 存 FIT → DB

调度：QwenPaw cron 每30分钟 (10:00~21:00)
依赖：db.py, fit_analysis.py, onelap_sync_v3.py（复用API调用）
"""

import base64
import hashlib
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / "logs"
ENV_FILE = BASE_DIR / ".env"

LOG_DIR.mkdir(parents=True, exist_ok=True)

# ─── Logger ────────────────────────────────────

logger = logging.getLogger("sync_worker")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = TimedRotatingFileHandler(LOG_DIR / "sync_worker.log", when="midnight", backupCount=14, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console)


# ─── Env 加载 ──────────────────────────────────

def load_dotenv(env_path: Path):
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_dotenv(ENV_FILE)

# ─── 顽鹿 API 常量 ────────────────────────────

ONELAP_SIGN_KEY = os.environ.get("ONELAP_SIGN_KEY", "fe9f8382418fcdeb136461cac6acae7b")
ONELAP_LOGIN_URL = os.environ.get("ONELAP_LOGIN_URL", "https://www.onelap.cn/api/login")
ONELAP_OTM_BASE = os.environ.get("ONELAP_OTM_BASE", "https://otm.onelap.cn")
STRAVA_API_BASE = os.environ.get("STRAVA_API_BASE", "https://www.strava.com/api/v3")
STRAVA_OAUTH_URL = os.environ.get("STRAVA_OAUTH_URL", "https://www.strava.com/oauth/token")

# ─── DB ────────────────────────────────────────

sys.path.insert(0, str(BASE_DIR))
from db import Database
from fit_analysis import FitAnalyzer
from log_utils import log_event, log_json
from strava_api import strava_request, StravaRateLimitError


# ─── 工具函数 ──────────────────────────────────

def fit_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


# ─── P2-9 并行上传：SyncResult ────────────────

@dataclass
class SyncResult:
    """线程安全的结果载体：线程内只做网络 I/O，不碰共享 DB。

    由 process_activity() 产出，主线程调用 persist_result() 统一写 DB。
    """
    onelap_id: str
    ok: bool = False
    rate_limited: bool = False
    strava_id: int | None = None
    fit_hash: str = ""
    fit_data: bytes = b""
    file_key: str = ""
    name: str = ""
    error: str = ""
    retry_after: int = 0
    pre_dedup: bool = False
    fit_hash_dup: bool = False
    fit_hash_dup_strava_id: int | None = None
    corrected: bool = False


@dataclass
class DownloadResult:
    """Phase B1 结果载体：纯下载 + SHA-256 哈希，不碰 DB，不调 Strava API。

    由 download_only() 产出，主线程在 Phase C1 做 fit_hash 去重后决定是否需要上传。
    """
    onelap_id: str
    ok: bool = False
    fit_hash: str = ""
    fit_data: bytes = b""
    file_key: str = ""
    name: str = ""
    error: str = ""


def format_activity_name(start_riding_time: str, distance_km: float, avg_heart: int) -> str:
    try:
        dt = datetime.strptime(start_riding_time[:19], "%Y-%m-%d %H:%M:%S")
        month_day = dt.strftime("%m月%d日")
        hour = dt.hour
        period = "早间骑行" if 5 <= hour < 11 else "午间骑行" if 11 <= hour < 14 else "下午骑行" if 14 <= hour < 18 else "夜间骑行"
        suffix = "（短活动）" if distance_km < 5 else "（长活动）" if distance_km >= 40 else ""
        heart_part = f"·{int(avg_heart)}bpm" if avg_heart else ""
        return f"{month_day}{period}{suffix}（{distance_km:.1f}km{heart_part}）"
    except (ValueError, TypeError, KeyError):
        return f"骑行活动（{distance_km:.1f}km）"


def send_bark(title: str, body: str):
    url = os.environ.get("BARK_URL")
    if not url:
        return
    try:
        requests.post(url, json={"title": title, "body": body}, timeout=10)
    except Exception as e:
        logger.warning(f"Bark通知失败: {e}")


# ─── 核心同步逻辑 ──────────────────────────────

class SyncWorker:
    def __init__(self, db: Database):
        self.db = db
        self.account = os.environ.get("ONELAP_USERNAME", "")
        self.password = os.environ.get("ONELAP_PASSWORD", "")
        self.bark_url = os.environ.get("BARK_URL", "")
        self._onelap_token = None
        self.strava_access_token = os.environ.get("STRAVA_ACCESS_TOKEN", "")
        self.timeout = 60
        self.max_uploads = 10
        self.min_duration_s = 180
        self.min_distance_km = 2.0
        self.force = False  # --force 模式开关

    # ── 顽鹿登录 ──────────────────────────────

    def login_onelap(self) -> str:
        import uuid
        nonce = uuid.uuid4().hex[:16]
        ts = str(int(time.time()))
        pwd_md5 = hashlib.md5(self.password.encode()).hexdigest()
        sign_str = f"account={self.account}&nonce={nonce}&password={pwd_md5}&ts={ts}&key={ONELAP_SIGN_KEY}"
        sign = hashlib.md5(sign_str.encode()).hexdigest()

        log_json(logger, "sync", "INFO", "login_start", "顽鹿登录开始")
        t0 = time.time()
        resp = requests.post(ONELAP_LOGIN_URL, json={
            "account": self.account, "password": pwd_md5,
            "nonce": nonce, "ts": ts, "sign": sign,
        }, timeout=self.timeout)
        data = resp.json()
        if data.get("code") != 200:
            elapsed_ms = int((time.time() - t0) * 1000)
            log_json(logger, "sync", "ERROR", "login_fail", f"顽鹿登录失败: {data}", duration_ms=elapsed_ms)
            raise RuntimeError(f"顽鹿登录失败: {data}")
        self._onelap_token = data["data"][0]["token"]
        nickname = data["data"][0]["userinfo"].get("nickname", "")
        elapsed_ms = int((time.time() - t0) * 1000)
        logger.info(f"顽鹿登录成功: {nickname}")
        log_json(logger, "sync", "INFO", "login_ok", f"顽鹿登录成功: {nickname}", nickname=nickname, duration_ms=elapsed_ms)
        return self._onelap_token

    # ── Strava token ──────────────────────────

    def refresh_strava_token(self):
        cid = os.environ.get("STRAVA_CLIENT_ID", "")
        secret = os.environ.get("STRAVA_CLIENT_SECRET", "")
        refresh = os.environ.get("STRAVA_REFRESH_TOKEN", "")
        if not all([cid, secret, refresh]):
            logger.warning("Strava OAuth 凭据不全，跳过 token 刷新")
            return
        resp = strava_request("POST", STRAVA_OAUTH_URL, data={
            "client_id": cid, "client_secret": secret,
            "grant_type": "refresh_token", "refresh_token": refresh,
        }, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f"Strava token 刷新失败: {resp.text[:200]}")
        data = resp.json()
        self.strava_access_token = data["access_token"]
        logger.info("Strava token 已刷新")

    # ── OTM 活动列表 ─────────────────────────

    def fetch_activities(self, days: int) -> list[dict]:
        if not self._onelap_token:
            raise RuntimeError("先调用 login_onelap()")

        s = requests.Session()
        s.headers.update({"Authorization": self._onelap_token, "Content-Type": "application/json"})
        all_acts = []
        page = 1
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        while True:
            resp = s.post(f"{ONELAP_OTM_BASE}/api/otm/ride_record/list",
                          json={"limit": 50, "page": page}, timeout=self.timeout)
            data = resp.json()
            if data.get("code") != 200:
                break
            records = data.get("data", {}).get("list", [])
            if not records:
                break
            for rec in records:
                st = rec.get("start_riding_time", "")
                if st and st[:10] < cutoff:
                    return all_acts
                all_acts.append(rec)
            page += 1
            if page > 10:
                break

        logger.info(f"顽鹿: {len(all_acts)} 个活动（{days}天内）")
        log_json(logger, "sync", "INFO", "fetch_list", f"拉取活动列表: {len(all_acts)} 个", count=len(all_acts), days=days)
        return all_acts

    def get_activity_detail(self, record_id: str) -> dict:
        s = requests.Session()
        s.headers.update({"Authorization": self._onelap_token, "Accept": "application/json"})
        resp = s.get(f"{ONELAP_OTM_BASE}/api/otm/ride_record/analysis/{record_id}", timeout=self.timeout)
        data = resp.json()
        if data.get("code") != 200:
            raise RuntimeError(f"获取详情失败: {data}")
        return data.get("data", {})

    def get_file_key(self, record_id: str) -> str:
        """获取活动的 fileKey（P1-6 去重前置）。"""
        detail = self.get_activity_detail(record_id)
        file_key = detail.get("fileKey", detail.get("filekey", ""))
        if not file_key:
            raise RuntimeError(f"无 fileKey: {detail}")
        log_json(logger, "sync", "INFO", "get_file_key", f"获取 fileKey: {file_key[:16]}...",
                 record_id=record_id, file_key_len=len(file_key))
        return file_key

    def download_fit(self, file_key: str, onelap_id: str = "") -> bytes:
        """用 fileKey 下载 FIT 文件。

        Args:
            file_key: 从 get_activity_detail 拿到的原始 fileKey
            onelap_id: 仅用于日志
        """
        file_key_b64 = base64.b64encode(file_key.encode()).decode()
        resp = requests.get(
            f"{ONELAP_OTM_BASE}/api/otm/ride_record/fit_content/{file_key_b64}",
            headers={"Authorization": self._onelap_token},
            timeout=self.timeout,
        )
        if resp.status_code != 200 or len(resp.content) < 500:
            log_json(logger, "sync", "ERROR", "download_fit_fail", f"FIT 下载失败: status={resp.status_code}, size={len(resp.content)}",
                     trace_id=onelap_id, record_id=onelap_id, status_code=resp.status_code, size_bytes=len(resp.content))
            raise RuntimeError(f"FIT 下载失败: status={resp.status_code}, size={len(resp.content)}")
        log_json(logger, "sync", "INFO", "download_fit", f"FIT 下载成功: {len(resp.content)} bytes",
                 trace_id=onelap_id, record_id=onelap_id, size_bytes=len(resp.content))
        return resp.content

    # ── Strava 上传 ──────────────────────────

    def upload_to_strava(self, fit_data: bytes, name: str, onelap_id: str = "") -> int | None:
        log_json(logger, "sync", "INFO", "upload_start", f"开始 Strava 上传: {name}",
                 trace_id=onelap_id, name=name, fit_size=len(fit_data))
        files = {"file": ("activity.fit", fit_data, "application/octet-stream")}
        data = {"data_type": "fit", "name": name, "description": "", "trainer": "true", "sport_type": "Ride"}
        resp = strava_request("POST",
            f"{STRAVA_API_BASE}/uploads",
            headers={"Authorization": f"Bearer {self.strava_access_token}"},
            files=files, data=data, timeout=120,
        )
        if resp.status_code == 409:
            body = resp.json()
            err = body.get("error", "")
            aid = body.get("activity_id")
            if aid:
                logger.info(f"  Strava 重复上传，已有 activity_id={aid}: {err}")
                return int(aid)
            logger.warning(f"  Strava 409 但无 activity_id: {err}")
            return None
        if resp.status_code != 201:
            logger.warning(f"  Strava 上传失败: {resp.status_code} {resp.text[:200]}")
            return None

        upload_id = resp.json().get("id")
        if not upload_id:
            return None

        # 轮询等待处理完成
        for interval in [2, 2, 2, 2, 2, 5, 5, 5, 5, 5, 10, 10, 10]:
            time.sleep(interval)
            pr = strava_request("GET",
                f"{STRAVA_API_BASE}/uploads/{upload_id}",
                headers={"Authorization": f"Bearer {self.strava_access_token}"},
                timeout=30,
            )
            if pr.status_code != 200:
                continue
            status_info = pr.json()
            status = status_info.get("status", "")
            error = status_info.get("error", "")
            if status == "Your activity is ready.":
                aid = status_info.get("activity_id")
                logger.info(f"  Strava 上传完成: activity_id={aid}")
                return int(aid) if aid else None
            if error:
                logger.warning(f"  Strava 上传失败: {error}")
                return None
        logger.warning(f"  Strava 上传轮询超时 (upload_id={upload_id})")
        return None

    def correct_activity(self, strava_id: int, name: str) -> bool:
        if not strava_id or not self.strava_access_token:
            return False
        try:
            resp = strava_request("PUT",
                f"{STRAVA_API_BASE}/activities/{strava_id}",
                headers={"Authorization": f"Bearer {self.strava_access_token}"},
                json={"name": name, "sport_type": "Ride"}, timeout=30,
            )
            return resp.status_code == 200
        except StravaRateLimitError:
            raise  # 向上传递，由 process_activity() 统一处理
        except Exception as e:
            logger.warning(f"纠正活动失败: {e}")
            return False

    # ── P2-9 Phase B1：download_only ───────────

    def download_only(self, file_key: str, name: str, onelap_id: str,
                      parallel: bool = False) -> DownloadResult:
        """Phase B1：纯 I/O 下载 FIT + SHA-256 哈希，不碰 DB，不调 Strava API。

        由线程池并行执行，结果在 Phase C1 主线程做 fit_hash 去重。
        """
        thread_name = threading.current_thread().name if parallel else ""
        try:
            fit_data = self.download_fit(file_key, onelap_id=onelap_id)
            fhash = fit_sha256(fit_data)
            if parallel:
                log_json(logger, "sync", "INFO", "download_ok",
                         f"[{thread_name}] 下载完成: {name}", trace_id=onelap_id,
                         fit_hash=fhash[:12], parallel=True, thread_name=thread_name)
            return DownloadResult(
                ok=True, onelap_id=onelap_id,
                fit_hash=fhash, fit_data=fit_data,
                file_key=file_key, name=name,
            )
        except Exception as e:
            if parallel:
                log_json(logger, "sync", "ERROR", "download_fail",
                         f"[{thread_name}] 下载失败: {name} — {e}", trace_id=onelap_id,
                         error=str(e), parallel=True, thread_name=thread_name)
            return DownloadResult(
                ok=False, onelap_id=onelap_id,
                file_key=file_key, name=name, error=str(e),
            )

    # ── P2-9 Phase B2：upload_only ─────────────

    def upload_only(self, fit_data: bytes, fit_hash: str, file_key: str,
                    name: str, onelap_id: str,
                    rate_limit_event: threading.Event | None = None,
                    parallel: bool = False) -> SyncResult:
        """Phase B2：上传 + 纠正（纯 I/O），配合 rate_limit_event 实现 429 收敛。

        Args:
            fit_data:  Phase B1 下载的 FIT 二进制内容
            fit_hash:  Phase B1 计算的 SHA-256
            rate_limit_event: 共享的 threading.Event，任意线程命中 429 后 set()，
                              其他线程检测到后尽早退出。
        """
        thread_name = threading.current_thread().name if parallel else ""
        log_kwargs: dict = {}
        if parallel:
            log_kwargs = {"parallel": True, "thread_name": thread_name}

        # 429 收敛：同批次已有线程命中限流，直接退出
        if rate_limit_event and rate_limit_event.is_set():
            log_json(logger, "sync", "INFO", "converged_exit",
                     f"[{thread_name}] 检测到 429 收敛信号，放弃上传: {name}",
                     trace_id=onelap_id, **log_kwargs)
            return SyncResult(
                ok=False, onelap_id=onelap_id,
                fit_hash=fit_hash, file_key=file_key, name=name,
                error="Rate limited (converged by peer thread)",
            )

        try:
            strava_id = self.upload_to_strava(fit_data, name, onelap_id=onelap_id)
            if not strava_id:
                return SyncResult(
                    ok=False, onelap_id=onelap_id,
                    fit_hash=fit_hash, file_key=file_key, name=name,
                    error="Strava上传失败",
                )

            # 纠正 sport_type
            time.sleep(3)
            corrected = self.correct_activity(strava_id, name)

            log_json(logger, "sync", "INFO", "process_ok",
                     f"[{thread_name}] 处理完成: {name}", trace_id=onelap_id,
                     strava_id=strava_id, onelap_id=onelap_id,
                     corrected=corrected, **log_kwargs)

            return SyncResult(
                ok=True, onelap_id=onelap_id,
                strava_id=strava_id, fit_hash=fit_hash,
                fit_data=fit_data,
                file_key=file_key, name=name, corrected=corrected,
            )

        except StravaRateLimitError as e:
            # 429 收敛：通知同批次其他线程
            if rate_limit_event:
                rate_limit_event.set()
            log_json(logger, "sync", "WARNING", "strava_429",
                     f"[{thread_name}] Strava 429 限流: {e}",
                     trace_id=onelap_id, endpoint=e.endpoint,
                     retry_after=e.retry_after, **log_kwargs)
            return SyncResult(
                rate_limited=True, onelap_id=onelap_id,
                retry_after=e.retry_after, error=str(e),
            )

        except Exception as e:
            log_json(logger, "sync", "ERROR", "process_fail",
                     f"[{thread_name}] 处理异常: {e}", trace_id=onelap_id,
                     error=str(e), **log_kwargs)
            return SyncResult(
                ok=False, onelap_id=onelap_id, error=str(e),
            )

    # ── P2-9 并行上传：process_activity ────────

    def process_activity(self, file_key: str, name: str, onelap_id: str,
                         parallel: bool = False) -> SyncResult:
        """线程内只做网络 I/O：下载 FIT → 上传 Strava → 纠正 sport_type。

        不碰 DB connection，所有结果通过 SyncResult 返回给主线程。
        """
        thread_name = threading.current_thread().name if parallel else ""
        log_kwargs: dict = {}
        if parallel:
            log_kwargs = {"parallel": True, "thread_name": thread_name}

        try:
            # 下载 FIT
            fit_data = self.download_fit(file_key, onelap_id=onelap_id)
            fhash = fit_sha256(fit_data)

            # 上传 Strava
            strava_id = self.upload_to_strava(fit_data, name, onelap_id=onelap_id)
            if not strava_id:
                return SyncResult(
                    ok=False, onelap_id=onelap_id,
                    fit_hash=fhash, file_key=file_key, name=name,
                    error="Strava上传失败",
                )

            # 纠正 sport_type
            time.sleep(3)
            corrected = self.correct_activity(strava_id, name)

            log_json(logger, "sync", "INFO", "process_ok",
                     f"[{thread_name}] 处理完成: {name}", trace_id=onelap_id,
                     strava_id=strava_id, onelap_id=onelap_id,
                     corrected=corrected, **log_kwargs)

            return SyncResult(
                ok=True, onelap_id=onelap_id,
                strava_id=strava_id, fit_hash=fhash,
                fit_data=fit_data,
                file_key=file_key, name=name, corrected=corrected,
            )

        except StravaRateLimitError as e:
            log_json(logger, "sync", "WARNING", "strava_429",
                     f"[{thread_name}] Strava 429 限流: {e}",
                     trace_id=onelap_id, endpoint=e.endpoint,
                     retry_after=e.retry_after, **log_kwargs)
            return SyncResult(
                rate_limited=True, onelap_id=onelap_id,
                retry_after=e.retry_after, error=str(e),
            )

        except Exception as e:
            log_json(logger, "sync", "ERROR", "process_fail",
                     f"[{thread_name}] 处理异常: {e}", trace_id=onelap_id,
                     error=str(e), **log_kwargs)
            return SyncResult(
                ok=False, onelap_id=onelap_id, error=str(e),
            )

    # ── P2-9 并行上传：persist_result ──────────

    def persist_result(self, result: SyncResult, act_info: dict) -> str:
        """主线程统一写 DB，保证线程安全。

        fit_hash 去重已前置到 Phase C1，此方法仅做结果落库。

        Args:
            result: upload_only() 返回的结果载体
            act_info: 活动原始 dict（start_riding_time, distance_km, avg_heart_bpm）

        Returns:
            状态标签: "pre_dedup" | "rate_limited" | "failed" | "success"
        """
        st = act_info.get("start_riding_time", "")
        dist = act_info.get("distance_km", 0)
        hr = act_info.get("avg_heart_bpm", 0)
        priority = act_info.get("priority", 0)
        name = result.name or format_activity_name(st, dist, hr)
        act_id = result.onelap_id

        if result.pre_dedup:
            return "pre_dedup"

        if result.rate_limited:
            # Strava 429：独立记录，不写 DB
            log_json(logger, "sync", "WARNING", "strava_429_recorded",
                     f"Strava 429 限流: {name}", trace_id=act_id,
                     retry_after=result.retry_after)
            return "rate_limited"

        if not result.ok:
            self.db.upsert_activity(
                act_id, name=name, start_time=st,
                distance_km=dist, avg_heart_rate=hr,
                status="failed", error_msg=result.error,
                file_key=result.file_key, priority=priority,
            )
            return "failed"

        # 成功：写入 DB
        self.db.upsert_activity(
            act_id, strava_id=result.strava_id,
            fit_sha256=result.fit_hash, name=name, start_time=st,
            distance_km=dist, avg_heart_rate=hr,
            status="uploaded", file_key=result.file_key,
            priority=priority,
        )

        if result.corrected:
            log_json(logger, "sync", "INFO", "correct_ok",
                     f"活动纠正成功: {name}", trace_id=act_id,
                     strava_id=result.strava_id)

        # 存 FIT 到缓存
        if result.fit_data:
            try:
                cache_path = self.db.fit_cache_path(act_id)
                cache_path.write_bytes(result.fit_data)
            except Exception as e:
                logger.warning(f"  FIT 缓存写入失败: {e}")

        return "success"

    # ── Strava 活动列表拉取 ──────────────────

    def fetch_strava_activities(self, days: int) -> list[dict]:
        """拉取最近 N 天 Strava 活动列表，用于比对去重。"""
        after_ts = int((datetime.now(timezone.UTC) - timedelta(days=days)).timestamp())
        result = []
        page = 1
        while page <= 10:
            resp = strava_request("GET",
                f"{STRAVA_API_BASE}/athlete/activities",
                headers={"Authorization": f"Bearer {self.strava_access_token}"},
                params={"after": after_ts, "per_page": 50, "page": page},
                timeout=30,
            )
            if resp.status_code != 200:
                logger.warning(f"  Strava 活动列表拉取失败: {resp.status_code}")
                break
            data = resp.json()
            if not data:
                break
            for a in data:
                result.append({
                    "id": a.get("id"),
                    "start_date": a.get("start_date", ""),
                    "distance": a.get("distance", 0),
                })
            page += 1
        logger.info(f"Strava: 最近{days}天 {len(result)} 个活动")
        return result

    def _is_on_strava(self, onelap_act: dict, strava_acts: list[dict]) -> bool:
        """判断顽鹿活动在 Strava 上是否已存在（按时间±5分钟 + 距离±20%）。"""
        start_time = onelap_act.get("start_riding_time", "")
        dist_km = onelap_act.get("distance_km", 0)
        if not start_time:
            return False
        try:
            onelap_ts = datetime.strptime(start_time[:19], "%Y-%m-%d %H:%M:%S")
            onelap_ts = onelap_ts.replace(tzinfo=timezone.utc)
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

    # ── 主流程 ───────────────────────────────

    def run(self, days: int = 1, force: bool = False, parallel: bool = False):
        """执行一次同步：登录→拉OneLap活动→DB去重(onelap_id优先)→下载FIT→hash去重→上传Strava→落库

        Args:
            days: 拉取最近 N 天的活动
            force: 强制重新同步，跳过前置去重
            parallel: 开启并行上传（max_workers=2），默认串行
        """
        run_t0 = time.time()
        uploaded = 0
        skipped = 0
        rate_limited_count = 0
        self.force = force
        log_event(logger, "sync_start", days=days, force=force, parallel=parallel)

        # 1. 登录顽鹿
        try:
            self.login_onelap()
        except Exception as e:
            logger.error(f"顽鹿登录失败: {e}")
            elapsed = int((time.time() - run_t0) * 1000)
            log_event(logger, "sync_done", count=0, uploaded=uploaded, skipped=skipped, failed=1, duration_ms=elapsed)
            return {"synced": 0, "failed": 0, "message": f"登录失败: {e}"}

        # 2. 拉活动列表
        try:
            activities = self.fetch_activities(days)
        except Exception as e:
            logger.error(f"拉活动列表失败: {e}")
            elapsed = int((time.time() - run_t0) * 1000)
            log_event(logger, "sync_done", count=0, uploaded=uploaded, skipped=skipped, failed=1, duration_ms=elapsed)
            return {"synced": 0, "failed": 0, "message": f"拉列表失败: {e}"}

        if not activities:
            logger.info("顽鹿无新增活动")
            elapsed = int((time.time() - run_t0) * 1000)
            log_event(logger, "sync_done", count=0, uploaded=uploaded, skipped=skipped, failed=0, duration_ms=elapsed)
            return {"synced": 0, "failed": 0, "message": "无新增活动"}

        # 3. 刷新 Strava token
        try:
            self.refresh_strava_token()
        except StravaRateLimitError as e:
            logger.error(f"Strava token 刷新遭遇限流: {e}")
            log_json(logger, "sync", "ERROR", "rate_limit", f"Token 刷新限流: {e}",
                     retry_after=e.retry_after)
            send_bark("Strava 限流告警", f"Token 刷新限流，建议 {e.retry_after} 秒后重试")
            elapsed = int((time.time() - run_t0) * 1000)
            log_event(logger, "sync_done", count=len(activities), uploaded=uploaded, skipped=skipped, failed=1, duration_ms=elapsed)
            return {"synced": 0, "failed": 0, "message": f"Strava 限流: {e}"}
        except Exception as e:
            logger.error(f"Strava token 刷新失败: {e}")
            elapsed = int((time.time() - run_t0) * 1000)
            log_event(logger, "sync_done", count=len(activities), uploaded=uploaded, skipped=skipped, failed=1, duration_ms=elapsed)
            return {"synced": 0, "failed": 0, "message": f"Strava token 失败: {e}"}

        # 4. 过滤新活动：DB 去重优先（本地为主，Strava 仅辅助）
        to_sync = []
        for act in activities:
            act_id = act.get("id", "")
            if not act_id:
                continue

            # ① 时长/距离过滤
            duration = act.get("time_seconds", 0)
            distance = act.get("distance_km", 0)
            if duration < self.min_duration_s or distance < self.min_distance_km:
                logger.info(f"  跳过短活动: {act.get('start_riding_time','')} {distance}km")
                # 计算优先级
                priority_val = 0
                try:
                    st_val = act.get("start_riding_time", "")
                    if st_val:
                        act_t = datetime.strptime(st_val[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        hours_ago = (datetime.now(timezone.utc) - act_t).total_seconds() / 3600
                        if hours_ago < 2:
                            priority_val = 10
                        elif hours_ago < 6:
                            priority_val = 8
                        elif hours_ago < 24:
                            priority_val = 5
                        else:
                            priority_val = 1
                except Exception:
                    pass
                self.db.upsert_activity(act_id, name=format_activity_name(
                    act.get("start_riding_time",""), distance, act.get("avg_heart_bpm",0)),
                    start_time=act.get("start_riding_time",""), distance_km=distance,
                    avg_heart_rate=act.get("avg_heart_bpm",0), status="skipped", priority=priority_val)
                skipped += 1
                continue

            # ② DB 主裁判：onelap_id 已存在且非失败态 → 跳过（零API成本）
            existing = self.db.get_activity(act_id)
            if existing and existing["status"] not in (
                "failed", "analyze_failed", "coach_failed", "write_failed",
            ):
                logger.info(f"  DB跳过 (onelap_id={act_id[:12]}, status={existing['status']})")
                skipped += 1
                continue

            to_sync.append(act)

        if not to_sync:
            logger.info("无可同步的新活动")
            elapsed = int((time.time() - run_t0) * 1000)
            log_event(logger, "sync_done", count=len(activities), uploaded=uploaded, skipped=skipped, failed=0, duration_ms=elapsed)
            return {"synced": 0, "failed": 0, "message": "无可同步的新活动"}

        if not parallel:
            # ═══════════════════════════════════════════
            # 串行路径（现有逻辑，保持不变）
            # ═══════════════════════════════════════════
            synced_names = []
            uploaded_names = []
            skipped_names = []
            failed_names = []
            for i, act in enumerate(to_sync[:self.max_uploads]):
                act_id = act.get("id", "")
                st = act.get("start_riding_time", "")
                dist = act.get("distance_km", 0)
                hr = act.get("avg_heart_bpm", 0)
                name = format_activity_name(st, dist, hr)

                # 计算优先级
                priority_val = 1
                try:
                    if st:
                        act_t = datetime.strptime(st[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        hours_ago = (datetime.now(timezone.utc) - act_t).total_seconds() / 3600
                        if hours_ago < 2:
                            priority_val = 10
                        elif hours_ago < 6:
                            priority_val = 8
                        elif hours_ago < 24:
                            priority_val = 5
                        else:
                            priority_val = 1
                except Exception:
                    pass

                logger.info(f"[{i+1}/{len(to_sync)}] {name}")
                t0 = time.time()
                file_key = ""

                try:
                    file_key = self.get_file_key(act_id)

                    if not self.force:
                        dup = self.db.check_activity_dedup(file_key)
                        if dup:
                            logger.info(f"  ⏭️ 已处理 (file_key={file_key[:16]}...), 跳过")
                            synced_names.append(f"{name} (已存在)")
                            skipped_names.append(name)
                            skipped += 1
                            elapsed = int((time.time() - t0) * 1000)
                            log_json(logger, "sync", "INFO", "upload_skip", f"前置去重跳过: {name}",
                                     trace_id=act_id, activity_id=act_id, name=name, reason="pre_dedup", duration_ms=elapsed)
                            continue

                    fit_data = self.download_fit(file_key, onelap_id=act_id)
                    fhash = fit_sha256(fit_data)

                    dup = self.db.get_activity_by_fit_hash(fhash)
                    if dup and dup["status"] != "failed":
                        logger.info(f"  FIT 已同步 (hash={fhash[:12]}), strava_id={dup['strava_id']}")
                        self.db.upsert_activity(act_id, strava_id=dup["strava_id"],
                            fit_sha256=fhash, name=name, start_time=st,
                            distance_km=dist, avg_heart_rate=hr, status="uploaded",
                            file_key=file_key, priority=priority_val)
                        synced_names.append(f"{name} (已存在)")
                        skipped_names.append(name)
                        skipped += 1
                        log_json(logger, "sync", "INFO", "upload_skip", f"FIT 已同步跳过: {name}",
                                 trace_id=act_id, activity_id=act_id, name=name, reason="fit_hash_dup")
                        continue

                    strava_id = self.upload_to_strava(fit_data, name, onelap_id=act_id)
                    if not strava_id:
                        failed_names.append(name)
                        elapsed = int((time.time() - t0) * 1000)
                        log_json(logger, "sync", "ERROR", "upload_fail", f"Strava 上传失败: {name}",
                                 trace_id=act_id, activity_id=act_id, name=name, duration_ms=elapsed)
                        self.db.upsert_activity(act_id, name=name, start_time=st,
                            distance_km=dist, avg_heart_rate=hr, status="failed", error_msg="Strava上传失败",
                            file_key=file_key, priority=priority_val)
                        continue

                    time.sleep(3)
                    corrected = self.correct_activity(strava_id, name)
                    if corrected:
                        log_json(logger, "sync", "INFO", "correct_ok", f"活动纠正成功: {name}",
                                 trace_id=act_id, strava_id=strava_id, activity_id=act_id)
                    else:
                        log_json(logger, "sync", "WARNING", "correct_fail", f"活动纠正失败: {name}",
                                 trace_id=act_id, strava_id=strava_id, activity_id=act_id)

                    cache_path = self.db.fit_cache_path(act_id)
                    cache_path.write_bytes(fit_data)

                    self.db.upsert_activity(act_id, strava_id=strava_id,
                        fit_sha256=fhash, name=name, start_time=st,
                        distance_km=dist, avg_heart_rate=hr, status="uploaded",
                        file_key=file_key, priority=priority_val)

                    if dup and dup["onelap_id"] != act_id:
                        logger.info(f"  FIT 哈希命中旧活动 {dup['onelap_id'][:12]}, 关联 strava_id")
                        self.db.upsert_activity(dup["onelap_id"], strava_id=strava_id, status="completed")

                    synced_names.append(name)
                    uploaded_names.append(name)
                    uploaded += 1
                    logger.info(f"  ✅ 完成: strava_id={strava_id}")
                    elapsed = int((time.time() - t0) * 1000)
                    log_event(logger, "upload_success", trace_id=act_id, activity_id=act_id, name=name, duration_ms=elapsed)
                    log_json(logger, "sync", "INFO", "upload_success", f"上传成功: {name}",
                             trace_id=act_id, activity_id=act_id, strava_id=strava_id, name=name, duration_ms=elapsed)

                except StravaRateLimitError as e:
                    logger.error(f"  ⚠️ Strava 限流: {e} (建议等待 {e.retry_after}s)")
                    log_json(logger, "sync", "ERROR", "rate_limit", f"Strava 限流: {e}",
                             trace_id=act_id, retry_after=e.retry_after, endpoint=e.endpoint,
                             activity_id=act_id, name=name)
                    send_bark("Strava 限流告警", f"当前限流时段 {e.retry_after} 秒后重试")
                    failed_names.append(name)
                    self.db.upsert_activity(act_id, name=name, start_time=st,
                        distance_km=dist, avg_heart_rate=hr, status="failed", error_msg=f"Strava限流: {e}",
                        file_key=file_key)
                    break

                except Exception as e:
                    logger.error(f"  ❌ {name} 失败: {e}")
                    elapsed = int((time.time() - t0) * 1000)
                    log_json(logger, "sync", "ERROR", "activity_error", f"活动处理失败: {name} — {e}",
                             trace_id=act_id, activity_id=act_id, name=name, error=str(e)[:200], duration_ms=elapsed)
                    failed_names.append(name)
                    self.db.upsert_activity(act_id, name=name, start_time=st,
                        distance_km=dist, avg_heart_rate=hr, status="failed", error_msg=str(e)[:200],
                        file_key=file_key)

                time.sleep(1)

            # 6. Bark 通知
            if uploaded_names:
                log_json(logger, "sync", "INFO", "bark_notify", "Bark 通知: upload_success",
                         reason="upload_success", uploaded=len(uploaded_names),
                         skipped=len(skipped_names), failed=len(failed_names))
                lines = [f"- {n} -> ✅ Strava" for n in uploaded_names]
                if failed_names:
                    lines.append(f"{len(failed_names)} 个失败")
                send_bark("🚴 骑行同步", "\n".join(lines))
            elif failed_names:
                log_json(logger, "sync", "INFO", "bark_notify", "Bark 通知: upload_fail",
                         reason="upload_fail", uploaded=len(uploaded_names),
                         skipped=len(skipped_names), failed=len(failed_names))
                lines = [f"- {n} -> ❌ 失败" for n in failed_names]
                send_bark("🚴 骑行同步失败", "\n".join(lines))

            elapsed = int((time.time() - run_t0) * 1000)
            log_event(logger, "sync_done", count=len(activities), uploaded=uploaded, skipped=skipped, failed=len(failed_names), duration_ms=elapsed)
            log_json(logger, "sync", "INFO", "sync_cycle_done", f"同步周期完成: {uploaded} 上传 / {skipped} 跳过 / {len(failed_names)} 失败",
                     uploaded=uploaded, skipped=skipped, failed=len(failed_names), total=len(activities), duration_ms=elapsed)
            return {"synced": len(synced_names), "failed": len(failed_names), "message": "完成"}

        else:
            # ═══════════════════════════════════════════════════
            # P2-9 并行路径：ThreadPoolExecutor(max_workers=2)
            # Phase A:  主线程串行 — get_fileKey + 前置去重
            # Phase B1: 线程池并行 — download_fit + sha256（纯 I/O，不碰 DB）
            # Phase C1: 主线程串行 — fit_hash 去重，分"待上传"/"已存在"
            # Phase B2: 线程池并行 — upload + correct（仅待上传，含 429 收敛）
            # Phase C2: 主线程串行 — persist_result 统一写 DB
            # ═══════════════════════════════════════════════════
            MAX_PARALLEL_WORKERS = 2

            synced_names = []
            uploaded_names = []
            skipped_names = []
            failed_names = []

            # ── Phase A: 主线程预处理（get_fileKey + 前置去重）──
            tasks: list[dict] = []
            for i, act in enumerate(to_sync[:self.max_uploads]):
                act_id = act.get("id", "")
                st = act.get("start_riding_time", "")
                dist = act.get("distance_km", 0)
                hr = act.get("avg_heart_bpm", 0)
                name = format_activity_name(st, dist, hr)

                priority_val = 1
                try:
                    if st:
                        act_t = datetime.strptime(st[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        hours_ago = (datetime.now(timezone.utc) - act_t).total_seconds() / 3600
                        if hours_ago < 2:
                            priority_val = 10
                        elif hours_ago < 6:
                            priority_val = 8
                        elif hours_ago < 24:
                            priority_val = 5
                        else:
                            priority_val = 1
                except Exception:
                    pass

                logger.info(f"[PhaseA {i+1}/{len(to_sync)}] {name}")
                try:
                    file_key = self.get_file_key(act_id)
                except Exception as e:
                    logger.error(f"  ❌ get_fileKey 失败: {name} — {e}")
                    failed_names.append(name)
                    self.db.upsert_activity(act_id, name=name, start_time=st,
                        distance_km=dist, avg_heart_rate=hr, status="failed",
                        error_msg=f"get_fileKey失败: {str(e)[:200]}",
                        file_key="", priority=priority_val)
                    continue

                if not self.force:
                    dup = self.db.check_activity_dedup(file_key)
                    if dup:
                        logger.info(f"  ⏭️ 已处理 (file_key={file_key[:16]}...), 跳过")
                        synced_names.append(f"{name} (已存在)")
                        skipped_names.append(name)
                        skipped += 1
                        log_json(logger, "sync", "INFO", "upload_skip", f"前置去重跳过: {name}",
                                 trace_id=act_id, activity_id=act_id, name=name, reason="pre_dedup",
                                 parallel=True)
                        continue

                tasks.append({
                    "act": act,
                    "file_key": file_key,
                    "name": name,
                    "onelap_id": act_id,
                    "priority": priority_val,
                })

            if not tasks:
                logger.info("并行模式：所有活动已被前置去重")
                elapsed = int((time.time() - run_t0) * 1000)
                log_json(logger, "sync", "INFO", "sync_summary",
                         f"同步汇总(并行): 0提交/0成功/0失败/0限流/{elapsed}ms",
                         submitted=0, success=0, failed=0, rate_limited=0,
                         duration_ms=elapsed, parallel=True)
                log_event(logger, "sync_done", count=len(activities), uploaded=uploaded,
                          skipped=skipped, failed=len(failed_names), duration_ms=elapsed)
                return {"synced": len(synced_names), "failed": len(failed_names), "message": "完成"}

            # ── Phase B1: 线程池并行下载 + 哈希 ──
            logger.info(f"Phase B1 并行下载: {len(tasks)} 个任务, max_workers={MAX_PARALLEL_WORKERS}")
            phase_b1_t0 = time.time()

            download_results: dict[str, DownloadResult] = {}
            with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self.download_only,
                        task["file_key"], task["name"], task["onelap_id"],
                        parallel=True,
                    ): task
                    for task in tasks
                }
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        result = future.result()
                    except Exception as e:
                        result = DownloadResult(
                            ok=False, onelap_id=task["onelap_id"],
                            file_key=task["file_key"], name=task["name"],
                            error=str(e),
                        )
                    download_results[task["onelap_id"]] = result

            phase_b1_ms = int((time.time() - phase_b1_t0) * 1000)
            logger.info(f"Phase B1 完成: {phase_b1_ms}ms")

            # ── Phase C1: 主线程串行 — fit_hash 去重 ──
            phase_c1_t0 = time.time()
            to_upload: list[tuple[DownloadResult, dict]] = []
            seen_hashes: set[str] = set()

            for task in tasks:
                onelap_id = task["onelap_id"]
                result = download_results[onelap_id]
                act = task["act"]
                st = act.get("start_riding_time", "")
                dist = act.get("distance_km", 0)
                hr = act.get("avg_heart_bpm", 0)
                name = result.name or format_activity_name(st, dist, hr)
                priority = task["priority"]

                if not result.ok:
                    # 下载失败 → 直接落库 failed
                    self.db.upsert_activity(onelap_id, name=name, start_time=st,
                        distance_km=dist, avg_heart_rate=hr, status="failed",
                        error_msg=result.error, file_key=result.file_key, priority=priority)
                    failed_names.append(name)
                    continue

                # FIT 哈希去重（跨批次 DB + 本批次 seen_hashes）
                dup = self.db.get_activity_by_fit_hash(result.fit_hash)
                if dup and dup["status"] != "failed":
                    log_json(logger, "sync", "INFO", "upload_skip",
                             f"FIT 已同步跳过: {name}", trace_id=onelap_id,
                             reason="fit_hash_dup", duplicated_strava_id=dup.get("strava_id"))
                    self.db.upsert_activity(onelap_id, strava_id=dup["strava_id"],
                        fit_sha256=result.fit_hash, name=name, start_time=st,
                        distance_km=dist, avg_heart_rate=hr, status="uploaded",
                        file_key=result.file_key, priority=priority)
                    synced_names.append(f"{name} (已存在)")
                    skipped_names.append(name)
                    skipped += 1
                    continue

                if result.fit_hash in seen_hashes:
                    log_json(logger, "sync", "INFO", "upload_skip",
                             f"FIT 批次内重复跳过: {name}", trace_id=onelap_id,
                             reason="batch_fit_hash_dup")
                    self.db.upsert_activity(onelap_id, name=name, start_time=st,
                        distance_km=dist, avg_heart_rate=hr, status="uploaded",
                        file_key=result.file_key, priority=priority)
                    synced_names.append(f"{name} (批次内重复)")
                    skipped_names.append(name)
                    skipped += 1
                    continue

                seen_hashes.add(result.fit_hash)
                to_upload.append((result, task))

            phase_c1_ms = int((time.time() - phase_c1_t0) * 1000)
            deduped = len(tasks) - len(to_upload)
            logger.info(f"Phase C1 完成: {phase_c1_ms}ms, 待上传={len(to_upload)}, 已去重={deduped}")

            # ── Phase B2: 线程池并行上传 + 纠正（仅待上传，含 429 收敛）──
            submitted = len(tasks)
            success_count = 0
            fail_count = 0

            if to_upload:
                rate_limit_event = threading.Event()
                logger.info(f"Phase B2 并行上传: {len(to_upload)} 个任务, max_workers={MAX_PARALLEL_WORKERS}")
                phase_b2_t0 = time.time()

                upload_result_map: dict[str, SyncResult] = {}
                with ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
                    futures = {
                        executor.submit(
                            self.upload_only,
                            res.fit_data, res.fit_hash, res.file_key,
                            res.name, res.onelap_id,
                            rate_limit_event=rate_limit_event, parallel=True,
                        ): (res, task)
                        for res, task in to_upload
                    }
                    for future in as_completed(futures):
                        res, task = futures[future]
                        onelap_id = task["onelap_id"]
                        try:
                            sync_result = future.result()
                        except Exception as e:
                            sync_result = SyncResult(
                                ok=False, onelap_id=onelap_id,
                                error=f"线程异常: {e}",
                            )
                        upload_result_map[onelap_id] = sync_result

                phase_b2_ms = int((time.time() - phase_b2_t0) * 1000)
                logger.info(f"Phase B2 完成: {phase_b2_ms}ms")

                # ── Phase C2: 主线程串行 — persist_result 统一写 DB ──
                phase_c2_t0 = time.time()
                for res, task in to_upload:
                    onelap_id = task["onelap_id"]
                    sync_result = upload_result_map.get(onelap_id)
                    if sync_result is None:
                        continue

                    name = task["name"]
                    act_info = {
                        "start_riding_time": task["act"].get("start_riding_time", ""),
                        "distance_km": task["act"].get("distance_km", 0),
                        "avg_heart_bpm": task["act"].get("avg_heart_bpm", 0),
                        "priority": task["priority"],
                    }
                    status = self.persist_result(sync_result, act_info)

                    if status == "success":
                        synced_names.append(name)
                        uploaded_names.append(name)
                        uploaded += 1
                        success_count += 1
                        logger.info(f"  ✅ 完成: {name} strava_id={sync_result.strava_id}")
                        log_event(logger, "upload_success", trace_id=onelap_id,
                                  activity_id=onelap_id, name=name, parallel=True)
                    elif status == "rate_limited":
                        rate_limited_count += 1
                        failed_names.append(name)
                        logger.warning(f"  ⚠️ Strava 429: {name}")
                    else:  # "failed"
                        fail_count += 1
                        failed_names.append(name)
                        logger.error(f"  ❌ 失败: {name} — {sync_result.error}")
                        # FIT 缓存（如果有数据）
                        if sync_result.fit_data:
                            try:
                                cache_path = self.db.fit_cache_path(onelap_id)
                                cache_path.write_bytes(sync_result.fit_data)
                            except Exception:
                                pass

                phase_c2_ms = int((time.time() - phase_c2_t0) * 1000)
                phase_b_elapsed = int((time.time() - phase_b1_t0) * 1000)
            else:
                phase_b_elapsed = phase_b1_ms + phase_c1_ms

            # ── 汇总日志 ──
            total_elapsed = int((time.time() - run_t0) * 1000)
            log_json(logger, "sync", "INFO", "sync_summary",
                     f"同步汇总(并行): {submitted}提交/{success_count}成功/{fail_count}失败/{rate_limited_count}限流/{total_elapsed}ms",
                     submitted=submitted, success=success_count, failed=fail_count,
                     rate_limited=rate_limited_count, duration_ms=total_elapsed,
                     parallel_b_elapsed_ms=phase_b_elapsed, parallel=True)

            # ── Bark 通知 ──
            if uploaded_names:
                log_json(logger, "sync", "INFO", "bark_notify", "Bark 通知: upload_success",
                         reason="upload_success", uploaded=len(uploaded_names),
                         skipped=len(skipped_names), failed=len(failed_names))
                lines = [f"- {n} -> ✅ Strava" for n in uploaded_names[:5]]
                if len(uploaded_names) > 5:
                    lines.append(f"... 及其他 {len(uploaded_names) - 5} 个")
                if failed_names:
                    lines.append(f"{len(failed_names)} 个失败")
                if rate_limited_count:
                    lines.append(f"{rate_limited_count} 个 Strava 429 限流")
                send_bark("🚴 骑行同步", "\n".join(lines))
            elif failed_names:
                log_json(logger, "sync", "INFO", "bark_notify", "Bark 通知: upload_fail",
                         reason="upload_fail", uploaded=len(uploaded_names),
                         skipped=len(skipped_names), failed=len(failed_names))
                lines = [f"- {n} -> ❌ 失败" for n in failed_names[:5]]
                send_bark("🚴 骑行同步失败", "\n".join(lines))

            log_event(logger, "sync_done", count=len(activities), uploaded=uploaded,
                      skipped=skipped, failed=len(failed_names), duration_ms=total_elapsed)
            log_json(logger, "sync", "INFO", "sync_cycle_done",
                     f"同步周期完成(并行): {uploaded} 上传 / {skipped} 跳过 / {len(failed_names)} 失败 / {rate_limited_count} 限流",
                     uploaded=uploaded, skipped=skipped, failed=len(failed_names),
                     rate_limited=rate_limited_count, total=len(activities),
                     duration_ms=total_elapsed, parallel=True)
            return {"synced": len(synced_names), "failed": len(failed_names), "message": "完成"}


# ─── 入口 ──────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="OneLap → Strava 同步 Worker")
    parser.add_argument("--force", action="store_true", help="强制重新同步，跳过前置去重")
    parser.add_argument("--days", type=int, default=1, help="拉取最近 N 天的活动 (默认 1)")
    parser.add_argument("--parallel", action="store_true", help="开启并行上传 (max_workers=2)")
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    try:
        db = Database()
        worker = SyncWorker(db)
        result = worker.run(days=args.days, force=args.force, parallel=args.parallel)
        logger.info(f"sync_worker 完成: {result}")
    except Exception as e:
        logger.error(f"sync_worker 异常: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
