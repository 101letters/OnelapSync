#!/usr/bin/env python3
"""Structured JSON event logging utilities.

提供两类工具：
1. log_event  — 旧版 JSON event 日志（向后兼容）
2. log_json   — 新版结构化 ndjson 日志（worker/level/ts 完整格式）
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# 东八区
_TZ_SHANGHAI = timezone(timedelta(hours=8))


def log_event(logger: logging.Logger, event: str, trace_id: str = "", **kwargs: Any) -> None:
    """输出 JSON 结构化日志事件（旧版，向后兼容）。

    示例输出：
    {"time": "2026-05-04T10:30:00", "event": "analyze_done", "trace_id": "OL20260504_001", "activity_id": "123", "duration_ms": 820, "status": "success"}
    """
    record = {
        "time": datetime.now().isoformat(timespec="seconds"),
        "event": event,
    }
    if trace_id:
        record["trace_id"] = trace_id
    record.update(kwargs)
    logger.info(json.dumps(record, ensure_ascii=False, default=str))


def log_json(
    logger: logging.Logger,
    worker: str,
    level: str,
    event: str,
    message: str,
    trace_id: str = "",
    **kwargs: Any,
) -> None:
    """输出新版结构化 ndjson 日志。

    每行一个 JSON 对象，格式：
    {"ts":"2026-05-04T12:00:00+08:00","level":"INFO","worker":"sync","trace_id":"OL20260504_001","event":"upload_success","activity_id":"xxx","message":"上传成功","duration_ms":1234}

    参数：
        logger:    标准 logging.Logger 实例
        worker:    worker 名 (sync/analyze/coach/writeback)
        level:     日志等级 (INFO/WARNING/ERROR)
        trace_id:  全链路追踪 ID (onelap_id)
        event:     事件名 (如 upload_success, parse_done 等)
        message:   人类可读描述
        **kwargs:  额外字段 (如 activity_id, duration_ms, error 等)
    """
    record: dict[str, Any] = {
        "ts": datetime.now(_TZ_SHANGHAI).isoformat(),
        "level": level.upper(),
        "worker": worker,
        "event": event,
        "message": message,
    }
    if trace_id:
        record["trace_id"] = trace_id
    record.update(kwargs)
    line = json.dumps(record, ensure_ascii=False, default=str)

    lvl = level.upper()
    if lvl == "ERROR":
        logger.error(line)
    elif lvl == "WARNING":
        logger.warning(line)
    else:
        logger.info(line)


def _extract_json_event(line: str) -> dict | None:
    """Extract a JSON event object from a log line.

    Supports both old (``time`` key) and new (``ts`` key) formats.
    Worker log format prepends timestamp/level before the message, so a JSON
    event line usually looks like:
    ``2026-05-04 ... - INFO - {"ts": ..., "event": ...}``
    """
    start = line.find("{")
    end = line.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        obj = json.loads(line[start:end + 1])
    except json.JSONDecodeError:
        return None
    if isinstance(obj, dict) and (obj.get("event") or obj.get("time") or obj.get("ts")):
        return obj
    return None


def _event_time(event: dict) -> datetime | None:
    """Extract datetime from either ``ts`` (new) or ``time`` (old) field."""
    for key in ("ts", "time"):
        raw = event.get(key)
        if not raw:
            continue
        try:
            return datetime.fromisoformat(str(raw))
        except ValueError:
            continue
    return None


def summarize_logs(log_path: str, hours: int = 24) -> dict:
    """从 JSON event 日志中汇总统计。

    扫描单个日志文件或目录下的 ``*.log*`` 文件，提取 JSON event 行，
    按 event/status 汇总成功率和平均耗时。
    """
    cutoff = datetime.now() - timedelta(hours=hours) if hours else None
    path = Path(log_path)
    files = sorted(path.glob("*.log*")) if path.is_dir() else [path]

    stats = {
        "total_events": 0,
        "analyze_success": 0,
        "analyze_failed": 0,
        "analyze_success_rate": 0.0,
        "coach_success": 0,
        "coach_failed": 0,
        "coach_cache_hit": 0,
        "writeback_success": 0,
        "writeback_failed": 0,
        "avg_analyze_ms": 0,
        "avg_coach_ms": 0,
    }
    analyze_ms: list[int] = []
    coach_ms: list[int] = []

    for file in files:
        if not file.exists() or not file.is_file():
            continue
        try:
            lines = file.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line in lines:
            event = _extract_json_event(line)
            if not event:
                continue
            if cutoff:
                event_time = _event_time(event)
                if event_time and event_time < cutoff:
                    continue

            stats["total_events"] += 1
            name = event.get("event")
            status = event.get("status")
            duration = event.get("duration_ms")

            if name == "analyze_done":
                if status == "success":
                    stats["analyze_success"] += 1
                elif status == "failed":
                    stats["analyze_failed"] += 1
                if isinstance(duration, (int, float)):
                    analyze_ms.append(int(duration))
            elif name == "coach_done":
                if status in ("success", "fallback"):
                    stats["coach_success"] += 1
                elif status == "failed":
                    stats["coach_failed"] += 1
                elif status == "cache_hit":
                    stats["coach_cache_hit"] += 1
                if isinstance(duration, (int, float)):
                    coach_ms.append(int(duration))
            elif name == "writeback_done":
                if status == "success":
                    stats["writeback_success"] += 1
                elif status == "failed":
                    stats["writeback_failed"] += 1

    analyze_total = stats["analyze_success"] + stats["analyze_failed"]
    if analyze_total:
        stats["analyze_success_rate"] = round(stats["analyze_success"] / analyze_total, 4)
    if analyze_ms:
        stats["avg_analyze_ms"] = int(sum(analyze_ms) / len(analyze_ms))
    if coach_ms:
        stats["avg_coach_ms"] = int(sum(coach_ms) / len(coach_ms))
    return stats
