#!/usr/bin/env python3
"""onelap_cli.py — OneLap pipeline status CLI (纯只读)"""

import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onelap_sync.db")

# ---- 展示用常量（非业务逻辑）----

PIPE_STATUSES = [
    ("uploaded",       "⏳ 待分析"),
    ("analyzing",      "🔄 分析中"),
    ("analyzed",       "✅ 待教练分析"),
    ("coaching",       "🔄 教练分析中"),
    ("coached",        "✅ 待写回"),
    ("writing",        "🔄 写回中"),
    ("completed",      "✅ 已完成"),
]

FAILED_STATUSES = [
    ("analyze_failed", "❌ 分析失败"),
    ("coach_failed",   "❌ 教练失败"),
    ("write_failed",   "❌ 写回失败"),
    ("failed",         "❌ 失败"),
]

OTHER_STATUSES = [
    ("dedup_by_fit_sha256", "🔁 已去重"),
    ("skipped",             "⏭️ 已跳过"),
]

# 可重试的失败态（含 next_retry_at 判断）
RETRYABLE_STATUSES = {"analyze_failed", "coach_failed", "write_failed"}

FINAL_STATUSES = {
    "completed", "failed", "analyze_failed", "coach_failed",
    "write_failed", "dedup_by_fit_sha256", "skipped",
}

MAX_RETRY = 3  # 可重试阈值（retry_count < MAX_RETRY 即为可重试）

# ---- 数据查询 ----

def _counts(conn):
    rows = conn.execute(
        "SELECT status, COUNT(*) FROM activities GROUP BY status"
    ).fetchall()
    return dict(rows)

def _retryable(conn):
    placeholders = ",".join("?" for _ in RETRYABLE_STATUSES)
    row = conn.execute(
        f"""SELECT COUNT(*) FROM activities
           WHERE status IN ({placeholders})
             AND retry_count < ?
             AND (next_retry_at IS NULL OR datetime(next_retry_at) <= datetime('now'))""",
        (*RETRYABLE_STATUSES, MAX_RETRY),
    ).fetchone()
    return row[0]


def _retryable_by_status(conn):
    """按失败态分别统计可重试数。"""
    row = conn.execute(
        """SELECT status, COUNT(*) AS cnt FROM activities
           WHERE status IN ('analyze_failed','coach_failed','write_failed')
             AND retry_count < ?
             AND (next_retry_at IS NULL OR datetime(next_retry_at) <= datetime('now'))
           GROUP BY status""",
        (MAX_RETRY,),
    ).fetchall()
    return {r["status"]: r["cnt"] for r in row}

def _stuck(conn):
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    placeholders = ",".join("?" for _ in FINAL_STATUSES)
    rows = conn.execute(
        f"""SELECT onelap_id, name, status, locked_by, locked_at
           FROM activities
           WHERE locked_by IS NOT NULL
             AND locked_at IS NOT NULL
             AND locked_at < ?
             AND status NOT IN ({placeholders})
           ORDER BY locked_at""",
        [cutoff, *FINAL_STATUSES],
    ).fetchall()
    return rows

# ---- 命令处理 ----

def cmd_status(args):
    if not os.path.exists(DB_PATH):
        print(f"❌ 数据库不存在: {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        counts = _counts(conn)

        if args.json:
            _print_json(args, conn, counts)
        else:
            _print_human(args, conn, counts)
    finally:
        conn.close()

def _print_json(args, conn, counts):
    """JSON 输出，给脚本消费"""
    total = sum(counts.values())
    retryable_by_status = _retryable_by_status(conn)
    total_retryable = sum(retryable_by_status.values())

    output = {
        "timestamp": datetime.now().isoformat(),
        "pipe": {s: counts.get(s, 0) for s, _ in PIPE_STATUSES},
        "failed": {
            s: {
                "count": counts.get(s, 0),
                "retryable": retryable_by_status.get(s, 0),
            }
            for s, _ in FAILED_STATUSES
        },
        "terminal": {s: counts.get(s, 0) for s, _ in OTHER_STATUSES},
        "total": total,
        "retryable": total_retryable,
    }
    if args.verbose:
        output["stuck"] = [dict(r) for r in _stuck(conn)]

    json.dump(output, sys.stdout, ensure_ascii=False, indent=2)
    print()

def _print_human(args, conn, counts):
    """人类可读输出"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n📡 管道状态 @ {now}")
    print("─" * 40)

    for status, label in PIPE_STATUSES:
        n = counts.get(status, 0)
        print(f"  {status:<18} {n:>3}  {label}")

    print("  " + "─" * 33)

    retryable_by_status = _retryable_by_status(conn)
    for status, label in FAILED_STATUSES:
        n = counts.get(status, 0)
        extra = ""
        r = retryable_by_status.get(status, 0)
        if n > 0 and r > 0:
            extra = f"  (可重试: {r})"
        print(f"  {status:<18} {n:>3}  {label}{extra}")

    for status, label in OTHER_STATUSES:
        n = counts.get(status, 0)
        if n > 0:
            print(f"  {status:<18} {n:>3}  {label}")

    total = sum(counts.values())
    print("  " + "─" * 33)
    print(f"  总计: {total}")
    print()

    if args.verbose:
        stuck = _stuck(conn)
        if stuck:
            print("🔒 卡住任务 (locked > 5min):")
            for r in stuck:
                print(f"  [{r['status']}] {r['name'] or r['onelap_id']}")
                print(f"    locked_by={r['locked_by']}  since {r['locked_at']}")
            print()
        else:
            print("🔒 无卡住任务\n")

# ---- 链路追踪 ----

# 日志文件映射
_TRACE_LOGS = {
    "sync":      "sync_worker.log",
    "analyze":   "analyze_worker.log",
    "coach":     "coach_worker.log",
    "writeback": "writeback_worker.log",
}

# worker → 图标
_WORKER_ICON = {
    "sync":      "⬆️",
    "analyze":   "🔬",
    "coach":     "🤖",
    "writeback": "✍️",
}

# 事件 → 简短描述
_EVENT_LABEL = {
    "sync_start":       "开始同步",
    "sync_done":        "同步完成",
    "upload_done":      "上传成功",
    "coach_start":      "开始教练分析",
    "coach_done":       "AI分析完成",
    "writeback_start":  "开始写回",
    "writeback_done":   "写回完成",
}


def _parse_log_line(line: str, worker: str, log_path: str):
    """解析一行日志，返回 dict 或 None。"""
    line = line.rstrip("\n")
    # 期望格式: YYYY-MM-DD HH:MM:SS,mmm - LEVEL - message
    parts = line.split(" - ", 2)
    if len(parts) < 3:
        return None

    ts_raw, level, message = parts
    # 解析时间: "2026-05-04 10:38:49,147"
    ts_clean = ts_raw.split(",")[0]  # 去掉毫秒
    try:
        ts = datetime.strptime(ts_clean, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

    # 尝试从 JSON message 提取结构化信息
    event = ""
    detail = message.strip()
    try:
        obj = json.loads(message)
        if isinstance(obj, dict):
            event = obj.get("event", "")
            detail = _format_json_event(obj, worker, level)
    except (json.JSONDecodeError, TypeError):
        pass

    if not event:
        event = _infer_event(message, worker)

    return {
        "ts": ts,
        "time": ts.strftime("%H:%M:%S"),
        "worker": worker,
        "icon": _WORKER_ICON.get(worker, "❓"),
        "event": event,
        "message": detail,
        "level": level,
        "raw": line,
    }


def _format_json_event(obj: dict, worker: str, level: str) -> str:
    """将 JSON 日志对象格式化为可读描述。"""
    event = obj.get("event", "")
    label = _EVENT_LABEL.get(event, event)

    parts = [label]
    status = obj.get("status", "")
    if status:
        status_icon = {"success": "✅", "failed": "❌", "fallback": "⚠️",
                       "cache_hit": "💾", "completed": "✅"}.get(status, status)
        parts.append(status_icon)

    if "duration_ms" in obj:
        ms = obj["duration_ms"]
        parts.append(f"({ms}ms)")

    if "count" in obj:
        parts.append(f"共{obj['count']}条")

    if "uploaded" in obj:
        parts.append(f"上传{obj['uploaded']},跳过{obj.get('skipped',0)},失败{obj.get('failed',0)}")

    if "strava_id" in obj:
        parts.append(f"Strava #{obj['strava_id']}")

    if "message" in obj:
        parts.append(obj["message"])

    return "  ".join(parts)


def _infer_event(message: str, worker: str) -> str:
    """从纯文本日志推断事件类型。"""
    msg_lower = message.lower()
    if worker == "sync":
        if "上传" in message or "upload" in msg_lower:
            return "upload"
        if "跳过" in message or "skip" in msg_lower:
            return "skip"
        if "完成" in message or "done" in msg_lower:
            return "sync_done"
    elif worker == "coach":
        if "教练" in message or "coach" in msg_lower:
            return "coach"
    elif worker == "writeback":
        if "写回" in message or "write" in msg_lower:
            return "writeback"
    elif worker == "analyze":
        if "分析" in message or "analyze" in msg_lower:
            return "analyze"
    return ""


def cmd_log(args):
    """onelap_cli.py log --trace <onelap_id>"""
    # --trace 校验
    trace = args.trace.strip()
    if not trace or len(trace) < 6:
        print("❌ --trace 至少 6 位，避免误匹配", file=sys.stderr)
        sys.exit(1)
    if not re.fullmatch(r"[0-9a-fA-F]{6,64}", trace):
        print("❌ --trace 须为 6-64 位十六进制字符", file=sys.stderr)
        sys.exit(1)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(base_dir, "logs")

    if not os.path.isdir(log_dir):
        print(f"❌ 日志目录不存在: {log_dir}", file=sys.stderr)
        sys.exit(1)

    entries = []  # type: list[dict]

    for worker_name, log_file in _TRACE_LOGS.items():
        log_path = os.path.join(log_dir, log_file)
        if not os.path.exists(log_path):
            continue
        try:
            with open(log_path, errors="replace") as f:
                for line in f:
                    if trace in line:
                        parsed = _parse_log_line(line, worker_name, log_path)
                        if parsed:
                            entries.append(parsed)
        except OSError as e:
            print(f"⚠️  读取 {log_file} 失败: {e}", file=sys.stderr)

    if not entries:
        print(f"🔍 未找到 {trace} 的日志记录")
        return

    # 按时间排序
    entries.sort(key=lambda e: e["ts"])

    # --tail
    if args.tail and args.tail > 0:
        entries = entries[-args.tail:]

    if args.json:
        output = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "onelap_id": trace,
            "count": len(entries),
            "entries": [{k: v for k, v in e.items() if k not in ("ts", "raw")}
                        for e in entries],
        }
        json.dump(output, sys.stdout, ensure_ascii=False, indent=2)
        print()
    else:
        _print_trace_human(trace, entries)


def _print_trace_human(trace_id: str, entries: list):
    """人类可读的链路追踪输出。"""
    print(f"\n📡 活动轨迹: {trace_id}")
    print("─" * 52)
    for e in entries:
        msg = e["message"][:60] + ("…" if len(e["message"]) > 60 else "")
        print(f"  {e['time']}  {e['worker']:<9} {e['icon']}  {msg}")
    print("─" * 52)
    print(f"  共 {len(entries)} 条记录\n")


# ---- 会话清理 ----

SESSION_DIR = "/root/.qwenpaw/workspaces/default/sessions"


def _parse_older_than(s: str) -> int:
    """解析 --older-than <Nd>，返回天数。格式错误返回 -1。"""
    if not s.endswith("d"):
        return -1
    try:
        days = int(s[:-1])
    except ValueError:
        return -1
    return days if days > 0 else -1


def _format_size(size: int) -> str:
    """人类可读的文件大小。"""
    if size < 1024:
        return f"{size}B"
    elif size < 1024 * 1024:
        return f"{size / 1024:.1f}KB"
    elif size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.1f}MB"
    else:
        return f"{size / (1024 * 1024 * 1024):.1f}GB"


def cmd_cleanup(args):
    """会话历史自动清理。"""
    # 未指定子选项时提示可用选项
    if not args.sessions:
        print("请指定清理类型，目前支持：")
        print()
        print("  --sessions    清理会话历史文件")
        print()
        print("示例：")
        print("  onelap_cli.py cleanup --sessions --older-than 30d")
        print("  onelap_cli.py cleanup --sessions --older-than 30d --dry-run")
        return

    # 解析 --older-than
    days = _parse_older_than(args.older_than)
    if days < 1:
        print("❌ --older-than 格式错误，应为 <Nd> （例如 30d）", file=sys.stderr)
        sys.exit(1)

    if not os.path.isdir(SESSION_DIR):
        print(f"❌ 会话目录不存在: {SESSION_DIR}", file=sys.stderr)
        sys.exit(1)

    cutoff = datetime.now() - timedelta(days=days)
    now = datetime.now()

    # 收集文件信息
    all_files = []          # type: list[dict]
    cleanup_names = set()   # 需要清理的文件名

    for entry in sorted(os.listdir(SESSION_DIR)):
        full = os.path.join(SESSION_DIR, entry)

        # 跳过目录
        if not os.path.isfile(full):
            continue
        # 跳过非 .json 文件（含 .keep）
        if not entry.endswith(".json"):
            continue

        mtime = datetime.fromtimestamp(os.path.getmtime(full))
        size = os.path.getsize(full)
        age_days = (now - mtime).days

        info = {
            "name": entry,
            "path": full,
            "mtime": mtime,
            "size": size,
            "age_days": age_days,
        }
        all_files.append(info)

        if mtime < cutoff:
            cleanup_names.add(entry)

    # 按修改时间降序（最新的在前）
    all_files.sort(key=lambda f: f["mtime"], reverse=True)

    # 计算清理大小
    cleanup_files = [f for f in all_files if f["name"] in cleanup_names]
    total_cleanup_size = sum(f["size"] for f in cleanup_files)

    label = "预览" if args.dry_run else "清理"
    print(f"\n📋 会话清理{label} (--older-than {args.older_than})")
    print("─" * 42)

    for f in all_files:
        date_str = f["mtime"].strftime("%Y-%m-%d")
        is_cleanup = f["name"] in cleanup_names

        icon = "🗑️" if is_cleanup else "⏭️"
        tag = "  ← 将被删除" if is_cleanup else ""
        print(f"  {icon} {date_str} sessions/{f['name']}  ({f['age_days']}天前){tag}")

    print("─" * 42)
    print(f"  共 {len(all_files)} 个文件，其中 {len(cleanup_files)} 个符合清理条件")

    if args.dry_run:
        if cleanup_files:
            print(f"  (dry-run) 若执行将释放约 {_format_size(total_cleanup_size)}")
        else:
            print("  (dry-run) 无文件需要清理")
    else:
        if cleanup_files:
            for f in cleanup_files:
                os.remove(f["path"])
            print(f"  ✅ 已删除 {len(cleanup_files)} 个文件，释放 {_format_size(total_cleanup_size)}")
        else:
            print("  ✅ 无需清理")
    print()


# ---- 入口 ----

def main():
    parser = argparse.ArgumentParser(description="OneLap Pipeline CLI")
    sub = parser.add_subparsers(dest="command")

    sp = sub.add_parser("status", help="查看管道状态")
    sp.add_argument("--json", action="store_true", help="JSON 输出（给脚本用）")
    sp.add_argument("--verbose", "-v", action="store_true", help="显示 stuck 任务")
    sp.set_defaults(func=cmd_status)

    sp_log = sub.add_parser("log", help="轻量链路追踪")
    sp_log.add_argument("--trace", required=True, metavar="ONELAP_ID",
                        help="onelap_id 搜索关键词")
    sp_log.add_argument("--tail", type=int, default=0, metavar="N",
                        help="只显示最后 N 条，0 表示全部")
    sp_log.add_argument("--json", action="store_true", help="JSON 结构化输出")
    sp_log.set_defaults(func=cmd_log)

    sp_clean = sub.add_parser("cleanup", help="会话历史自动清理")
    sp_clean.add_argument("--sessions", action="store_true",
                          help="清理会话历史文件")
    sp_clean.add_argument("--older-than", default="30d", metavar="Nd",
                          help="清理 N 天前的文件（默认 30d）")
    sp_clean.add_argument("--dry-run", action="store_true",
                          help="预览模式，不实际删除")
    sp_clean.set_defaults(func=cmd_cleanup)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    args.func(args)

if __name__ == "__main__":
    main()
