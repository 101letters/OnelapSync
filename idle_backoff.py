"""
idle_backoff.py — Worker 空闲退避机制
======================================
当 worker 连续 N 次空转时，自动跳过一轮（30 分钟），避免夜里空转。

设计：
- 每个 worker 维护 .{worker_name}_backoff.state JSON 文件
- 文件格式：{"consecutive_idle": 0, "skip_until": ""}
- 使用独立 .lock 文件保护并发：所有读改写操作在同一把排他锁内完成
- 写入采用 tmp + os.replace 原子替换，避免文件损坏及先 truncate 后加锁的竞态窗口
- --force 模式跳过退避检查

并发安全模型：
- 锁文件：{state_file}.lock
- 只读操作（预留）：LOCK_SH on lock file
- 读改写操作：LOCK_EX on lock file → 读 state → 改 → 写 tmp → os.replace → fsync dir → 释放锁
"""

import fcntl
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# ── 配置常量 ────────────────────────────────
IDLE_THRESHOLD = 3          # 连续空转次数阈值
SKIP_DURATION_MIN = 30      # 跳过时长（分钟）


# ═══════════════════════════════════════════════════════════════
#  内部 helpers
# ═══════════════════════════════════════════════════════════════

def _has_force_flag() -> bool:
    """检查命令行是否包含 --force。"""
    return "--force" in sys.argv


def _state_file_path(worker_name: str, state_dir: Path) -> Path:
    """返回状态文件路径。"""
    return state_dir / f".backoff_{worker_name}.state"


def _lock_file_path(file_path: Path) -> Path:
    """返回对应的独立锁文件路径。"""
    return file_path.with_suffix(file_path.suffix + ".lock")


def _backup_corrupt(file_path: Path) -> Path | None:
    """
    将损坏的状态文件重命名为 .corrupt.<timestamp> 备份。
    调用方应已持有锁。
    """
    if not file_path.exists():
        return None
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    backup = file_path.with_suffix(file_path.suffix + f".corrupt.{ts}")
    try:
        os.rename(file_path, backup)
        logger.warning("⚠️  backoff 状态文件损坏，已备份至 %s", backup)
        return backup
    except OSError as e:
        logger.error("备份损坏文件失败 (%s): %s", file_path, e)
        return None


def _read_state_unlocked(file_path: Path) -> dict:
    """
    无锁读取状态文件。
    调用方必须已持有 lock file 上的锁（LOCK_SH 或 LOCK_EX）。
    文件不存在或 JSON 损坏时返回默认值；损坏文件会被备份。
    """
    if not file_path.exists():
        return {"consecutive_idle": 0, "skip_until": ""}

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, ValueError, OSError) as e:
        logger.warning("读取 backoff 状态文件失败 (%s): %s", file_path, e)
        _backup_corrupt(file_path)
        return {"consecutive_idle": 0, "skip_until": ""}

    return {
        "consecutive_idle": int(data.get("consecutive_idle", 0)),
        "skip_until": str(data.get("skip_until", "")),
    }


def _atomic_write_state(file_path: Path, state: dict):
    """
    原子写入状态文件。
    调用方必须已持有 lock file 上的 LOCK_EX 锁。

    流程：写入 .tmp → flush+fsync → os.replace(原文件) → fsync 父目录
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")

    try:
        with open(tmp_path, "w", encoding="utf-8") as tf:
            json.dump(state, tf, ensure_ascii=False)
            tf.flush()
            os.fsync(tf.fileno())

        # 原子替换
        os.replace(tmp_path, file_path)

        # fsync 目录，确保 rename 持久化到磁盘
        dir_fd = os.open(str(file_path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    except OSError as e:
        logger.error("写入 backoff 状态文件失败 (%s): %s", file_path, e)
        # 清理残留 tmp
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


# ═══════════════════════════════════════════════════════════════
#  公共 API
# ═══════════════════════════════════════════════════════════════

def should_skip(worker_name: str, state_dir: Path | None = None) -> bool:
    """
    检查是否应跳过本轮执行。

    持有排他锁完成「读 → 判断 → 必要时清除过期 skip_until → 写回」，
    避免读-改-写之间的 TOCTOU 竞态。

    如果 --force 在命令行中，始终返回 False。

    Returns:
        True  → 跳过本轮
        False → 正常执行
    """
    if _has_force_flag():
        logger.info("🔧 %s: --force 模式，跳过退避检查", worker_name)
        return False

    if state_dir is None:
        state_dir = Path(__file__).resolve().parent

    file_path = _state_file_path(worker_name, state_dir)
    lock_path = _lock_file_path(file_path)

    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        try:
            state = _read_state_unlocked(file_path)
            skip_until_str = state.get("skip_until", "")

            if not skip_until_str:
                return False

            try:
                skip_until = datetime.fromisoformat(skip_until_str)
                now = datetime.now(timezone.utc)
                if skip_until > now:
                    remaining = int((skip_until - now).total_seconds() // 60)
                    logger.info(
                        "⏸️  %s: 空闲退避中，跳过本轮（约 %d 分钟后恢复）",
                        worker_name, remaining,
                    )
                    return True
                else:
                    # 退避期已过，清除 skip_until 并写回
                    state["skip_until"] = ""
                    _atomic_write_state(file_path, state)
            except (ValueError, TypeError):
                # skip_until 格式损坏
                state["skip_until"] = ""
                _atomic_write_state(file_path, state)

        finally:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)

    return False


def update_state(worker_name: str, had_work: bool, state_dir: Path | None = None):
    """
    更新退避状态（每轮结束后调用）。

    持有排他锁完成「读 → 修改 → 原子写入」，保证并发安全。

    - had_work=True  → 重置空闲计数
    - had_work=False → 递增空闲计数；达到阈值后设置 skip_until
    """
    if state_dir is None:
        state_dir = Path(__file__).resolve().parent

    file_path = _state_file_path(worker_name, state_dir)
    lock_path = _lock_file_path(file_path)

    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        try:
            state = _read_state_unlocked(file_path)

            if had_work:
                old_idle = state.get("consecutive_idle", 0)
                state["consecutive_idle"] = 0
                state["skip_until"] = ""
                if old_idle > 0:
                    logger.info(
                        "✅ %s: 有新任务，重置空闲计数（之前连续 %d 次空转）",
                        worker_name, old_idle,
                    )
            else:
                state["consecutive_idle"] = state.get("consecutive_idle", 0) + 1
                idle_count = state["consecutive_idle"]
                logger.debug("😴 %s: 本轮空闲（%d/%d）", worker_name, idle_count, IDLE_THRESHOLD)

                if idle_count >= IDLE_THRESHOLD:
                    skip_until = datetime.now(timezone.utc) + timedelta(minutes=SKIP_DURATION_MIN)
                    state["skip_until"] = skip_until.isoformat()
                    state["consecutive_idle"] = 0
                    logger.info(
                        "💤 %s: 连续 %d 次空转，进入退避，跳过至 %s（UTC）",
                        worker_name, idle_count, skip_until.isoformat(),
                    )

            _atomic_write_state(file_path, state)
        finally:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
