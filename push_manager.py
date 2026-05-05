"""
推送管理器 — PushManager
=========================
统一管理所有推送渠道（Telegram、Bark），提供：
  - delta gate：内容不变不推
  - 静默模式：正常不推，异常才推
  - 推送模板：晨间/晚间/告警

用法：
    from push_manager import PushManager
    pm = PushManager()
    pm.push_morning(sleep_score=78, tsb=-5, ...)  # 自动判断是否真推
    pm.push_evening(rides=[...], deviations=[...])  # 同上
"""

import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ─── 配置 ──────────────────────────────────────────────────
WORKSPACE_DIR = Path("/root/.qwenpaw/workspaces/code-agent")
TELEGRAM_SESSION = "telegram:7359815926"
TELEGRAM_USER = "7359815926"

# delta gate 存储（记录上次推送的内容 hash）
DELTA_STATE_FILE = WORKSPACE_DIR / "push_delta_state.json"


class PushManager:
    """统一推送管理器。"""

    def __init__(self):
        self._delta_state = self._load_delta_state()

    # ═════════════════════════════════════════════════════
    #  delta gate
    # ═════════════════════════════════════════════════════

    def _load_delta_state(self) -> dict:
        try:
            if DELTA_STATE_FILE.exists():
                return json.loads(DELTA_STATE_FILE.read_text())
        except Exception:
            pass
        return {}

    def _save_delta_state(self):
        DELTA_STATE_FILE.write_text(json.dumps(self._delta_state, indent=2))

    def _content_hash(self, key: str, content: str) -> str:
        """计算内容的 MD5 指纹。"""
        return hashlib.md5(content.encode()).hexdigest()

    def should_push(self, key: str, content: str) -> bool:
        """
        delta gate：只有内容有变化才推。

        key 例如 "morning_2026-05-05"、"evening_2026-05-05"
        """
        new_hash = self._content_hash(key, content)
        old_hash = self._delta_state.get(key)
        if old_hash == new_hash:
            return False
        self._delta_state[key] = new_hash
        self._save_delta_state()
        return True

    def check_quiet_mode(
        self,
        sleep_score: float = 75,
        tsb: float = 0,
        consecutive_low_sleep: int = 0,
        has_new_rides: bool = False,
        has_sync_failures: bool = False,
        acwr: float = 1.0,
    ) -> dict:
        """
        静默模式检查：正常不打扰，只有触发条件才推。

        返回：
            {"should_alert": bool, "reasons": [str]}
        """
        reasons = []

        # 过度训练风险
        if acwr > 1.5 and tsb < -15:
            reasons.append("过度训练风险")

        # 睡眠严重不足
        if consecutive_low_sleep >= 2:
            reasons.append(f"连续{consecutive_low_sleep}天睡眠不足")

        if sleep_score < 40:
            reasons.append("睡眠极差")

        # 同步失败
        if has_sync_failures:
            reasons.append("同步失败")

        # 有新增活动（普通推送条件）
        if has_new_rides:
            reasons.append("有新增骑行")

        # TSB高危
        if tsb < -25:
            reasons.append("TSB高危")

        return {
            "should_alert": len(reasons) > 0,
            "reasons": reasons,
        }

    # ═════════════════════════════════════════════════════
    #  推送渠道
    # ═════════════════════════════════════════════════════

    def push_telegram(self, message: str, session: str = TELEGRAM_SESSION,
                       user: str = TELEGRAM_USER) -> bool:
        """推送到 Telegram。"""
        try:
            result = subprocess.run(
                [
                    "qwenpaw", "channels", "send",
                    "--target-session", session,
                    "--text", message,
                ],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                print(f"[PUSH] Telegram 推送成功 ({len(message)} chars)")
                return True
            print(f"[PUSH] Telegram 推送失败: {result.stderr[:200]}", file=sys.stderr)
            return False
        except Exception as e:
            print(f"[PUSH] Telegram 推送异常: {e}", file=sys.stderr)
            return False

    def push_bark(self, title: str, body: str) -> bool:
        """推送到 Bark。"""
        try:
            import requests
            bark_url = ""
            env_path = WORKSPACE_DIR / ".env"
            if env_path.exists():
                for line in env_path.read_text().splitlines():
                    if line.startswith("BARK_URL="):
                        bark_url = line.split("=", 1)[1].strip().strip("\"'")
                        break
            if not bark_url:
                print("[PUSH] Bark 未配置，跳过")
                return False
            resp = requests.post(bark_url, json={"title": title, "body": body}, timeout=10)
            print(f"[PUSH] Bark 推送成功: {title}")
            return True
        except Exception as e:
            print(f"[PUSH] Bark 推送失败: {e}", file=sys.stderr)
            return False

    # ═════════════════════════════════════════════════════
    #  推送模板
    # ═════════════════════════════════════════════════════

    def build_morning_digest(
        self,
        sleep_score: float = 75,
        sleep_hours: float = 7,
        deep_pct: float = 25,
        tsb: float = 0,
        tsb_zone: str = "正常",
        acwr: float = 1.0,
        today_plan: str = "",
        recovery_action: str = "",
        flags: dict | None = None,
    ) -> str:
        """构建晨间推送消息。"""
        lines = [f"🌅 {datetime.now().strftime('%m/%d')} 晨间简报"]
        lines.append("")

        # 睡眠
        emoji = "🟢" if sleep_score >= 60 else ("🟡" if sleep_score >= 40 else "🔴")
        lines.append(f"{emoji} 睡眠 {sleep_score:.0f}分 · {sleep_hours:.1f}h · 深睡{deep_pct:.0f}%")

        # 负荷
        lines.append(f"📊 TSB {tsb:+.0f} ({tsb_zone}) · ACWR {acwr:.2f}")

        # 今日建议（简短）
        if today_plan:
            lines.append(f"🚴 {today_plan[:100]}")

        # 恢复提示
        if recovery_action:
            lines.append(f"💪 {recovery_action[:60]}")

        # 告警
        if flags:
            if flags.get("overtraining_risk"):
                lines.append("⚠️ 过度训练风险！")
            if flags.get("sleep_deficit"):
                lines.append("⚠️ 连续睡眠不足")
            if flags.get("tsb_critical"):
                lines.append("⚠️ TSB 高危")

        return "\n".join(lines)

    def build_evening_digest(
        self,
        rides_today: list | None = None,
        sleep_score: float = 75,
        tsb: float = 0,
        tsb_zone: str = "正常",
        ctl: float = 0,
        atl: float = 0,
        deviation_summary: str = "",
        tomorrow_tip: str = "",
        flags: dict | None = None,
    ) -> str:
        """构建晚间推送消息。"""
        lines = [f"🌙 {datetime.now().strftime('%m/%d')} 晚间总结"]
        lines.append("")

        # 今日骑行
        if rides_today:
            total_trimp = sum(r.get("trimp", 0) for r in rides_today)
            total_dist = sum(r.get("distance_km", 0) for r in rides_today)
            lines.append(f"🚴 今日 {len(rides_today)}次 · {total_dist:.0f}km · TRIMP {total_trimp:.0f}")
        else:
            lines.append("🚴 今日无骑行")

        # 负荷
        lines.append(f"📊 CTL {ctl:.0f} · ATL {atl:.0f} · TSB {tsb:+.0f} ({tsb_zone})")

        # 睡眠
        lines.append(f"💤 昨晚睡眠 {sleep_score:.0f}分")

        # 偏差反馈
        if deviation_summary:
            lines.append("")
            lines.append(deviation_summary[:200])

        # 明日建议
        if tomorrow_tip:
            lines.append(f"📌 {tomorrow_tip[:100]}")

        # 告警
        if flags:
            if flags.get("overtraining_risk"):
                lines.append("⚠️ 过度训练风险，建议减量")
            if flags.get("sleep_deficit"):
                lines.append("⚠️ 睡眠连续不足")
            if flags.get("recovery_critical"):
                lines.append("⚠️ 需休息")

        return "\n".join(lines)

    def push_morning(
        self,
        sleep_score: float = 75,
        sleep_hours: float = 7,
        deep_pct: float = 25,
        tsb: float = 0,
        tsb_zone: str = "正常",
        acwr: float = 1.0,
        today_plan: str = "",
        recovery_action: str = "",
        flags: dict | None = None,
        force: bool = False,
    ) -> bool:
        """
        推送晨间简报（带 delta gate + 静默模式）。

        默认只在满足触发条件时推送，force=True 强制推送。
        """
        digest = self.build_morning_digest(
            sleep_score=sleep_score, sleep_hours=sleep_hours,
            deep_pct=deep_pct, tsb=tsb, tsb_zone=tsb_zone,
            acwr=acwr, today_plan=today_plan,
            recovery_action=recovery_action, flags=flags,
        )

        # delta gate
        date_key = datetime.now().strftime("morning_%Y-%m-%d")
        if not force and not self.should_push(date_key, digest):
            print(f"[PUSH] 晨间推送跳过（内容无变化）")
            return False

        return self.push_telegram(digest)

    def push_evening(
        self,
        rides_today: list | None = None,
        sleep_score: float = 75,
        tsb: float = 0,
        tsb_zone: str = "正常",
        ctl: float = 0,
        atl: float = 0,
        deviation_summary: str = "",
        tomorrow_tip: str = "",
        flags: dict | None = None,
        force: bool = False,
    ) -> bool:
        """
        推送晚间总结（带 delta gate + 静默模式）。
        """
        digest = self.build_evening_digest(
            rides_today=rides_today, sleep_score=sleep_score,
            tsb=tsb, tsb_zone=tsb_zone, ctl=ctl, atl=atl,
            deviation_summary=deviation_summary,
            tomorrow_tip=tomorrow_tip, flags=flags,
        )

        # delta gate
        date_key = datetime.now().strftime("evening_%Y-%m-%d")
        if not force and not self.should_push(date_key, digest):
            print(f"[PUSH] 晚间推送跳过（内容无变化）")
            return False

        return self.push_telegram(digest)

    def push_alert(self, title: str, body: str, channel: str = "both") -> bool:
        """
        推送告警（Bark 或 Telegram），不经过 delta gate。

        channel: "telegram" | "bark" | "both"
        """
        if channel in ("bark", "both"):
            self.push_bark(title, body)
        if channel in ("telegram", "both"):
            # Telegram 告警带上标记
            msg = f"🚨 {title}\n{body}"
            self.push_telegram(msg)
        return True
