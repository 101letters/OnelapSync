"""
训练计划与执行偏差 — TrainingPlansRepo
=======================================
存储每周教练生成的周计划，支持与实际执行对比做偏差分析。

表结构：
  training_plans      周计划条目（一天一条）
  training_errors     执行偏差记录（自动分析生成）
"""

import json
from datetime import datetime


class TrainingPlansRepo:
    """训练计划与偏差分析。需要 self.conn（由 ConnMixin 提供）。"""

    # ═══════════════════════════════════════════════
    #  计划写入
    # ═══════════════════════════════════════════════

    def save_plan(self, plan: dict) -> None:
        """
        写入或更新一条日计划。

        plan dict:
            plan_id:    "2026-W19-MON"
            week_id:    "2026-W19"
            date:       "2026-05-11"
            planned_type:   "rest" | "z1" | "z2" | "z3" | "z4" | "interval"
            planned_duration_min: 45.0
            planned_trimp:  85.0
            planned_zones:  {"Z1": 10, "Z2": 30}  (JSON)
            description:    "Z2有氧耐力 40min"
            plan_context:   {"tsb": -5, "sleep_score": 72}  (JSON)
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO training_plans
                (plan_id, week_id, date, planned_type,
                 planned_duration_min, planned_trimp, planned_zones,
                 description, plan_context)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            plan["plan_id"],
            plan.get("week_id", ""),
            plan["date"],
            plan.get("planned_type", ""),
            plan.get("planned_duration_min"),
            plan.get("planned_trimp"),
            json.dumps(plan.get("planned_zones", {}), ensure_ascii=False),
            plan.get("description", ""),
            json.dumps(plan.get("plan_context", {}), ensure_ascii=False),
        ))
        self.conn.commit()

    def save_week_plans(self, plans: list[dict]) -> int:
        """批量写入一周计划。返回写入条数。"""
        for p in plans:
            self.save_plan(p)
        return len(plans)

    # ═══════════════════════════════════════════════
    #  计划读取
    # ═══════════════════════════════════════════════

    def get_plan(self, plan_id: str) -> dict | None:
        """按 plan_id 获取单条计划。"""
        row = self.conn.execute(
            "SELECT * FROM training_plans WHERE plan_id = ?", (plan_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_plan_by_date(self, date_str: str) -> dict | None:
        """按日期获取计划（一天最多一条）。"""
        row = self.conn.execute(
            "SELECT * FROM training_plans WHERE date = ?", (date_str,)
        ).fetchone()
        return dict(row) if row else None

    def get_week_plans(self, week_id: str) -> list[dict]:
        """获取一周的所有计划。"""
        rows = self.conn.execute(
            "SELECT * FROM training_plans WHERE week_id = ? ORDER BY date",
            (week_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_latest_plan_before(self, date_str: str, limit: int = 7) -> list[dict]:
        """获取某日期之前的最近 N 条计划（含当天）。"""
        rows = self.conn.execute("""
            SELECT * FROM training_plans
            WHERE date <= ?
            ORDER BY date DESC LIMIT ?
        """, (date_str, limit)).fetchall()
        return [dict(r) for r in rows]

    # ═══════════════════════════════════════════════
    #  偏差写入
    # ═══════════════════════════════════════════════

    def save_deviation(self, dev: dict) -> None:
        """
        写入一条执行偏差记录。

        dev dict:
            plan_id:     "2026-W19-MON"
            date:        "2026-05-11"
            strava_id:   12345678
            actual_duration_min: 45.0
            actual_trimp:  62.0
            actual_type:    "z2"
            intensity_factor: 0.82
            deviation_type: "completed" | "partial" | "skipped" | "over"
            deviation_pct:  -27.0   (TRIMP偏差百分比)
            note:           "时长不足，强度匹配"
            created_at:     "2026-05-11T20:00:00"
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO training_errors
                (plan_id, date, strava_id,
                 actual_duration_min, actual_trimp, actual_type,
                 intensity_factor,
                 deviation_type, deviation_pct, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            dev["plan_id"],
            dev["date"],
            dev.get("strava_id"),
            dev.get("actual_duration_min"),
            dev.get("actual_trimp"),
            dev.get("actual_type", ""),
            dev.get("intensity_factor"),
            dev.get("deviation_type", "unknown"),
            dev.get("deviation_pct"),
            dev.get("note", ""),
        ))
        self.conn.commit()

    def get_latest_deviations(self, limit: int = 7) -> list[dict]:
        """获取最近 N 条偏差记录。"""
        rows = self.conn.execute("""
            SELECT * FROM training_errors
            ORDER BY date DESC LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def get_deviations_since(self, date_str: str) -> list[dict]:
        """获取某日期之后的偏差记录。"""
        rows = self.conn.execute("""
            SELECT * FROM training_errors
            WHERE date >= ?
            ORDER BY date
        """, (date_str,)).fetchall()
        return [dict(r) for r in rows]

    # ═══════════════════════════════════════════════
    #  偏差分析
    # ═══════════════════════════════════════════════

    def analyze_deviation(
        self,
        plan: dict,
        ride: dict,
        plan_context: dict | None = None,
    ) -> dict:
        """
        分析单次骑行与计划的偏差。

        plan: 来自 training_plans 的 dict
        ride: 实际骑行数据 {trimp, duration_min, avg_hr, z4_plus_pct, ...}

        返回:
            {deviation_type, deviation_pct, note, possible_reasons, adjustment}
        """
        plan_dur = plan.get("planned_duration_min", 0) or 0
        plan_trimp = plan.get("planned_trimp", 0) or 0
        actual_dur = ride.get("duration_min", 0) or 0
        actual_trimp = ride.get("trimp", 0) or 0

        # 跳过休息日（有骑行就算 over）
        if plan.get("planned_type") == "rest":
            if actual_trimp > 0:
                return {
                    "deviation_type": "over",
                    "deviation_pct": 100.0,
                    "note": "休息日安排了训练",
                    "possible_reasons": [],
                    "adjustment": {"action": "none", "note": ""},
                }
            return {
                "deviation_type": "completed",
                "deviation_pct": 0,
                "note": "休息日",
                "possible_reasons": [],
                "adjustment": {"action": "none", "note": ""},
            }

        # 未执行
        if actual_trimp <= 0:
            return {
                "deviation_type": "skipped",
                "deviation_pct": -100.0,
                "note": "计划未执行",
                "possible_reasons": _infer_reasons_skipped(plan_context),
                "adjustment": {"action": "reduce", "note": "明日关注是否要补"},
            }

        # 计算偏差
        dur_delta = ((actual_dur - plan_dur) / max(plan_dur, 1)) * 100 if plan_dur > 0 else 0
        trimp_delta = ((actual_trimp - plan_trimp) / max(plan_trimp, 1)) * 100 if plan_trimp > 0 else 0

        # 偏差类型判定
        if trimp_delta < -30:
            dev_type = "partial"
            note = f"TRIMP不足（{trimp_delta:+.0f}%）"
        elif trimp_delta > 30:
            dev_type = "over"
            note = f"TRIMP超额（{trimp_delta:+.0f}%）"
        else:
            dev_type = "completed"
            note = f"基本完成（TRIMP偏差{trimp_delta:+.0f}%）"

        # 推断原因
        reasons = _infer_reasons(trimp_delta, dur_delta, plan_context)

        # 调整建议
        adj = _suggest_adjustment(dev_type, reasons, plan_context)

        return {
            "deviation_type": dev_type,
            "deviation_pct": round(trimp_delta, 1),
            "note": note,
            "possible_reasons": reasons,
            "adjustment": adj,
        }


# ─── 辅助函数 ──────────────────────────────────

def _infer_reasons_skipped(ctx: dict | None) -> list[str]:
    """推断跳过训练的可能原因。"""
    reasons = []
    if not ctx:
        return reasons
    if ctx.get("sleep_score", 75) < 60:
        reasons.append("睡眠不足")
    if ctx.get("tsb", 0) < -20:
        reasons.append("疲劳积累严重")
    if ctx.get("consecutive_low_sleep", 0) >= 2:
        reasons.append("连续睡眠差")
    if not reasons:
        reasons.append("未知原因")
    return reasons


def _infer_reasons(
    trimp_delta: float,
    dur_delta: float,
    ctx: dict | None,
) -> list[str]:
    """推断执行偏差的原因。"""
    reasons = []
    if not ctx:
        return reasons

    if trimp_delta < -20 and ctx.get("sleep_score", 75) < 60:
        reasons.append("睡眠不足影响表现")
    if trimp_delta < -20 and ctx.get("tsb", 0) < -15:
        reasons.append("疲劳积累导致减量")
    if trimp_delta > 30:
        reasons.append("状态超预期发挥")
    if dur_delta < -20 and trimp_delta > 0:
        reasons.append("强度高于计划，时长短于计划")
    if not reasons:
        reasons.append("计划制定偏差")

    return reasons


def _suggest_adjustment(
    dev_type: str,
    reasons: list[str],
    ctx: dict | None,
) -> dict:
    """根据偏差类型和原因给出调整建议。"""
    has_sleep_issue = any("睡眠" in r for r in reasons)
    has_fatigue = any("疲劳" in r for r in reasons)

    if dev_type == "skipped" and (has_sleep_issue or has_fatigue):
        return {"action": "rest", "note": "继续休息，恢复优先"}
    elif dev_type == "partial" and has_fatigue:
        return {"action": "reduce", "note": "降低明日负荷10-20%"}
    elif dev_type == "over":
        return {"action": "monitor", "note": "关注明天恢复情况，可正常训练"}
    elif dev_type == "partial":
        return {"action": "maintain", "note": "保持计划，注意恢复"}
    else:
        return {"action": "proceed", "note": "按计划继续"}
