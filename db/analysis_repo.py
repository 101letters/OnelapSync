"""
FIT 分析数据访问层 — AnalysisRepo
====================================
analysis 表的 CRUD 操作（保存/读取 FIT 分析结果）。

使用方式：通过 Database 多继承混入，self.conn 由 ConnMixin 提供。
"""

from __future__ import annotations

import json


class AnalysisRepo:
    """分析结果数据库操作。需要 self.conn（由 ConnMixin 提供）。"""

    def save_analysis(self, onelap_id: str, structured_data: dict) -> bool:
        """保存 FIT 分析结果。已存在则跳过（幂等）。"""
        cur = self.conn.execute("SELECT 1 FROM analysis WHERE onelap_id=?", (onelap_id,))
        if cur.fetchone():
            return False

        basic = structured_data.get("basic", {})
        distributions = structured_data.get("distributions", {})
        elevation = structured_data.get("elevation", {})
        temperature = structured_data.get("temperature", {})
        capabilities = structured_data.get("capabilities", {})

        self.conn.execute(
            """INSERT INTO analysis
               (onelap_id, basic_stats, hr_zones, cadence_zones, speed_zones,
                grade_zones, elevation, temperature, capabilities)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                onelap_id,
                json.dumps(basic, ensure_ascii=False),
                json.dumps(distributions.get("heart_rate_zones", {}), ensure_ascii=False),
                json.dumps(distributions.get("cadence_zones", {}), ensure_ascii=False),
                json.dumps(distributions.get("speed_zones", {}), ensure_ascii=False),
                json.dumps(distributions.get("grade_zones", {}), ensure_ascii=False),
                json.dumps(elevation, ensure_ascii=False),
                json.dumps(temperature, ensure_ascii=False),
                json.dumps(capabilities, ensure_ascii=False),
            ),
        )
        self.conn.commit()
        return True

    def get_analysis(self, onelap_id: str) -> dict | None:
        cur = self.conn.execute("SELECT * FROM analysis WHERE onelap_id=?", (onelap_id,))
        row = cur.fetchone()
        if row is None:
            return None
        d = dict(row)
        for key in ("basic_stats", "hr_zones", "cadence_zones", "speed_zones",
                     "grade_zones", "elevation", "temperature", "capabilities"):
            if d.get(key):
                try:
                    d[key] = json.loads(d[key])
                except (json.JSONDecodeError, TypeError):
                    pass
        return d

    def get_all_analysis(self) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM analysis ORDER BY onelap_id")
        return [dict(row) for row in cur.fetchall()]

    def update_analysis_type(self, onelap_id: str, training_type: str):
        """更新分析记录的训练类型分类。"""
        self.conn.execute(
            "UPDATE analysis SET training_type=? WHERE onelap_id=?",
            (training_type, onelap_id),
        )
        self.conn.commit()
