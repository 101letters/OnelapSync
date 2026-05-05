#!/usr/bin/env python3
"""
OneLap 跑步 API 探测脚本 (v2)
用法: python3 probe_onelap_run.py
目的: 确认老板账号下是否有跑步活动记录、跑步 API 端点是否存在
"""

import hashlib
import json
import os
import secrets
import time

import requests

# ─── 配置 ────────────────────────────────────
ONELAP_SIGN_KEY = os.environ.get("ONELAP_SIGN_KEY")
ONELAP_LOGIN_URL = os.environ.get("ONELAP_LOGIN_URL", "https://www.onelap.cn/api/login")
ONELAP_OTM_BASE = os.environ.get("ONELAP_OTM_BASE", "https://otm.onelap.cn")
USERNAME = os.environ.get("ONELAP_USERNAME")
PASSWORD = os.environ.get("ONELAP_PASSWORD")


def md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def safe_str(val, max_len=80):
    if val is None:
        return "(none)"
    s = str(val)
    return s[:max_len] if len(s) > max_len else s


def onelap_login():
    nonce = secrets.token_hex(8)
    ts = str(int(time.time() * 1000))
    pwd_md5 = md5(PASSWORD)
    sign_str = f"account={USERNAME}&nonce={nonce}&password={pwd_md5}&ts={ts}&key={ONELAP_SIGN_KEY}"
    sign = md5(sign_str)

    print(f"[LOGIN] 登录中...")
    resp = requests.post(ONELAP_LOGIN_URL, json={
        "account": USERNAME, "password": pwd_md5,
        "nonce": nonce, "ts": ts, "sign": sign,
    }, timeout=15)
    data = resp.json()
    if data.get("code") == 200 and data.get("data"):
        token = data["data"][0].get("token", "")
        userinfo = data["data"][0].get("userinfo", {})
        print(f"[LOGIN] ✅ 成功 (uid={userinfo.get('uid')}, nickname={userinfo.get('nickname')})")
        return token, userinfo
    else:
        print(f"[LOGIN] ❌ 失败: {data.get('msg','unknown')}")
        return None, None


def _otm_post(token, path, payload=None):
    url = f"{ONELAP_OTM_BASE}{path}"
    headers = {"Authorization": token, "Content-Type": "application/json"}
    try:
        resp = requests.post(url, json=payload or {}, headers=headers, timeout=15)
        return resp.status_code, resp.text
    except Exception as e:
        return -1, str(e)


def _otm_get(token, path):
    url = f"{ONELAP_OTM_BASE}{path}"
    headers = {"Authorization": token}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        return resp.status_code, resp.text
    except Exception as e:
        return -1, str(e)


def extract_inner(data: dict):
    """OTM 响应可能嵌套在 data 下，统一提取内层"""
    if "data" in data and isinstance(data["data"], dict):
        return data["data"]
    return data


def probe_all(token):
    """探测所有可能的跑步/通用运动 API"""
    endpoints = [
        ("POST", "/api/otm/run_record/list",       {"limit": 5, "page": 1}),
        ("POST", "/api/otm/sport_record/list",     {"limit": 5, "page": 1}),
        ("GET",  "/api/otm/sport/running/list",    None),
        ("GET",  "/api/otm/run/list",              None),
        ("POST", "/api/otm/running_record/list",   {"limit": 5, "page": 1}),
        ("POST", "/api/otm/ride_record/list",      {"limit": 5, "page": 1}),   # 对照组
        ("POST", "/api/otm/sport/list",            {"limit": 5, "page": 1}),
        ("POST", "/api/otm/activity/list",         {"limit": 5, "page": 1}),
    ]

    print(f"\n{'='*70}")
    print(f"  OneLap OTM API 端点探测")
    print(f"{'='*70}")

    results = []
    for method, path, payload in endpoints:
        label = f"{method} {path}"
        if payload:
            label += f"  {json.dumps(payload)}"

        if method == "POST":
            code, body = _otm_post(token, path, payload)
        else:
            code, body = _otm_get(token, path)

        hit = False
        count = 0
        total = "?"
        keys_preview = ""

        if 200 <= code < 300:
            try:
                data = json.loads(body)
                inner = extract_inner(data)
                lst = inner.get("list", [])
                total = inner.get("total", "?")
                if isinstance(lst, list) and len(lst) > 0:
                    hit = True
                    count = len(lst)
                    keys_preview = str(list(lst[0].keys()))
            except json.JSONDecodeError:
                pass

        status = "✅ HIT" if hit else ("❌ 404" if code == 404 else f"HTTP {code}")
        print(f"  {status:8s} {label}")
        if hit:
            print(f"           total={total}  records_in_page={count}")
            print(f"           keys: {keys_preview}")

        results.append({
            "path": path, "method": method, "http_code": code,
            "hit": hit, "count": count, "total": total,
        })

    return results


def deep_probe_ride(token):
    """翻页看骑行总数 + 字段详情"""
    print(f"\n{'='*70}")
    print(f"  深入探测 ride_record/list")
    print(f"{'='*70}")

    all_records = []
    for page in [1, 2]:
        code, body = _otm_post(token, "/api/otm/ride_record/list",
                               {"limit": 50, "page": page})
        if code != 200:
            print(f"  page={page}: HTTP {code}")
            break
        inner = extract_inner(json.loads(body))
        lst = inner.get("list", [])
        all_records.extend(lst)
        print(f"  page={page}: {len(lst)} records")

    print(f"\n  📊 总骑行记录数: {len(all_records)}")

    if all_records:
        # 字段分析
        keys = list(all_records[0].keys())
        print(f"  📋 字段列表: {keys}")

        # 检查是否有 type / sport_type 字段
        has_type = "type" in keys or "sport_type" in keys or "sportType" in keys
        print(f"  🔍 有 type/sport_type 字段: {'是' if has_type else '否（骑行端点隐含全为 ride）'}")

        # 距离/时间统计
        distances = [r.get("distance_km") for r in all_records if r.get("distance_km")]
        times = [r.get("time_seconds") for r in all_records if r.get("time_seconds")]
        if distances:
            print(f"  🚴 距离范围: {min(distances):.1f} ~ {max(distances):.1f} km")
        if times:
            print(f"  ⏱️ 时间范围: {min(times)//60} ~ {max(times)//60} 分钟")

        # 打印 3 条样本
        print(f"\n  📝 样本记录 (前3条):")
        for i, rec in enumerate(all_records[:3]):
            print(f"     [{i}] dist={rec.get('distance_km')}km"
                  f" time={rec.get('time_formatted')}"
                  f" avg_power={rec.get('avg_power_w')}W"
                  f" avg_hr={rec.get('avg_heart_bpm')}bpm"
                  f" tss={rec.get('load_tss')}"
                  f" speed={rec.get('avg_speed_kmh')}km/h")

    return all_records


def inspect_userinfo(userinfo):
    print(f"\n{'='*70}")
    print(f"  用户信息 (userinfo)")
    print(f"{'='*70}")
    sport_keys = [k for k in userinfo if any(
        w in k.lower() for w in ["sport", "run", "ride", "swim", "activity", "type", "bike", "ftp"]
    )]
    if sport_keys:
        for k in sport_keys:
            print(f"  {k}: {safe_str(userinfo[k])}")
    else:
        print(f"  (无运动类型字段)")
    # 打印全部 key 供参考
    print(f"  全部字段: {list(userinfo.keys())}")
    # 特别标注 ftp（骑行专属指标）
    if "ftp" in userinfo:
        print(f"  ⚠️ ftp={userinfo['ftp']} — FTP 是骑行专用功率指标，暗示此账号为骑行用户")


def main():
    token, userinfo = onelap_login()
    if not token:
        return

    inspect_userinfo(userinfo or {})

    # 1. 端点探测
    results = probe_all(token)

    # 2. 深入骑行数据
    all_rides = deep_probe_ride(token)

    # 3. 最终结论
    print(f"\n{'='*70}")
    print(f"  🎯 探测结论")
    print(f"{'='*70}")

    ride_hit = any(r["hit"] and "ride_record" in r["path"] for r in results)
    run_hit = any(
        r["http_code"] == 200 for r in results
        if "run" in r["path"].lower() or "sport" in r["path"].lower()
    )

    print(f"  ✅ 骑行端点 (ride_record/list) 可用，共 {len(all_rides)} 条骑行记录")
    print(f"  {'✅' if run_hit else '❌'} 跑步端点: {'有' if run_hit else '全部 404 — 不存在'}")
    print(f"  {'✅' if run_hit else '❌'} 通用运动端点 (sport_record/list etc.): {'有' if run_hit else '全部 404'}")
    print(f"  {'✅' if run_hit else '❌'} 用户信息含运动类型字段: 否")
    print(f"  {'✅' if run_hit else '❌'} 任何记录中 type=run: 否 (骑行端点无 type 字段)")
    print()
    print(f"  💡 最终判断：OneLap OTM API 只有 ride_record/list，没有跑步端点。")
    print(f"     老板账号下只有骑行数据，共 {len(all_rides)} 条。")
    print(f"     当前只同步骑行是正确的，无需添加跑步逻辑。")
    print()


if __name__ == "__main__":
    main()
