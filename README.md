# Magene → Strava 骑行活动自动同步

将 Magene（顽鹿/OneLap）骑行活动自动同步到 Strava，并写入科学的训练分析。

## 功能

- 🚴 **自动同步**：定时拉取 Magene 活动 → 下载 FIT 文件 → 上传 Strava
- 📊 **FIT 解析**：心率区间（ACSM 5-zone）、爬升、速度、踏频、坡度
- 🧠 **AI 分析**：调 coach agent 生成训练建议，自动写入 Strava 活动描述
- 📈 **训练负荷计算**：Edwards TRIMP + ACWR 急慢性负荷比
- 🔔 **通知**：Bark 推送同步结果摘要
- 🛡 **防重复**：文件锁 + 状态追踪，不会重复上传

## 快速开始

### 1. 克隆

```bash
git clone https://github.com/YOUR_USER/magene-to-strava.git
cd magene-to-strava
```

### 2. 配置

```bash
cp .env.example .env
# 编辑 .env，填入你的认证信息
```

### 3. 安装依赖

```bash
pip install requests
```

### 4. 配置环境变量

```bash
cp .env.example .env
```

编辑 `.env`，填入你的认证信息，参考格式如下：

```bash
# ─── 骑行平台认证 ─────────────────────
# Magene / 顽鹿 OneLap
MAGENE_TOKEN=your_magene_jwt_token_here
MAGENE_OUID=your_ouid_here

# iGPSport（备用平台）
IGPSPORT_USERNAME=your_phone_or_email
IGPSPORT_PASSWORD=your_password
IGPSPORT_ACCESS_TOKEN=your_igpsport_token

# Giant（备用平台）
GIANT_TOKEN=your_giant_token

# ─── Strava ──────────────────────────
STRAVA_ACCESS_TOKEN=your_strava_access_token
STRAVA_REFRESH_TOKEN=your_strava_refresh_token
STRAVA_CLIENT_ID=your_strava_api_client_id
STRAVA_CLIENT_SECRET=your_strava_api_client_secret

# ─── 通知 ────────────────────────────
BARK_URL=https://api.day.app/your_bark_device_key

# ─── 数据分析（可选）────────────────
# ANALYSIS_MAX_HR=194
```

### 5. 运行

```bash
# 试运行（不实际上传）
python3 magene_sync_v3.py --dry-run --days 1 --verbose

# 正式同步
python3 magene_sync_v3.py --days 1

# 强制补同步最近7天
python3 magene_sync_v3.py --days 7 --force
```

## 工作流程

```
定时触发
  ├→ 1. 登录顽鹿 OTM API
  ├→ 2. 获取活动列表（ride_record/list）
  ├→ 3. 获取活动详情 → 拿到 fileKey
  ├→ 4. 下载 FIT 文件
  ├→ 5. 跟 Strava 已有活动比对（按时间+距离去重）
  ├→ 6. 上传 FIT 到 Strava
  ├→ 7. 修正 Strava 活动类型为 Ride
  ├→ 8. FIT 解析（心率/速度/踏频/爬升/坡度）
  ├→ 9. AI 教练分析 → 清洗 → 写入 Strava 描述
  └→ 10. Bark 推送同步结果
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `magene_sync_v3.py` | 主同步脚本 |
| `fit_analysis.py` | FIT 文件解析模块（心率区间/爬升/速度等） |
| `sync_config.json` | 同步配置（API 地址、超时等） |
| `.env.example` | 环境变量模板（需填入真实值） |

## 心率区间

采用 ACSM（American College of Sports Medicine）5-zone 模型，基于最大心率百分比：

| 区间 | 名称 | %HRmax | 用途 |
|------|------|--------|------|
| Z1 | 恢复 | 50-60% | 热身/恢复骑行 |
| Z2 | 有氧基础 | 60-70% | 耐力训练 |
| Z3 | 有氧进阶 | 70-80% | 节奏训练 |
| Z4 | 乳酸阈值 | 80-90% | 阈值训练 |
| Z5 | 无氧极限 | 90-100% | 高强度间歇 |

默认最大心率：194 bpm（可在 `.env` 中通过 `ANALYSIS_MAX_HR` 覆盖）

## 致谢

本项目参考和借鉴了以下优秀开源项目/资料，特此感谢：

- **[Onelap-Strava-GoGoGo](https://github.com/Tyan66666/Onelap-Strava-GoGoGo)** — 感谢作者的分享思路，为本项目的实现提供了重要参考
- **[fitparse](https://github.com/dtcooper/python-fitparse)** — FIT 文件解析的 Python 实现，为本项目的 FIT 解析器提供了技术参考
- **[Strava API Docs](https://developers.strava.com/)** — Strava 官方 API 文档，上传和数据管理的核心参考
- **[TrainingPeaks Blog - ACWR](https://www.trainingpeaks.com/blog/acute-chronic-workload-ratio/)** — 急慢性负荷比（ACWR）理论，指导训练负荷量化
- **[Tim Gabbett 的 ACWR 研究](https://journals.lww.com/nsca-jscr/abstract/2016/03000/the_training_injury_prevention_paradox_.1.aspx)** — 运动医学领域关于训练负荷与受伤风险的经典研究
- **Banister EW, et al. "A systems model of training for athletic performance"** — TRIMP（训练冲量）方法的原始论文
- **Edwards S. "The Heart Rate Monitor Book"** — Edwards TRIMP 加权模型（Z1×1 ~ Z5×5）的提出者
- **[QwenPaw](https://github.com/openclaw/QwenPaw)** — 多智能体协作框架，本项目中的运动教练、数据分析等功能基于 AI 智能体实现

> 💡 本项目完全由 **AI 驱动** — 从代码编写、训练分析到持续迭代均由多智能体系统自动完成。
