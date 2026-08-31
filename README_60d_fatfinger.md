# 币安乌龙指猎手 · 60 天扫描仓库

本仓库包含两部分的完整交付物：
1. **60 天乌龙指分析检测（脚本）** —— `fatfinger_scan_60d/`
2. **60 天结果分析（结论）** —— `report_60d.md` + `fatfinger_scan_60d/` 下的结果 JSON

## 一、目录与文件索引（另一个 AI 按此路径取用）

### 检测脚本（fatfinger_scan_60d/）
| 文件 | 作用 |
|---|---|
| `scan_1h_wicks.py` | 第一道粗筛：全市场 1h K 线单腿 wick ≥ 5% 候选 |
| `drill_1m.py` | 第二道下钻：1m 定位孤立尖针（single_needle）锚点 |
| `verify_agg.py` | 第三道终审：拉 ±90s aggTrades 跨腿对齐，max_spread≥5% 判真针 |
| `run_all_60d.py` | 60 天编排器：pull 1h → scan → drill → verify，断点续跑 |
| `scan_divergence_focus.py` | 盲区补扫：1m 跨腿 high/low 极值偏离预筛（报价背离型） |
| `verify_divergence.py` | 盲区终审：对候选做 aggTrades 真实成交验证，过滤假阳性 |
| `focus_run.py` | 盲区 scan→verify 串联 driver |
| `monitor_focus.py` / `monitor_focus2.py` | 运行期监控（时间戳驱动，防后台回收误判） |
| `precursor_analysis.py` / `precursor_precision.py` | 提前检测（前兆命中率/精度）统计 |
| `pull_full_1m.py` / `pull_1h_klines.py` | 原始 1m/1h K 线分页拉取 |
| `build_targets*.py` / `make_report.py` / `run_all.py` / `scan_1h_*.py` | 配套工具与旧版脚本 |

### 结果数据（fatfinger_scan_60d/，已落盘，可直接分析）
| 文件 | 内容 |
|---|---|
| `verify_results_full.json` | **60 天主体终审结果**：891 候选 → 确凿真针 472 / 弱 379（字段 verdict=真\|弱，含 base/leg/anchor_iso/peak_pct） |
| `verify_divergence_results.json` | **60 天盲区终审结果**：285 候选 → 真 130 / 弱 149 |
| `divergence_candidates.json` | 盲区预筛候选（base/leg/anchor_ms/div_pct） |
| `drill_full.json` | 891 条单针锚点下钻明细 |
| `targets.json` | 267 base / 598 腿监控名单 |

### 结论报告
| 文件 | 内容 |
|---|---|
| `report_60d.md` | **完整结论**：四条可下单规律 + 主体 472 / 盲区 130 合并 538 真针 + 系统性事件日处置规则 |

## 二、复现命令（需代理 + Binance API，缓存已排除需重拉）
```bash
export HTTPS_PROXY=http://127.0.0.1:7897 HTTP_PROXY=http://127.0.0.1:7897
PY=python3
# 主体 60 天
$PY fatfinger_scan_60d/run_all_60d.py
# 盲区补扫（14 天窗口）
$PY fatfinger_scan_60d/focus_run.py
```

## 三、关键数字速览（详见 report_60d.md）
- 唯一真针池 = 472（主体）+ 66（盲区纯新增）= **538 条 / 60 天**
- 主峰时段：**05:00 UTC（北京 13:00）**，两种独立方法互验一致
- 接刀方向：USDC 腿（主体 61%、盲区 93%）
- 系统性事件日：08-20、08-22 占盲区 81.5%，按「流动性错乱」降级处置，不接刀
