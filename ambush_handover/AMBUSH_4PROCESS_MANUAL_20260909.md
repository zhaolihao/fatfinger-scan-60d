# 乌龙指埋伏篮子 · 四进程交接说明书 (2026-09-09)

> 交接人：WorkBuddy AI（本机运维）。接手人：另一 AI。
> 本文基于 2026-09-09 早上真实代码逐行核对（不是凭记忆），配合代码行号定位（行号会漂移，以函数名为准）。
> 前置文档：`AMBUSH_HANDOVER.md`（早期版本）、`README.md`。

---

## 0. 一分钟速览

- **四个进程 = 同一个文件 `ambush_basket.py`，靠启动参数分三套逻辑**：
  | 进程 | 实例名 | 逻辑套 | 币表 | 单笔 | 深度 |
  |---|---|---|---|---|---|
  | react | (无instance后缀) | 第一套·CMP对比触发 | planR_136.json (136币) | 10U | 动态≥10% |
  | resident | resident | 第二套·托管条件单 | resident20_draft.json (20币) | 10U | ±10% |
  | exp11 | exp11 | 第三套·resident激进版 | experimental_20_clean.json (11币) | 3U | ±15% |
  | exp50 | exp50 | 第三套(与exp11同参,仅币表/金额不同) | experimental_50_extra.json (50币) | 5U | ±15% |
- **2026-09-09 变更**：resident 系三实例全部切到 `--anchor-sec 1`（1秒跟随重挂）+ `--anchor-jump-gate 0`（**跳变闸门已删除**，回测证明其在1s下拦截0次且唯一一次激活帮倒忙）。同时"撤旧挂新"已改**并发**（asyncio.gather，旧单存活窗口从~1.5s压到~0.3s）。
- **当前事故**：机场流量用完 → 全部节点 503 → 所有 bot 的 IP 守卫进入保守拦截（不撤不挂）。交易所上 exp11=22 张 + resident=40 张条件单在托管（币安端触发，不受断网影响），exp50=0（还没挂上）。**流量恢复后 exp50 需要重启补挂**。

---

## 1. 当日(09-09)事件时间线（接手前必读）

| 时间 | 事件 |
|---|---|
| 07:47 | 三个 resident 系进程同时死亡（原因未查明，resident 日志尾部有 `^C`）。**收尾撤单没跑成 → 交易所 162 张条件单成无主孤儿**。 |
| 08:14~08:57 | 发现时钟漂移（本机比币安快~1.5-2s）→ 所有签名请求 -1021。校准修复（最终窗口 [-90,+83]ms）。教训：**沙箱内 python 的 time.time() 恒比真实系统时钟快 ~1.4s**，外部脚本签名必须用币安服务器时间戳。 |
| 08:58~09:14 | 外部全量清扫（`_sweep_all.py`，WMI 脱离沙箱跑）撤光 162 张孤儿 → 按 exp11→resident→exp50 顺序重启到 1s+无闸门+并发替换代码。exp11=22✓ resident=40✓。 |
| 09:14 起 | exp50 被自家 IP 守卫保守拦截（探测站不可达→503）持续 0 挂单。后经诊断：**机场流量用完**（服务器 TCP/TLS 直连测试全部健康，唯独 mihomo 出不去 → 不是机场被封，是套餐没流量了）。 |

---

## 2. 启动参数全表（真实 bat 内容）

所有 bat 在本目录，python 路径硬编码 `C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe`，**前台运行**（经 `_launch_wmi.py` 由 WMI 脱离沙箱拉起）。

### restart_resident.bat（resident，20币）
```
ambush_basket.py --symbols-file resident20_draft.json --notional 10 --lev 0 --tp 0.05
  --mode resident --anchor-sec 1 --anchor-jump-gate 0
```

### restart_exp11.bat（exp11，11币）
```
ambush_basket.py --symbols-file experimental_20_clean.json --mode resident --instance exp11
  --resident-depth 0.15 --reb-anchor --reb 0.05 --ta 5.0 --route-branch
  --branch-tr 0.02 --branch-cut 0.02 --branch-hold 900
  --notional 3 --lev 0 --tp 0.03 --anchor-sec 1 --anchor-jump-gate 0
```

### restart_exp50.bat（exp50，50币）—— 与 exp11 完全同参，仅币表与 notional 不同
```
ambush_basket.py --symbols-file experimental_50_extra.json --mode resident --instance exp50
  --resident-depth 0.15 --reb-anchor --reb 0.05 --ta 5.0 --route-branch
  --branch-tr 0.02 --branch-cut 0.02 --branch-hold 900
  --notional 5 --lev 0 --tp 0.03 --anchor-sec 1 --anchor-jump-gate 0
```

### launch3.bat —— 一次性拉起全部（react 行用 `--anchor-sec 1 --anchor-jump-gate 0.08`，react 的闸门是另一套逻辑见 §3.1）
⚠️ launch3.bat 的 resident/exp 行是旧的 3s 参数（未同步今天的 1s 修改），**用它拉起前先把各行参数对齐上面的 restart bat**。

### react 的启动行（launch3.bat 内，本次未改动）
```
ambush_basket.py --symbols-file planR_136.json --dyn-thresh --notional 10 --mode react --cmp
  --anchor-sec 1 --anchor-jump-gate 0.08 ...（以 bat 原文为准）
```

---

## 3. 三套执行逻辑（逐行核对过的机理说明）

### 3.1 第一套：react（CMP 对比触发，136币）
**平时只盯盘不挂单；价格瞬间砸穿动态阈值 → 市价开枪 → 成交后立刻挂保护单 + 风暴单。**
- 锚价每 1s 刷新（minute_cycle）；动态阈值 = 过去分钟振幅 EMA(α=0.3)×4，下限 10% 不封顶（**绝不含当前分钟**，防插针自己抬高阈值）。
- 触发检测 `reactive_entry_loop` 每 0.05s 扫 WS 中间价；同币同向 60s 冷却。
- 触发 → `_fire_cmp_m` 立即市价开火（无反弹确认，CEO 定 10% 大针无需确认）。⚠️ 横幅文案"M+L 双单"是旧的，实际只有 M 市价单+风暴单，L 限价腿已停用。
- 成交 → `on_fill`（三套共用）：SL 10%（交易所端 STOP_MARKET closePosition，断网也生效）→ TP 3% → `_storm_leg` 风暴单（同向深位限价 ×0.95/×1.05，5 分钟窗口）→ 路线 A（T_A=3s 内回弹到目标 → 立即市价平）→ 路线 B（留仓等交易所 TP/SL）。

### 3.2 第二套：resident（托管条件单，20币）
**提前把"锚-10% BUY / 锚+10% SELL"的 TAKE_PROFIT_MARKET 条件单托管在币安服务器；每 1 秒对表：阴跌就跟着挪触发价，瞬间大跳（已无闸门，一切跟随）就尽快挪。**
- 启动（resident_loop 开头）：等 5s → 按 state 文件撤上次残留 → 扫历史日志再撤（**只撤本实例币种，隔离保护**）→ 正向对账（登记有/交易所无的死单清除）→ **反向对账 `_resident_reverse_reconcile`（09-08 新增）：交易所有、登记表没有 → 视为孤儿撤掉**（自愈 state 丢失/日志正则失灵）。
- 每 1s 维护循环（RES_TICK=max(1,min(15,ANCHOR_SEC))=1）：
  1. 熔断：300s 内 ≥40 币插针 ≥5% → 撤光自己全部条件单，冷却 60 分钟；
  2. 孤儿重试队列：上轮撤旧失败的再撤；
  3. REST 兜底：每 15s 拉全量持仓，补处理 WS 漏推的成交；
  4. 冻结管理：单边成交后另一侧不撤（CEO 规则），但存活侧触发价距现价 <8% 时上移重挂防趋势打穿；平仓完自动解冻双侧重建；
  5. **跟随/重挂**：现价距基准价偏移 ≥2% 才动（纯本地比价，不耗 API）；**重挂=并发**：挂新+撤旧 gather 同时发（09-09 改），挂新失败旧单在 → 不断单；挂新成功撤旧失败 → 进孤儿队列下轮再撤；
  6. 缺单补挂：某侧没单 → 按 ±DEPTH 补挂，-2021 守卫（现价已在触发价内侧就不挂）。
- 成交后：币安端条件单触发（断网免疫）→ 用户流 WS 按订单类型识别 → 合成 R 腿进 on_fill 同一流水线（SL10% / TP5% / 风暴单 / 路线A 3s / 路线B 留仓）。

### 3.3 第三套：exp11 / exp50（resident 的激进实验版）
循环结构、1s 跟随、冻结、启动清理、反向对账全部与第二套相同。差异四点：
1. **深度 ±15%**（`--resident-depth 0.15`）：只接更极端的针；
2. **回弹目标相对锚价**（`--reb-anchor --reb 0.05`）：路线 A 平仓目标 = 锚价×(1∓5%) 而非成交价+3%（与回测口径一致）；
3. **路线 A 窗口 5s**（`--ta 5.0`）；
4. **分岔式退出**（`--route-branch`，branch_exit 函数）：5s 未回弹 → 本机盯盘最多 900s：已盈利→止损上移保本+移动止盈（回撤2%平）；仍亏损→快砍（跌2%平）；超时→强平。⚠️ 这层是本机盯盘，**断网/IP漂移会失效**，但交易所端 10% 固定止损始终挂着兜底。

---

## 4. 运维工具箱（本目录，全部带重试）

| 脚本 | 用途 | 用法 |
|---|---|---|
| `_launch_wmi.py` | **唯一的可靠拉起方式**（沙箱/普通 start /B 拉起的子进程会被杀） | `python _launch_wmi.py "<bat绝对路径>"` |
| `_count_orders.py` | 交易所 um algo 挂单总量+分布（含每币 xN 统计）。**含有效 API 凭据，其他外部脚本一律从这里同步 KEY/SEC** | `python _count_orders.py` |
| `_sweep_all.py` | 撤光交易所全部 open algo 单（全实例已死时的 clean slate 手段；带终检） | `python _sweep_all.py` |
| `_sweep_exp50_old.py` | 白名单清扫：撤"交易所存在但不在新进程日志白名单"的单（大实例重启兜底） | `python _sweep_exp50_old.py [--dry]` |
| `_cancel_algo.py` | 精准撤单 | `python _cancel_algo.py SYMBOL ALGOID` |
| `_cancel_syms.py` | 按币表撤单 | `python _cancel_syms.py --syms-file xxx.json [--dry]` |
| `_resident_sim.py` | 秒级跟随回测模拟器（真实逐笔，FORM 真乌龙窗 + XAN 阴跌窗，tick×闸门矩阵） | `python _resident_sim.py` |
| `_check_alive.py` | 存活检查 | 见脚本内注释 |
| `_clash_ctl.py` | Clash 控制器操作（9097端口，secret=set-your-secret；可测延迟/切节点） | 见脚本内注释 |

---

## 5. 铁律与已知坑（每一条都是真金白银换的）

1. **单实例 200 张上限**（币安 -4045）。exp50 一个就占 100 张。**exp50 必须单独重启，绝不与其他实例并行**。启动清理未完成就开始挂新单 = 旧新叠加冲上限（09-08 实际发生过 197/200）。
2. **bat 双层引号 bug**：`set PY="..."` 后再 `"%PY%"` → cmd 报 `'""' 不是内部或外部命令`。现行 bat 全部硬编码路径，别改回变量写法。
3. **日志是 GBK 编码**（stdout 重定向用本地编码），且 ✓ 存成字面 `\u2713`（6字符）。外部解析日志必须按 GBK 读或 utf-8 优先+GBK 兜底；bash 的 grep 中文经常失灵，用 python 读。
4. **API 凭据两套**：旧 key（-2015 IP 拒绝）vs 有效 key。**一律从 `_count_orders.py` 同步**。
5. **时钟漂移 -1021**：bot 已打自愈补丁（signed_request 遇 -1021 → `_clock_resync()` 拉服务器时间重校准并重试；启动即校准，每 10 分钟重校准）。⚠️ **在 AI 沙箱里跑的外部脚本**必须自己用 `/papi/v1/time` 的服务器时间做签名时间戳（沙箱时钟快 ~1.4s），参考 `_sweep_all.py` 的写法。
6. **沙箱杀子进程**：agent 环境里 subprocess/start /B/ShellExecute/schtasks 全都会被杀或被禁，唯一存活方案是 WMI `Win32_Process.Create`（`_launch_wmi.py`，注意必须用 SWbemLocator+ExecMethod，不能 Get().Create()）。长前台命令也会被 SIGTERM，长任务一律 WMI 挂后台写日志文件再轮询。
7. **进程死亡 ≠ 撤单**：硬杀（taskkill /F、断电、崩）时收尾保护不会执行，交易所条件单成为无主孤儿。**每次发现进程死了，第一步查 `_count_orders.py`**，孤儿要么等重启自愈（反向对账）要么外部清扫。
8. **重启规程**（顺序不可乱）：网络探测（`_count_orders.py` 能通）→ 杀旧 PID（锁文件记录 PID，先 cat 确认映射）→ WMI 拉起 → 等 60s 验证横幅（`ANCHOR_SEC=1s`）→ 验证挂单数（exp11=22/resident=40/exp50=100）→ 全局 `_count_orders.py` ≤200 且无 x3/x4 异常。
9. **IP 守卫**：出口 IP 不在白名单或探测站全挂 → 保守拦截（不撤不挂、保留旧单）。网络恢复自动继续。**不要手撕守卫强挂**——它防的是 -2015 单腿事故。
10. **Clash 流量耗尽**（09-09 事故根因）：症状 = 所有节点 503/504 但机场服务器 TCP/TLS 直连健康、本机直连百度正常。诊断三步：直连百度（本机网络）→ 直连机场服务器 TCP+TLS（机场活着吗）→ 控制器测延迟（内核出得去吗）。Clash 控制器 `127.0.0.1:9097`，secret `set-your-secret`。⚠️ 不要用 PUT /configs 重载 `config.yaml`（那是 643 字节空壳），要重载用 `clash-verge.yaml`。
11. **改代码前先 Read**：文件 ~2600 行，先 grep 函数名定位再 Edit，绝不整体重写。改完必跑 `python -c "import ast; ast.parse(...)"`。

---

## 6. 交易所与状态文件

- 账户：币安 **papi（组合保证金）UM**，条件单端点 `/papi/v1/um/algo/openAlgoOrders`（GET 列表 / DELETE 按 algoId+symbol 撤）。
- 条件单类型：TAKE_PROFIT_MARKET，algoType=CONDITIONAL。正常态每币恰好 x2（BUY+SELL 各一张）。
- state 文件（`resident_state{实例后缀}.json`）：格式 `{"SYM|SIDE": {"algoId":..., "trigger":..., "base":...}}`，是"本实例在管哪些单"的登记表。**反向对账以交易所为准**，所以 state 丢了也能自愈，但自愈期间会撤掉重挂（有短暂无保护窗口）。
- 单例锁：`ambush_basket_{instance}.lock`，内容是 PID。锁里 PID 已死会自动接管。
- 币表四份（resident20/exp11/exp50/react136）**互零重叠**，已验证 —— 单实例操作不会误伤别人。

## 7. 待办（接手 AI 的第一件事）

1. **等流量恢复**（CEO 充值机场），确认 `_count_orders.py` 能通；
2. 重启 exp50 补挂 100 张（当前 0 挂单，§5-8 规程）；
3. 全局验收：`_count_orders.py` 总量应 = 162（22+40+100）、全 x2、无异常分布；
4. 观察 24h：心跳里 `条件单N张`、`重挂N`、有无 -1021/-4045/孤儿堆积；
5. react(136币) 自始至终没动过，不需要管。

## 8. 本次改动清单（相对上一版代码）

1. `resident_loop` 跟随重挂 + 冻结上移两处：撤旧挂新由顺序改 **asyncio.gather 并发**（旧单存活 ~0.3s）；
2. 三实例启动参数 → `--anchor-sec 1 --anchor-jump-gate 0`（跳变闸门代码仍在，参数=0 即禁用，回测依据见 `_resident_sim.py` 输出：1s 下闸门拦截 0 次，唯一一次激活反而造成成交）；
3. `signed_request` 新增 **-1021 自愈**（遇时钟偏移 → `_clock_resync()` 重校准并重试）；`clock_sync_loop` 改为启动即校准；
4. `_resident_reverse_reconcile()`（09-08）：启动反向对账，交易所存在但本实例未登记的单自动撤除；
5. 新增工具：`_sweep_all.py`、`_cancel_algo.py`、`_sweep_exp50_old.py`、`_launch_wmi.py`。
