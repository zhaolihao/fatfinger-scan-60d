# ambush_basket 实盘脚本 — 接手说明书

> 给另一位 AI / 接手者阅读。本文件描述如何**安全地把这套 Binance 乌龙指埋伏实盘脚本跑起来**，以及当前已知的坑。
> 最后更新：2026-09-03 09:50（北京时间）。账户深度已调为 `--depth 0.08`（更易触发成交）；bot 当前运行中，处于 `-4400` 冷却 parking 态（0 挂单、每 30 轮自动重试，等币安窗口滚过自愈）。**⚠️ 见 4.3 节：`-4400` 期间禁止重启，会让愈合清零。**

---

## 0. 一句话是什么

`ambush_basket.py` 是一个 **Binance U 本位合约（UM）乌龙指插针埋伏机器人**：

- 对 **58 个「惯犯币」**（历史上频繁乌龙指的合约，见 `contract_spike_scan/data/top58_offenders.json`），每个币每边（BUY 低价 / SELL 高价）挂一张 **GTC 限价单**，距离当前中间价 ±8%（`--depth 0.08`，2026-09-03 由 10% 下调以更易触发成交）。
- 一旦某币闪崩/闪涨插针触发其中一张单 → 成交 → 立刻按路线 A（N 秒内回归 ≥3% 就平）或路线 B（强制持有 N 秒后平）平仓获利。
- 每分钟边界：撤掉自己 track 的旧单 → 按新中间价重挂双侧埋伏单。
- 架构：1 条组合 WS(bookTicker) 取全币实时中间价 + 1 条 listenKey 用户数据流收成交推送（零轮询，不撞限流）。

**这不是回测，是实盘。** 跑之前务必读完下面的「已知致命问题」。

---

## 1. 环境依赖

| 依赖 | 说明 |
|---|---|
| Python | 3.10+（实测 3.13 可跑） |
| 第三方包 | `httpx`、`websockets`（纯标准库之外仅这两个） |
| 代理 | **必须**本机运行 Clash/FlClash，HTTP 代理在 `127.0.0.1:7890`（代码里 `PROXY` 写死，不可改环境变量） |
| Binance 账户 | 必须是 **Portfolio Margin（组合保证金）** 账户，开通 U 本位合约交易 |
| API key | 带**交易权限**的 key，且**强制开启 IP 访问限制**（币安对交易 key 的硬性要求） |
| FlClash 外部控制器 | 建议开启 Clash 的 External Controller（默认 `127.0.0.1:9090`，无需 secret），用于自动切白名单节点（`fix_clash_node.py` 需要） |

装包：
```bash
pip install httpx websockets
```

---

## 2. 配置文件 `.env`

脚本读取同目录下的 `.env`（**已被 .gitignore 排除，不会上传**，需手动放置）。复制 `.env.example` 改之：

```bash
cp .env.example .env
# 然后填 BN_KEY / BN_SECRET / BN_IP
```

- `BN_KEY` / `BN_SECRET`：币安 API key/secret（带交易权限）。
- `BN_IP`：币安后台「API管理 → IP访问限制」里**允许**的出口 IP，逗号分隔。
  - ⚠️ 这个 IP 必须**等于 Clash 当前出口节点 IP**，否则每个请求都回 `-2015` 拒绝。
  - Clash 节点池常轮换 IP → 这就是整套系统最大的运维痛点（见 §4.2）。

> **另一个 AI 接手注意**：你 clone 下来后**没有 `.env`**，必须自己建。API key 由用户给你，或你用自己的测试 key（但测试 key 也要加白名单 IP）。

---

## 3. 如何运行

### 3.1 冒烟自测（不用 key，先确认代码能跑）
```bash
python ambush_basket.py --smoke
```
只用公开行情自测 30 秒，验证 WS/计算无 import 错误。**不会下任何单。**

### 3.2 小样实盘（先拿 2 个币试，强烈建议第一次接手先跑这个）
```bash
python ambush_basket.py --symbols VELVETUSDT,HUSDT --max-rounds 3
```
只跑 2 个币、3 轮就停。用来验证：key 能下单、IP 白名单通、撤单/平仓逻辑正常。

### 3.3 全量实盘（58 币无限轮）
```bash
python ambush_basket.py --notional 10 --depth 0.08 --target 0.03 --ta 3 --hold-b 5
```
参数含义（`--help` 可见全部）：
- `--notional`：每侧名义本金（USDT），默认 10
- `--depth`：埋伏深度，0.10 = 距中间价 ±10%，默认 0.10
- `--target`：路线 A 回归目标，默认 0.03（3%）
- `--ta`：路线 A 监控窗口秒数，默认 3
- `--hold-b`：路线 B 强制平仓秒数，默认 5
- `--max-rounds 0`：无限轮（默认）；设 N 跑 N 轮停
- `--no-lev`：跳过设置最高杠杆（调试用）
- `--ip`：手动覆盖允许的出口 IP（默认取 `.env` 的 `BN_IP`）
- `--reprice 0.02`：**重挂阈值（默认 2%）**。价格相对上次挂单价偏移 ≥ 此值才撤旧挂新；否则**保留原单不撤不挂**（继续接针）。这是为**降低撤挂比、避免 `-4400` 量化限制复发**加的核心优化——平静市可把每分钟 116 笔撤挂降到接近 0。插针成交后自动清锚价，平完仓必重挂。

**后台运行**（推荐，断会话不中断）：
```bash
nohup python ambush_basket.py --notional 10 --depth 0.08 --target 0.03 --ta 3 --hold-b 5 > /dev/null 2>&1 &
# 或 Windows：
start /B python ambush_basket.py ...
```

### 3.4 单实例锁（重要）
脚本启动会写 `ambush_basket.lock`（PID）。**同一台机器只能跑一个实例**，否则会双开 → 每个币挂 4 单 → 孤儿单爆仓（详见 §4.1 事故）。
- 锁冲突时启动会拒绝。
- 正常退出（Ctrl+C / `--max-rounds` 跑完）会清锁。
- 异常被杀（任务管理器/kill -9）锁可能残留 → 确认无进程后手动删 `ambush_basket.lock` 再启动。

---

## 4. 已知致命问题（接手者必读）

### 4.1 ⚠️ 双实例 → 孤儿单事故（已修复，但机制要懂）
**现象**：账户挂单数从预期的 116（58 币 × 2 边）暴涨到 200+。
**根因**：两个 `ambush_basket.py` 实例同时跑 + Clash 漂移导致撤单失败("-2015")，旧单撤不掉又叠加新单，每币累积 4 单。
**已做的防护**：① 单实例 PID 锁；② 撤单只撤自己 track 的 orderId（绝不 `cancelAll`，避免误伤同账户其他策略如 BOMEUSDC/BTCUSDC 网格）；③ 监控加了「账户实查对账」（见 §5）。
**接手铁律**：**永远只跑一个实例**；换机器/重启前先确认旧进程已死、锁已清。

### 4.2 ⚠️ Clash 出口 IP 漂移 → `-2015`（最大运维痛点）
**现象**：脚本突然大量 `retries_exhausted` / `-2015`，挂不上单、平不掉仓。
**根因**：Clash 负载均衡在多个节点间轮换，节点出口 IP 经常**不在币安白名单** → 币安拒 `-2015`。即使节点「名字」固定（如「香港HK-A」），该节点背后也是**多 IP 池**，会漂。
**代码里的自愈**：`signed_request` 现在会**解析真实错误码**——遇到 `-2015/-1021/-1022` 等直接 return 真实错误（不再误报成 `retries_exhausted`）；IP 守卫每轮查出口 IP，不在白名单就**只撤不挂**，杜绝「单边成交、对侧平不掉」的单腿事故；平仓无限重试，IP 漂回白名单即平掉。
**接手要做（外部修复，代码管不了）**：
1. **最快**：把当前 Clash 出口 IP 加进币安 key 的 IP 白名单（治标，节点池 >4 仍会冒新 IP）。
2. **钉节点**：FlClash GUI 把主代理组从「负载均衡/自动」钉到单一节点（节点 IP 也可能变）。
3. **根治**：上 **VPS 固定 IP 代理**（香港轻量 ~20-30 元/月），白名单填服务器 IP —— 挂机长跑唯一稳妥方案。
**自动化切节点**：`fix_clash_node.py` 经 Clash 9090 API 把「节点选择」selector 切到出口 IP 在白名单的节点：
```bash
python fix_clash_node.py
```
测漂移频率（量化要不要上 VPS）：
```bash
python drift_sample.py   # 30 次 × 3 秒采样，输出非白名单占比
```

### 4.3 ⚠️ 币安账户级风控 `-4400`（交接时**正处于此状态**）
**现象**：IP 白名单正常、key 正确，但**任何新挂单/开仓**都失败：
```
-4400 "Futures Trading Quantitative Rules violated, only reduceOnly order is allowed"
```
**根因**：币安对账户实施了「量化交易规则」限制，**只允许减仓单（reduceOnly），禁止新开仓/挂双向埋伏单**。这是**账户级**限制，不是某个币的问题（连 BTCUSDT 都挂不了）。**真正触发原因是订单/成交比过高**（每分钟撤挂 116 笔、成交≈0，撤挂比数千:1），**不是换 IP**（IP 漂移触发的是另一套 `-2015` 鉴权重控）。
**交接时状态**：账户 **0 挂单**、bot 已停。之前挂上的 114 单已用 `cancel_all_ambush.py` 全部安全撤掉（未误伤其他策略）。该限制约半天可自动解除（实测前晚 23:52 触发 → 次日 06:26 自动恢复）。
**已加的缓解（代码层）**：`--reprice 0.02`（默认 2%）——价格相对上次挂单价偏移 ≥2% 才撤旧挂新，否则保留原单。平静市把每分钟 116 笔撤挂降到接近 0（实测 5 轮仅 ~59 笔，降约 90%），从源头压低撤挂比、**大幅降低 `-4400` 复发概率**。阈值可调：波动大的市调小（如 0.01），想更激进降频调大（如 0.05）。
**接手必做**：
1. **先确认限制是否解除**——诊断脚本 `diag_ip.py` 或手动下一笔测试限价单，看是否还 `-4400`。
2. **限制解除前不要启动 bot**，否则每分钟徒劳重试。
3. 解除方式通常是等币安自动解封（数小时~数天），或联系客服申诉，或换子账户/新账户跑。

> 🚨 **`-4400` 期间绝对不要重启正在运行的 bot**（2026-09-03 血的教训）：`-4400` 靠「零订单活动的静默时间」让滚动窗口慢慢愈合。bot 原地冷却 parking（零撤挂）约 68 分钟才挂回 116；一旦强杀+`cancel_all_ambush.py` 撤单+新 bot 重挂这一连串操作，会立刻把比值重新顶过线、-4400 全币种复活、覆盖归零，**愈合进度直接清零**。正确做法：让 bot 原地跑，它每 30 轮自动重试，窗口滚过即自愈。**宁可等，不要重启。**

---

## 5. 监控与诊断工具

| 脚本 | 用途 |
|---|---|
| `ambush_monitor.py` | 持续监控：读最新日志 + **账户实查** `papi/v1/um/openOrders`，对比预期 116 单。埋伏币 >130 或任意币 >2 单 → 告警（双实例/孤儿单）。`python ambush_monitor.py` |
| `check_orders.py` | 一键查账户实时挂单：总数 / 埋伏币数 / 重复(>2)币。 |
| `cancel_all_ambush.py` | **安全清场**：只撤 58 个 ambush 币的挂单（不碰其他策略），用于 `-4400` 限制期或紧急止损。 |
| `diag_ip.py` | 诊断：用真实签名请求打币安，打印真实错误码（区分 `-2015` IP 未白名单 vs 真网络异常）。 |
| `drift_sample.py` | 量化 Clash 漂移：30 次采样，输出非白名单 IP 占比。 |
| `fix_clash_node.py` | 经 Clash 9090 API 自动把 selector 切到白名单 IP 节点。 |

**诊断流程建议（每次启动前）**：
```bash
python drift_sample.py      # 1) 看 IP 漂不漂
python diag_ip.py           # 2) 看 key/IP/限制状态（是否还 -4400）
python check_orders.py      # 3) 看账户当前有什么单
```
三步都绿（IP 白名单、无 -4400、单数和预期一致）再启动 bot。

---

## 6. 安全设计（代码已内置，了解即可）

- **只撤自己记录的 orderId**：`tracked_orders` 字典维护本进程下的单，撤单只撤这些，**绝不用 `cancelAll`**，保护同账户其他策略（BOMEUSDC/BTCUSDC 网格等）。
- **启动不自动 flatten 既有持仓**：只告警，不替你平掉别人的仓。
- **撤单查不到终态 → 保留追踪下一轮再试**：杜绝孤儿单。
- **WS 掉线自动重连** + 每 60s REST 兜底核对挂单（防漏成交事件）。
- **退出兜底（Ctrl+C / max-rounds）**：撤自己的单 + 市价平自己开的仓。
- **IP 守卫**：见 §4.2。

---

## 7. 交接时的事实状态（2026-09-03 00:11 北京时间）

- 当前分支：`contract-spike-scan`
- 账户挂单：**0**（已用 `cancel_all_ambush.py` 全撤）
- bot 进程：**已停**
- 币安限制：**`-4400` 量化规则限制中**（只允许 reduceOnly，禁止新挂单）—— 启动 bot 前必须先确认解除
- Clash 出口 IP（最近一次）：`45.149.92.90`（在白名单内，但节点会漂）
- 篮子文件：`contract_spike_scan/data/top58_offenders.json`（58 币，bot 默认读取）

---

## 8. 接手者第一步 checklist

1. `pip install httpx websockets`
2. 建 `.env`（BN_KEY/BN_SECRET/BN_IP），key 加白名单 IP
3. 确认 Clash 在 `127.0.0.1:7890`，FlClash 外部控制器 9090 开着
4. `python ambush_basket.py --smoke` 确认代码无 import 错误
5. `python drift_sample.py` + `python diag_ip.py` 确认 IP 白名单且无 `-4400`
6. 小样：`python ambush_basket.py --symbols VELVETUSDT,HUSDT --max-rounds 3`
7. 小样正常 → 全量：`python ambush_basket.py --notional 10 --depth 0.08 --target 0.03 --ta 3 --hold-b 5`
8. 另开一个会话跑 `python ambush_monitor.py` 盯账户实单数

> 若第 5 步 `diag_ip.py` 仍报 `-4400`：**不要启动 bot**，等限制解除（或换账户）。
> 若 IP 漂得厉害：先 `python fix_clash_node.py`，仍频繁漂就上 VPS 固定 IP（§4.2）。
