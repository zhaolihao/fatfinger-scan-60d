# B2USDT T0007 逐笔分析与优化方案

## 一、事件回顾

| 时间(UTC) | 事件 | 价格 | 延迟 |
|-----------|------|------|------|
| 08:31:28.952 | 最后一波拉盘峰值 | 0.8834 | — |
| 08:31:28.955 | 砸盘开始(1ms暴跌5%) | 0.8340 | — |
| **08:31:28.958** | **WS bookTicker触发**(锚0.82525 +6%=0.8748) | ~0.870 | — |
| 08:31:29.007 | 市价卖空下单 x11 | ≈0.876 | **49ms** |
| 08:31:29.0xx | 交易所实际成交 | 0.857(avgPrice) | — |
| **08:31:29.177** | **REST轮询确认成交** @0.857 | 0.857 | **170ms** |
| 08:31:29.178 | on_fill启动, `await place_stop_algo` | — | — |
| ~29.378(估) | 止损挂完, 路线A开始轮询 | ~0.858 | **~200ms阻塞** |
| 08:31:29.187 | 价格反弹高点 | 0.8667 | — |
| 08:31:29.652 | 29秒平仓前最低 | 0.8504 | — |
| **08:31:29.654** | **路线A触发**(mid<=0.85826) | ~0.851 | **~270ms等待** |
| **08:31:29.754** | **平仓确认** @0.8531 | 0.8531 | **100ms API** |
| 08:31:29.812 | 全秒最低(平仓后56ms) | 0.8418 | — |

**实际pnl = +0.0363U | 理想pnl(0.865做空→0.850平仓) = +0.165U | 损失78%**

---

## 二、逐笔数据关键发现

### 做空方向纠正

做空 = 卖高买低。价格越低 → 买回越便宜 → 利润越高。

- **入场(卖空) 0.857** → **平仓(买回) 0.8531** → 净赚0.0039/单位
- 29秒有两个对做空有利的低点：
  - 29.652：0.8504（平仓前最低）
  - 29.812：0.8418（平仓后56ms出现的全秒最低，吃不到）

### 利润损失分解（做空方向）

| 环节 | 损失 | 原因 |
|------|------|------|
| 入场延迟 | 0.8834→0.857 = 0.0264/单位 | WS触发时已在峰值末端，不可避 |
| **止损阻塞** | missed立即平仓窗口 | `await place_stop_algo`阻塞200ms |
| **轮询等待** | 0.85826→0.851 missed | 价格反弹超目标，等270ms才跌回 |
| **市价滑点** | 0.851→0.8531 = 0.0021/单位 | 市价买入11个吃盘口滑点0.32% |
| 错失29.812低点 | 0.8531→0.8418 = 0.0113/单位 | 路线A已平仓，56ms后才出现 |

**最大可避免损失：市价滑点0.0231U（占实际利润的64%）**

---

## 三、当前on_fill流程（代码实际逻辑）

```
on_fill(order_id, entry_px, qty, sym):
  1. 撤同币其他挂单
  2. 统计/通知/登记仓位
  3. await place_stop_algo(...)        ← 阻塞 ~200ms（API往返）
  4. 计算路线A目标:
       SHORT: target = O * (1 + REB_PCT)  = 锚价 * 1.04
       LONG:  target = O * (1 - REB_PCT)  = 锚价 * 0.96
  5. 路线A轮询 (T_A=1s, 每0.05s查mid):
       if SHORT and mid <= target → market_close
       if LONG  and mid >= target → market_close
  6. 路线A未触发 → route_b_adaptive():
       ① 亏损上限8% → 硬砍
       ② 利润带O±6% → 平
       ③ 反转撤退1% → 平
       ④ 时间止损10s → 强制平
```

**问题：**
1. 步骤3阻塞200ms → 步骤5才开始轮询 → 入场时已满足条件但无法触发
2. 步骤5用市价平仓 → 滑点0.32%
3. `_poll_m_fill`首次轮询0.5s → 确认成交延迟

---

## 四、优化方案（5项）

### 建议1：止损改异步 + 加限价止盈（双轨止盈）

**改动位置：** `on_fill` 函数，约第930-960行

**当前代码：**
```python
await place_stop_algo(sym, pos_side, entry_px, sub, stop_pct=B_LOSS)  # 阻塞200ms
# ... 路线A轮询 ...
if route == "A":
    ok = await market_close(sym, pos_side, qty, entry_px, O, "A", ...)
```

**改为：**
```python
# (1) 止损异步挂（不阻塞，路线B 8%止损仍兜底）
spawn(place_stop_algo(sym, pos_side, entry_px, sub, stop_pct=B_LOSS))

# (2) 限价止盈异步挂（BUY/SELL LIMIT @ route_A_target，不阻塞）
#     SHORT: BUY LIMIT @ O*(1+REB_PCT)
#     LONG:  SELL LIMIT @ O*(1-REB_PCT)
#     价格已满足条件时立即在交易所成交，无滑点
#     未成交时挂在交易所，价格到达自动成交
target = O * (1 + REB_PCT) if pos_side == "SHORT" else O * (1 - REB_PCT)
spawn(place_limit_tp(sym, pos_side, qty, target, sub))

# (3) 市价止盈同步进行（现有路线A轮询，不变）
#     如果限价单先成交完 → 路线A检测到仓位已空，跳过
#     如果路线A先触发 → market_close + 撤限价单
```

**新增函数 `place_limit_tp`：**
```python
async def place_limit_tp(sym, pos_side, qty, tp_px, sub=""):
    """限价止盈单（异步）：在route A目标价挂BUY/SELL LIMIT平仓。
    与市价路线A并行，先成交者胜出。"""
    side = "BUY" if pos_side == "SHORT" else "SELL"  # 平仓方向
    params = {
        "symbol": sym, "side": side, "positionSide": pos_side,
        "type": "LIMIT", "timeInForce": "GTC",
        "price": f"{rnd_price(sym, tp_px):.8f}".rstrip("0").rstrip("."),
        "quantity": f"{qty:.8f}".rstrip("0").rstrip("."),
    }
    c, r = await asyncio.to_thread(signed_request, "POST", _ep("order"), params, tries=3)
    if c == 200:
        tp_limits[(sym, pos_side, sub)] = int(r["orderId"])
        log.info(f"[限价止盈] {sym} {pos_side} LIMIT @{r.get('price','?')} "
                 f"(oid={r['orderId']}) → 异步等待成交, 与市价路线A并行")
    else:
        log.warning(f"[限价止盈✗] {sym} {pos_side} 挂单被拒: {str(r)[:70]} → 仅靠市价路线A")
```

**路线A触发时撤限价单（已有逻辑）：**
- `market_close` 成功后已有 `await cancel_tp_limit(sym, pos_side, sub=sub)` 调用
- 限价单先成交时，路线A轮询中 `mid(sym)` 检测到仓位已空 → 跳过

**限价单成交后的处理：**
- 需要监听WS用户流成交事件 → 检测到限价止盈单成交 → `positions_open.pop()` + 撤止损 + 统计pnl
- 或依赖现有兜底轮询 `fallback_poll`（60s间隔太慢，需要更快的检查）
- **建议新增：** 限价止盈单挂出后，同时spawn一个0.1s轮询检查该orderId状态，成交后清理

**预期效果：**
- T0007案例：入场时price=0.8536 < target=0.85826 → 限价单立即成交在~0.8536（best ask）
- vs 市价平仓0.8531 → 限价可能省0.0021/单位的滑点 = 0.0231U
- 限价单有价格保护，不会吃穿过目标价的盘口

**风险：**
- 薄盘口时限价单可能部分成交 → 剩余量由市价路线A兜底
- 限价单挂出有~100ms API延迟 → 如果价格在这100ms内快速回弹超过目标，限价单不会成交，市价路线A也不触发 → 进入路线B（可接受，路线B利润带更宽）

---

### 建议2：入场即检查路线A

**改动位置：** `on_fill` 函数，路线A轮询之前

**在路线A轮询前加一段：**
```python
# 入场即检查：如果入场价已满足路线A条件，直接市价平仓
target = O * (1 + REB_PCT) if pos_side == "SHORT" else O * (1 - REB_PCT)
if (pos_side == "SHORT" and entry_px <= target) or \
   (pos_side == "LONG"  and entry_px >= target):
    # 入场价已在目标内，立即平仓（限价止盈已经在路上，这里用市价兜底）
    ok = await market_close(sym, pos_side, qty, entry_px, O, "A立即", ...)
    if ok:
        positions_open.pop((sym, pos_side, sub), None)
        await cancel_stop_algo(sym, pos_side, sub=sub)
        await cancel_tp_limit(sym, pos_side, sub=sub)  # 撤限价止盈
    return
```

**预期效果：**
- T0007：entry=0.857 < target=0.85826 → 立即触发
- 省掉200ms止损阻塞 + 270ms轮询等待 = 省470ms
- 平仓在29.178（成交确认时刻），价格~0.8536

**注意：** 限价止盈（建议1）已经异步挂出，可能在market_close之前就已成交。需要检查仓位是否已空，避免重复平仓。

---

### 建议3：路线A平仓用限价单

**改动位置：** `market_close` 函数或在路线A触发时

**当前路线A触发后调用 `market_close`（MARKET order）：**
```python
ok = await market_close(sym, pos_side, qty, entry_px, O, "A", ...)
```

**改为限价单：**
```python
# 路线A触发时，用LIMIT @ target平仓（已有建议1的限价单在跑，这里其实可以省略）
# 如果建议1已实施，路线A的市价平仓主要是作为限价单未成交时的兜底
# 保持market_close不变即可，因为限价单已经在跑
```

**实际上建议1已经覆盖了这个需求** — 限价止盈单在route A目标价挂着，如果价格到达目标会自动成交。市价路线A是限价单的兜底。

**如果限价单因API延迟未挂上** → 市价路线A仍会触发 → market_close仍可用。

**结论：建议3被建议1覆盖，不需要单独改动。** 市价路线A保持不变作为兜底。

---

### 建议4：_poll_m_fill 轮询改快（仅开仓查询时）

**改动位置：** `_poll_m_fill` 函数，第1951行

**当前：**
```python
for i in range(30):
    await asyncio.sleep(0.5)    # ← 首次等0.5s
    # ...查订单状态...
```

**改为：**
```python
for i in range(150):            # 15s / 0.1s = 150次（总时间不变）
    await asyncio.sleep(0.1)    # ← 首次等0.1s
    # ...查订单状态...
```

**API频率：** 0.1s间隔 = 10次/s，GET /fapi/v1/order weight=1，远低于1200/min限制。
**仅在开仓后运行：** `_poll_m_fill` 只在`_fire_cmp_m`发出市价单后spawn，不是持续运行。无触发时无轮询。

**预期效果：**
- T0007：首次轮询从0.5s提前到0.1s → 成交确认从29.177提前到~29.1xx → 省~400ms
- on_fill提前400ms启动 → 止损提前挂 → 路线A提前开始 → 整体提前400ms

---

### 建议5：路线A改用WS bookTicker事件驱动（替代轮询）

**当前：** 路线A用 `mid(sym)` 轮询，每0.05s查一次。`mid(sym)`读的是WS bookTicker推送的`mids[sym]`，已经是WS数据，但检查时机受0.05s轮询间隔限制。

**改为：** 在WS bookTicker回调中，对持仓币种直接检查路线A条件：

```python
# 在WS bookTicker回调中（约第649行）：
mids[d["s"]] = {"bid": float(d["b"]), "ask": float(d["a"]), "ts": int(d.get("T") or time.time()*1000)}

# 新增：对持仓币种检查路线A条件
sym = d["s"]
m = (float(d["b"]) + float(d["a"])) / 2
for (s, ps, sub), p in list(positions_open.items()):
    if s != sym:
        continue
    target = p["O"] * (1 + REB_PCT) if ps == "SHORT" else p["O"] * (1 - REB_PCT)
    if (ps == "SHORT" and m <= target) or (ps == "LONG" and m >= target):
        # WS推送触发路线A → 不用等0.05s轮询
        spawn(_ws_route_a_trigger(sym, ps, sub, p))
```

**新增函数：**
```python
async def _ws_route_a_trigger(sym, pos_side, sub, p):
    """WS bookTicker直接触发路线A平仓（替代0.05s轮询）。"""
    # 幂等：检查仓位是否还在（可能已被限价止盈或市价路线A平掉）
    if (sym, pos_side, sub) not in positions_open:
        return
    # 标记正在退出（防止与其他退出路径冲突）
    if (sym, pos_side, sub) in ws_exit_in_progress:
        return
    ws_exit_in_progress.add((sym, pos_side, sub))
    ok = await market_close(sym, pos_side, p["qty"], p["entry"], p["O"], "A", ...)
    if ok:
        positions_open.pop((sym, pos_side, sub), None)
        await cancel_stop_algo(sym, pos_side, sub=sub)
        await cancel_tp_limit(sym, pos_side, sub=sub)
    ws_exit_in_progress.discard((sym, pos_side, sub))
```

**预期效果：**
- 路线A从0.05s轮询改为WS推送即时触发 → 省最多0.05s延迟
- 对T0007影响不大（0.05s很小），但在高频场景下累积有效

**风险：**
- 增加WS回调路径的复杂度 → 需要确保幂等性（防止与市价路线A轮询、限价止盈重复触发）
- WS回调是高频路径 → 需要轻量检查（只查`positions_open`字典）

---

## 五、改动汇总

| # | 建议 | 改动文件 | 改动位置 | 改动内容 | 阻塞/非阻塞 |
|---|------|---------|---------|---------|------------|
| 1 | 止损改异步+限价止盈 | ambush_basket_fapi.py | on_fill (~L930) + 新函数 | `await place_stop_algo` → `spawn(place_stop_algo)`; 新增`place_limit_tp`异步挂限价止盈 | 非阻塞 |
| 2 | 入场即检查路线A | ambush_basket_fapi.py | on_fill (~L940) | 路线A轮询前加entry vs target检查 | 同步(但快速return) |
| 3 | (被建议1覆盖) | — | — | 限价止盈已在建议1中实现 | — |
| 4 | _poll_m_fill改0.1s | ambush_basket_fapi.py | _poll_m_fill (~L1951) | `sleep(0.5)` → `sleep(0.1)`, `range(30)` → `range(150)` | — |
| 5 | WS事件驱动路线A | ambush_basket_fapi.py | WS回调 (~L649) + 新函数 | bookTicker回调中检查持仓路线A条件 | 非阻塞 |

---

## 六、我的问题

1. **限价止盈单成交后的仓位清理：** 限价止盈单在交易所成交后，我们需要检测到并清理`positions_open`、撤止损、统计pnl。目前有两条路径：(a) WS用户流成交推送 (b) REST轮询。WS用户流在Clash代理下可能延迟11s+。建议新增一个0.1s间隔的REST轮询专门查限价止盈单状态，成交后立即清理。这个轮询只在该币有未成交限价止盈单时运行，不持续。

2. **限价止盈与市价路线A的冲突：** 两者并行可能同时触发。需要用`exits_in_progress`或类似的幂等机制确保只平一次。当前`on_fill`已有`exits_in_progress`检查，但限价止盈是独立路径，需要自己的幂等集合。

3. **限价止盈部分成交：** 如果限价止盈单只成交了部分（薄盘口），剩余仓位怎么办？建议：(a) 限价单用`closePosition=true`而非固定qty，让交易所处理全平 (b) 或者部分成交后，市价路线A检测到剩余仓位继续平。

4. **WS事件驱动的幂等性：** 建议5的WS回调触发路线A，需要确保不与市价路线A轮询、限价止盈同时触发。需要一个统一的退出锁。

---

## 七、专业优化建议

1. **优先级排序：** 建议4（_poll_m_fill改0.1s）改动最小、风险最低、收益最直接 → **建议第一个改**。建议1+2（止损异步+入场即检查+限价止盈）改动较大但收益最大 → **第二个改**。建议5（WS事件驱动）最复杂但提升最小 → **可选，延后**。

2. **限价止盈用closePosition=true：** 建议限价止盈单用`closePosition=true`而非固定qty。这样即使部分平仓后又有加仓，限价单也会平掉全部仓位。同时避免了部分成交的残留仓位问题。

3. **限价止盈单的价格：** 应该挂在route A目标价（O*(1±REB_PCT)），不是TP_PCT价格。这两个价格目前不同：route A用锚价±4%，TP_PCT用成交价±3%。限价止盈的目的是替代route A的市价平仓，所以应该用route A的目标价。

4. **分步实施：** 建议先改建议4（5分钟改动），实测确认效果后再改建议1+2（需30分钟+测试）。建议5观察实测数据后再决定是否值得复杂度。

5. **回测对比：** 改动后可以跑相同的时段对比触发→成交→平仓的延迟变化，确认优化效果。建议在日志中增加`entry_to_close_ms`字段方便统计。
