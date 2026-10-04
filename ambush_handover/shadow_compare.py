#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shadow_compare.py — bookTicker vs aggTrade 插针检测对照实验
=============================================================
对照组设计：
  A组(对照组): bookTicker中间价检测（与生产217脚本完全相同）
  B组(实验组): aggTrade成交价检测
  两组共用同一锚价（bookTicker中间价，每1秒刷新，anchor_jump_gate=0.08）
  不下任何真实订单，纯观测记录。

输出：
  logs/shadow_compare_YYYYMMDD_HHMMSS.log — 详细日志（每条信号/近miss）
  logs/shadow_compare_signals.csv         — 信号记录CSV（方便分析）

启动：
  python3 shadow_compare.py
"""
import asyncio
import json
import time
import logging
import os
import csv
from datetime import datetime
from urllib.parse import quote
from collections import defaultdict

import websockets

# ── 配置 ──────────────────────────────────────────────
DEPTH = 0.08           # 触发阈值 8%
ANCHOR_SEC = 1         # 锚价刷新周期 1秒
ANCHOR_JUMP_GATE = 0.08  # 锚价跳变闸门 8%
WS_CHUNK = 50          # 每WS连接最多订阅币数
SYMBOLS_FILE = "planR_217.json"
LOG_DIR = "logs"
NEAR_MISS_RATIO = 0.5  # 近miss门槛 = DEPTH × 0.5 = 4%

# ── 状态 ──────────────────────────────────────────────
mids = {}              # sym -> {"bid","ask","mid","ts"}  from bookTicker
last_trade = {}        # sym -> {"price","qty","ts"}      from aggTrade
anchors = {}           # sym -> float
anchor_ts = {}         # sym -> last update epoch
bt_counts = defaultdict(int)  # per-sym bookTicker 消息计数
at_counts = defaultdict(int)  # per-sym aggTrade 消息计数
bt_signals = []        # [(ts, sym, price, anchor, dev, side)]
at_signals = []
bt_near = []
at_near = []
# 去重：同币同侧 5 秒内只记一次
_bt_dedup = {}  # sym+side -> last_ts
_at_dedup = {}

# ── 日志 ──────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
_ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
log_path = os.path.join(LOG_DIR, f"shadow_compare_{_ts_str}.log")
csv_path = os.path.join(LOG_DIR, "shadow_compare_signals.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("shadow_compare")

# CSV writer
csv_file = open(csv_path, "w", newline="")
csv_writer = csv.writer(csv_file)
csv_writer.writerow(["datetime", "source", "symbol", "price", "anchor",
                      "dev_pct", "side", "type"])
csv_file.flush()


# ── 信号检测 ──────────────────────────────────────────
def check_signal(source, sym, price, ts_ms):
    """检查价格是否偏离锚价超过阈值。"""
    anchor = anchors.get(sym)
    if anchor is None or anchor <= 0:
        return
    dev_down = (anchor - price) / anchor  # LONG: 价格下跌
    dev_up = (price - anchor) / anchor    # SHORT: 价格上涨
    dev = max(dev_down, dev_up)
    side = "LONG" if dev_down >= dev_up else "SHORT"

    if dev >= DEPTH:
        _log_signal(source, sym, price, anchor, dev, side, "SIGNAL", ts_ms)
    elif dev >= NEAR_MISS_RATIO * DEPTH:
        _log_signal(source, sym, price, anchor, dev, side, "NEAR", ts_ms)


def _log_signal(source, sym, price, anchor, dev, side, sig_type, ts_ms):
    """记录信号到日志和CSV。含5秒去重。"""
    dedup_key = f"{sym}_{side}"
    dedup_map = _bt_dedup if source == "bookTicker" else _at_dedup
    now = time.time()
    if dedup_key in dedup_map and now - dedup_map[dedup_key] < 5.0:
        return
    dedup_map[dedup_key] = now

    ts_str = datetime.utcfromtimestamp(ts_ms / 1000).strftime("%H:%M:%S.") + f"{ts_ms % 1000:03d}"
    log.info(f"[{sig_type}] {ts_str} src={source} sym={sym} "
             f"price={price:.8g} anchor={anchor:.8f} dev={dev:.2%} side={side}")
    csv_writer.writerow([ts_str, source, sym, f"{price:.8g}",
                         f"{anchor:.8f}", f"{dev:.4f}", side, sig_type])
    csv_file.flush()

    if sig_type == "SIGNAL":
        if source == "bookTicker":
            bt_signals.append((ts_ms, sym, price, anchor, dev, side))
        else:
            at_signals.append((ts_ms, sym, price, anchor, dev, side))
    else:
        if source == "bookTicker":
            bt_near.append((ts_ms, sym, price, anchor, dev, side))
        else:
            at_near.append((ts_ms, sym, price, anchor, dev, side))


# ── WS bookTicker ─────────────────────────────────────
async def ws_bookticker_loop(symbols_chunk, chunk_idx):
    """订阅一批 symbol 的 bookTicker。"""
    streams = "/".join(f"{s.lower()}@bookTicker" for s in symbols_chunk)
    uri = f"wss://fstream.binance.com/stream?streams={quote(streams, safe='')}"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20,
                                          ping_timeout=30, open_timeout=30) as ws:
                log.info(f"[WS] bookTicker#{chunk_idx} connected ({len(symbols_chunk)}s)")
                async for raw in ws:
                    d = json.loads(raw).get("data") or json.loads(raw)
                    if d.get("e") != "bookTicker":
                        continue
                    sym = d["s"]
                    bid = float(d["b"])
                    ask = float(d["a"])
                    mid = (bid + ask) / 2
                    ts = int(d.get("T") or d.get("E") or time.time() * 1000)
                    mids[sym] = {"bid": bid, "ask": ask, "mid": mid, "ts": ts}
                    bt_counts[sym] += 1
                    check_signal("bookTicker", sym, mid, ts)
        except Exception as e:
            log.warning(f"[WS] bookTicker#{chunk_idx} disconnected: {str(e)[:80]} → 5s reconnect")
            await asyncio.sleep(5)


# ── WS aggTrade ───────────────────────────────────────
async def ws_aggtrade_loop(symbols_chunk, chunk_idx):
    """订阅一批 symbol 的 aggTrade（逐笔成交）。"""
    streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols_chunk)
    uri = f"wss://fstream.binance.com/stream?streams={quote(streams, safe='')}"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20,
                                          ping_timeout=30, open_timeout=30) as ws:
                log.info(f"[WS] aggTrade#{chunk_idx} connected ({len(symbols_chunk)}s)")
                async for raw in ws:
                    d = json.loads(raw).get("data") or json.loads(raw)
                    if d.get("e") != "aggTrade":
                        continue
                    sym = d["s"]
                    price = float(d["p"])
                    ts = int(d.get("T") or d.get("E") or time.time() * 1000)
                    last_trade[sym] = {"price": price, "ts": ts}
                    at_counts[sym] += 1
                    check_signal("aggTrade", sym, price, ts)
        except Exception as e:
            log.warning(f"[WS] aggTrade#{chunk_idx} disconnected: {str(e)[:80]} → 5s reconnect")
            await asyncio.sleep(5)


# ── 锚价刷新 ──────────────────────────────────────────
async def anchor_loop():
    """每 ANCHOR_SEC 秒刷新锚价，用 bookTicker 中间价。与生产217脚本逻辑一致。"""
    while True:
        now = time.time()
        # 对齐到下一秒边界
        boundary = (int(now) // ANCHOR_SEC + 1) * ANCHOR_SEC
        await asyncio.sleep(max(0.05, boundary - now))

        now = time.time()
        updated = 0
        skipped_jump = 0
        for sym in list(mids.keys()):
            m = mids.get(sym)
            if m is None:
                continue
            if now * 1000 - m["ts"] > 10_000:  # 超过10秒未更新 → stale
                continue
            new_anchor = m["mid"]
            old = anchors.get(sym)
            if old and ANCHOR_JUMP_GATE > 0:
                jump = abs(new_anchor - old) / old
                if jump > ANCHOR_JUMP_GATE:
                    skipped_jump += 1
                    continue  # 跳变太大，不更新（防针尖污染）
            anchors[sym] = new_anchor
            anchor_ts[sym] = now
            updated += 1


# ── 定期统计 ──────────────────────────────────────────
async def stats_loop():
    """每60秒输出统计。"""
    last_bt = 0
    last_at = 0
    while True:
        await asyncio.sleep(60)
        bt_total = sum(bt_counts.values())
        at_total = sum(at_counts.values())
        bt_active = len([s for s in bt_counts if bt_counts[s] > 0])
        at_active = len([s for s in at_counts if at_counts[s] > 0])
        bt_new = bt_total - last_bt
        at_new = at_total - last_at

        log.info(f"[STATS] bookTicker: {bt_new}msg/min ({bt_active} active) | "
                 f"aggTrade: {at_new}msg/min ({at_active} active) | "
                 f"anchors: {len(anchors)} | "
                 f"BT signals:{len(bt_signals)} near:{len(bt_near)} | "
                 f"AT signals:{len(at_signals)} near:{len(at_near)}")

        # 对比：哪些信号只被一方捕捉到
        if bt_signals or at_signals:
            bt_syms = {(s[1], s[5]) for s in bt_signals}  # (sym, side)
            at_syms = {(s[1], s[5]) for s in at_signals}
            both = bt_syms & at_syms
            only_bt = bt_syms - at_syms
            only_at = at_syms - bt_syms
            if only_bt or only_at:
                log.info(f"[COMPARE] both={len(both)} only_BT={len(only_bt)} "
                         f"only_AT={len(only_at)}")
                if only_bt:
                    log.info(f"  only_bookTicker: {list(only_bt)[:10]}")
                if only_at:
                    log.info(f"  only_aggTrade: {list(only_at)[:10]}")

        last_bt = bt_total
        last_at = at_total


# ── 加载币种 ──────────────────────────────────────────
def load_symbols():
    with open(SYMBOLS_FILE) as f:
        syms = json.load(f)
    if isinstance(syms, dict):
        syms = list(syms.keys())
    return syms


# ── 主函数 ────────────────────────────────────────────
async def main():
    symbols = load_symbols()
    log.info(f"{'=' * 70}")
    log.info(f"shadow_compare 启动 | {len(symbols)}币 | depth={DEPTH} | "
             f"anchor_sec={ANCHOR_SEC} | jump_gate={ANCHOR_JUMP_GATE}")
    log.info(f"日志: {log_path}")
    log.info(f"CSV:  {csv_path}")
    log.info(f"{'=' * 70}")

    # 拆分WS连接
    chunks = [symbols[i:i + WS_CHUNK] for i in range(0, len(symbols), WS_CHUNK)]
    log.info(f"WS分组: {len(chunks)}组 × 2频道(bookTicker+aggTrade) = {len(chunks)*2}连接")

    tasks = []

    # 先逐个启动 bookTicker（串行，等前一个连上再开下一个）
    for i, chunk in enumerate(chunks):
        task = asyncio.create_task(ws_bookticker_loop(chunk, i))
        tasks.append(task)
        await asyncio.sleep(5)

    # 再逐个启动 aggTrade
    for i, chunk in enumerate(chunks):
        task = asyncio.create_task(ws_aggtrade_loop(chunk, i))
        tasks.append(task)
        await asyncio.sleep(5)

    # 锚价刷新
    tasks.append(asyncio.create_task(anchor_loop()))

    # 定期统计
    tasks.append(asyncio.create_task(stats_loop()))

    log.info(f"全部启动完成，共 {len(tasks)} 个异步任务。开始对照观测...")

    # 等待所有任务（实际上不会退出）
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("收到 Ctrl+C，退出。")
        csv_file.close()
