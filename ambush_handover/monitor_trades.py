#!/usr/bin/env python3
"""
监控脚本（修女版 20260923）：观察 ambush_basket_local.py 的执行情况
- 日志匹配：ambush_basket_*.log（原脚本找 react136_*.log → 永远拿不到日志 = 瞎跑）
- 解码：errors="replace"（原脚本裸 open 遇非UTF-8字节直接抛异常 → 主循环每10s重试一轮, 监控瘫痪）
- 只消费增量（seek + last_pos），全量扫描只在换日志文件时做一次
- 检测: 成交 / 平仓 / 触发偏离度是否达标 / -2022重试 / WS延迟告警 / IP封禁
"""

import re
import json
import time
from datetime import datetime
from pathlib import Path

LOGDIR = Path(__file__).parent / "logs"
MONITOR_LOG = Path(__file__).parent / "monitor_trades.log"
STATE_FILE = Path(__file__).parent / "monitor_state.json"

# 当前运行的日志文件前缀（start_767.sh 的 LOG 命名）
LOG_GLOB = "ambush_basket_*.log"

RE_FILL = re.compile(r'\[成交\]\s+(\S+)\s+(LONG|SHORT)\s+@([\d.]+)\s+x([\d.]+)\s+cmp=(\S*)\s+tid=(\S+)')
RE_CLOSE = re.compile(r'\[平仓✓\].*?(\S+)\s+(LONG|SHORT)\s+路线(\S+)\s+出场@([\d.]+)\s+pnl=([+-][\d.]+)U')
RE_TRIGGER = re.compile(r'\[CMP触发[^\]]*\](?:\[[^\]]*\]\s*)*\s*(\S+)\s+tid=(\S+)\s+src=(\w+)\s+锚([\d.]+)\s+([+-]?\d+)%')
RE_WS_FAST = re.compile(r'\[WS快平\]\s+(\S+)\s+(LONG|SHORT)\s+@([\d.]+)\s+命中(\S+)\s+→\s+市价平仓')
RE_TP_FILLED = re.compile(r'\[TP限价成交✓\]\s+(\S+)\s+(LONG|SHORT)')
RE_2022 = re.compile(r'\[平仓✗\]\s+(\S+)\s+(LONG|SHORT)\s+第(\d+)次失败.*?-2022')
RE_BAN = re.compile(r'IP\(([^)]+)\) banned')
RE_LAT = re.compile(r'\[WS延迟')


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(MONITOR_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    print(f"[{ts}] {msg}", flush=True)


def load_state():
    if STATE_FILE.exists():
        try:
            return json.load(open(STATE_FILE, encoding="utf-8"))
        except Exception:
            pass
    return {"cur_log": "", "last_pos": 0, "trades_analyzed": []}


def save_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log(f"保存状态失败: {e}")


def get_latest_log():
    logs = sorted(LOGDIR.glob(LOG_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    if not logs:
        return None
    newest = logs[0]
    # 【别名日志防护】沙箱里同一进程的日志可能出现两个字节级完全相同的条目
    # (mtime纳秒级相同, 目录序不定)。锁定策略: 如果当前锁定的日志仍然在增长,
    # 继续用旧文件, 不切换 — 只在旧文件确实停止增长(1s以上无变化)且新文件更新时才换。
    state0 = load_state()
    cur = state0.get("cur_log") if state0 else ""
    if cur:
        cur_path = LOGDIR / cur
        if cur_path.exists():
            size_now = cur_path.stat().st_size
            size_prev = state0.get("cur_size", -1)
            if size_now > size_prev or size_now == size_prev and (newest.name == cur):
                return cur_path   # 当前日志还在增长(或静止但就是最新) → 不切
            # 当前文件已停止增长: 只有 newest 比它更新(mtime更大)才切换
            if newest.stat().st_mtime > cur_path.stat().st_mtime:
                return newest
            return cur_path
    return newest


def _read_new(log_file, last_pos):
    """增量读取, errors=replace 不炸非UTF-8字节。返回(新文本, 新offset)。"""
    size = log_file.stat().st_size
    if size < last_pos:
        return "", size          # 日志被轮转/清空 → 重置
    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
        f.seek(last_pos)
        chunk = f.read()
        return chunk, f.tell()


def check_new_trades():
    log_file = get_latest_log()
    if not log_file:
        return
    state = load_state()
    fname = log_file.name
    if state.get("cur_log") != fname:
        # 换新日志 → 从头读一次（抓启动参数行），offset从0起
        state["cur_log"] = fname
        state["last_pos"] = 0
        log(f"切换监控日志 → {fname}")
    new_content, state["last_pos"] = _read_new(log_file, state.get("last_pos", 0))
    state["cur_size"] = log_file.stat().st_size
    if not new_content:
        save_state(state)
        return

    # 启动参数确认（每次新日志打一次）
    if state.get("last_pos", 0) <= 8000 and "乌龙指埋伏篮子" in new_content:
        for ln in new_content.splitlines():
            if ln.startswith("乌龙指埋伏篮子") or ln.startswith("阈值DEPTH") or "对比模式CMP" in ln:
                log(f"[启动参数] {ln.strip()}")

    # 成交
    for m in RE_FILL.finditer(new_content):
        sym, side, price, qty, cmp_tag, tid = m.groups()
        anchor = None
        # 在本段增量里找同tid触发记录(通常在前几行)
        tm = RE_TRIGGER.search(new_content, max(0, m.start() - 4000), m.start())
        if tm and tm.group(2).split(":")[0] == tid.split(":")[0]:
            anchor = float(tm.group(4))
        info = f"成交: {tid} {sym} {side} @{price} x{qty} 腿{cmp_tag}"
        if anchor:
            a_pct = (anchor - float(price)) / anchor * 100 if side == "LONG" else (float(price) - anchor) / anchor * 100
            info += f" | 锚{anchor} 实际偏离{a_pct:.2f}% (阈值6%) {'✓达标' if a_pct >= 6.0 else '✗未达标'}"
        log(info)
        state.setdefault("trades_analyzed", []).append({
            "t": datetime.now().isoformat(), "sym": sym, "side": side,
            "price": float(price), "qty": float(qty), "tid": tid, "anchor": anchor})

    # WS快路径平仓（新路径, 必须监控到）
    for m in RE_WS_FAST.finditer(new_content):
        log(f"WS快平: {m.group(1)} {m.group(2)} @{m.group(3)} 命中{m.group(4)}")

    # TP限价成交（交易所侧平仓）
    for m in RE_TP_FILLED.finditer(new_content):
        log(f"TP限价成交(交易所侧): {m.group(1)} {m.group(2)}")

    # 正常平仓
    for m in RE_CLOSE.finditer(new_content):
        sym, side, route, exit_px, pnl = m.groups()
        log(f"平仓✓: {sym} {side} 路线{route} 出场@{exit_px} pnl={pnl}U")

    # -2022（修复后应只在-race残留时出现, 持仓=0立即终结）
    for m in RE_2022.finditer(new_content):
        sym, side, n = m.groups()
        log(f"⚠ -2022: {sym} {side} 第{n}次 (修复后: 第1次应即查持仓并终结)")
        if int(n) > 1:
            log(f"⚠⚠ -2022 已重试{n}次 → 修法7未生效? 立即检查!")

    # IP封禁 / WS延迟
    for m in RE_BAN.finditer(new_content):
        log(f"⚠ IP封禁: {m.group(1)} banned")
    if RE_LAT.search(new_content):
        for ln in [l for l in new_content.splitlines() if "WS延迟" in l]:
            log(f"⚠ WS延迟: {ln.strip()[:120]}")

    # 限制 trades_analyzed 大小
    if len(state["trades_analyzed"]) > 500:
        state["trades_analyzed"] = state["trades_analyzed"][-200:]
    save_state(state)


def main():
    log("监控脚本启动 (修女版: ambush_basket_*.log + utf-8/replace)")
    while True:
        try:
            check_new_trades()
            time.sleep(5)
        except KeyboardInterrupt:
            log("监控脚本停止")
            break
        except Exception as e:
            log(f"监控错误: {type(e).__name__}: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
