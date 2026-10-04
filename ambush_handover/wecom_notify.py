# -*- coding: utf-8 -*-
"""
wecom_notify — ambush_basket_local.py 的企业微信通知模块
=========================================================
复用 scalp_notify.py 同一个企业微信机器人 webhook。
所有函数对外只返回 bool 或 str，绝不抛异常。
网络失败一律吞掉，不影响交易主流程。
"""
import json
import logging
import threading
import time as _time
import urllib.request

log = logging.getLogger("wecom_notify")

# ── 企业微信 webhook（复用 scalp_notify.py 同一群机器人）──
WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=86323fd7-97fd-49b0-9722-a4369cdfb5d3"

_sent_cache: dict = {}  # {content[:80]: last_sent_ts}


def _now() -> str:
    """返回当前时间字符串 HH:MM:SS。"""
    return _time.strftime("%H:%M:%S")


def _send_wechat(content: str) -> bool:
    """发送到企业微信 Webhook。任何异常吞掉返回 False。"""
    try:
        payload = json.dumps({"msgtype": "text", "text": {"content": content}}).encode("utf-8")
        req = urllib.request.Request(WEBHOOK_URL, data=payload,
                                     headers={"Content-Type": "application/json"})
        resp = urllib.request.urlopen(req, timeout=5)
        result = json.loads(resp.read().decode())
        if result.get("errcode") == 0:
            return True
        log.warning(f"[WECHAT] 失败: {result}")
        return False
    except Exception as e:
        log.warning(f"[WECHAT] 异常: {e}")
        return False


def _send_text(content: str) -> bool:
    """同步发送文本到企业微信，2 秒内同内容去重。绝不抛异常。"""
    try:
        dedup_key = content[:80]
        now = _time.time()
        if dedup_key in _sent_cache and (now - _sent_cache[dedup_key]) < 2.0:
            return True
        _sent_cache[dedup_key] = now
        return _send_wechat(content)
    except Exception as e:
        log.warning(f"[NOTIFY] _send_text 异常吞掉: {e}")
        return False


def send_async(text: str) -> None:
    """异步发送文本（守护线程，不阻塞调用方）。"""
    def _worker():
        _send_text(text)
    threading.Thread(target=_worker, daemon=True).start()


def notify_start(n_symbols: int, depth, notional, mode, lev, tp, sl) -> bool:
    """启动播报：币种数/阈值/模式/止盈止损。"""
    try:
        lines = [
            "埋伏合约通知",
            f"🚀 合约埋伏已启动",
            f"币种数：{n_symbols}",
            f"阈值：{depth}",
            f"每侧：{notional}U",
            f"模式：{mode}",
            f"杠杆：{lev}",
            f"止盈：{tp} | 止损：{sl}",
            f"时间：{_now()}",
        ]
        return _send_text('\n'.join(lines))
    except Exception as e:
        log.warning(f"[NOTIFY] notify_start 异常: {e}")
        return False


def notify_fill(sym, pos_side, entry_px, qty, cmp_tag="", tid="", anchor=0.0) -> bool:
    """成交通报。"""
    try:
        lines = [
            "埋伏合约通知",
            f"✅ 成交 {sym} {pos_side}",
            f"入场价：{entry_px}",
            f"数量：{qty}",
            f"标记：{cmp_tag} | 订单：{tid}",
            f"锚价：{anchor}",
            f"时间：{_now()}",
        ]
        return _send_text('\n'.join(lines))
    except Exception as e:
        log.warning(f"[NOTIFY] notify_fill 异常: {e}")
        return False


def notify_close(sym, pos_side, entry, exit_px, pnl,
                 route="", tag="", total_pnl=0.0, fills=0) -> bool:
    """平仓通报。"""
    try:
        emoji = "🟢" if pnl >= 0 else "🔴"
        lines = [
            "埋伏合约通知",
            f"{emoji} 平仓 {sym} {pos_side}",
            f"入场：{entry} → 出场：{exit_px}",
            f"路线：{route} {('['+tag+']') if tag else ''}",
            f"本笔盈亏：{pnl:+.4f} U",
            f"累计：{fills}次 {total_pnl:+.4f} U",
            f"时间：{_now()}",
        ]
        return _send_text('\n'.join(lines))
    except Exception as e:
        log.warning(f"[NOTIFY] notify_close 异常: {e}")
        return False


if __name__ == "__main__":
    print("发送测试消息...")
    ok = _send_text(f"埋伏合约通知\n🎉 wecom_notify 模块配置成功\n时间：{_now()}")
    print(f"结果: {'成功' if ok else '失败'}")
