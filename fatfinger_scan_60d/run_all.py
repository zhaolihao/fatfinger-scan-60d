#!/usr/bin/env python3
"""
run_all.py —— 全市场乌龙指扫描编排器（带断点续跑 + 网络抖动重试）
网络说明：币安经 Clash 代理 127.0.0.1:7897，代理上游会偶发超时。
本脚本每阶段跑完检查成功率，未达标则 sleep 后重跑（缓存命中即续跑），
网络恢复后自动推进。直接后台运行即可：
  python3 run_all.py  > run_all.log 2>&1 &
"""
import json, os, subprocess, sys, time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
PY = r"C:\Users\Administrator\.workbuddy\binaries\python\versions\3.13.12\python3.exe"
PROXY = "http://127.0.0.1:7897"
os.environ["HTTPS_PROXY"] = PROXY
os.environ["HTTP_PROXY"] = PROXY

STAGES = [
    ("build", "build_targets.py", "targets.json"),
    ("pull",  "pull_1h_klines.py", "klines_1h/_marker"),
    ("scan",  "scan_1h_wicks.py", "candidates.json"),
    ("drill", "drill_1m.py", "drill_full.json"),
    ("verify", "verify_agg.py", "verify_results_full.json"),
]
MAX_RETRY = 200
SLEEP = 60


def run_stage(script):
    p = subprocess.run([PY, script], capture_output=True, text=True, timeout=3600,
                       env={**os.environ})
    return p.returncode, p.stdout, p.stderr


def coverage_ok():
    # pull 覆盖率：klines_1h 文件数 / 期望符号数
    try:
        tg = json.load(open("targets.json"))
    except Exception:
        return False, 0, 0
    expect = sum(len(t["legs"]) for t in tg)
    have = len([f for f in os.listdir("klines_1h") if f.endswith(".json")]) if os.path.isdir("klines_1h") else 0
    return (have >= expect * 0.95), have, expect


def drill_ok():
    try:
        d = json.load(open("drill_full.json"))
    except Exception:
        return False, 0, 0
    tot = len(d)
    anc = sum(1 for x in d if x.get("anchor_ms"))
    return (tot == 0 or anc >= tot * 0.95), anc, tot


def main():
    log = open("run_all.log", "a", buffering=1)
    def L(s):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{ts}] {s}"
        print(line, flush=True)
        log.write(line + "\n")

    for name, script, marker in STAGES:
        done = os.path.exists(marker)
        if name == "pull":
            ok, have, expect = coverage_ok()
            done = ok
        if name == "drill":
            ok, anc, tot = drill_ok()
            done = ok
        if done:
            L(f"[skip] {name} 已完成")
            continue
        for attempt in range(1, MAX_RETRY + 1):
            L(f"=== run {name} (attempt {attempt}) ===")
            rc, out, err = run_stage(script)
            for line in out.strip().splitlines()[-15:]:
                L("  " + line)
            if err.strip():
                L("  STDERR: " + err.strip().splitlines()[-3:])
            if name == "pull":
                ok, have, expect = coverage_ok()
                L(f"  pull 覆盖率 {have}/{expect} ({100*have/max(expect,1):.0f}%)")
                if ok:
                    break
            elif name == "drill":
                ok, anc, tot = drill_ok()
                L(f"  drill anchor {anc}/{tot}")
                if ok:
                    break
            elif os.path.exists(marker):
                break
            else:
                L(f"  {name} 未完成，{SLEEP}s 后重试")
                time.sleep(SLEEP)
        else:
            L(f"!!! {name} 重试 {MAX_RETRY} 次仍未达标，停止")
            log.close()
            return
    L("=== 全流水线完成 ===")
    # 汇总
    try:
        res = json.load(open("verify_results_full.json"))
        real = sum(1 for r in res if r.get("verdict") == "真")
        weak = sum(1 for r in res if r.get("verdict") == "弱")
        L(f"真乌龙指: {real}  弱: {weak}  总候选: {len(res)}")
    except Exception as e:
        L(f"汇总失败: {e}")
    log.close()


if __name__ == "__main__":
    main()
