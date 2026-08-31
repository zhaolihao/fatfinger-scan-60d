#!/usr/bin/env python3
"""
verify_divergence.py —— 对 divergence_candidates.json 批量做 ±90s aggTrades 跨腿终审
复用 verify_agg.fetch_agg / cross_align（与 153 真针互验同一把尺子）。
对每条候选（base, leg, anchor_ms）：
  - sym = base+leg，另一腿 = base+USDT
  - 拉 [anchor_ms-90s, anchor_ms+90s] 两腿 aggTrades，cross_align 算 max 跨腿偏离
  - max>=5% -> 真（可捕获乌龙指）；3~5% -> 弱；否则弱/无背离；无成交 -> 无成交
输出：verify_divergence_results.json（含 verdict, peak_pct）
"""
import json, os, sys
from datetime import datetime, timezone
HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
PROXY = os.environ.get("HTTPS_PROXY") or "http://127.0.0.1:7897"
os.environ["HTTPS_PROXY"] = PROXY
os.environ["HTTP_PROXY"] = PROXY
sys.path.insert(0, HERE)
import verify_agg as V


def main():
    cands = json.load(open("divergence_candidates.json"))
    print(f"verify {len(cands)} divergence candidates (60天)", flush=True)
    res = []
    real = weak = 0
    for i, c in enumerate(cands):
        base = c["base"]; leg = c["leg"]; ams = c["anchor_ms"]
        sym = base + leg; other = base + "USDT"
        s = ams - 90 * 1000; e = ams + 90 * 1000
        try:
            a = V.fetch_agg(sym, s, e)
            b = V.fetch_agg(other, s, e)
            r = V.cross_align(a, b)
        except Exception as ex:
            r = None
            print(f"  {base}{leg} ERR {ex}", flush=True)
        if r is None:
            verdict = "无成交"; peak = 0.0; n = 0
        else:
            peak = r["max"]; n = r["n"]
            if peak >= 5:
                verdict = "真"; real += 1
            else:
                verdict = "弱"; weak += 1
        rec = {**c, "verdict": verdict, "peak_pct": round(peak, 2), "n_pairs": n}
        res.append(rec)
        print(f"  {i+1}/{len(cands)} {base}{leg} div预筛={c['div_pct']}% peak={peak:.2f}% {verdict}", flush=True)
    res.sort(key=lambda x: (x["verdict"] != "真", -x["peak_pct"]))
    json.dump(res, open("verify_divergence_results.json", "w"), ensure_ascii=False, indent=1)
    print(f"\nDONE real={real} weak={weak} total={len(res)}", flush=True)


if __name__ == "__main__":
    main()
