# -*- coding: utf-8 -*-
"""把大 JSON 压缩为 .json.gz，便于 git push（GitHub 单文件硬限 100MB）"""
import json, gzip, os, shutil

BASE = os.path.dirname(os.path.abspath(__file__))

TARGETS = [
    "poly_trades_1h_all4_30d.json",
    "poly_trades_1h_last_30d.json",
    "poly_meta_1h_all4_30d.json",
    "poly_meta_1h_last_30d.json",
    "poly_meta_30d_4h.json",
]

for name in TARGETS:
    src = os.path.join(BASE, name)
    dst = src + ".gz"
    if not os.path.exists(src):
        print(f"SKIP (missing) {name}")
        continue
    mb_in = os.path.getsize(src) / 1048576
    with open(src, "rb") as f:
        raw = f.read()
    with gzip.open(dst, "wb", compresslevel=9) as f:
        f.write(raw)
    mb_out = os.path.getsize(dst) / 1048576
    print(f"OK {name}: {mb_in:.2f} MB -> {mb_out:.2f} MB ({mb_out/mb_in*100:.1f}%)")

print("COMPRESS DONE")
