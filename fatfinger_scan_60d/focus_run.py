#!/usr/bin/env python3
import subprocess, sys, os
HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE)
env = dict(os.environ)
env["HTTPS_PROXY"] = "http://127.0.0.1:7897"
env["HTTP_PROXY"] = "http://127.0.0.1:7897"
PY = sys.executable

print("=== SCAN (聚焦14天盲区) ===", flush=True)
r = subprocess.run([PY, "scan_divergence_focus.py"], env=env)
print("scan rc =", r.returncode, flush=True)

print("=== VERIFY (aggTrades 终审) ===", flush=True)
r2 = subprocess.run([PY, "verify_divergence.py"], env=env)
print("verify rc =", r2.returncode, flush=True)

print("=== ALL DONE ===", flush=True)
