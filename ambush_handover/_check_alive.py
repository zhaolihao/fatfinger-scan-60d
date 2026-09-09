# -*- coding: utf-8 -*-
"""SAFE process-alive check for Windows (read-only query, never kills).
Usage: python _check_alive.py
"""
import ctypes, os

k32 = ctypes.windll.kernel32
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
still_active = 259

def alive(pid):
    h = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not h:
        return False
    code = ctypes.c_ulong()
    ok = k32.GetExitCodeProcess(h, ctypes.byref(code))
    k32.CloseHandle(h)
    return bool(ok) and code.value == still_active

CHECKS = [("react", "ambush_basket_react.lock"),
          ("resident", "ambush_basket_resident.lock"),
          ("exp11", "ambush_basket_resident_exp11.lock"),
          ("exp50", "ambush_basket_resident_exp50.lock")]
total = 0
for name, f in CHECKS:
    try:
        pid = int(open(f).read().strip())
        a = alive(pid)
        print(f"{name}: PID {pid} {'存活' if a else '已死'}")
        total += 1 if a else 0
    except Exception as e:
        print(f"{name}: 锁异常 {e}")
print(f"--- 存活 {total}/{len(CHECKS)} ---")
