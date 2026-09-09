# -*- coding: utf-8 -*-
"""Clash 控制接口工具：查看/切换策略组节点
用法:
  python _clash_ctl.py list                 # 列出所有策略组及当前选中节点
  python _clash_ctl.py nodes "<组名>"       # 列出某组可选节点
  python _clash_ctl.py switch "<组名>" "<节点名>"  # 切换组到指定节点
"""
import json, sys, urllib.request

API = "http://127.0.0.1:9097"
SECRET = "set-your-secret"

def req(method, path, body=None):
    r = urllib.request.Request(
        API + path,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": "Bearer " + SECRET, "Content-Type": "application/json"},
        method=method)
    with urllib.request.urlopen(r, timeout=8) as resp:
        txt = resp.read().decode()
    return json.loads(txt) if txt.strip() else {}

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "list"
    if cmd == "list":
        d = req("GET", "/proxies")["proxies"]
        for name, p in d.items():
            t = p.get("type", "")
            if t in ("Selector", "URLTest", "Fallback", "LoadBalance"):
                print("[%s] %s | 当前: %s | 可选数: %d" % (t, name, p.get("now", ""), len(p.get("all", []))))
    elif cmd == "nodes":
        grp = sys.argv[2]
        p = req("GET", "/proxies/" + urllib.parse.quote(grp))
        for i, n in enumerate(p.get("all", [])):
            mark = " <- 当前" if n == p.get("now") else ""
            print("%2d. %s%s" % (i + 1, n, mark))
    elif cmd == "switch":
        grp, node = sys.argv[2], sys.argv[3]
        req("PUT", "/proxies/" + urllib.parse.quote(grp), {"name": node})
        p = req("GET", "/proxies/" + urllib.parse.quote(grp))
        print("已切换: %s -> %s" % (grp, p.get("now")))

if __name__ == "__main__":
    main()
