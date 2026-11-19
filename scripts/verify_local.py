#!/usr/bin/env python3
"""本地端到端验证：不依赖 Docker，直接以子进程启动两个接收器、代理和
独立配额控制服务，逐项验证需求行为。用法：python3 scripts/verify_local.py
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROXY_PY = os.path.join(ROOT, "proxy", "app.py")
QUOTA_PY = os.path.join(ROOT, "quota", "app.py")
RECEIVER_PY = os.path.join(ROOT, "receiver", "receiver.py")

# 主环境端口
UDP_PORT = 19000
HTTP_PORT = 18080          # 代理网页
QUOTA_HTTP = 18081         # 配额控制网页
# 第二套代理（验证“控制服务宕机时代理重启先拉保存版”）
UDP_PORT2 = 19100
HTTP_PORT2 = 18180

PROXY_BASE = f"http://127.0.0.1:{HTTP_PORT}"
QUOTA_BASE = f"http://127.0.0.1:{QUOTA_HTTP}"
PROXY2_BASE = f"http://127.0.0.1:{HTTP_PORT2}"
TOKEN = "quota-shared-secret"
IDLE = 2  # 会话空闲超时（秒），缩短以加速验证

_failures = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def http_json(base, path, method="GET", data=None, token=False, timeout=10):
    headers = {}
    body = None
    if data is not None:
        body = urllib.parse.urlencode(data).encode()
    if token:
        headers["X-Quota-Token"] = TOKEN
    req = urllib.request.Request(base + path, data=body, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        ctype = r.headers.get("Content-Type", "")
    if not raw or "application/json" not in ctype:
        return None  # 表单 POST 返回 302/HTML，无需 JSON
    return json.loads(raw)


def http_post(base, path, data, timeout=40):
    return http_json(base, path, "POST", data, timeout=timeout)


def wait_up(url_path, base, timeout=25):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(base + url_path, timeout=3) as r:
                json.load(r)
            return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_cond(fn, timeout=15, interval=0.3):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if fn():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def udp_client(source_ip="127.0.0.1", source_port=0):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((source_ip, source_port))
    s.connect(("127.0.0.1", UDP_PORT))
    return s


def send_recv(sock, payload, timeout=5.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def expect_silent(sock, duration=1.5):
    """duration 内不应收到任何回包。"""
    sock.settimeout(0.2)
    t0 = time.time()
    got = []
    while time.time() - t0 < duration:
        try:
            got.append(sock.recv(65535))
        except socket.timeout:
            pass
    return got


def keep_alive(socks, payload=b"ka"):
    """给已有会话各发一条并收回包，刷新 last_activity，防止短空闲超时过期。"""
    for sx in socks:
        try:
            sx.settimeout(2)
            sx.send(payload)
            sx.recv(65535)
        except Exception:
            pass


def log_entries(client=None, direction=None, text=None, base=PROXY_BASE):
    msgs = http_json(base, "/api/state")["messages"]
    out = []
    for m in msgs:
        if client and m["client"] != client:
            continue
        if direction and m["direction"] != direction:
            continue
        if text and text not in m["payload"] and text not in m["note"]:
            continue
        out.append(m)
    return out


def start_receiver(name, port, logdir):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port), REPLY_DELAY="0")
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag, udp=UDP_PORT, http=HTTP_PORT,
                quota_url=f"http://127.0.0.1:{QUOTA_HTTP}"):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(udp), HTTP_PORT=str(http),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
        RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19002",
        SESSION_IDLE_TIMEOUT=str(IDLE), DB_PATH=db_path,
        QUOTA_SERVICE_URL=quota_url, QUOTA_TOKEN=TOKEN,
    )
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_quota(db_path, logdir, tag, proxy_url=PROXY_BASE, http=QUOTA_HTTP):
    env = dict(
        os.environ,
        HTTP_PORT=str(http), DB_PATH=db_path,
        PROXY_BASE_URL=proxy_url, QUOTA_TOKEN=TOKEN, SYNC_INTERVAL="1",
    )
    log = open(os.path.join(logdir, f"quota-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, QUOTA_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def set_quota(global_limit, per_source):
    http_post(QUOTA_BASE, "/save",
              {"global_limit": str(global_limit), "per_source": str(per_source)})
    # 等待代理确认到新版本
    def synced():
        q = http_json(PROXY_BASE, "/api/state")["quota"]
        st = http_json(QUOTA_BASE, "/api/status")
        return (q["global"] == global_limit and q["per_source"] == per_source
                and st["applied"] and st["applied"]["version"] == st["saved"]["version"]
                and st["proxy_reachable"])
    ok = wait_cond(synced, timeout=15)
    return ok


def wait_idle_clear():
    """等待所有会话空闲过期、名额归还。"""
    return wait_cond(
        lambda: http_json(PROXY_BASE, "/api/state")["quota"]["active_sessions"] == 0,
        timeout=IDLE + 6)


def main():
    tmp = tempfile.mkdtemp(prefix="quota-verify-")
    proxy_db = os.path.join(tmp, "proxy.db")
    quota_db = os.path.join(tmp, "quota.db")
    procs = [start_receiver("A", 19001, tmp), start_receiver("B", 19002, tmp)]
    quota = start_quota(quota_db, tmp, "run1")
    proxy = start_proxy(proxy_db, tmp, "run1")
    procs += [quota, proxy]
    try:
        # ---------- 启动与默认配额 ----------
        check("配额服务启动", wait_up("/api/status", QUOTA_BASE), f"日志目录 {tmp}")
        check("代理启动（先取得保存版再监听）", wait_up("/api/state", PROXY_BASE),
              f"日志目录 {tmp}")
        print(f"--- 环境就绪（日志目录 {tmp}）---")
        st = http_json(PROXY_BASE, "/api/state")
        check("初始目标为 A（切流能力保留）", st["target"] == "A")
        check("代理取得默认保存版配额 8/2",
              st["quota"]["global"] == 8 and st["quota"]["per_source"] == 2
              and st["quota"]["version"] >= 1)
        # 代理 bootstrap 完成后，配额服务的后台同步才会推送并确认首版
        check("配额服务初始保存版被代理确认",
              wait_cond(lambda: (lambda qs: qs["applied"] is not None
                                 and qs["saved"]["version"] == qs["applied"]["version"]
                                 and qs["pending"] is False)
                        (http_json(QUOTA_BASE, "/api/status")), timeout=15))
        qs = http_json(QUOTA_BASE, "/api/status")
        check("配额服务显示保存版=已确认版",
              qs["saved"]["version"] == qs["applied"]["version"] and qs["pending"] is False)

        # ---------- 原切流/透传行为回归 ----------
        c1 = udp_client("127.0.0.1")
        client1 = f"127.0.0.1:{c1.getsockname()[1]}"
        check("报文原样回传", send_recv(c1, b"m1-hello") == b"m1-hello")
        blob = bytes(range(256))
        check("二进制报文原样透传", send_recv(c1, blob) == blob)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m1-hello")
        check("m1 实际去向为接收器 A", e and e[0]["receiver"] == "A", str(e))

        http_post(PROXY_BASE, "/switch", {"target": "B"})
        check("网页切流到 B", http_json(PROXY_BASE, "/api/state")["target"] == "B")
        check("旧会话切流后仍走 A", send_recv(c1, b"m2-stick") == b"m2-stick")
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m2-stick")
        check("旧会话继续走接收器 A", e and e[0]["receiver"] == "A", str(e))
        print(f"--- 等待 {IDLE + 1}s 让会话空闲过期 ---")
        time.sleep(IDLE + 1)
        check("过期重建的新会话走 B", send_recv(c1, b"m3-new") == b"m3-new")
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m3-new")
        check("新会话固定到接收器 B", e and e[0]["receiver"] == "B", str(e))

        # 迟到回包不投递（原行为）
        c2 = udp_client("127.0.0.1")
        t0 = time.time()
        c2.send(b"DELAY:4:late1")
        time.sleep(max(0, IDLE + 0.5 - (time.time() - t0)))
        check("过期后新会话报文有回包", send_recv(c2, b"ping2", timeout=3) == b"ping2")
        got = expect_silent(c2, duration=3.0)
        check("旧会话迟到的回包未送达", not got)
        check("等待名额归还", wait_idle_clear())

        # ---------- 试传：来源 IP 可选 127.0.0.1~3 + 源端口 ----------
        http_post(PROXY_BASE, "/test",
                  {"payload": "webtest", "wait": "5", "delay": "0",
                   "client_ip": "127.0.0.2", "client_port": ""})
        lt = http_json(PROXY_BASE, "/api/state")["last_test"]
        check("试传可选来源 127.0.0.2",
              lt and lt["ok"] and lt["client"].startswith("127.0.0.2:"), str(lt))
        http_post(PROXY_BASE, "/test",
                  {"payload": "webtest3", "wait": "5", "delay": "0",
                   "client_ip": "127.0.0.3", "client_port": "33333"})
        lt = http_json(PROXY_BASE, "/api/state")["last_test"]
        check("试传可选来源 127.0.0.3 且固定源端口",
              lt and lt["ok"] and lt["client"] == "127.0.0.3:33333", str(lt))
        check("试传显示实际去向为 B", lt and lt["receiver"] == "B", str(lt))
        check("等待名额归还", wait_idle_clear())

        # ---------- 单来源上限：首包即计入，满额拒绝 ----------
        check("设置配额 全局4/单来源2", set_quota(4, 2))
        sa = udp_client("127.0.0.1")
        sb = udp_client("127.0.0.1")  # 同来源不同源端口
        check("来源1 会话1 接纳", send_recv(sa, b"a1") == b"a1")
        check("来源1 会话2 接纳", send_recv(sb, b"a2") == b"a2")
        # 建连首包即计入：两个会话都在名额内
        q = http_json(PROXY_BASE, "/api/state")["quota"]
        check("首包即计入：来源1 占用 2 席",
              q["active_sessions"] == 2 and q["per_source_usage"].get("127.0.0.1") == 2,
              str(q))
        sc = udp_client("127.0.0.1")
        sc.settimeout(2)
        sc.send(b"a3-denied")
        try:
            sc.recv(65535)
            denied = False
        except socket.timeout:
            denied = True
        check("同来源第 3 个会话被拒绝（无回包）", denied)
        time.sleep(0.3)
        e = log_entries(direction="配额拒绝", text="单来源上限")
        check("拒绝原因记录为单来源满额", bool(e), str(e[:1]))
        # 保活后，满额来源之外的新来源仍可接纳，且被拒会话不占名额
        keep_alive([sa, sb])
        sd = udp_client("127.0.0.2")
        check("满额来源之外的新来源仍可接纳", send_recv(sd, b"b1") == b"b1")
        q = http_json(PROXY_BASE, "/api/state")["quota"]
        check("用量统计：被拒会话不占名额，新来源占 1 席",
              q["active_sessions"] == 3 and q["per_source_usage"].get("127.0.0.1") == 2
              and q["per_source_usage"].get("127.0.0.2") == 1, str(q))
        for s in (sa, sb, sc, sd):
            s.close()
        check("空闲过期后名额归还", wait_idle_clear())

        # ---------- 全局满额腾退（多席来源 LRA）+ 迟到回包不投递 ----------
        check("设置配额 全局3/单来源3", set_quota(3, 3))
        s1 = udp_client("127.0.0.1")
        s2 = udp_client("127.0.0.1")
        s3 = udp_client("127.0.0.2")
        check("占席 s1", send_recv(s1, b"s1") == b"s1")
        time.sleep(0.6)
        check("占席 s2", send_recv(s2, b"s2") == b"s2")
        time.sleep(0.6)
        check("占席 s3（另一来源）", send_recv(s3, b"s3") == b"s3")
        time.sleep(0.3)
        # 刷新 s1 后再让 s2 成为最久未活动；随后令 s1 最久未活动
        s1.send(b"s1-refresh")
        check("s1 刷新有回包", s1.recv(65535) == b"s1-refresh")
        time.sleep(0.8)  # 现在 LRA 顺序：s2 最久，s1 次之，s3 最新
        s4 = udp_client("127.0.0.3")  # 新来源，全局已满
        check("全局满额新来源报文获回包（腾退成功）",
              send_recv(s4, b"s4-new", timeout=3) == b"s4-new")
        time.sleep(0.4)
        q = http_json(PROXY_BASE, "/api/state")["quota"]
        check("腾退后总量仍为全局上限 3", q["active_sessions"] == 3, str(q))
        ev = log_entries(direction="事件", text="quota-eviction")
        check("被腾退的是多席来源(127.0.0.1)最久未活动会话",
              bool(ev) and "127.0.0.1" in ev[0]["client"], str(ev[:1]))
        # 被腾退会话的迟到回包不能投递：让接收器延迟回包给一个随后被腾退的会话
        check("等待名额归还", wait_idle_clear())
        v1 = udp_client("127.0.0.1")
        v2 = udp_client("127.0.0.1")
        v3 = udp_client("127.0.0.2")
        check("占席 v1", send_recv(v1, b"v1") == b"v1")
        time.sleep(0.6)
        check("占席 v2", send_recv(v2, b"v2") == b"v2")
        time.sleep(0.6)
        check("占席 v3", send_recv(v3, b"v3") == b"v3")
        tv = time.time()
        v1.send(b"DELAY:3:v1-late")  # v1 最久未活动，将被腾退，3s 后的回包须丢弃
        time.sleep(0.4)
        v4 = udp_client("127.0.0.3")
        check("腾退发生、新会话接纳", send_recv(v4, b"v4", timeout=3) == b"v4")
        got = expect_silent(v1, duration=4.5)
        check("被腾退旧会话的迟到回包未投递", not got and time.time() - tv > 3)
        for s in (s1, s2, s3, s4, v1, v2, v3, v4):
            s.close()
        check("等待名额归还", wait_idle_clear())

        # ---------- 全局满额且各来源仅一席：拒绝 ----------
        check("设置配额 全局2/单来源1", set_quota(2, 1))
        o1 = udp_client("127.0.0.1")
        o2 = udp_client("127.0.0.2")
        check("来源1 占一席", send_recv(o1, b"o1") == b"o1")
        check("来源2 占一席", send_recv(o2, b"o2") == b"o2")
        o3 = udp_client("127.0.0.3")
        o3.settimeout(2)
        o3.send(b"o3-denied")
        try:
            o3.recv(65535)
            denied2 = False
        except socket.timeout:
            denied2 = True
        check("各来源仅一席时全局满额拒绝新来源", denied2)
        time.sleep(0.3)
        e = log_entries(direction="配额拒绝", text="各来源均仅一席")
        check("拒绝原因记录为无可腾退来源", bool(e), str(e[:1]))
        o1.close(); o2.close(); o3.close()
        check("等待名额归还", wait_idle_clear())

        # ---------- 收紧上限不踢旧会话，回落前不接纳 ----------
        check("设置配额 全局4/单来源4", set_quota(4, 4))
        # 三个会话各占一个来源 IP：收紧后各来源均仅一席，无可腾退来源
        t_socks = [udp_client(ip) for ip in ("127.0.0.1", "127.0.0.2", "127.0.0.3")]
        for i, sx in enumerate(t_socks):
            check(f"建立会话 {i + 1}", send_recv(sx, f"t{i}".encode()) == f"t{i}".encode())
        check("收紧到 全局2/单来源2（不踢旧会话）", set_quota(2, 2))
        q = http_json(PROXY_BASE, "/api/state")["quota"]
        check("收紧后已有 3 个会话照常保留", q["active_sessions"] == 3, str(q))
        # 先确认旧会话透传正常（保持活跃，避免空闲过期干扰）
        check("已有会话照常透传", send_recv(t_socks[0], b"still-alive") == b"still-alive")
        keep_alive(t_socks[1:])  # 刷新其余会话，确保 3 个都存活
        q = http_json(PROXY_BASE, "/api/state")["quota"]
        check("拒绝前确认 3 个会话仍在且各来源仅一席",
              q["active_sessions"] == 3 and all(v == 1 for v in q["per_source_usage"].values()),
              str(q))
        tn = udp_client("127.0.0.1")  # 复用某来源端口段，但作为新会话（新源端口）
        tn.settimeout(2)
        tn.send(b"tn-denied")
        try:
            tn.recv(65535)
            denied3 = False
        except socket.timeout:
            denied3 = True
        check("用量高于新上限且无可腾退来源期间不接纳新会话", denied3)
        time.sleep(0.3)
        e = log_entries(direction="配额拒绝", text="各来源均仅一席")
        check("记录为全局满额无可腾退", bool(e), str(e[:1]))
        tn.close()
        for sx in t_socks:
            sx.close()
        check("等待用量回落到新上限以下", wait_idle_clear())
        tn2 = udp_client("127.0.0.2")
        check("回落后恢复接纳", send_recv(tn2, b"tn2-ok") == b"tn2-ok")
        tn2.close()
        check("等待名额归还", wait_idle_clear())

        # ---------- 联系失败保留待应用版，恢复后自动应用 ----------
        saved_before = http_json(QUOTA_BASE, "/api/status")["saved"]["version"]
        proxy.terminate()
        proxy.wait(timeout=10)
        time.sleep(1)
        http_post(QUOTA_BASE, "/save", {"global_limit": "5", "per_source": "2"})
        time.sleep(2.5)  # 等同步周期尝试推送并失败
        qs = http_json(QUOTA_BASE, "/api/status")
        check("代理联系失败时保留待应用版",
              qs["saved"]["version"] > saved_before and qs["saved"]["status"] == "saved"
              and qs["proxy_reachable"] is False and bool(qs["last_error"]), str(qs))
        # 控制服务重启：待处理版本仍在 SQLite，重启后继续应用
        quota.terminate()
        quota.wait(timeout=10)
        quota = start_quota(quota_db, tmp, "run2")
        procs.append(quota)
        check("控制服务重启后恢复", wait_up("/api/status", QUOTA_BASE))
        proxy = start_proxy(proxy_db, tmp, "run2")
        procs.append(proxy)
        check("代理重启后恢复", wait_up("/api/state", PROXY_BASE))
        ok = wait_cond(lambda: (
            lambda q: q["global"] == 5 and q["per_source"] == 2
        )(http_json(PROXY_BASE, "/api/state")["quota"]), timeout=15)
        check("控制服务重启后继续应用待处理版本", ok)
        confirmed = wait_cond(lambda: (lambda d: d["applied"] is not None
                                       and d["applied"]["version"] == d["saved"]["version"]
                                       and d["pending"] is False)
                              (http_json(QUOTA_BASE, "/api/status")), timeout=15)
        check("待应用版变为代理已确认版", confirmed)
        check("等待名额归还", wait_idle_clear())

        # ---------- 代理重启须先取得保存版再接纳 ----------
        # 停掉控制服务，另起一个代理：它必须一直等待、不接纳新会话
        quota.terminate()
        quota.wait(timeout=10)
        proxy2_db = os.path.join(tmp, "proxy2.db")
        proxy2 = start_proxy(proxy2_db, tmp, "p2", udp=UDP_PORT2, http=HTTP_PORT2,
                             quota_url=f"http://127.0.0.1:{QUOTA_HTTP}")
        procs.append(proxy2)
        time.sleep(4)  # 控制服务宕机，代理 2 应仍在 bootstrap 重试
        check("控制服务宕机时新代理未开放网页（未取得保存版）",
              not wait_up("/api/state", PROXY2_BASE, timeout=2))
        early = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        early.sendto(b"before-quota", ("127.0.0.1", UDP_PORT2))
        early.settimeout(1.5)
        try:
            early.recv(65535)
            early_got = True
        except socket.timeout:
            early_got = False
        check("未取得保存版前首包被丢弃", not early_got)
        early.close()
        # 恢复控制服务（指向代理2），代理2 应拉到保存版并开始接纳
        quota = start_quota(quota_db, tmp, "run3", proxy_url=PROXY2_BASE)
        procs.append(quota)
        check("控制服务恢复", wait_up("/api/status", QUOTA_BASE))
        check("代理2 取得保存版后开放", wait_up("/api/state", PROXY2_BASE, timeout=20))
        q2 = http_json(PROXY2_BASE, "/api/state")["quota"]
        check("代理2 应用的是保存版 5/2",
              q2["global"] == 5 and q2["per_source"] == 2, str(q2))
        ok_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ok_sock.connect(("127.0.0.1", UDP_PORT2))
        check("取得保存版后正常接纳并回包",
              send_recv(ok_sock, b"after-quota") == b"after-quota")
        ok_sock.close()

        # ---------- 网页输入校验 + 保存失败不改版本 ----------
        bad_resp = None
        try:
            http_post(QUOTA_BASE, "/save", {"global_limit": "99", "per_source": "2"})
        except urllib.error.HTTPError as e:
            bad_resp = e
        # Flask 重定向视为 200；校验失败走 flash 不抛 5xx，故用 API 复查版本未变
        before = http_json(QUOTA_BASE, "/api/status")["saved"]["version"]
        http_post(QUOTA_BASE, "/save", {"global_limit": "1", "per_source": "2"})   # 全局越界
        http_post(QUOTA_BASE, "/save", {"global_limit": "8", "per_source": "9"})   # 单来源越界
        http_post(QUOTA_BASE, "/save", {"global_limit": "abc", "per_source": "2"})  # 非整数
        after = http_json(QUOTA_BASE, "/api/status")["saved"]["version"]
        check("非法上限被拒绝且不产生新版本", before == after, f"{before} -> {after}")

        # 存储故障：保存失败不改当前版/待应用版
        sys.path.insert(0, os.path.join(ROOT, "quota"))
        import importlib
        import app as quota_app
        importlib.reload(quota_app)
        qstore = quota_app.QuotaStore(os.path.join(tmp, "qfail.db"))
        v0 = qstore.latest()["version"]
        qstore._conn.close()
        save_failed = False
        try:
            qstore.add_version(6, 2)
        except Exception:
            save_failed = True
        # 关闭后读取也会失败，改用新连接核对磁盘版本未变
        qstore2 = quota_app.QuotaStore(os.path.join(tmp, "qfail.db"))
        check("保存失败报错且当前版不变",
              save_failed and qstore2.latest()["version"] == v0)

        # 代理侧 apply 接口校验
        req = urllib.request.Request(
            PROXY2_BASE + "/admin/quota/apply",
            data=json.dumps({"version": 99, "global": 99, "per_source": 1}).encode(),
            method="POST", headers={"X-Quota-Token": TOKEN,
                                    "Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=5)
            apply_rejected = False
        except urllib.error.HTTPError as e:
            apply_rejected = e.code == 400
        check("代理拒绝越界配额推送", apply_rejected)
        # 无令牌拒绝
        req = urllib.request.Request(PROXY2_BASE + "/admin/quota", method="GET")
        try:
            urllib.request.urlopen(req, timeout=5)
            auth_ok = False
        except urllib.error.HTTPError as e:
            auth_ok = e.code == 401
        check("管理接口需令牌", auth_ok)

    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    print("---")
    if _failures:
        print(f"共 {len(_failures)} 项失败: {_failures}")
        return 1
    print("全部验证通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
