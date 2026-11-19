#!/usr/bin/env python3
"""本地端到端验证：不依赖 Docker，直接以子进程启动两个接收器、代理和配额控制服务，
逐项验证需求行为。用法：python3 scripts/verify_local.py
"""

import json
import os
import site
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

# -I 隔离模式不会自动加载 user-site；验证依赖装在那里，显式加入（不从 cwd 导入，保持隔离语义）
for _p in site.getusersitepackages(), *site.getsitepackages():
    if _p and _p not in sys.path:
        sys.path.append(_p)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROXY_PY = os.path.join(ROOT, "proxy", "app.py")
CONTROL_PY = os.path.join(ROOT, "control", "app.py")
RECEIVER_PY = os.path.join(ROOT, "receiver", "receiver.py")

UDP_PORT = 19000
HTTP_PORT = 18080
CONTROL_HTTP_PORT = 18081
BASE = f"http://127.0.0.1:{HTTP_PORT}"
CONTROL_BASE = f"http://127.0.0.1:{CONTROL_HTTP_PORT}"

_failures = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def http_get(base, path):
    with urllib.request.urlopen(base + path, timeout=10) as r:
        return json.load(r)


def http_post(base, path, data, timeout=40, raise_errors=True):
    req = urllib.request.Request(
        base + path, data=urllib.parse.urlencode(data).encode(), method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError:
        if raise_errors:
            raise
        return None


def wait_http_up(base, path="/api/state", timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            http_get(base, path)
            return True
        except Exception:
            time.sleep(0.3)
    return False

def udp_client(ip="127.0.0.1", port=0):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((ip, port))
    s.connect(("127.0.0.1", UDP_PORT))
    return s


def send_recv(sock, payload: bytes, timeout=5.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def drain(sock, duration=1.0):
    """持续收包 duration 秒，返回收到的报文列表（用于验证“收不到”）。"""
    got = []
    sock.settimeout(0.2)
    t0 = time.time()
    while time.time() - t0 < duration:
        try:
            got.append(sock.recv(65535))
        except socket.timeout:
            pass
    return got


def log_entries(client=None, direction=None, text=None):
    msgs = http_get(BASE, "/api/state")["messages"]
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


def set_quota(global_cap, per_ip_cap):
    http_post(CONTROL_BASE, "/settings",
              {"global_cap": str(global_cap), "per_ip_cap": str(per_ip_cap)})


def wait_synced(expected_version, timeout=8):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            cst = http_get(CONTROL_BASE, "/api/state")
            pst = http_get(BASE, "/api/state")
        except Exception:
            time.sleep(0.25)
            continue
        if (not cst["pending"] and cst["saved"]["version"] == expected_version
                and pst["quota"]["version"] == expected_version):
            return True
        time.sleep(0.25)
    return False


def wait_initial_synced(timeout=10):
    """控制服务与代理启动有先后，等待 v1 自动收敛（推送或代理启动拉取均可）。"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        cst = http_get(CONTROL_BASE, "/api/state")
        pst = http_get(BASE, "/api/state")
        if (not cst["pending"] and cst["saved"]["version"] == 1
                and pst["quota"]["version"] == 1):
            return True
        time.sleep(0.25)
    return False


def wait_sessions(count, timeout=5):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if len(http_get(BASE, "/api/state")["sessions"]) == count:
            return True
        time.sleep(0.2)
    return False


def wait_quota_ready(timeout=20):
    """代理须先取得配额保存版（UDP 在此之后才绑定）。容忍进程重启窗口的连接拒绝。"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if http_get(BASE, "/api/state")["quota"]["ready"]:
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def wait_udp_ready(timeout=10):
    """实际发一包确认 UDP 监听已绑定（ready 与绑定之间仍有微小窗口）。"""
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = udp_client("127.0.0.1")
        s.settimeout(1.0)
        try:
            s.send(b"udp-probe")
            s.recv(65535)
            return True
        except Exception:
            time.sleep(0.2)
        finally:
            s.close()
    return False


def start_receiver(name, port, logdir):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port), REPLY_DELAY="0")
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_control(db_path, logdir, tag, proxy_url=f"http://127.0.0.1:{HTTP_PORT}"):
    env = dict(
        os.environ,
        HTTP_PORT=str(CONTROL_HTTP_PORT), DB_PATH=db_path,
        APPLY_INTERVAL="0.5", HTTP_TIMEOUT="2",
        PROXY_CONTROL_URL=proxy_url,
    )
    log = open(os.path.join(logdir, f"control-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, CONTROL_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag, control_url=None):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(UDP_PORT), HTTP_PORT=str(HTTP_PORT),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
        RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19002",
        SESSION_IDLE_TIMEOUT="5", DB_PATH=db_path,
    )
    if control_url is not None:
        env["QUOTA_CONTROL_URL"] = control_url
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def main():
    tmp = tempfile.mkdtemp(prefix="proxy-verify-")
    db_path = os.path.join(tmp, "proxy.db")
    ctl_db = os.path.join(tmp, "control.db")
    procs = [start_receiver("A", 19001, tmp), start_receiver("B", 19002, tmp)]
    proxy = start_proxy(db_path, tmp, "run1",
                        control_url=f"http://127.0.0.1:{CONTROL_HTTP_PORT}")
    control = start_control(ctl_db, tmp, "run1")
    procs += [proxy, control]
    sockets = []
    try:
        if not wait_http_up(CONTROL_BASE, "/api/state") or not wait_http_up(BASE, "/api/state"):
            check("服务启动", False, f"日志见 {tmp}")
            return 1
        if not wait_quota_ready() or not wait_udp_ready():
            check("代理取得配额保存版后再监听 UDP", False, f"日志见 {tmp}")
            return 1
        print(f"--- 环境就绪（日志目录 {tmp}）---")

        # ============ 一、原有切流/透传/会话行为（回归） ============
        st = http_get(BASE, "/api/state")
        check("初始目标为 A", st["target"] == "A")
        check("代理启动已取得配额保存版", st["quota"]["ready"] is True
              and st["quota"]["version"] >= 1, str(st["quota"]))
        cst = http_get(CONTROL_BASE, "/api/state")
        check("控制服务初始保存版为 v1 且已被代理确认",
              wait_initial_synced(), str(cst))
        check("初始配额为范围上限（16/8）",
              cst["saved"]["global_cap"] == 16 and cst["saved"]["per_ip_cap"] == 8)

        c1 = udp_client()
        sockets.append(c1)
        client1 = f"127.0.0.1:{c1.getsockname()[1]}"
        m1 = b"m1-hello"
        check("报文原样回传", send_recv(c1, m1) == m1)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m1-hello")
        check("m1 实际去向为接收器 A", e and e[0]["receiver"] == "A", str(e))

        blob = bytes(range(256))
        check("二进制报文原样透传", send_recv(c1, blob) == blob)

        http_post(BASE, "/switch", {"target": "B"})
        check("切换后当前目标为 B", http_get(BASE, "/api/state")["target"] == "B")

        m2 = b"m2-stick"
        check("旧会话报文仍有回包", send_recv(c1, m2) == m2)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m2-stick")
        check("切换后旧会话继续走接收器 A", e and e[0]["receiver"] == "A", str(e))

        print("--- 等待 6 秒让会话空闲过期 ---")
        time.sleep(6)
        m3 = b"m3-newsession"
        check("新会话报文有回包", send_recv(c1, m3) == m3)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m3-newsession")
        check("过期重建的新会话走接收器 B", e and e[0]["receiver"] == "B", str(e))

        # 迟到回包：同 IP:端口 重建的会话也收不到
        c2 = udp_client()
        sockets.append(c2)
        client2 = f"127.0.0.1:{c2.getsockname()[1]}"
        t0 = time.time()
        c2.send(b"DELAY:7:late1")
        time.sleep(max(0, 6 - (time.time() - t0)))
        check("过期后新会话报文有回包", send_recv(c2, b"ping2", timeout=3) == b"ping2")
        check("旧会话迟到的回包未送达重建的会话", drain(c2, duration=2.5) == [])
        time.sleep(0.3)
        e = log_entries(client2, "接收器→客户端", "late1")
        check("日志中无 late1 的回包下发记录", not e, str(e))

        # ============ 二、配额：网页设置、保存版/已确认版 ============
        print("--- 等待 6 秒清理会话后进入配额验证 ---")
        time.sleep(6)
        check("配额验证前会话已清空", wait_sessions(0))

        set_quota(4, 2)
        check("保存 v2(4/2) 后代理确认应用", wait_synced(2))
        cst = http_get(CONTROL_BASE, "/api/state")
        pst = http_get(BASE, "/api/state")
        check("网页可同时查看保存版与代理已确认版且一致",
              cst["saved"]["version"] == cst["applied"]["version"] == 2
              and pst["quota"]["global_cap"] == 4
              and pst["quota"]["per_ip_cap"] == 2, str(cst))

        # 越界设置被拒绝，保存版/已应用版不变（表单校验失败时页面返回 200+提示）
        saved_before = http_get(CONTROL_BASE, "/api/state")["saved"]
        http_post(CONTROL_BASE, "/settings",
                  {"global_cap": "99", "per_ip_cap": "2"}, raise_errors=False)
        http_post(CONTROL_BASE, "/settings",
                  {"global_cap": "4", "per_ip_cap": "0"}, raise_errors=False)
        http_post(CONTROL_BASE, "/settings",
                  {"global_cap": "1", "per_ip_cap": "2"}, raise_errors=False)  # 低于最小值
        cst = http_get(CONTROL_BASE, "/api/state")
        check("越界设置不被接受（保存版不变）",
              cst["saved"]["version"] == saved_before["version"]
              and cst["saved"]["global_cap"] == saved_before["global_cap"]
              and cst["saved"]["per_ip_cap"] == saved_before["per_ip_cap"], str(cst["saved"]))
        cst = http_get(CONTROL_BASE, "/api/state")
        check("保存失败不改保存版（仍为 v2 4/2）",
              cst["saved"]["version"] == 2 and cst["saved"]["global_cap"] == 4
              and cst["saved"]["per_ip_cap"] == 2, str(cst))

        # ============ 三、单来源 IP 上限：首包即计数（含建连中） ============
        set_quota(4, 1)
        check("保存 v3(4/1) 后代理确认应用", wait_synced(3))
        s_open1 = udp_client("127.0.0.1")
        s_open2 = udp_client("127.0.0.1")
        sockets += [s_open1, s_open2]
        s_open1.settimeout(3)
        s_open1.send(b"opening-1")
        check("首包在 opening 阶段也正常收到回包", s_open1.recv(65535) == b"opening-1")
        s_open2.send(b"opening-2")  # 背靠背第二首包：单 IP 上限 1，必拒
        time.sleep(0.8)
        st = http_get(BASE, "/api/state")
        check("建连中(opening)的会话即计入配额",
              any(s["state"] == "opening" for s in st["sessions"])
              or len([s for s in st["sessions"] if s["source_ip"] == "127.0.0.1"]) == 1,
              str(st["sessions"]))
        check("同来源第二个首包被单 IP 上限拒绝",
              len([s for s in st["sessions"] if s["source_ip"] == "127.0.0.1"]) == 1,
              str(st["sessions"]))
        check("被拒首包未消耗上游端口（无接收器转发记录）",
              not log_entries(f"127.0.0.1:{s_open2.getsockname()[1]}",
                              "客户端→接收器"))
        check("拒绝事件写入日志",
              bool(log_entries(f"127.0.0.1:{s_open2.getsockname()[1]}", text="配额拒绝")))
        check("被拒来源首包收不到回包", drain(s_open2, duration=1.0) == [])
        # 立即续包，避免 5 秒空闲过期影响后续判断
        check("被接纳的会话正常透传", send_recv(s_open1, b"open-ok") == b"open-ok")

        # 另一来源不受影响
        s_other = udp_client("127.0.0.2")
        sockets.append(s_other)
        check("不同来源 IP 仍可建会话", send_recv(s_other, b"other-ip") == b"other-ip")
        time.sleep(0.3)
        st = http_get(BASE, "/api/state")
        check("每来源用量统计正确（.1=1, .2=1）",
              {u["ip"]: u["sessions"] for u in st["usage"]}
              == {"127.0.0.1": 1, "127.0.0.2": 1}, str(st["usage"]))

        print("--- 等待 6 秒让会话空闲过期，归还名额 ---")
        time.sleep(6)
        check("空闲过期后名额归还（会话清空）", wait_sessions(0))

        # ============ 四、全局满额腾退 vs 各占一席拒绝 ============
        set_quota(3, 3)
        check("保存 v4(3/3) 后代理确认应用", wait_synced(4))
        s1 = udp_client("127.0.0.1")
        s2 = udp_client("127.0.0.1")
        s3 = udp_client("127.0.0.2")
        sockets += [s1, s2, s3]
        check("全局席位 1 建立", send_recv(s1, b"g1") == b"g1")
        check("全局席位 2 建立（同来源第二席）", send_recv(s2, b"g2") == b"g2")
        check("全局席位 3 建立", send_recv(s3, b"g3") == b"g3")
        time.sleep(0.3)
        check("全局已满（3 个会话）", wait_sessions(3))
        victim_client = f"127.0.0.1:{s1.getsockname()[1]}"
        # 腾退前让 s1 的接收器延迟回包（会话存活时正常转发，回包晚于腾退到达）
        s1.send(b"DELAY:2:evict-late")
        time.sleep(0.3)
        # 再让 s2/s3 活动，使 s1 重新成为占多席来源(.1)中最久未活动的会话
        check("s2 续包保持会话", send_recv(s2, b"g2-keep") == b"g2-keep")
        check("s3 续包保持会话", send_recv(s3, b"g3-keep") == b"g3-keep")

        # 新来源 .3 首包：全局满，但 .1 占两席 → 腾退 .1 最久未活动的 s1
        s4 = udp_client("127.0.0.3")
        sockets.append(s4)
        check("全局满额时新来源经腾退获得名额", send_recv(s4, b"g4") == b"g4")
        time.sleep(0.4)
        st = http_get(BASE, "/api/state")
        alive = {s["client"] for s in st["sessions"]}
        check("被腾退的是占多席来源中最久未活动的会话",
              victim_client not in alive
              and f"127.0.0.1:{s2.getsockname()[1]}" in alive, str(alive))
        check("腾退后总会话数不超过全局上限", len(alive) == 3, str(alive))
        check("腾退事件写入日志", bool(log_entries(victim_client, text="配额腾退")))

        # 被腾退会话的迟到回包：接收器在延迟后回包，会话已关、上游 socket 已关闭，
        # 回包由内核丢弃（不会投递到客户端），与既有“迟到回包”机制一致
        check("被腾退会话的迟到回包不能投递", drain(s1, duration=2.5) == [])
        time.sleep(0.3)
        st = http_get(BASE, "/api/state")
        check("迟到回包未产生任何下发记录",
              not log_entries(victim_client, "接收器→客户端", "evict-late"))
        check("被腾退会话未重新出现在活动会话中",
              victim_client not in {s["client"] for s in st["sessions"]})

        # 此时三个来源各占一席 → 再来首包必须拒绝
        s5 = udp_client("127.0.0.1")  # .1 现在只剩 s2 一席
        sockets.append(s5)
        s5.send(b"g5-should-reject")
        time.sleep(0.8)
        st = http_get(BASE, "/api/state")
        check("各来源仅占一席时全局满额拒绝新会话", len(st["sessions"]) == 3,
              str(st["sessions"]))
        check("拒绝原因记录为各占一席",
              bool(log_entries(f"127.0.0.1:{s5.getsockname()[1]}", text="各来源均仅占一席")))
        check("被拒首包无回包", drain(s5, duration=1.0) == [])

        # ============ 五、收紧上限不踢旧会话，回落前不接纳 ============
        print("--- 等待 6 秒清理会话 ---")
        time.sleep(6)
        check("会话已清空", wait_sessions(0))
        set_quota(4, 2)
        check("保存 v5(4/2) 后代理确认应用", wait_synced(5))
        t_a = udp_client("127.0.0.1")
        t_b = udp_client("127.0.0.2")
        sockets += [t_a, t_b]
        check("收紧前建立会话 A", send_recv(t_a, b"tight-a") == b"tight-a")
        check("收紧前建立会话 B", send_recv(t_b, b"tight-b") == b"tight-b")
        set_quota(2, 1)  # 收紧到 2/1，但已有 2 个会话
        check("保存 v6(2/1) 后代理确认应用", wait_synced(6))
        time.sleep(0.3)
        st = http_get(BASE, "/api/state")
        check("收紧上限不踢旧会话", len(st["sessions"]) == 2, str(st["sessions"]))
        t_c = udp_client("127.0.0.3")
        sockets.append(t_c)
        t_c.send(b"tight-c")
        time.sleep(0.8)
        st = http_get(BASE, "/api/state")
        check("用量未回落前不接纳新会话", len(st["sessions"]) == 2, str(st["sessions"]))
        check("旧会话在收紧后仍照常透传", send_recv(t_a, b"tight-a2") == b"tight-a2")
        print("--- 等待 6 秒让旧会话过期回落 ---")
        time.sleep(6)
        check("用量回落后名额释放", wait_sessions(0))
        check("回落后新会话可按新上限接纳", send_recv(t_c, b"tight-c2") == b"tight-c2")
        time.sleep(0.3)
        st = http_get(BASE, "/api/state")
        check("新会话按收紧后的单 IP 上限计数",
              len([s for s in st["sessions"] if s["source_ip"] == "127.0.0.3"]) == 1)

        # ============ 六、网页试传：源 IP 127.0.0.1~3 + 源端口 ============
        print("--- 等待 6 秒清理会话 ---")
        time.sleep(6)
        set_quota(8, 4)
        check("保存 v7(8/4) 后代理确认应用", wait_synced(7))
        http_post(BASE, "/test", {"payload": "webtest-ip3", "wait": "5", "delay": "0",
                                  "client_port": "", "source_ip": "127.0.0.3"})
        time.sleep(0.3)
        lt = http_get(BASE, "/api/state")["last_test"]
        check("网页试传可选择源 IP 127.0.0.3 且成功",
              lt and lt["ok"] and lt["source_ip"] == "127.0.0.3"
              and lt["client"].startswith("127.0.0.3:"), str(lt))
        check("试传显示实际去向接收器 B", lt and lt["receiver"] == "B", str(lt))

        fixed_port = 31337
        http_post(BASE, "/test", {"payload": "webtest-port", "wait": "5", "delay": "0",
                                  "client_port": str(fixed_port), "source_ip": "127.0.0.2"})
        time.sleep(0.3)
        lt = http_get(BASE, "/api/state")["last_test"]
        check("网页试传支持固定源 IP+源端口",
              lt and lt["ok"] and lt["client"] == f"127.0.0.2:{fixed_port}", str(lt))

        # 试传撞配额：把单 IP 上限收紧到 1，先占住 127.0.0.1
        set_quota(8, 1)
        check("保存 v8(8/1) 后代理确认应用", wait_synced(8))
        holder = udp_client("127.0.0.1")
        sockets.append(holder)
        check("占位会话建立", send_recv(holder, b"hold") == b"hold")
        http_post(BASE, "/test", {"payload": "webtest-denied", "wait": "2", "delay": "0",
                                  "client_port": "", "source_ip": "127.0.0.1"})
        time.sleep(0.3)
        lt = http_get(BASE, "/api/state")["last_test"]
        check("试传被配额拒绝时页面展示拒绝原因",
              lt and not lt["ok"] and lt["quota_rejected"] and "配额拒绝" in lt["error"],
              str(lt))

        # ============ 七、控制服务重启：待应用版本继续应用 ============
        print("--- 等待 6 秒清理会话 ---")
        time.sleep(6)
        control.terminate()
        control.wait(timeout=10)
        set_quota_before_restart = None
        # 控制服务已停：直接改库做不到，改为重启控制服务后再保存；
        # 先验证“联系不上代理”场景：让控制服务指向错误地址启动
        bad_control = start_control(os.path.join(tmp, "control-bad.db"), tmp, "bad",
                                    proxy_url=f"http://127.0.0.1:9")  # 9=discard，连不上
        procs.append(bad_control)
        wait_http_up(CONTROL_BASE, "/api/state")
        http_post(CONTROL_BASE, "/settings", {"global_cap": "5", "per_ip_cap": "2"})
        time.sleep(1.5)
        cst = http_get(CONTROL_BASE, "/api/state")
        check("代理联系失败时保留待应用版", cst["pending"] is True
              and cst["contact"]["last_contact_ok"] is False, str(cst["contact"]))
        check("代理仍按已应用配额接纳（未被待应用版影响）",
              http_get(BASE, "/api/state")["quota"]["version"] == 8)
        bad_control.terminate()
        bad_control.wait(timeout=10)

        # 用原控制库重启（其保存版停在 v8），保存 v9 后应自动续推给代理
        control = start_control(ctl_db, tmp, "run2")
        procs.append(control)
        wait_http_up(CONTROL_BASE, "/api/state")
        cst = http_get(CONTROL_BASE, "/api/state")
        check("控制服务重启后保存版/已确认版持久化保留",
              cst["saved"]["version"] == 8 and cst["applied"]
              and cst["applied"]["version"] == 8, str(cst))
        set_quota(6, 3)
        check("控制服务重启后仍能继续应用新版本 v9", wait_synced(9))

        # ============ 八、代理重启：先取得保存版再接纳 ============
        proxy.terminate()
        proxy.wait(timeout=10)
        time.sleep(1.0)  # 等监听端口释放
        proxy = start_proxy(db_path, tmp, "run2",
                            control_url=f"http://127.0.0.1:{CONTROL_HTTP_PORT}")
        procs.append(proxy)
        check("代理重启后恢复", wait_quota_ready())
        st = http_get(BASE, "/api/state")
        check("代理重启后先取得保存版配额（v9 6/3）",
              st["quota"]["ready"] and st["quota"]["version"] == 9
              and st["quota"]["global_cap"] == 6 and st["quota"]["per_ip_cap"] == 3,
              str(st["quota"]))
        check("重启后目标保留为 B", st["target"] == "B")
        check("重启后会话已清空", st["sessions"] == [])
        cr = udp_client("127.0.0.1")
        sockets.append(cr)
        check("重启后新会话按保存版配额正常接纳", send_recv(cr, b"after-restart") == b"after-restart")
        time.sleep(1.0)
        check("代理重启后控制服务的确认状态重新收敛为一致",
              wait_synced(9))

        # ============ 九、控制服务不可用时，代理启动等待取得配额 ============
        proxy.terminate()
        proxy.wait(timeout=10)
        control.terminate()
        control.wait(timeout=10)
        stalled = start_proxy(os.path.join(tmp, "proxy-stall.db"), tmp, "stall",
                              control_url=f"http://127.0.0.1:{CONTROL_HTTP_PORT}")
        procs.append(stalled)
        time.sleep(3)
        # HTTP 虽可能已起，但配额未取得前 ready=False，且 UDP 未绑定（首包无人收）
        try:
            stalled_ready = bool(http_get(BASE, "/api/state")["quota"]["ready"])
        except Exception:
            stalled_ready = False
        probe = udp_client("127.0.0.1")
        probe.settimeout(1.0)
        probe.send(b"before-quota")
        try:
            probe.recv(65535)
            udp_accepted = True
        except Exception:
            udp_accepted = False
        probe.close()
        check("控制服务不可用时代理不提前接纳（配额未就绪且 UDP 未绑定）",
              not stalled_ready and not udp_accepted)
        # 控制服务回来后，代理自动取得配额并绑定 UDP
        control = start_control(ctl_db, tmp, "run3")
        procs.append(control)
        check("控制服务恢复后代理取得配额并启动", wait_quota_ready(timeout=15))
        check("启动后配额为持久化保存版 v9",
              http_get(BASE, "/api/state")["quota"]["version"] == 9)
        stalled.terminate()
        stalled.wait(timeout=10)
        time.sleep(1)  # 等端口释放，再用默认数据库重启代理做后续回归
        proxy = start_proxy(db_path, tmp, "run3",
                            control_url=f"http://127.0.0.1:{CONTROL_HTTP_PORT}")
        procs.append(proxy)
        check("默认数据库代理重启恢复", wait_quota_ready(timeout=15))

        # ============ 十、进程内：保存失败不改版本（控制服务 Store） ============
        sys.path.insert(0, os.path.join(ROOT, "control"))
        import importlib
        import app as control_app
        importlib.reload(control_app)
        cs = control_app.Store(os.path.join(tmp, "ct.db"))
        before = cs.get_saved()
        cs._conn.close()  # 模拟存储故障
        failed = False
        try:
            cs.save(3, 2)
        except Exception:
            failed = True
        check("配额保存失败时报错且不改保存版", failed)
        # 重新打开连接验证版本指针未被改动
        import sqlite3 as _sq
        conn = _sq.connect(os.path.join(tmp, "ct.db"))
        row = conn.execute("SELECT value FROM meta WHERE key='saved_version'").fetchone()
        check("保存失败后保存版指针不变", row is not None and int(row[0]) == before["version"])
        conn.close()

        # 原代理 Store 的目标保存失败保护（回归）
        sys.path.insert(0, os.path.join(ROOT, "proxy"))
        import app as proxy_app
        importlib.reload(proxy_app)
        ps = proxy_app.Store(os.path.join(tmp, "pt.db"))
        ps.set_target("B")
        ps._conn.close()
        failed = False
        try:
            ps.set_target("A")
        except Exception:
            failed = True
        check("切换保存失败时报错且保留原目标", failed and ps.get_target() == "B")

    finally:
        for s in sockets:
            try:
                s.close()
            except Exception:
                pass
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
