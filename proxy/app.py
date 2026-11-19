#!/usr/bin/env python3
"""遥测 UDP 代理 + 操作网页。

功能要点：
- 按客户端 IP:端口建立会话，空闲超时（默认 5 秒）后过期；
- 建会话时把该会话固定到当前目标接收器（A/B），会话期间不随切换改变；
- 每个会话使用一条独立的、已 connect 的上游 UDP 套接字（独立本地端口），
  因此回包天然只能属于一个会话：只把该会话对应接收器的回包送回该客户端；
- 会话过期即关闭其上游端口，且端口进入冷却期不再分配，
  所以旧会话迟到的回包不可能落到同 IP:端口重建的新会话里；
- 报文与回包原样透传，不做任何修改；
- 当前目标保存在 SQLite，重启后保留；会话全在内存，重启即清空；
- 切换目标时先落库再生效，保存失败则保留原目标；
- Flask 网页可切换目标、发送试传报文、查看会话与报文日志（实际去向/回包）。
"""

import asyncio
import hmac
import json
import os
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

# ---------------- 配置 ----------------
UDP_LISTEN_HOST = os.environ.get("UDP_LISTEN_HOST", "0.0.0.0")
UDP_LISTEN_PORT = int(os.environ.get("UDP_LISTEN_PORT", "9000"))
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy.db")
)
SESSION_IDLE_TIMEOUT = float(os.environ.get("SESSION_IDLE_TIMEOUT", "5"))
UPSTREAM_PORT_MIN = int(os.environ.get("UPSTREAM_PORT_MIN", "20000"))
UPSTREAM_PORT_MAX = int(os.environ.get("UPSTREAM_PORT_MAX", "29999"))
# 上游端口关闭后的冷却时间：冷却期内不再分配该端口，
# 保证旧会话迟到的回包不会被复用同端口的新会话收到。应大于接收器最大回包延迟。
UPSTREAM_PORT_COOLDOWN = float(os.environ.get("UPSTREAM_PORT_COOLDOWN", "30"))
LOG_KEEP = int(os.environ.get("LOG_KEEP", "1000"))

RECEIVERS = {
    "A": (
        os.environ.get("RECEIVER_A_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_A_PORT", "9001")),
    ),
    "B": (
        os.environ.get("RECEIVER_B_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_B_PORT", "9002")),
    ),
}
DEFAULT_TARGET = os.environ.get("DEFAULT_TARGET", "A")

# ---------------- 配额控制服务 ----------------
QUOTA_SERVICE_URL = os.environ.get("QUOTA_SERVICE_URL", "").rstrip("/")
QUOTA_TOKEN = os.environ.get("QUOTA_TOKEN", "quota-shared-secret")
QUOTA_BOOTSTRAP_RETRY = float(os.environ.get("QUOTA_BOOTSTRAP_RETRY", "2"))
# 未配置配额服务时使用的内置默认（独立配额服务未接入的场景）
DEFAULT_GLOBAL_LIMIT = int(os.environ.get("DEFAULT_GLOBAL_LIMIT", "8"))
DEFAULT_PER_SOURCE_LIMIT = int(os.environ.get("DEFAULT_PER_SOURCE_LIMIT", "2"))
GLOBAL_LIMIT_RANGE = (2, 16)
PER_SOURCE_LIMIT_RANGE = (1, 8)
# 试传允许的来源 IP（网页可选）
TEST_SOURCE_IPS = ("127.0.0.1", "127.0.0.2", "127.0.0.3")


def fmt_addr(addr):
    return f"{addr[0]}:{addr[1]}"


def preview(data: bytes) -> str:
    """报文内容展示：可打印文本原样显示，否则用 hex。"""
    try:
        text = data.decode("utf-8")
        if all(ch.isprintable() or ch in "\r\n\t" for ch in text):
            return text if len(text) <= 200 else text[:200] + "…"
    except UnicodeDecodeError:
        pass
    hexed = data.hex()
    return "hex:" + (hexed if len(hexed) <= 200 else hexed[:200] + "…")


def fmt_ts(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


# ---------------- SQLite：目标持久化 + 报文日志 ----------------
class Store:
    """线程安全的 SQLite 存取。目标(target)落库，重启后保留。"""

    def __init__(self, path):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS message_log (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ts REAL NOT NULL,
                   client TEXT NOT NULL DEFAULT '',
                   session_id INTEGER NOT NULL DEFAULT 0,
                   direction TEXT NOT NULL,
                   receiver TEXT NOT NULL DEFAULT '',
                   payload TEXT NOT NULL DEFAULT '',
                   note TEXT NOT NULL DEFAULT '')"""
        )
        self._conn.commit()
        row = self._conn.execute("SELECT value FROM settings WHERE key='target'").fetchone()
        if row and row[0] in RECEIVERS:
            self._target = row[0]
        else:
            self._target = DEFAULT_TARGET if DEFAULT_TARGET in RECEIVERS else sorted(RECEIVERS)[0]
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (self._target,)
            )
            self._conn.commit()
        self._inserts = 0

    def get_target(self):
        with self._lock:
            return self._target

    def set_target(self, target):
        """先持久化，成功后才切换内存中的当前目标；失败则抛异常并保留原目标。"""
        if target not in RECEIVERS:
            raise ValueError(f"未知接收器: {target!r}")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (target,)
            )
            self._conn.commit()
            self._target = target

    def log(self, direction, client="", session_id=0, receiver="", payload="", note=""):
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO message_log(ts,client,session_id,direction,receiver,payload,note)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (time.time(), client, session_id, direction, receiver, payload, note),
                )
                self._conn.commit()
                self._inserts += 1
                if self._inserts % 200 == 0:
                    self._conn.execute(
                        "DELETE FROM message_log WHERE id < "
                        "(SELECT COALESCE(MIN(id),0) FROM "
                        "(SELECT id FROM message_log ORDER BY id DESC LIMIT ?))",
                        (LOG_KEEP,),
                    )
                    self._conn.commit()
        except sqlite3.Error:
            pass  # 日志失败绝不影响转发

    def recent_messages(self, limit=100):
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,ts,client,session_id,direction,receiver,payload,note "
                "FROM message_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        cols = ("id", "ts", "client", "session_id", "direction", "receiver", "payload", "note")
        return [dict(zip(cols, r)) for r in rows]

    def latest_receiver_for_client(self, client):
        with self._lock:
            row = self._conn.execute(
                "SELECT receiver FROM message_log WHERE client=? AND direction='c2r' "
                "ORDER BY id DESC LIMIT 1",
                (client,),
            ).fetchone()
        return row[0] if row else None


# ---------------- 上游端口分配（带冷却期） ----------------
class PortAllocator:
    """显式管理上游本地端口：关闭后冷却期内不再分配，
    确保旧会话迟到的回包不会被复用同端口的新会话收到。"""

    def __init__(self, low, high, cooldown):
        self.low, self.high, self.cooldown = low, high, cooldown
        self._next = low
        self._in_use = set()
        self._closed = {}  # port -> 关闭时刻(monotonic)
        self._lock = threading.Lock()

    def allocate(self):
        with self._lock:
            now = time.monotonic()
            for _ in range(self.high - self.low + 1):
                port = self._next
                self._next += 1
                if self._next > self.high:
                    self._next = self.low
                if port in self._in_use:
                    continue
                closed_at = self._closed.get(port)
                if closed_at is not None and now - closed_at < self.cooldown:
                    continue
                self._in_use.add(port)
                self._closed.pop(port, None)
                return port
        raise RuntimeError("上游 UDP 端口耗尽")

    def release(self, port):
        with self._lock:
            self._in_use.discard(port)
            self._closed[port] = time.monotonic()

    def discard(self, port):
        """分配后绑定失败：不进入冷却，直接可用。"""
        with self._lock:
            self._in_use.discard(port)


# ---------------- 会话与转发 ----------------
class Session:
    __slots__ = (
        "id", "client_addr", "receiver_name", "receiver_addr",
        "upstream_port", "transport", "state", "closed",
        "created", "last_activity", "pending",
    )

    def __init__(self, sid, client_addr):
        self.id = sid
        self.client_addr = client_addr
        self.receiver_name = ""
        self.receiver_addr = None
        self.upstream_port = None
        self.transport = None
        self.state = "opening"  # opening -> open -> closed
        self.closed = False
        self.created = time.monotonic()
        self.last_activity = self.created
        self.pending = []


class ClientProtocol(asyncio.DatagramProtocol):
    """面向遥测终端的 UDP 端。"""

    def __init__(self, state):
        self.state = state

    def datagram_received(self, data, addr):
        self.state.on_client_datagram(data, addr)

    def error_received(self, exc):
        pass


class UpstreamProtocol(asyncio.DatagramProtocol):
    """每个会话一条、已 connect 到其接收器的上游 UDP 端。
    只有该会话对应接收器的回包会到达这里。"""

    def __init__(self, state):
        self.state = state
        self.session = None

    def datagram_received(self, data, addr):
        sess = self.session
        if sess is not None:
            self.state.on_upstream_datagram(sess, data)

    def error_received(self, exc):
        pass  # 如对端 ICMP 不可达，忽略


class ProxyState:
    def __init__(self, store):
        self.store = store
        self.loop = None
        self.client_transport = None
        self.sessions = {}  # client_addr -> Session（仅事件循环线程修改）
        self.lock = threading.RLock()
        self.next_session_id = 1
        self.ports = PortAllocator(UPSTREAM_PORT_MIN, UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)
        # ---- 配额状态 ----
        # 已应用的配额（版本号 + 上限）。None 表示尚未从配额控制服务取得保存版，
        # 此时代理不接纳任何新会话（首包直接丢弃）。
        self.quota = None  # {"version": int, "global": int, "per_source": int}
        # 来源 IP -> 当前占用席位数（含建连中的会话，首包即计入）
        self.source_usage = {}
        self._reject_seq = 0

    def bootstrap_quota(self):
        """启动前从配额控制服务拉取保存版；未取得前不得接纳新会话。

        联系失败则一直重试，直到成功（或未配置配额服务时使用内置默认）。
        """
        if not QUOTA_SERVICE_URL:
            self.quota = {
                "version": 0,
                "global": DEFAULT_GLOBAL_LIMIT,
                "per_source": DEFAULT_PER_SOURCE_LIMIT,
            }
            print("[proxy] 未配置 QUOTA_SERVICE_URL，使用内置默认配额 "
                  f"全局 {self.quota['global']} / 单来源 {self.quota['per_source']}",
                  flush=True)
            return
        url = QUOTA_SERVICE_URL + "/quota/saved"
        while True:
            try:
                req = urllib.request.Request(
                    url, headers={"X-Quota-Token": QUOTA_TOKEN}, method="GET"
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    data = json.load(resp)
                q = {
                    "version": int(data["version"]),
                    "global": int(data["global"]),
                    "per_source": int(data["per_source"]),
                }
                if not (GLOBAL_LIMIT_RANGE[0] <= q["global"] <= GLOBAL_LIMIT_RANGE[1]
                        and PER_SOURCE_LIMIT_RANGE[0] <= q["per_source"]
                        <= PER_SOURCE_LIMIT_RANGE[1]):
                    raise ValueError(f"保存版超出合法范围: {data}")
                with self.lock:
                    self.quota = q
                print(f"[proxy] 已从配额控制服务取得保存版 v{q['version']}："
                      f"全局 {q['global']} / 单来源 {q['per_source']}，开始接纳新会话",
                      flush=True)
                return
            except Exception as exc:
                print(f"[proxy] 启动配额拉取失败（{QUOTA_SERVICE_URL}）：{exc}；"
                      f"{QUOTA_BOOTSTRAP_RETRY}s 后重试，期间不接纳新会话", flush=True)
                time.sleep(QUOTA_BOOTSTRAP_RETRY)

    def apply_quota(self, version, global_limit, per_source):
        """配额控制服务推送的新版本：校验并应用到内存。

        只更新上限，不踢已有会话；收紧后用量回落前不接纳新会话。
        """
        if not isinstance(version, int) or version < 0:
            raise ValueError("version 必须是非负整数")
        if not (GLOBAL_LIMIT_RANGE[0] <= global_limit <= GLOBAL_LIMIT_RANGE[1]):
            raise ValueError("全局上限超出 2~16")
        if not (PER_SOURCE_LIMIT_RANGE[0] <= per_source <= PER_SOURCE_LIMIT_RANGE[1]):
            raise ValueError("单来源上限超出 1~8")
        with self.lock:
            old = self.quota
            self.quota = {"version": version, "global": global_limit,
                          "per_source": per_source}
        if old is None or old["version"] != version:
            print(f"[proxy] 应用配额 v{version}：全局 {global_limit} / "
                  f"单来源 {per_source}（已有会话照常透传）", flush=True)
        return self.quota

    # ---- 配额准入 ----
    def _admit(self, source_ip):
        """决定是否接纳一个新会话（调用方持有 self.lock）。

        规则：
        - 会话从首包起（建连中）即计入；
        - 单来源满额直接拒绝；
        - 全局未满直接接纳；
        - 全局满额时：若存在占有多于一席的来源，腾退其中最久未活动的会话后接纳；
          若各来源均仅一席，则拒绝。
        返回 (admitted: bool, reason: str, victim: Session|None)。
        """
        if self.quota is None:
            return False, "配额尚未就绪（未取得保存版），拒绝新会话", None
        used = len(self.sessions)
        src_used = self.source_usage.get(source_ip, 0)
        if src_used >= self.quota["per_source"]:
            return False, (f"来源 {source_ip} 会话数 {src_used} 已达单来源上限 "
                           f"{self.quota['per_source']}，拒绝"), None
        if used < self.quota["global"]:
            return True, "", None
        # 全局满额：寻找占有多于一席的来源中最久未活动的会话
        candidates = [
            s for s in self.sessions.values()
            if not s.closed and self.source_usage.get(s.client_addr[0], 0) > 1
        ]
        if not candidates:
            return False, (f"全局 {self.quota['global']} 席已满且各来源均仅一席，"
                           "无可腾退来源，拒绝"), None
        victim = min(candidates, key=lambda s: s.last_activity)
        return True, ("全局满额，腾退来源 %s 的最久未活动会话 #%s（空闲 %.1fs）"
                      % (victim.client_addr[0], victim.id,
                         time.monotonic() - victim.last_activity)), victim

    def _reject_datagram(self, data, addr, reason):
        self._reject_seq += 1
        self.store.log("reject", client=fmt_addr(addr),
                       payload=preview(data), note=reason)

    async def start(self):
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: ClientProtocol(self),
            local_addr=(UDP_LISTEN_HOST, UDP_LISTEN_PORT),
        )
        self.client_transport = transport
        self.loop.create_task(self._sweeper())
        print(f"[proxy] 客户端 UDP 监听 {UDP_LISTEN_HOST}:{UDP_LISTEN_PORT}", flush=True)
        print(f"[proxy] 空闲会话超时 {SESSION_IDLE_TIMEOUT}s，当前目标 {self.store.get_target()}",
              flush=True)

    # ---- 客户端方向 ----
    def on_client_datagram(self, data, addr):
        now = time.monotonic()
        with self.lock:
            sess = self.sessions.get(addr)
            if sess is not None and sess.closed:
                sess = None
            if (sess is not None and sess.state == "open"
                    and now - sess.last_activity > SESSION_IDLE_TIMEOUT):
                self._close_session(sess, "idle-timeout")
                sess = None
            if sess is None:
                # 建会话：先做配额准入（会话从首包起即计入）
                admitted, reason, victim = self._admit(addr[0])
                if not admitted:
                    self._reject_datagram(data, addr, reason)
                    return
                if victim is not None:
                    # 全局满额：先腾退多席来源中最久未活动的会话，
                    # 关闭其上游端口，迟到回包不可能再投递
                    self._close_session(victim, "quota-eviction")
                    self.store.log(
                        "event", client=fmt_addr(addr), note=reason
                    )
                sess = Session(self.next_session_id, addr)
                self.next_session_id += 1
                self.sessions[addr] = sess
                self.source_usage[addr[0]] = self.source_usage.get(addr[0], 0) + 1
                sess.pending.append(data)
                self.loop.create_task(self._open_session(sess))
                return
            sess.last_activity = now
            if sess.state == "opening":
                sess.pending.append(data)
                return
        self._forward(sess, data)

    async def _open_session(self, sess):
        target = self.store.get_target()  # 建会话时固定接收器
        host, port = RECEIVERS[target]
        client = fmt_addr(sess.client_addr)
        try:
            transport = None
            upstream_port = None
            last_err = None
            for _ in range(8):
                upstream_port = self.ports.allocate()
                try:
                    transport, proto = await self.loop.create_datagram_endpoint(
                        lambda: UpstreamProtocol(self),
                        local_addr=("0.0.0.0", upstream_port),
                        remote_addr=(host, port),  # connect：只收该接收器的回包
                    )
                    break
                except OSError as exc:
                    last_err = exc
                    self.ports.discard(upstream_port)
            if transport is None:
                raise last_err or RuntimeError("无可用上游端口")
            with self.lock:
                if sess.closed:
                    transport.close()
                    self.ports.release(upstream_port)
                    return
                sess.receiver_name = target
                sess.receiver_addr = (host, port)
                sess.upstream_port = upstream_port
                sess.transport = transport
                sess.state = "open"
                proto.session = sess
                pending, sess.pending = sess.pending, []
            self.store.log("event", client=client, session_id=sess.id, receiver=target,
                           note=f"会话建立，固定到接收器 {target}（上游端口 {upstream_port}）")
            for data in pending:
                self._forward(sess, data)
        except Exception as exc:
            self.store.log("event", client=client, session_id=sess.id,
                           note=f"会话建立失败: {exc}")
            self.close_session(sess, f"open-failed: {exc}")

    def _forward(self, sess, data):
        with self.lock:
            if sess.closed or sess.transport is None:
                return
            sess.last_activity = time.monotonic()
        try:
            sess.transport.sendto(data)  # 已 connect，原样发往该会话的接收器
        except Exception as exc:
            self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           note=f"转发失败: {exc}")
            return
        self.store.log("c2r", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))

    # ---- 接收器方向 ----
    def on_upstream_datagram(self, sess, data):
        now = time.monotonic()
        with self.lock:
            if sess.closed or self.sessions.get(sess.client_addr) is not sess:
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="旧会话迟到的回包，已丢弃")
                return
            if now - sess.last_activity > SESSION_IDLE_TIMEOUT:
                self._close_session(sess, "idle-timeout")
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="回包到达时会话已过期，已丢弃")
                return
            sess.last_activity = now
        self.client_transport.sendto(data, sess.client_addr)  # 原样送回该客户端
        self.store.log("r2c", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))

    # ---- 会话生命周期 ----
    def _close_session(self, sess, reason):
        """调用方需持有 self.lock。关闭即归还配额名额（空闲过期/建连失败/腾退）。"""
        if sess.closed:
            return
        sess.closed = True
        sess.state = "closed"
        if self.sessions.get(sess.client_addr) is sess:
            del self.sessions[sess.client_addr]
        # 归还来源名额（会话从首包起占位，关闭即还位）
        ip = sess.client_addr[0]
        if ip in self.source_usage:
            self.source_usage[ip] -= 1
            if self.source_usage[ip] <= 0:
                del self.source_usage[ip]
        transport, sess.transport = sess.transport, None
        port, sess.upstream_port = sess.upstream_port, None
        if transport is not None:
            transport.close()  # 迟到的回包到此端口将被系统丢弃
        if port is not None:
            self.ports.release(port)  # 进入冷却期，冷却内不再分配
        self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, note=f"会话关闭（{reason}）")

    def close_session(self, sess, reason):
        with self.lock:
            self._close_session(sess, reason)

    async def _sweeper(self):
        while True:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            with self.lock:
                expired = [s for s in self.sessions.values()
                           if not s.closed and now - s.last_activity > SESSION_IDLE_TIMEOUT]
                for sess in expired:
                    self._close_session(sess, "idle-timeout")

    def snapshot_sessions(self):
        now = time.monotonic()
        with self.lock:
            return [
                {
                    "id": s.id,
                    "client": fmt_addr(s.client_addr),
                    "source_ip": s.client_addr[0],
                    "receiver": s.receiver_name or "?",
                    "upstream_port": s.upstream_port,
                    "state": s.state,
                    "idle": round(now - s.last_activity, 1),
                    "age": round(now - s.created, 1),
                }
                for s in sorted(self.sessions.values(), key=lambda x: x.id)
            ]

    def snapshot_quota(self):
        with self.lock:
            return {
                "version": self.quota["version"] if self.quota else None,
                "global": self.quota["global"] if self.quota else None,
                "per_source": self.quota["per_source"] if self.quota else None,
                "active_sessions": len(self.sessions),
                "source_count": len(self.source_usage),
                "per_source_usage": dict(self.source_usage),
            }


# ---------------- Flask 网页 ----------------
store = Store(DB_PATH)
state = ProxyState(store)
app = Flask(__name__)
app.secret_key = os.urandom(16)

_last_test = {}
_last_test_lock = threading.Lock()

DIRECTION_LABELS = {
    "c2r": "客户端→接收器",
    "r2c": "接收器→客户端",
    "event": "事件",
    "drop": "丢弃",
    "reject": "配额拒绝",
}


def _quota_authorized():
    return hmac.compare_digest(request.headers.get("X-Quota-Token", ""), QUOTA_TOKEN)


def _format_message(m):
    m = dict(m)
    m["ts"] = fmt_ts(m["ts"])
    m["direction"] = DIRECTION_LABELS.get(m["direction"], m["direction"])
    return m


@app.get("/")
def index():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return render_template(
        "index.html",
        target=store.get_target(),
        receivers={k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        idle_timeout=SESSION_IDLE_TIMEOUT,
        udp_listen_port=UDP_LISTEN_PORT,
        last_test=lt,
        quota=state.snapshot_quota(),
        test_source_ips=TEST_SOURCE_IPS,
    )


@app.get("/api/state")
def api_state():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return jsonify({
        "target": store.get_target(),
        "receivers": {k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        "idle_timeout": SESSION_IDLE_TIMEOUT,
        "sessions": state.snapshot_sessions(),
        "messages": [_format_message(m) for m in store.recent_messages(100)],
        "last_test": lt,
        "quota": state.snapshot_quota(),
    })


# ---------------- 配额控制服务对接接口 ----------------
@app.get("/admin/quota")
def admin_quota_get():
    if not _quota_authorized():
        return jsonify({"error": "unauthorized"}), 401
    return jsonify(state.snapshot_quota())


@app.post("/admin/quota/apply")
def admin_quota_apply():
    if not _quota_authorized():
        return jsonify({"error": "unauthorized"}), 401
    payload = request.get_json(silent=True) or {}
    try:
        q = state.apply_quota(
            int(payload["version"]),
            int(payload["global"]),
            int(payload["per_source"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    store.log("event", note=f"应用配额控制服务推送 v{q['version']}："
                            f"全局 {q['global']} / 单来源 {q['per_source']}")
    return jsonify({"ok": True, "applied": q})


@app.post("/switch")
def switch():
    target = request.form.get("target", "")
    try:
        store.set_target(target)
        store.log("event", note=f"网页切换目标为接收器 {target}")
        flash(f"已切换到接收器 {target}。新会话走 {target}，旧会话继续走原接收器直至过期。", "ok")
    except Exception as exc:
        flash(f"切换保存失败，保留原目标 {store.get_target()}。原因：{exc}", "error")
    return redirect(url_for("index"))


@app.post("/test")
def test():
    global _last_test
    result = run_test(
        payload=request.form.get("payload", "PING"),
        delay_s=request.form.get("delay", "").strip(),
        client_ip_s=request.form.get("client_ip", "127.0.0.1").strip(),
        client_port_s=request.form.get("client_port", "").strip(),
        wait_s=request.form.get("wait", "10").strip(),
    )
    with _last_test_lock:
        _last_test = result
    return redirect(url_for("index"))


def run_test(payload, delay_s, client_ip_s, client_port_s, wait_s):
    """从网页后端经代理发一条试传报文并等待回包。

    支持选择来源 IP（127.0.0.1~127.0.0.3）与固定源端口；
    用于模拟异常终端不断更换 UDP 源端口等场景。
    """
    try:
        wait = min(max(float(wait_s), 0.5), 60.0)
    except ValueError:
        wait = 10.0
    if client_ip_s not in TEST_SOURCE_IPS:
        client_ip_s = "127.0.0.1"
    wire = payload
    try:
        delay = float(delay_s)
        if delay > 0:
            wire = f"DELAY:{delay}:{payload}"  # 接收器识别的延迟回包指令
    except ValueError:
        pass
    data = wire.encode()
    result = {"time": fmt_ts(time.time()), "payload": wire, "ok": False,
              "reply": "", "error": "", "rtt": None, "client": "", "receiver": None}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if client_port_s:
            # 固定源端口
            sock.bind((client_ip_s, int(client_port_s)))
        else:
            # 不填端口：绑定到选定来源 IP，由系统分配端口
            sock.bind((client_ip_s, 0))
        sock.settimeout(wait)
        sock.connect(("127.0.0.1", UDP_LISTEN_PORT))
        result["client"] = fmt_addr(sock.getsockname())
        t0 = time.monotonic()
        sock.send(data)
        try:
            reply = sock.recv(65535)
            result["ok"] = True
            result["rtt"] = round(time.monotonic() - t0, 3)
            result["reply"] = preview(reply)
        except socket.timeout:
            result["error"] = f"等待回包超时（{wait}s）——可能被配额拒绝"
    except Exception as exc:
        result["error"] = f"发送失败：{exc}"
    finally:
        sock.close()
    time.sleep(0.05)  # 等代理把日志落库
    if result["client"]:
        result["receiver"] = store.latest_receiver_for_client(result["client"])
    return result


# ---------------- 启动：asyncio 线程 + Flask 主线程 ----------------
def _run_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state.loop = loop
    loop.run_until_complete(state.start())
    loop.run_forever()


def main():
    # 代理重启须先取得保存版配额再接纳新会话：拉取成功前不绑定 UDP、不提供网页
    state.bootstrap_quota()
    t = threading.Thread(target=_run_loop, name="udp-proxy-loop", daemon=True)
    t.start()
    print(f"[proxy] 操作网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
