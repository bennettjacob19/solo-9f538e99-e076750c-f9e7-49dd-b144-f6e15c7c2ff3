#!/usr/bin/env python3
"""遥测 UDP 代理 + 操作网页（含会话配额控制）。

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
- 配额（全局/单来源 IP 会话上限）由独立的配额控制服务下发：
  * 代理启动时必须先取得控制服务的“保存版”配额，然后才开始接纳新会话；
  * 会话从首包起（含建连中 opening）即计入配额；
  * 单来源 IP 满额：首包直接拒绝（丢弃并记录，不分配上游端口）；
  * 全局满额：可腾退“占有多于一席的来源”中最久未活动的会话腾出名额；
    若所有来源都只占一席，则拒绝新会话；
  * 收紧上限不踢旧会话，用量回落前不接纳新会话；
    空闲过期或建连失败会归还名额；被腾退会话的迟到回包不能投递；
  * 代理只按“已应用版”配额接纳，控制服务联系失败不影响已有会话照常透传。
- Flask 网页可切换目标、发送试传报文（可选择源 IP 127.0.0.1~3 与源端口）、
  查看会话、每来源用量与报文日志。
"""

import asyncio
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

# 配额控制服务：留空则不对接（使用内置默认配额，启动不等待）。
QUOTA_CONTROL_URL = os.environ.get("QUOTA_CONTROL_URL", "").rstrip("/")
QUOTA_FETCH_INTERVAL = float(os.environ.get("QUOTA_FETCH_INTERVAL", "0.2"))
QUOTA_HTTP_TIMEOUT = float(os.environ.get("QUOTA_HTTP_TIMEOUT", "3.0"))
GLOBAL_CAP_MIN, GLOBAL_CAP_MAX = 2, 16
PER_IP_CAP_MIN, PER_IP_CAP_MAX = 1, 8
DEFAULT_GLOBAL_CAP, DEFAULT_PER_IP_CAP = GLOBAL_CAP_MAX, PER_IP_CAP_MAX

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
# 网页试传允许选择的源 IP（均为 loopback 别名）。
TEST_SOURCE_IPS = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]


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

    def latest_note_for_client(self, client):
        with self._lock:
            row = self._conn.execute(
                "SELECT note FROM message_log WHERE client=? AND note<>'' "
                "ORDER BY id DESC LIMIT 1",
                (client,),
            ).fetchone()
        return row[0] if row else ""


# ---------------- 配额状态（代理已应用版） ----------------
class QuotaState:
    """代理内存中的“已应用版”配额。控制服务联系失败也不影响它，代理继续按此接纳。"""

    def __init__(self):
        self._lock = threading.RLock()
        self.version = 0
        self.global_cap = DEFAULT_GLOBAL_CAP
        self.per_ip_cap = DEFAULT_PER_IP_CAP
        self.ready = False          # 启动时是否已取得保存版
        self.applied_at = None      # 最近一次应用时间
        self.ready_event = threading.Event()  # 任一路径（拉取/推送）应用配额即触发

    @staticmethod
    def validate(version, global_cap, per_ip_cap):
        if not isinstance(version, int) or version < 1:
            raise ValueError("version 必须是 >=1 的整数")
        if not (GLOBAL_CAP_MIN <= global_cap <= GLOBAL_CAP_MAX):
            raise ValueError(f"全局上限必须在 {GLOBAL_CAP_MIN}～{GLOBAL_CAP_MAX} 之间")
        if not (PER_IP_CAP_MIN <= per_ip_cap <= PER_IP_CAP_MAX):
            raise ValueError(f"单来源 IP 上限必须在 {PER_IP_CAP_MIN}～{PER_IP_CAP_MAX} 之间")
        if per_ip_cap > global_cap:
            raise ValueError("单来源 IP 上限不能大于全局上限")

    def apply(self, version, global_cap, per_ip_cap):
        """应用控制服务下发的版本。相同版本幂等；更旧版本拒绝。返回是否发生变更。"""
        self.validate(version, global_cap, per_ip_cap)
        with self._lock:
            if version < self.version:
                raise ValueError(f"版本过旧：当前已应用 v{self.version}，拒绝 v{version}")
            changed = version != self.version
            self.version = version
            self.global_cap = global_cap
            self.per_ip_cap = per_ip_cap
            self.ready = True
            if changed:
                self.applied_at = time.time()
            self.ready_event.set()
            return changed

    def snapshot(self):
        with self._lock:
            return {
                "version": self.version,
                "global_cap": self.global_cap,
                "per_ip_cap": self.per_ip_cap,
                "ready": self.ready,
                "applied_at": fmt_ts(self.applied_at) if self.applied_at else "",
            }


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
    def __init__(self, store, quota):
        self.store = store
        self.quota = quota
        self.loop = None
        self.client_transport = None
        self.sessions = {}  # client_addr -> Session（仅事件循环线程修改）
        self.lock = threading.RLock()
        self.next_session_id = 1
        self.ports = PortAllocator(UPSTREAM_PORT_MIN, UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)

    async def start(self):
        # 先取得配额保存版，再绑定 UDP：绑定之前任何首包都进不来，
        # 因此“未取得配额不会接纳新会话”由启动顺序直接保证。
        await self._fetch_initial_quota()
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: ClientProtocol(self),
            local_addr=(UDP_LISTEN_HOST, UDP_LISTEN_PORT),
        )
        self.client_transport = transport
        self.loop.create_task(self._sweeper())
        print(f"[proxy] 客户端 UDP 监听 {UDP_LISTEN_HOST}:{UDP_LISTEN_PORT}", flush=True)
        print(f"[proxy] 空闲会话超时 {SESSION_IDLE_TIMEOUT}s，当前目标 {self.store.get_target()}",
              flush=True)

    async def _fetch_initial_quota(self):
        if not QUOTA_CONTROL_URL:
            print("[proxy] 未配置 QUOTA_CONTROL_URL，使用内置默认配额", flush=True)
            self.quota.ready = True
            self.quota.ready_event.set()
            return
        url = QUOTA_CONTROL_URL + "/api/quota/saved"

        def _blocking_fetch():
            with urllib.request.urlopen(url, timeout=QUOTA_HTTP_TIMEOUT) as resp:
                return json.loads(resp.read().decode())

        attempt = 0
        while True:
            # 控制服务的推送可能先一步应用配额：事件触发后立即继续绑定 UDP，
            # 不被重试间隔的睡眠卡住。
            if await self.loop.run_in_executor(
                None, lambda: self.quota.ready_event.wait(QUOTA_FETCH_INTERVAL)
            ):
                snap = self.quota.snapshot()
                print(f"[proxy] 配额已就绪 v{snap['version']}："
                      f"全局 {snap['global_cap']} / 单 IP {snap['per_ip_cap']}", flush=True)
                return
            attempt += 1
            try:
                payload = await self.loop.run_in_executor(None, _blocking_fetch)
                self.quota.apply(
                    int(payload["version"]),
                    int(payload["global_cap"]),
                    int(payload["per_ip_cap"]),
                )
                snap = self.quota.snapshot()
                print(f"[proxy] 启动取得配额保存版 v{snap['version']}："
                      f"全局 {snap['global_cap']} / 单 IP {snap['per_ip_cap']}", flush=True)
                return
            except Exception as exc:
                print(f"[proxy] 第 {attempt} 次获取配额保存版失败，{QUOTA_FETCH_INTERVAL}s 后重试："
                      f"{exc}", flush=True)

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
                # 首包：先过配额（从首包起即计入，含 opening），通过才建会话
                ok, reason = self._admit(addr[0], now)
                if not ok:
                    self.store.log(
                        "drop", client=fmt_addr(addr), note=f"配额拒绝首包：{reason}"
                    )
                    return
                sess = Session(self.next_session_id, addr)
                self.next_session_id += 1
                self.sessions[addr] = sess  # 立刻占名额：opening 也计数
                sess.pending.append(data)
                self.loop.create_task(self._open_session(sess))
                return
            sess.last_activity = now
            if sess.state == "opening":
                sess.pending.append(data)
                return
        self._forward(sess, data)

    def _admit(self, src_ip, now):
        """配额接纳。调用方持有 self.lock。返回 (是否接纳, 拒绝原因)。"""
        q = self.quota.snapshot()
        per_ip_count = sum(1 for s in self.sessions.values() if s.client_addr[0] == src_ip)
        if per_ip_count >= q["per_ip_cap"]:
            return False, (f"来源 {src_ip} 已有 {per_ip_count} 个会话，"
                           f"达到单来源 IP 上限 {q['per_ip_cap']}（已应用 v{q['version']}）")
        if len(self.sessions) < q["global_cap"]:
            return True, ""
        # 全局满额：从占有多于一席的来源中，腾退最久未活动的会话
        source_counts = {}
        for s in self.sessions.values():
            source_counts[s.client_addr[0]] = source_counts.get(s.client_addr[0], 0) + 1
        multi_slot_sessions = [
            s for s in self.sessions.values()
            if source_counts[s.client_addr[0]] >= 2
        ]
        if not multi_slot_sessions:
            return False, (f"全局会话上限 {q['global_cap']} 已满，且各来源均仅占一席"
                           f"（已应用 v{q['version']}）")
        victim = min(multi_slot_sessions, key=lambda s: (s.last_activity, s.id))
        victim_client = fmt_addr(victim.client_addr)
        self._close_session(
            victim,
            f"quota-evict：全局满额，为来源 {src_ip} 腾退占多席来源 "
            f"{victim.client_addr[0]} 中最久未活动的会话",
        )
        self.store.log(
            "event", client=victim_client, session_id=victim.id,
            note=f"配额腾退：关闭 {victim_client} 为新来源 {src_ip} 腾出名额"
        )
        return True, ""

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
            self.close_session(sess, f"open-failed: {exc}")  # 建连失败归还名额

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
        """调用方需持有 self.lock。"""
        if sess.closed:
            return
        sess.closed = True
        sess.state = "closed"
        if self.sessions.get(sess.client_addr) is sess:
            del self.sessions[sess.client_addr]
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
                    self._close_session(sess, "idle-timeout")  # 空闲过期归还名额

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

    def snapshot_usage(self):
        """每来源 IP 的会话用量（含建连中）。"""
        with self.lock:
            counts = {}
            for s in self.sessions.values():
                counts[s.client_addr[0]] = counts.get(s.client_addr[0], 0) + 1
        return [{"ip": ip, "sessions": n}
                for ip, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


# ---------------- Flask 网页 ----------------
store = Store(DB_PATH)
quota = QuotaState()
state = ProxyState(store, quota)
app = Flask(__name__)
app.secret_key = os.urandom(16)

_last_test = {}
_last_test_lock = threading.Lock()

DIRECTION_LABELS = {
    "c2r": "客户端→接收器",
    "r2c": "接收器→客户端",
    "event": "事件",
    "drop": "丢弃",
}


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
        quota=quota.snapshot(),
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
        "usage": state.snapshot_usage(),
        "quota": quota.snapshot(),
        "messages": [_format_message(m) for m in store.recent_messages(100)],
        "last_test": lt,
    })


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


# ---- 配额控制服务的内部接口 ----
@app.post("/internal/quota")
def internal_apply_quota():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "请求体必须是 JSON"}), 400
    try:
        changed = quota.apply(
            int(payload["version"]),
            int(payload["global_cap"]),
            int(payload["per_ip_cap"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    snap = quota.snapshot()
    store.log("event",
              note=f"应用配额控制服务版本 v{snap['version']}：全局 {snap['global_cap']} / "
                   f"单 IP {snap['per_ip_cap']}（收紧不踢旧会话）")
    print(f"[proxy] 配额已应用 v{snap['version']}（变更={changed}）", flush=True)
    return jsonify({"ok": True, "changed": changed, "version": snap["version"],
                    "global_cap": snap["global_cap"], "per_ip_cap": snap["per_ip_cap"]})


@app.get("/internal/quota")
def internal_get_quota():
    return jsonify(quota.snapshot())


@app.post("/test")
def test():
    global _last_test
    result = run_test(
        payload=request.form.get("payload", "PING"),
        delay_s=request.form.get("delay", "").strip(),
        client_port_s=request.form.get("client_port", "").strip(),
        wait_s=request.form.get("wait", "10").strip(),
        source_ip=request.form.get("source_ip", TEST_SOURCE_IPS[0]).strip(),
    )
    with _last_test_lock:
        _last_test = result
    return redirect(url_for("index"))


def run_test(payload, delay_s, client_port_s, wait_s, source_ip):
    """从网页后端经代理发一条试传报文并等待回包。可选择源 IP（127.0.0.1~3）与源端口。"""
    try:
        wait = min(max(float(wait_s), 0.5), 60.0)
    except ValueError:
        wait = 10.0
    if source_ip not in TEST_SOURCE_IPS:
        source_ip = TEST_SOURCE_IPS[0]
    wire = payload
    try:
        delay = float(delay_s)
        if delay > 0:
            wire = f"DELAY:{delay}:{payload}"  # 接收器识别的延迟回包指令
    except ValueError:
        pass
    data = wire.encode()
    result = {"time": fmt_ts(time.time()), "payload": wire, "ok": False,
              "reply": "", "error": "", "rtt": None, "client": "", "receiver": None,
              "source_ip": source_ip, "quota_rejected": False}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bind_port = int(client_port_s) if client_port_s else 0
        sock.bind((source_ip, bind_port))  # 源 IP 可选 127.0.0.1~3，源端口可固定
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
            result["error"] = f"等待回包超时（{wait}s）"
    except Exception as exc:
        result["error"] = f"发送失败：{exc}"
    finally:
        sock.close()
    time.sleep(0.05)  # 等代理把日志落库
    if result["client"]:
        result["receiver"] = store.latest_receiver_for_client(result["client"])
        note = store.latest_note_for_client(result["client"])
        if "配额拒绝" in note:
            result["quota_rejected"] = True
            result["error"] = f"被配额拒绝：{note}"
    return result


# ---------------- 启动：asyncio 线程 + Flask 主线程 ----------------
def _run_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state.loop = loop
    loop.run_until_complete(state.start())
    loop.run_forever()


def main():
    t = threading.Thread(target=_run_loop, name="udp-proxy-loop", daemon=True)
    t.start()
    print(f"[proxy] 操作网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
