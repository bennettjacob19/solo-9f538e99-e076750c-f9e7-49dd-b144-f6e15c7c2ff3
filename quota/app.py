#!/usr/bin/env python3
"""独立运行的配额控制服务。

职责：
- 提供操作网页：值班员设置“全局活动会话上限”(2~16) 与“单来源 IP 上限”(1~8)；
- 每个版本先落 SQLite（保存版），再由后台同步线程推送给代理；
  只有代理确认应用后，该版本才标记为“已生效（代理已确认版）”；
- 联系代理失败时保留“待应用版”并周期性重试，控制服务重启后仍能继续应用；
- 网页同时展示保存版、代理已确认版、代理连通状态与代理侧实时用量。

代理侧接口（带 X-Quota-Token）：
- GET  /quota/saved        返回最新保存版（代理重启启动时拉取）
代理须实现：
- GET  /admin/quota        返回代理已应用版本
- POST /admin/quota/apply  应用新版本并回确认
"""

import json
import hmac
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

# ---------------- 配置 ----------------
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8081"))
DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "quota.db")
)
PROXY_BASE_URL = os.environ.get("PROXY_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
QUOTA_TOKEN = os.environ.get("QUOTA_TOKEN", "quota-shared-secret")
SYNC_INTERVAL = float(os.environ.get("SYNC_INTERVAL", "2"))

GLOBAL_MIN, GLOBAL_MAX = 2, 16
PER_SOURCE_MIN, PER_SOURCE_MAX = 1, 8
DEFAULT_GLOBAL, DEFAULT_PER_SOURCE = 8, 2

# 版本状态：saved=待应用（保存版），applied=代理已确认（已生效），
# superseded=被更新的保存版取代且从未应用
ST_SAVED, ST_APPLIED, ST_SUPERSEDED = "saved", "applied", "superseded"


def fmt_ts(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


def validate_limits(global_limit, per_source):
    """返回 (global:int, per_source:int)；非法输入抛 ValueError。"""
    try:
        g = int(global_limit)
        p = int(per_source)
    except (TypeError, ValueError):
        raise ValueError("上限必须是整数")
    if not (GLOBAL_MIN <= g <= GLOBAL_MAX):
        raise ValueError(f"全局活动会话上限须在 {GLOBAL_MIN}~{GLOBAL_MAX} 之间")
    if not (PER_SOURCE_MIN <= p <= PER_SOURCE_MAX):
        raise ValueError(f"单来源 IP 上限须在 {PER_SOURCE_MIN}~{PER_SOURCE_MAX} 之间")
    return g, p


# ---------------- SQLite：配额版本持久化 ----------------
class QuotaStore:
    """所有版本保留在 SQLite；保存失败则整体回滚，不改当前版或待应用版。"""

    def __init__(self, path):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS quota_versions (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   version INTEGER NOT NULL UNIQUE,
                   global_limit INTEGER NOT NULL,
                   per_source INTEGER NOT NULL,
                   status TEXT NOT NULL,
                   created_at REAL NOT NULL,
                   applied_at REAL)"""
        )
        self._conn.commit()
        if self._conn.execute("SELECT COUNT(*) FROM quota_versions").fetchone()[0] == 0:
            self._conn.execute(
                "INSERT INTO quota_versions(version,global_limit,per_source,status,created_at)"
                " VALUES(1,?,?,?,?)",
                (DEFAULT_GLOBAL, DEFAULT_PER_SOURCE, ST_SAVED, time.time()),
            )
            self._conn.commit()

    def _row_to_dict(self, row):
        if row is None:
            return None
        return {
            "version": row[1],
            "global": row[2],
            "per_source": row[3],
            "status": row[4],
            "created_at": row[5],
            "created_at_text": fmt_ts(row[5]),
            "applied_at": row[6],
            "applied_at_text": fmt_ts(row[6]) if row[6] else None,
        }

    def latest(self):
        """最新保存版（无论是否已应用）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT id,version,global_limit,per_source,status,created_at,applied_at "
                "FROM quota_versions ORDER BY version DESC LIMIT 1"
            ).fetchone()
        return self._row_to_dict(row)

    def pending(self):
        """待应用版：最新一行状态为 saved 时返回，否则 None。"""
        row = self.latest()
        return row if row and row["status"] == ST_SAVED else None

    def applied(self):
        """代理已确认版：最近一个 applied 版本。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT id,version,global_limit,per_source,status,created_at,applied_at "
                "FROM quota_versions WHERE status=? ORDER BY version DESC LIMIT 1",
                (ST_APPLIED,),
            ).fetchone()
        return self._row_to_dict(row)

    def add_version(self, global_limit, per_source):
        """校验后落库新保存版。

        旧的待应用版标记为 superseded；任何一步失败都抛异常并回滚，
        已保存版/待应用版保持不变。返回新版本号。
        """
        g, p = validate_limits(global_limit, per_source)
        with self._lock:
            try:
                latest = self._conn.execute(
                    "SELECT version,global_limit,per_source,status FROM quota_versions"
                    " ORDER BY version DESC LIMIT 1"
                ).fetchone()
                if latest and latest[1] == g and latest[2] == p:
                    # 与最新版完全相同：不产生新版本，直接返回现有版本
                    self._conn.commit()
                    return latest[0]
                new_version = (latest[0] if latest else 0) + 1
                self._conn.execute(
                    "UPDATE quota_versions SET status=? WHERE status=?",
                    (ST_SUPERSEDED, ST_SAVED),
                )
                cur = self._conn.execute(
                    "INSERT INTO quota_versions(version,global_limit,per_source,"
                    "status,created_at) VALUES(?,?,?,?,?)",
                    (new_version, g, p, ST_SAVED, time.time()),
                )
                self._conn.commit()
                return cur.lastrowid
            except sqlite3.Error:
                self._conn.rollback()
                raise

    def mark_applied(self, version):
        with self._lock:
            self._conn.execute(
                "UPDATE quota_versions SET status=?, applied_at=? WHERE version=?",
                (ST_APPLIED, time.time(), version),
            )
            self._conn.commit()


# ---------------- 与代理的 HTTP 通信 ----------------
class ProxyClient:
    def __init__(self, base_url, token):
        self.base_url = base_url
        self.token = token

    def _request(self, method, path, body=None, timeout=5):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={
                "X-Quota-Token": self.token,
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.load(resp)

    def get_applied(self):
        return self._request("GET", "/admin/quota")

    def apply(self, version, global_limit, per_source):
        return self._request(
            "POST",
            "/admin/quota/apply",
            {"version": version, "global": global_limit, "per_source": per_source},
        )

    def get_state(self):
        # 代理操作网页的公开接口，无需令牌
        req = urllib.request.Request(self.base_url + "/api/state", method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.load(resp)


# ---------------- 后台同步线程 ----------------
class Syncer(threading.Thread):
    """周期性把待应用版推送给代理，并拉取代理侧实时用量。

    - 代理确认（回包且 GET 版本一致）后才把保存版标记为 applied；
    - 联系失败保留待应用版，记录最近错误，下个周期重试；
    - 线程随进程启动，控制服务重启后自动继续应用 SQLite 里的待处理版本。
    """

    def __init__(self, store, client):
        super().__init__(name="quota-syncer", daemon=True)
        self.store = store
        self.client = client
        self.last_error = ""
        self.last_error_at = None
        self.proxy_reachable = False
        self.proxy_state = None
        self._stat_lock = threading.Lock()

    def _set_error(self, msg):
        with self._stat_lock:
            self.last_error = msg
            self.last_error_at = time.time()

    def snapshot(self):
        with self._stat_lock:
            return {
                "proxy_reachable": self.proxy_reachable,
                "last_error": self.last_error,
                "last_error_at": self.last_error_at,
                "last_error_at_text": fmt_ts(self.last_error_at)
                if self.last_error_at
                else None,
                "proxy_state": self.proxy_state,
            }

    def run(self):
        while True:
            try:
                self._tick()
            except Exception as exc:  # 同步线程绝不能退出
                self._set_error(f"同步周期异常: {exc}")
            time.sleep(SYNC_INTERVAL)

    def _tick(self):
        pending = self.store.pending()
        if pending is not None:
            try:
                self.client.apply(
                    pending["version"], pending["global"], pending["per_source"]
                )
                confirmed = self.client.get_applied()
                if confirmed.get("version") != pending["version"]:
                    self._set_error(
                        f"代理确认版本不一致：期望 v{pending['version']}，"
                        f"实际 v{confirmed.get('version')}"
                    )
                else:
                    self.store.mark_applied(pending["version"])
                    self._set_error("")
            except (urllib.error.URLError, OSError, ValueError, KeyError) as exc:
                # 联系失败：保留待应用版，等待下个周期重试
                self._set_error(f"推送代理失败（保留待应用版）：{exc}")
        try:
            state = self.client.get_state()
            with self._stat_lock:
                self.proxy_reachable = True
                self.proxy_state = state
        except (urllib.error.URLError, OSError, ValueError):
            with self._stat_lock:
                self.proxy_reachable = False
                self.proxy_state = None


# ---------------- Flask 网页 ----------------
store = QuotaStore(DB_PATH)
proxy_client = ProxyClient(PROXY_BASE_URL, QUOTA_TOKEN)
syncer = Syncer(store, proxy_client)

app = Flask(__name__)
app.secret_key = os.urandom(16)


def _check_token():
    return hmac.compare_digest(request.headers.get("X-Quota-Token", ""), QUOTA_TOKEN)


@app.get("/")
def index():
    return render_template(
        "index.html",
        saved=store.latest(),
        applied=store.applied(),
        sync=syncer.snapshot(),
        global_min=GLOBAL_MIN,
        global_max=GLOBAL_MAX,
        per_source_min=PER_SOURCE_MIN,
        per_source_max=PER_SOURCE_MAX,
    )


@app.get("/api/status")
def api_status():
    saved = store.latest()
    applied = store.applied()
    sync = syncer.snapshot()
    pending = saved is not None and (
        applied is None or saved["version"] != applied["version"]
    )
    return jsonify(
        {
            "saved": saved,
            "applied": applied,
            "pending": pending,
            "proxy_reachable": sync["proxy_reachable"],
            "last_error": sync["last_error"],
            "last_error_at_text": sync["last_error_at_text"],
            "proxy_state": sync["proxy_state"],
            "limits": {
                "global": [GLOBAL_MIN, GLOBAL_MAX],
                "per_source": [PER_SOURCE_MIN, PER_SOURCE_MAX],
            },
        }
    )


@app.post("/save")
def save():
    try:
        version = store.add_version(
            request.form.get("global_limit", ""),
            request.form.get("per_source", ""),
        )
        flash(
            f"已保存为 v{version}（保存版）。代理确认应用后才会显示为已生效；"
            "若联系代理失败则保留为待应用版并自动重试。",
            "ok",
        )
    except (ValueError, sqlite3.Error) as exc:
        # 保存失败：当前版与待应用版均不变
        flash(f"保存失败，未改动任何版本。原因：{exc}", "error")
    return redirect(url_for("index"))


@app.get("/quota/saved")
def quota_saved():
    """代理重启启动时拉取保存版用。"""
    if not _check_token():
        return jsonify({"error": "unauthorized"}), 401
    row = store.latest()
    if row is None:
        return jsonify({"error": "no saved quota"}), 404
    return jsonify(
        {
            "version": row["version"],
            "global": row["global"],
            "per_source": row["per_source"],
        }
    )


def main():
    syncer.start()
    print(f"[quota] 配额控制网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    print(f"[quota] 代理地址 {PROXY_BASE_URL}，同步间隔 {SYNC_INTERVAL}s", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
