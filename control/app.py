#!/usr/bin/env python3
"""独立配额控制服务：网页维护全局/单来源 IP 会话配额，并把“保存版”推送给代理确认。

职责边界：
- 本服务只负责配额版本的持久化（保存版）、向代理推送、记录代理确认版（已应用版）；
- 真正的会话接纳/腾退由代理按“已应用版”执行，本服务不参与数据面转发。

版本生命周期：
- 每次“保存”产生一个新版本（版本号自增），事务落库；保存失败不会改动任何已有版本。
- 后台推送线程把最新保存版 POST 给代理的内部接口；代理确认应用后记录为“已应用版”。
- 联系不上代理时，保存版原样保留为“待应用版”，推送线程持续重试；
  本服务重启后仍能从 SQLite 读出未应用的版本并继续推送。

网页展示：保存版（最新版本）与代理已确认版（已应用版），以及二者是否一致。

环境变量：
  HTTP_PORT          本服务网页端口（默认 8081）
  DB_PATH            SQLite 路径（默认 /data/control.db）
  PROXY_CONTROL_URL  代理内部接口基址（默认 http://proxy:8080）
  APPLY_INTERVAL     待应用版本的重试推送间隔秒（默认 1.0）
  HTTP_TIMEOUT       调用代理的超时秒（默认 3.0）
"""

import json
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
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "control.db")
)
PROXY_CONTROL_URL = os.environ.get("PROXY_CONTROL_URL", "http://proxy:8080").rstrip("/")
APPLY_INTERVAL = float(os.environ.get("APPLY_INTERVAL", "1.0"))
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "3.0"))
HISTORY_KEEP = int(os.environ.get("HISTORY_KEEP", "50"))

GLOBAL_MIN, GLOBAL_MAX = 2, 16
PER_IP_MIN, PER_IP_MAX = 1, 8
# 初始保存版：取范围上限，等价于“不限制”，与旧行为兼容。
DEFAULT_GLOBAL, DEFAULT_PER_IP = GLOBAL_MAX, PER_IP_MAX


def fmt_ts(ts):
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


def validate_caps(global_cap, per_ip_cap):
    if not (GLOBAL_MIN <= global_cap <= GLOBAL_MAX):
        raise ValueError(f"全局活动会话上限必须在 {GLOBAL_MIN}～{GLOBAL_MAX} 之间")
    if not (PER_IP_MIN <= per_ip_cap <= PER_IP_MAX):
        raise ValueError(f"单来源 IP 上限必须在 {PER_IP_MIN}～{PER_IP_MAX} 之间")
    if per_ip_cap > global_cap:
        raise ValueError("单来源 IP 上限不能大于全局上限")


# ---------------- SQLite：配额版本持久化 ----------------
class Store:
    """所有版本只增不改；meta 记录“最新保存版”和“代理确认版”两个指针。"""

    def __init__(self, path):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS quota_versions (
                   version INTEGER PRIMARY KEY,
                   ts REAL NOT NULL,
                   global_cap INTEGER NOT NULL,
                   per_ip_cap INTEGER NOT NULL)"""
        )
        self._conn.commit()
        if self._conn.execute("SELECT COUNT(*) FROM quota_versions").fetchone()[0] == 0:
            # 首版：默认配额，事务写入
            with self._conn:
                self._conn.execute(
                    "INSERT INTO quota_versions(version, ts, global_cap, per_ip_cap)"
                    " VALUES(1, ?, ?, ?)",
                    (time.time(), DEFAULT_GLOBAL, DEFAULT_PER_IP),
                )
                self._conn.execute(
                    "INSERT INTO meta(key, value) VALUES('saved_version', '1')"
                )

    def _version_row(self, version):
        row = self._conn.execute(
            "SELECT version, ts, global_cap, per_ip_cap FROM quota_versions"
            " WHERE version=?",
            (version,),
        ).fetchone()
        if row is None:
            return None
        return {"version": row[0], "ts": row[1],
                "global_cap": row[2], "per_ip_cap": row[3]}

    def get_saved(self):
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='saved_version'"
            ).fetchone()
            return self._version_row(int(row[0]))

    def get_applied(self):
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM meta WHERE key='applied_version'"
            ).fetchone()
            if row is None:
                return None
            return self._version_row(int(row[0]))

    def save(self, global_cap, per_ip_cap):
        """事务写入新版本并移动保存版指针；任何失败都不改动现有版本。"""
        validate_caps(global_cap, per_ip_cap)
        with self._lock:
            top = self._conn.execute(
                "SELECT COALESCE(MAX(version), 0) FROM quota_versions"
            ).fetchone()[0]
            new_version = top + 1
            with self._conn:
                self._conn.execute(
                    "INSERT INTO quota_versions(version, ts, global_cap, per_ip_cap)"
                    " VALUES(?, ?, ?, ?)",
                    (new_version, time.time(), global_cap, per_ip_cap),
                )
                self._conn.execute(
                    "INSERT OR REPLACE INTO meta(key, value) VALUES('saved_version', ?)",
                    (str(new_version),),
                )
                # 仅保留最近 HISTORY_KEEP 个版本
                self._conn.execute(
                    "DELETE FROM quota_versions WHERE version <= ?",
                    (max(0, new_version - HISTORY_KEEP),),
                )
            return self._version_row(new_version)

    def mark_applied(self, version):
        """代理确认某版本已应用。版本必须存在（历史清理后旧确认直接忽略）。"""
        with self._lock:
            if self._version_row(version) is None:
                return False
            self._conn.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES('applied_version', ?)",
                (str(version),),
            )
            self._conn.commit()
            return True

    def history(self, limit=20):
        with self._lock:
            rows = self._conn.execute(
                "SELECT version, ts, global_cap, per_ip_cap FROM quota_versions"
                " ORDER BY version DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [{"version": r[0], "ts": r[1], "global_cap": r[2], "per_ip_cap": r[3]}
                for r in rows]


# ---------------- 推送线程：保存版 -> 代理确认 ----------------
class ApplyWorker(threading.Thread):
    """把最新保存版推给代理；失败则保留待应用版并按间隔重试，重启后自动续推。"""

    def __init__(self, store):
        super().__init__(name="quota-apply-worker", daemon=True)
        self.store = store
        self._status_lock = threading.Lock()
        self.last_attempt_ts = None
        self.last_contact_ok = None   # None=尚未尝试；True/False=最近一次联系结果
        self.last_error = ""

    def _set_status(self, ok, error=""):
        with self._status_lock:
            self.last_attempt_ts = time.time()
            self.last_contact_ok = ok
            self.last_error = error

    def status(self):
        with self._status_lock:
            return {
                "last_attempt": fmt_ts(self.last_attempt_ts) if self.last_attempt_ts else "",
                "last_contact_ok": self.last_contact_ok,
                "last_error": self.last_error,
            }

    def _push_once(self, quota):
        url = PROXY_CONTROL_URL + "/internal/quota"
        body = (
            '{"version": %d, "global_cap": %d, "per_ip_cap": %d}'
            % (quota["version"], quota["global_cap"], quota["per_ip_cap"])
        ).encode()
        req = urllib.request.Request(
            url, data=body, method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            if resp.status != 200:
                raise RuntimeError(f"代理返回 HTTP {resp.status}")
            payload = json.loads(resp.read().decode())
        if int(payload.get("version", -1)) != quota["version"]:
            raise RuntimeError("代理确认的版本号与推送版本不一致")

    def run(self):
        while True:
            try:
                saved = self.store.get_saved()
                applied = self.store.get_applied()
                if applied is None or applied["version"] != saved["version"]:
                    try:
                        self._push_once(saved)
                        self.store.mark_applied(saved["version"])
                        self._set_status(True)
                        print(f"[control] 版本 {saved['version']} 已被代理确认应用", flush=True)
                    except Exception as exc:  # 联系失败：保留待应用版，下轮继续重试
                        self._set_status(False, str(exc))
                        print(f"[control] 推送版本 {saved['version']} 失败（保留待应用）：{exc}",
                              flush=True)
            except Exception as exc:
                self._set_status(False, f"内部错误: {exc}")
            time.sleep(APPLY_INTERVAL)


# ---------------- Flask 网页 ----------------
store = Store(DB_PATH)
worker = ApplyWorker(store)
app = Flask(__name__)
app.secret_key = os.urandom(16)
app.jinja_env.filters["tsfmt"] = fmt_ts


@app.get("/")
def index():
    saved = store.get_saved()
    applied = store.get_applied()
    return render_template(
        "index.html",
        saved=saved,
        applied=applied,
        pending=applied is None or applied["version"] != saved["version"],
        status=worker.status(),
        history=store.history(20),
        global_min=GLOBAL_MIN, global_max=GLOBAL_MAX,
        per_ip_min=PER_IP_MIN, per_ip_max=PER_IP_MAX,
        proxy_url=PROXY_CONTROL_URL,
    )


@app.get("/api/state")
def api_state():
    saved = store.get_saved()
    applied = store.get_applied()
    return jsonify({
        "saved": saved,
        "applied": applied,
        "pending": applied is None or applied["version"] != saved["version"],
        "limits": {"global": [GLOBAL_MIN, GLOBAL_MAX], "per_ip": [PER_IP_MIN, PER_IP_MAX]},
        "proxy_url": PROXY_CONTROL_URL,
        "contact": worker.status(),
        "history": store.history(20),
    })


@app.get("/api/quota/saved")
def api_quota_saved():
    """代理启动时调用：取得当前保存版（必须先取到它才接纳新会话）。"""
    return jsonify(store.get_saved())


@app.post("/settings")
def settings_post():
    try:
        global_cap = int(request.form.get("global_cap", "").strip())
        per_ip_cap = int(request.form.get("per_ip_cap", "").strip())
    except (TypeError, ValueError):
        flash("保存失败：上限必须是整数。当前保存版与待应用版均未改动。", "error")
        return redirect(url_for("index"))
    try:
        saved = store.save(global_cap, per_ip_cap)
    except ValueError as exc:
        flash(f"保存失败：{exc}。当前保存版与待应用版均未改动。", "error")
        return redirect(url_for("index"))
    except sqlite3.Error as exc:
        flash(f"保存失败（数据库错误）：{exc}。当前保存版与待应用版均未改动。", "error")
        return redirect(url_for("index"))
    flash(
        f"版本 {saved['version']} 已保存（全局 {saved['global_cap']} / 单 IP "
        f"{saved['per_ip_cap']}）。代理确认应用后才会显示为已生效。",
        "ok",
    )
    return redirect(url_for("index"))


def main():
    worker.start()
    print(f"[control] 配额控制网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    print(f"[control] 代理内部接口 {PROXY_CONTROL_URL}，推送间隔 {APPLY_INTERVAL}s", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
