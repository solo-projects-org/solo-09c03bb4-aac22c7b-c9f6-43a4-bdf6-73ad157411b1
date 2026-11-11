"""核查结果的共享持久化存储。

网页首屏只返回逐版计数，用户点开某一版本时才向分页接口索取文件明细。
为了让分页请求可以落到**任意一个** gunicorn worker，也为了让另一个窗口
提交的新核查不影响旧核查，这里用 SQLite（WAL 模式）按 ``analysis_id``
保存：

* 首屏报告（不含任何文件路径明细）；
* 原始上传的日志与现存清单，分页时按需重建指定版本；
* 首次打开某版本后生成的明细缓存（惰性生成，后续翻页直接复用）。

SQLite 文件通过 ``ANALYSIS_DB_PATH`` 指定；Docker Compose 将其放在共享
卷上，多个服务进程看到的是同一份状态。
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .analyzer import InputFile

_DEFAULT_DB_PATH = Path(
    os.environ.get(
        "ANALYSIS_DB_PATH",
        str(Path(os.environ.get("STATE_DIR", tempfile.gettempdir()))
            / "delta_recovery_analyses.sqlite3"),
    )
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS analyses (
    id          TEXT PRIMARY KEY,
    created_at  REAL NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis_inputs (
    analysis_id TEXT NOT NULL,
    kind        TEXT NOT NULL,
    ordinal     INTEGER NOT NULL,
    filename    TEXT NOT NULL,
    data        BLOB NOT NULL,
    PRIMARY KEY (analysis_id, kind, ordinal)
);

CREATE TABLE IF NOT EXISTS version_details (
    analysis_id TEXT NOT NULL,
    version     INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    created_at  REAL NOT NULL,
    PRIMARY KEY (analysis_id, version)
);
"""


class AnalysisStore:
    """基于单个 SQLite 文件的核查存储。每次操作独立连接，进程间安全。"""

    def __init__(self, db_path: str | os.PathLike[str] | None = None):
        self.db_path = str(db_path or _DEFAULT_DB_PATH)
        self._init_lock = threading.Lock()
        self._initialized = False
        self.initialize()

    # ---------- 基础设施 ----------

    def _connect(self) -> sqlite3.Connection:
        parent = Path(self.db_path).parent
        parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.DatabaseError:
            # 某些文件系统不支持 WAL，退回默认日志模式即可。
            pass
        return conn

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            with self._connect() as conn:
                conn.executescript(_SCHEMA)
            self._initialized = True

    # ---------- 写入 / 读取 ----------

    def save_analysis(
        self,
        report: dict[str, Any],
        log_files: list[InputFile],
        manifest_files: list[InputFile],
    ) -> dict[str, Any]:
        """保存一次核查，返回带 ``analysis_id`` 的报告副本。"""
        analysis_id = uuid.uuid4().hex
        stored_report = dict(report)
        stored_report["analysis_id"] = analysis_id
        report_json = json.dumps(stored_report, ensure_ascii=False,
                                 separators=(",", ":"))
        now = time.time()

        rows: list[tuple[str, str, int, str, bytes]] = []
        for ordinal, item in enumerate(log_files):
            rows.append((analysis_id, "log", ordinal, item.filename,
                         sqlite3.Binary(item.data)))
        for ordinal, item in enumerate(manifest_files):
            rows.append((analysis_id, "manifest", ordinal, item.filename,
                         sqlite3.Binary(item.data)))

        with self._connect() as conn:
            conn.execute(
                "INSERT INTO analyses (id, created_at, report_json) "
                "VALUES (?, ?, ?)",
                (analysis_id, now, report_json),
            )
            conn.executemany(
                "INSERT INTO analysis_inputs "
                "(analysis_id, kind, ordinal, filename, data) "
                "VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        return stored_report

    def get_report(self, analysis_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT report_json FROM analyses WHERE id = ?",
                (analysis_id,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def get_inputs(
        self, analysis_id: str
    ) -> tuple[list[InputFile], list[InputFile]] | None:
        with self._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM analyses WHERE id = ?", (analysis_id,)
            ).fetchone()
            if exists is None:
                return None
            rows = conn.execute(
                "SELECT kind, filename, data FROM analysis_inputs "
                "WHERE analysis_id = ? ORDER BY kind, ordinal",
                (analysis_id,),
            ).fetchall()
        logs = [InputFile(filename=name, data=bytes(data))
                for kind, name, data in rows if kind == "log"]
        manifests = [InputFile(filename=name, data=bytes(data))
                     for kind, name, data in rows if kind == "manifest"]
        return logs, manifests

    def get_version_detail(
        self, analysis_id: str, version: int
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT detail_json FROM version_details "
                "WHERE analysis_id = ? AND version = ?",
                (analysis_id, version),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    def put_version_detail(
        self, analysis_id: str, version: int, detail: dict[str, Any]
    ) -> None:
        detail_json = json.dumps(detail, ensure_ascii=False,
                                 separators=(",", ":"))
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO version_details "
                "(analysis_id, version, detail_json, created_at) "
                "VALUES (?, ?, ?, ?)",
                (analysis_id, version, detail_json, time.time()),
            )


def resolve_db_path(app_config: dict[str, Any] | None = None) -> str:
    """解析数据库路径：应用配置 > 环境变量 > 临时目录默认值。"""
    if app_config:
        configured = app_config.get("ANALYSIS_DB_PATH")
        if configured:
            return str(configured)
    return str(_DEFAULT_DB_PATH)


_STORES: dict[str, AnalysisStore] = {}
_STORES_LOCK = threading.Lock()


def get_store(path: str | None = None) -> AnalysisStore:
    """按路径复用存储实例；不同进程各自持有连接工厂，状态在同一个文件。"""
    resolved = str(path or _DEFAULT_DB_PATH)
    with _STORES_LOCK:
        store = _STORES.get(resolved)
        if store is None:
            store = AnalysisStore(resolved)
            _STORES[resolved] = store
        return store
