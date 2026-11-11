"""Flask 路由层测试（含本地样例加载与分页明细）。"""

from __future__ import annotations

import io
import json
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app import store as store_module
from app.web import app


@pytest.fixture(autouse=True)
def isolated_analysis_db(tmp_path):
    """每个测试用独立的 SQLite 状态文件，避免相互污染。"""
    app.config["ANALYSIS_DB_PATH"] = str(tmp_path / "analyses.sqlite3")


def _commit_bytes(actions):
    return ("\n".join(json.dumps(a) for a in actions)).encode("utf-8")


def _embedded_report(resp):
    match = re.search(
        b'<script type="application/json" id="report-data">(.*?)</script>',
        resp.data, re.S,
    )
    assert match, resp.data[:500]
    return json.loads(match.group(1))


def _upload(client, log_specs, manifest_text, from_version=0, to_version=1):
    data = {
        "from_version": str(from_version),
        "to_version": str(to_version),
        "log_files": [
            (io.BytesIO(payload), filename)
            for filename, payload in log_specs
        ],
        "manifest_files": [
            (io.BytesIO(manifest_text.encode("utf-8")), "existing.txt"),
        ],
    }
    return client.post("/analyze", data=data, content_type="multipart/form-data")


def test_index_renders():
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Delta Lake 版本可恢复性分析".encode() in resp.data


def test_upload_analyze_flow():
    client = app.test_client()
    data = {
        "from_version": "0",
        "to_version": "1",
        "log_files": [
            (io.BytesIO(_commit_bytes([{"add": {"path": "data/a.parquet"}}])),
             "00000000000000000000.json"),
            (io.BytesIO(_commit_bytes([{"add": {"path": "data/b.parquet"}}])),
             "00000000000000000001.json"),
        ],
        "manifest_files": [
            (io.BytesIO(b"data/a.parquet\ndata/b.parquet\n"), "existing.txt"),
        ],
    }
    resp = client.post("/analyze", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    assert "可重建 · 文件齐全".encode() in resp.data


def test_upload_missing_file_reports_400():
    client = app.test_client()
    resp = client.post("/analyze", data={
        "from_version": "0", "to_version": "1",
    }, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "至少上传".encode() in resp.data


def test_sample_api_healthy_and_incident():
    client = app.test_client()
    for name, expected_recoverable in (("healthy_table", 5),
                                       ("incident_table", None)):
        resp = client.post(f"/api/analyze-sample/{name}")
        assert resp.status_code == 200, resp.data
        report = resp.get_json()
        assert report["range"] == {"from": 0, "to": (4 if name == "healthy_table" else 5)}
        if expected_recoverable:
            assert report["summary"]["recoverable"] == expected_recoverable


def test_initial_report_has_counts_but_no_file_details():
    files = [f"data/file-{i:04d}.parquet" for i in range(450)]
    payload = _commit_bytes([{"add": {"path": p}} for p in files])
    resp = _upload(
        app.test_client(),
        [("00000000000000000000.json", payload)],
        "\n".join(files) + "\n",
        0, 0,
    )
    assert resp.status_code == 200
    report = _embedded_report(resp)
    assert report["analysis_id"]
    version = report["versions"][0]
    assert version["referenced_file_count"] == 450
    assert version["missing_file_count"] == 0
    # 首屏响应不预生成、不发送任何版本的文件路径明细
    assert "referenced_files" not in version
    assert "missing_files" not in version
    assert files[0].encode() not in resp.data


def test_detail_endpoint_paginates_by_path_without_reupload():
    files = [f"data/file-{i:04d}.parquet" for i in range(450)]
    payload = _commit_bytes([{"add": {"path": p}} for p in files])
    resp = _upload(
        app.test_client(),
        [("00000000000000000000.json", payload)],
        "\n".join(files) + "\n",
        0, 0,
    )
    report = _embedded_report(resp)
    analysis_id = report["analysis_id"]

    client = app.test_client()
    first = client.get(
        f"/api/analyses/{analysis_id}/versions/0/files?page=1&page_size=200")
    assert first.status_code == 200
    data = first.get_json()
    assert data["total"] == 450
    assert data["total_pages"] == 3
    assert data["page_size"] == 200
    assert len(data["files"]) == 200
    assert [f["path"] for f in data["files"]] == files[:200]
    assert all(f["exists"] for f in data["files"])

    third = client.get(
        f"/api/analyses/{analysis_id}/versions/0/files?page=3&page_size=200")
    data = third.get_json()
    assert [f["path"] for f in data["files"]] == files[400:]
    assert data["has_previous"] is True and data["has_next"] is False

    # 超过上限时每页最多 200 条
    capped = client.get(
        f"/api/analyses/{analysis_id}/versions/0/files?page=1&page_size=5000")
    assert capped.get_json()["page_size"] == 200
    assert len(capped.get_json()["files"]) == 200


def test_detail_survives_new_store_instance_and_newer_analysis():
    client = app.test_client()
    alpha = [f"data/alpha-{i:04d}.parquet" for i in range(3)]
    beta = [f"data/beta-{i:04d}.parquet" for i in range(3)]
    first = _embedded_report(_upload(
        client,
        [("00000000000000000000.json",
          _commit_bytes([{"add": {"path": p}} for p in alpha]))],
        "\n".join(alpha) + "\n", 0, 0,
    ))
    second = _embedded_report(_upload(
        client,
        [("00000000000000000000.json",
          _commit_bytes([{"add": {"path": p}} for p in beta]))],
        "\n".join(beta) + "\n", 0, 0,
    ))
    assert first["analysis_id"] != second["analysis_id"]

    # 模拟另一个服务进程：清空进程内存储实例，仅剩磁盘上的 SQLite。
    store_module._STORES.clear()
    detail = client.get(
        f"/api/analyses/{first['analysis_id']}/versions/0/files").get_json()
    assert [f["path"] for f in detail["files"]] == alpha
    assert [f["exists"] for f in detail["files"]] == [True, True, True]

    # 新提交的核查（second）也不影响旧核查 first
    detail_second = client.get(
        f"/api/analyses/{second['analysis_id']}/versions/0/files").get_json()
    assert [f["path"] for f in detail_second["files"]] == beta


def test_detail_marks_missing_files_and_rejects_incomplete_versions():
    files = ["data/a.parquet", "data/b.parquet", "data/c.parquet"]
    client = app.test_client()
    report = _embedded_report(_upload(
        client,
        [("00000000000000000000.json",
          _commit_bytes([{"add": {"path": p}} for p in files]))],
        "data/a.parquet\ndata/c.parquet\n", 0, 0,
    ))
    version = report["versions"][0]
    assert version["status"] == "missing_files"
    assert version["replay_complete"] is True
    assert version["missing_file_count"] == 1

    detail = client.get(
        f"/api/analyses/{report['analysis_id']}/versions/0/files").get_json()
    assert detail["missing_file_count"] == 1
    assert [(f["path"], f["exists"]) for f in detail["files"]] == [
        ("data/a.parquet", True), ("data/b.parquet", False),
        ("data/c.parquet", True),
    ]

    # 缺失提交的版本重放不完整，不能打开明细；越界版本 / 未知核查也是 404。
    broken = _embedded_report(_upload(
        client,
        [("00000000000000000001.json",
          _commit_bytes([{"add": {"path": "data/x.parquet"}}]))],
        "data/x.parquet\n", 0, 1,
    ))
    assert client.get(
        f"/api/analyses/{broken['analysis_id']}/versions/0/files").status_code == 409
    assert client.get(
        f"/api/analyses/{broken['analysis_id']}/versions/9/files").status_code == 404
    assert client.get(
        "/api/analyses/not-a-real-id/versions/0/files").status_code == 404


def test_large_sample_shares_files_and_paginates():
    client = app.test_client()
    resp = client.post("/api/analyze-sample/large_table")
    assert resp.status_code == 200, resp.data
    report = resp.get_json()
    assert report["range"] == {"from": 0, "to": 29}
    last = report["versions"][-1]
    assert last["referenced_file_count"] > 10000
    assert last["missing_file_count"] >= 1
    assert "referenced_files" not in last and "missing_files" not in last

    detail = client.get(
        f"/api/analyses/{report['analysis_id']}/versions/29/files"
        "?page=2&page_size=200").get_json()
    assert detail["total"] == last["referenced_file_count"]
    assert len(detail["files"]) == 200
    paths = [f["path"] for f in detail["files"]]
    assert paths == sorted(paths)
    assert any(f["exists"] is False for f in detail["files"])


def test_unknown_sample_404():
    resp = app.test_client().post("/api/analyze-sample/nope")
    assert resp.status_code == 404


def test_sample_api_list():
    resp = app.test_client().get("/api/samples")
    assert resp.status_code == 200
    names = resp.get_json()
    assert "healthy_table" in names and "incident_table" in names
    assert "large_table" in names
