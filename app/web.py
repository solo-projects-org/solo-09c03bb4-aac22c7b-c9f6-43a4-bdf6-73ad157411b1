"""Flask 网页：Delta Lake 版本可恢复性分析。

值班员在页面上填写版本区间，上传 _delta_log 留存的 JSON 提交、
经典单文件 Parquet 检查点，以及现存数据文件的表内相对路径清单，
提交后逐版展示可恢复性结论。页面也可一键加载仓库内置的本地样例。
"""

from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, jsonify, render_template, request

from .analyzer import AnalyzerError, InputFile, analyze, build_version_detail
from .store import get_store, resolve_db_path

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024  # 单次请求上限 256MB

# 核查状态保存在 SQLite 文件中（可通过 ANALYSIS_DB_PATH 覆盖），
# 这样分页请求即使由不同 worker 处理，也能找回原核查的输入与缓存。
app.config.setdefault("ANALYSIS_DB_PATH",
                      os.environ.get("ANALYSIS_DB_PATH", ""))

SAMPLE_ROOT = Path(os.environ.get("SAMPLE_ROOT",
                                  Path(__file__).resolve().parent.parent / "samples"))

# 样例目录名 -> 页面展示名 / 默认版本区间
SAMPLES = {
    "healthy_table": {
        "label": "健康表样例（0-4 版均可重建）",
        "range": (0, 4),
    },
    "incident_table": {
        "label": "误删事故样例（部分版本缺提交或缺数据文件）",
        "range": (0, 5),
    },
    "large_table": {
        "label": "大表样例（30 个版本共享上万份文件，验证分页明细）",
        "range": (0, 29),
    },
}


def _read_uploads(field_name: str) -> list[InputFile]:
    files = []
    for storage in request.files.getlist(field_name):
        if not storage or storage.filename == "":
            continue
        files.append(InputFile(filename=storage.filename,
                               data=storage.read()))
    return files


def _parse_version(raw: str, field_label: str) -> int:
    raw = (raw or "").strip()
    if raw == "":
        raise AnalyzerError(f"请填写{field_label}")
    try:
        return int(raw)
    except ValueError as exc:
        raise AnalyzerError(f"{field_label}必须是整数，收到：{raw!r}") from exc


def _run_analysis(log_files, manifest_files, from_version, to_version):
    report = analyze(log_files=log_files,
                     manifest_files=manifest_files,
                     from_version=from_version,
                     to_version=to_version)
    # 保存原核查的输入与首屏报告；返回的报告带上 analysis_id，
    # 后续分页只凭 id + 版本号即可，不需要重新上传日志和清单。
    return get_store(resolve_db_path(app.config)).save_analysis(
        report, log_files, manifest_files
    )


@app.get("/")
def index():
    return render_template("index.html", samples=SAMPLES)


@app.post("/analyze")
def analyze_upload():
    try:
        from_version = _parse_version(request.form.get("from_version"), "起始版本")
        to_version = _parse_version(request.form.get("to_version"), "结束版本")
        log_files = _read_uploads("log_files")
        manifest_files = _read_uploads("manifest_files")
        report = _run_analysis(log_files, manifest_files,
                               from_version, to_version)
    except AnalyzerError as exc:
        return render_template("index.html", samples=SAMPLES, error=str(exc),
                               form=request.form), 400
    return render_template("index.html", samples=SAMPLES, report=report)


@app.get("/api/samples")
def list_samples():
    return jsonify({
        key: {"label": meta["label"], "range": list(meta["range"])}
        for key, meta in SAMPLES.items()
    })


@app.post("/api/analyze-sample/<sample_name>")
def analyze_sample(sample_name: str):
    """直接加载服务器本地样例目录，无需手动上传。"""
    if sample_name not in SAMPLES:
        return jsonify({"error": f"未知样例：{sample_name}"}), 404
    sample_dir = SAMPLE_ROOT / sample_name
    if not sample_dir.is_dir():
        return jsonify({"error": f"样例目录不存在：{sample_dir}"}), 404

    try:
        payload = request.get_json(silent=True) or {}
        meta = SAMPLES[sample_name]
        from_version = int(payload.get("from_version", meta["range"][0]))
        to_version = int(payload.get("to_version", meta["range"][1]))

        log_dir = sample_dir / "_delta_log"
        log_files = []
        for path in sorted(log_dir.iterdir()):
            if path.is_file():
                log_files.append(InputFile(filename=path.name,
                                           data=path.read_bytes()))
        manifest_path = sample_dir / "existing_files.txt"
        manifest_files = [InputFile(
            filename=manifest_path.name,
            data=manifest_path.read_bytes(),
        )]
        report = _run_analysis(log_files, manifest_files,
                               from_version, to_version)
    except AnalyzerError as exc:
        return jsonify({"error": str(exc)}), 400
    except (OSError, ValueError) as exc:
        return jsonify({"error": f"读取样例失败：{exc}"}), 500
    return jsonify(report)


@app.get("/api/analyses/<analysis_id>/versions/<int:version>/files")
@app.get("/api/analysis/<analysis_id>/versions/<int:version>/files")
def analysis_version_files(analysis_id: str, version: int):
    """按需返回某个版本的文件明细，按路径排序并分页（每页最多 200 条）。

    只用 ``analysis_id`` 与版本号定位原核查；日志和现存清单在提交核查时
    已持久化，翻页不需要重新上传。请求可以由任意服务进程处理。
    """
    store = get_store(resolve_db_path(app.config))
    report = store.get_report(analysis_id)
    if report is None:
        return jsonify({"error": "核查不存在或已过期，请重新提交分析"}), 404

    version_meta = next(
        (v for v in report.get("versions", []) if v.get("version") == version),
        None,
    )
    if version_meta is None:
        return jsonify({
            "error": f"版本 v{version} 不在该核查的区间 "
                     f"v{report['range']['from']}–v{report['range']['to']} 内"
        }), 404
    if not version_meta.get("replay_complete"):
        return jsonify({
            "error": f"版本 v{version} 的重放不完整（缺失提交 "
                     f"{version_meta.get('missing_commits', [])}），没有可展示的完整快照"
        }), 409

    try:
        page = int(request.args.get("page", "1"))
    except ValueError:
        return jsonify({"error": "page 必须是整数"}), 400
    if page < 1:
        page = 1

    raw_size = (request.args.get("page_size")
                or request.args.get("per_page") or "200")
    try:
        page_size = int(raw_size)
    except ValueError:
        return jsonify({"error": "page_size 必须是整数"}), 400
    # 每页最多 200 条，超过时截断；至少 1 条。
    page_size = max(1, min(page_size, 200))

    detail = store.get_version_detail(analysis_id, version)
    if detail is None:
        inputs = store.get_inputs(analysis_id)
        if inputs is None:
            return jsonify({"error": "核查输入已过期，请重新提交分析"}), 404
        log_files, manifest_files = inputs
        try:
            detail = build_version_detail(log_files, manifest_files, version)
        except AnalyzerError as exc:
            return jsonify({"error": str(exc)}), 400
        # 惰性缓存：首屏从未生成该版本的明细，只有用户点开时才生成。
        store.put_version_detail(analysis_id, version, detail)

    files = detail.get("files", [])
    total = len(files)
    total_pages = (total + page_size - 1) // page_size
    start = (page - 1) * page_size
    page_files = files[start:start + page_size]

    return jsonify({
        "analysis_id": analysis_id,
        "version": version,
        "status": detail.get("status"),
        "replay_complete": detail.get("replay_complete", True),
        "base": detail.get("base"),
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "referenced_file_count": total,
        "missing_file_count": detail.get("missing_file_count", 0),
        "has_previous": page > 1,
        "has_next": page < total_pages,
        "files": page_files,
    })


@app.errorhandler(413)
def too_large(_err):
    return render_template("index.html", samples=SAMPLES,
                           error="上传内容超过 256MB 上限"), 413


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
