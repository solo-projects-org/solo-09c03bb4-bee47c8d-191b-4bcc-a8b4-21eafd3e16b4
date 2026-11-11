"""Delta Lake 版本可恢复性分析核心逻辑。

值班员上传 ``_delta_log`` 中留存的 JSON 提交（``NNN.json``）、经典单文件
Parquet 检查点（``NNN.checkpoint.parquet``）以及现存数据文件的表内相对路径
清单。本模块逐版重放提交 / 检查点，重建每个版本快照仍引用的数据文件，
再与现存清单核对，判断该版本能否完整重建。

仅支持**不包含删除向量（Deletion Vector）**的表；一旦在协议特性或任意
add 动作中发现删除向量，立即拒绝分析并提示文件来源。
"""

from __future__ import annotations

import io
import json
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

# Delta 日志文件名均为 20 位补零版本号
_COMMIT_RE = re.compile(r"^(\d{20})\.json$")
# 经典单文件检查点：00000000000000000010.checkpoint.parquet
_CHECKPOINT_RE = re.compile(r"^(\d{20})\.checkpoint\.parquet$")
# 多文件（分片）检查点，本工具不支持，仅登记并忽略
_MULTI_CHECKPOINT_RE = re.compile(r"^\d{20}\.checkpoint\.\d+\.\d+\.parquet$")

DV_FEATURE_NAME = "deletionVectors"

STATUS_RECOVERABLE = "recoverable"
STATUS_MISSING_COMMIT = "missing_commit"
STATUS_MISSING_FILES = "missing_files"


class AnalyzerError(Exception):
    """面向用户的输入 / 能力限制错误。"""


@dataclass
class InputFile:
    """一次上传的文件（文件名 + 已读入内存的字节）。"""

    filename: str
    data: bytes


@dataclass
class ParsedCommit:
    version: int
    source: str
    adds: list[tuple[str, dict]] = field(default_factory=list)
    removes: list[str] = field(default_factory=list)
    dv_detected: bool = False
    protocol_features: list[str] = field(default_factory=list)


@dataclass
class ParsedCheckpoint:
    version: int
    source: str
    adds: list[str] = field(default_factory=list)
    dv_detected: bool = False
    protocol_features: list[str] = field(default_factory=list)
    tombstones: list[str] = field(default_factory=list)


def normalize_data_path(raw: Any) -> str:
    """把日志 / 清单中的路径归一化为表内相对路径。

    - 去掉前导 ``./`` 与 ``/``
    - Delta 日志中的路径是 URI 编码的，统一做 percent-decode
    日志与清单两侧都经过同一函数处理，保证可比较。
    """
    if not isinstance(raw, str):
        raise AnalyzerError(f"数据文件路径不是字符串：{raw!r}")
    path = raw.strip()
    while path.startswith("./"):
        path = path[2:]
    while path.startswith("/"):
        path = path[1:]
    return urllib.parse.unquote(path)


def _decode_utf8(data: bytes, source: str) -> str:
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AnalyzerError(f"{source} 不是合法的 UTF-8 文本：{exc}") from exc


def _iter_json_objects(text: str, source: str):
    """Delta 提交是 ndjson（每行一个动作对象），同时兼容整体 JSON 数组。"""
    stripped = text.strip()
    if not stripped:
        return
    # 先完整解析 ndjson；整行恰好是 JSON 数组 / 对象，或任意行失败则整体回退，
    # 避免把单行 JSON 数组误当成 ndjson、或吐出半截动作
    line_objs = []
    ndjson_ok = True
    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            ndjson_ok = False
            break
        if not isinstance(obj, dict):
            ndjson_ok = False
            break
        line_objs.append(obj)
    if ndjson_ok and line_objs:
        yield from line_objs
        return
    try:
        whole = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise AnalyzerError(f"{source} 既不是 ndjson 也不是合法 JSON：{exc}") from exc
    if isinstance(whole, list):
        yield from whole
    else:
        yield whole


def _extract_protocol_features(protocol: dict) -> list[str]:
    features: list[str] = []
    for key in ("readerFeatures", "writerFeatures", "features"):
        value = protocol.get(key)
        if isinstance(value, list):
            features.extend(str(v) for v in value)
    return features


def _add_has_dv(add_action: dict) -> bool:
    dv = add_action.get("deletionVector")
    return dv is not None


def parse_commit(version: int, data: bytes, source: str) -> ParsedCommit:
    """解析一个 JSON 提交文件。"""
    parsed = ParsedCommit(version=version, source=source)
    text = _decode_utf8(data, source)
    for obj in _iter_json_objects(text, source):
        if not isinstance(obj, dict):
            continue
        if "add" in obj and isinstance(obj["add"], dict):
            add = obj["add"]
            path = normalize_data_path(add.get("path"))
            parsed.adds.append((path, add))
            if _add_has_dv(add):
                parsed.dv_detected = True
        if "remove" in obj and isinstance(obj["remove"], dict):
            remove = obj["remove"]
            parsed.removes.append(normalize_data_path(remove.get("path")))
            if remove.get("deletionVector") is not None:
                parsed.dv_detected = True
        if "protocol" in obj and isinstance(obj["protocol"], dict):
            parsed.protocol_features.extend(
                _extract_protocol_features(obj["protocol"])
            )
    return parsed


def parse_checkpoint(version: int, data: bytes, source: str) -> ParsedCheckpoint:
    """用 PyArrow 解析经典单文件 Parquet 检查点。"""
    parsed = ParsedCheckpoint(version=version, source=source)
    try:
        table = pq.read_table(io.BytesIO(data))
    except Exception as exc:  # parquet 损坏 / 非 parquet
        raise AnalyzerError(f"{source} 无法作为 Parquet 检查点读取：{exc}") from exc

    column_names = set(table.column_names)
    if "add" not in column_names:
        raise AnalyzerError(f"{source} 中缺少 add 列，不是合法的 Delta 检查点")

    # Table.column 返回 ChunkedArray，需 combine_chunks 后才能按 struct 取子字段
    add_col = table.column("add").combine_chunks()
    add_type = add_col.type
    n = table.num_rows
    # 以列类型而非逐行值判断是否带 DV：全 null 行也能正确识别
    add_has_dv_column = pa.types.is_struct(add_type) and \
        (add_type.get_field_index("deletionVector") != -1)
    if add_has_dv_column:
        dv_values = add_col.field("deletionVector").to_pylist()
    else:
        dv_values = [None] * n
    paths = add_col.field("path").to_pylist()
    for path, dv in zip(paths, dv_values):
        if path is None:
            continue
        parsed.adds.append(normalize_data_path(path))
        if dv is not None:
            parsed.dv_detected = True

    # 检查点里的 remove 是墓碑（tombstone），不属于当前快照，
    # 仅记录用于展示，重放基线时不据此删除文件。
    if "remove" in column_names:
        remove_col = table.column("remove").combine_chunks()
        for path in remove_col.field("path").to_pylist():
            if path is not None:
                parsed.tombstones.append(normalize_data_path(path))
        if pa.types.is_struct(remove_col.type) and \
                remove_col.type.get_field_index("deletionVector") != -1:
            if any(v is not None for v in
                   remove_col.field("deletionVector").to_pylist()):
                parsed.dv_detected = True

    if "protocol" in column_names:
        proto_col = table.column("protocol").combine_chunks()
        for field_name in ("readerFeatures", "writerFeatures", "features"):
            if pa.types.is_struct(proto_col.type) and \
                    proto_col.type.get_field_index(field_name) != -1:
                for value in proto_col.field(field_name).to_pylist():
                    if isinstance(value, list):
                        parsed.protocol_features.extend(str(v) for v in value)
    return parsed


def parse_manifest(data: bytes, source: str) -> set[str]:
    """解析现存数据文件相对路径清单（每行一个路径）。"""
    text = _decode_utf8(data, source)
    existing: set[str] = set()
    for line_no, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            existing.add(normalize_data_path(line))
        except AnalyzerError as exc:
            raise AnalyzerError(f"{source} 第 {line_no} 行：{exc}") from exc
    return existing


def _classify_logs(log_files: list[InputFile]):
    commits: dict[int, ParsedCommit] = {}
    checkpoints: dict[int, ParsedCheckpoint] = {}
    ignored: list[dict[str, str]] = []
    duplicate: list[str] = []

    for item in log_files:
        name = item.filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
        m_commit = _COMMIT_RE.match(name)
        m_ckpt = _CHECKPOINT_RE.match(name)
        if m_commit:
            version = int(m_commit.group(1))
            if version in commits:
                duplicate.append(name)
                continue
            commits[version] = parse_commit(version, item.data, name)
        elif m_ckpt:
            version = int(m_ckpt.group(1))
            if version in checkpoints:
                duplicate.append(name)
                continue
            checkpoints[version] = parse_checkpoint(version, item.data, name)
        elif _MULTI_CHECKPOINT_RE.match(name):
            ignored.append({"name": name,
                            "reason": "分片（多文件）检查点，本工具仅支持经典单文件检查点"})
        elif name == "_last_checkpoint":
            ignored.append({"name": name, "reason": "检查点指针文件，不参与重放"})
        else:
            ignored.append({"name": name, "reason": "无法识别的 _delta_log 文件名"})
    return commits, checkpoints, ignored, duplicate


def _guard_deletion_vectors(commits: dict[int, ParsedCommit],
                            checkpoints: dict[int, ParsedCheckpoint]) -> None:
    for version, parsed in sorted(commits.items()):
        if DV_FEATURE_NAME in parsed.protocol_features:
            raise AnalyzerError(
                f"表启用了删除向量：提交 {parsed.source} 的 protocol 特性包含 "
                f"{DV_FEATURE_NAME}。本工具只处理无删除向量的表。"
            )
        if parsed.dv_detected:
            raise AnalyzerError(
                f"表包含删除向量：提交 {parsed.source} 的 add/remove 动作带有 "
                f"deletionVector 字段。本工具只处理无删除向量的表。"
            )
    for version, parsed in sorted(checkpoints.items()):
        if DV_FEATURE_NAME in parsed.protocol_features:
            raise AnalyzerError(
                f"表启用了删除向量：检查点 {parsed.source} 的 protocol 特性包含 "
                f"{DV_FEATURE_NAME}。本工具只处理无删除向量的表。"
            )
        if parsed.dv_detected:
            raise AnalyzerError(
                f"表包含删除向量：检查点 {parsed.source} 的 add 动作带有非空 "
                f"deletionVector 结构。本工具只处理无删除向量的表。"
            )


def _choose_base(target: int,
                 commits: dict[int, ParsedCommit],
                 checkpoints: dict[int, ParsedCheckpoint]):
    """选择重放起点。

    优先使用不晚于目标版本的最新检查点（重放距离最短）；
    没有可用检查点时，从版本 0 的提交出发。
    返回 (base_kind, base_version) 或 (None, None)。
    """
    eligible = [v for v in checkpoints if v <= target]
    if eligible:
        base_version = max(eligible)
        return "checkpoint", base_version
    if 0 in commits:
        return "commit", 0
    return None, None


def _replay(target: int,
            commits: dict[int, ParsedCommit],
            checkpoints: dict[int, ParsedCheckpoint]) -> dict[str, Any]:
    """对单个目标版本执行一次完整重放并核对快照引用文件。"""
    base_kind, base_version = _choose_base(target, commits, checkpoints)
    missing_commits: list[int] = []
    active: dict[str, dict] = {}

    if base_kind is None:
        # 连起点都没有：版本 0 提交与任何检查点均缺失
        missing_commits = list(range(0, target + 1))
        return {
            "base": None,
            "missing_commits": missing_commits,
            "active": active,
            "replay_complete": False,
            "note": "缺少重放起点：版本 0 的提交与不晚于该版本的检查点均未上传",
        }

    if base_kind == "checkpoint":
        ckpt = checkpoints[base_version]
        for path in ckpt.adds:
            active[path] = {"from": f"checkpoint-{base_version}"}
        required_commits = range(base_version + 1, target + 1)
        base_label = f"checkpoint-{base_version}"
    else:
        seed = commits[0]
        for path, meta in seed.adds:
            active[path] = {"from": "commit-0"}
        required_commits = range(1, target + 1)
        base_label = "commit-0"

    for version in required_commits:
        commit = commits.get(version)
        if commit is None:
            missing_commits.append(version)
            continue
        # 同一提交内按文件中的动作顺序应用；remove 先于 add 生效
        for path in commit.removes:
            active.pop(path, None)
        for path, meta in commit.adds:
            active[path] = {"from": f"commit-{version}"}

    return {
        "base": base_label,
        "missing_commits": missing_commits,
        "active": active,
        "replay_complete": not missing_commits,
        "note": None,
    }


def analyze(log_files: list[InputFile],
            manifest_files: list[InputFile],
            from_version: int,
            to_version: int) -> dict[str, Any]:
    """分析入口：逐版重放并核对现存数据文件。"""
    if not isinstance(from_version, int) or not isinstance(to_version, int):
        raise AnalyzerError("版本号必须是整数")
    if from_version < 0 or to_version < 0:
        raise AnalyzerError("版本号不能为负数")
    if from_version > to_version:
        raise AnalyzerError("版本区间起点不能大于终点")
    if not log_files:
        raise AnalyzerError("请至少上传一个 _delta_log 中的 JSON 提交或 Parquet 检查点")

    commits, checkpoints, ignored, duplicate = _classify_logs(log_files)
    if not commits and not checkpoints:
        raise AnalyzerError(
            "上传文件中没有可识别的 Delta 日志（应为 20 位版本号命名的 "
            ".json 提交或 .checkpoint.parquet 检查点）"
        )

    _guard_deletion_vectors(commits, checkpoints)

    existing: set[str] = set()
    manifest_names: list[str] = []
    for item in manifest_files:
        manifest_names.append(item.filename.rsplit("/", 1)[-1])
        existing |= parse_manifest(item.data, item.filename)

    version_reports = []
    summary = {STATUS_RECOVERABLE: 0,
               STATUS_MISSING_COMMIT: 0,
               STATUS_MISSING_FILES: 0}

    for target in range(from_version, to_version + 1):
        result = _replay(target, commits, checkpoints)
        active: dict[str, dict] = result["active"]
        referenced = sorted(active.keys())
        missing_files = sorted(p for p in referenced if p not in existing)

        if result["missing_commits"]:
            status = STATUS_MISSING_COMMIT
        elif missing_files:
            status = STATUS_MISSING_FILES
        else:
            status = STATUS_RECOVERABLE
        summary[status] += 1

        version_reports.append({
            "version": target,
            "status": status,
            "replay_complete": result["replay_complete"],
            "base": result["base"],
            "referenced_files": referenced,
            "referenced_file_count": len(referenced),
            "missing_files": missing_files,
            "missing_commits": result["missing_commits"],
            "note": result["note"],
        })

    return {
        "supported": True,
        "range": {"from": from_version, "to": to_version},
        "available_commits": sorted(commits.keys()),
        "available_checkpoints": sorted(checkpoints.keys()),
        "ignored_files": ignored,
        "duplicate_files": sorted(duplicate),
        "manifests": manifest_names,
        "existing_file_count": len(existing),
        "summary": summary,
        "versions": version_reports,
    }
