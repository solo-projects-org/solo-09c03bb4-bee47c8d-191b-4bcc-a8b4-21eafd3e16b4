"""Delta Lake 版本可恢复性分析核心逻辑。

值班员上传 ``_delta_log`` 中留存的 JSON 提交（``NNN.json``）、经典单文件
Parquet 检查点（``NNN.checkpoint.parquet``）以及现存数据文件的表内相对路径
清单。本模块对整个查询区间做**单趟增量重放**：同一提交在一次查询中最多
应用一次，遇到更晚的检查点即从该检查点起算（前段文件与断档不带入），
逐版冻结当时的快照引用，再与现存清单核对，判断每个版本能否完整重建。

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


_NO_BASE_NOTE = "缺少重放起点：版本 0 的提交与不晚于该版本的检查点均未上传"


def _replay_range(from_version: int,
                  to_version: int,
                  commits: dict[int, ParsedCommit],
                  checkpoints: dict[int, ParsedCheckpoint]
                  ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """对整个查询区间执行**单趟增量重放**。

    关键约束（修复反复重放旧提交导致的性能问题）：

    - 同一次查询中，同一提交版本的 add/remove **最多应用一次**：
      多个目标版本共用的旧提交只在单趟遍历中应用一次，后续版本直接在
      已累积的快照状态上前进；
    - 遇到（不晚于当前版本的）检查点时，用检查点的 add 列**整体替换**
      当前快照并清空断档记录，前段版本的文件集合与缺失提交不带入新段；
    - 每个目标版本输出时立即冻结当时的快照引用与缺失提交列表，
      后续版本的增删不会改写较早版本的报告。
    """
    base_kind, base_version = _choose_base(from_version, commits, checkpoints)

    # 重放统计：供页面展示“每个提交确实只应用了一次”
    stats: dict[str, Any] = {
        "commits_applied": 0,
        "applied_commit_versions": [],
        "checkpoint_resets": [],
    }
    applied_versions: set[int] = set()
    active: dict[str, dict] = {}
    missing_commits: list[int] = []
    base_label: str | None = None
    note: str | None = None
    have_base = base_kind is not None
    reports: list[dict[str, Any]] = []

    def apply_commit(version: int) -> None:
        """应用一个提交；同一版本在本次查询中只会进入一次。"""
        if version in applied_versions:  # 结构性保险：单趟遍历下不应发生
            return
        commit = commits[version]
        # 同一提交内按文件中的动作顺序应用；remove 先于 add 生效
        for path in commit.removes:
            active.pop(path, None)
        for path, _meta in commit.adds:
            active[path] = {"from": f"commit-{version}"}
        applied_versions.add(version)
        stats["commits_applied"] += 1
        stats["applied_commit_versions"].append(version)

    def reset_to_checkpoint(version: int) -> None:
        """从检查点起算：丢弃前段快照与断档，不重放该版本的 JSON 提交。"""
        ckpt = checkpoints[version]
        active.clear()
        for path in ckpt.adds:
            active[path] = {"from": f"checkpoint-{version}"}
        missing_commits.clear()
        # 检查点已含该版本提交的效果，标记后禁止再应用同版本 JSON 提交
        applied_versions.add(version)
        stats["checkpoint_resets"].append(version)

    def emit(target: int) -> None:
        # 立即冻结快照：引用列表在此刻排序拷贝，后续增删无法影响本版报告
        reports.append({
            "version": target,
            "base": base_label,
            "missing_commits": list(missing_commits),
            "replay_complete": not missing_commits,
            "note": note,
            "referenced_files": sorted(active.keys()),
        })

    if base_kind == "checkpoint":
        base_label = f"checkpoint-{base_version}"
        reset_to_checkpoint(base_version)
        walk_start = base_version + 1
    elif base_kind == "commit":
        seed = commits[0]
        for path, _meta in seed.adds:
            active[path] = {"from": "commit-0"}
        applied_versions.add(0)
        stats["commits_applied"] += 1
        stats["applied_commit_versions"].append(0)
        base_label = "commit-0"
        walk_start = 1
    else:
        # 连起点都没有：在遇到第一个检查点之前，每个版本都计为缺失提交，
        # 且不把零散提交应用到无基线的空快照上（与逐版重放语义保持一致）
        note = _NO_BASE_NOTE
        walk_start = 0

    # 起点本身就是目标版本时（如 commit-0 对 v0、checkpoint-N 对 vN）先出一版
    if have_base and from_version < walk_start:
        emit(from_version)

    for version in range(walk_start, to_version + 1):
        if version in checkpoints:
            # 更新的检查点：从该检查点起算，前段文件与断档一律不带入
            reset_to_checkpoint(version)
            base_label = f"checkpoint-{version}"
            note = None
            have_base = True
        elif not have_base:
            # 无基线阶段：即便该版本提交恰好存在也不能应用，缺失照实标出
            missing_commits.append(version)
        else:
            commit = commits.get(version)
            if commit is None:
                missing_commits.append(version)
            else:
                apply_commit(version)

        if version >= from_version:
            emit(version)

    stats["applied_commit_versions"].sort()
    stats["checkpoint_resets"].sort()
    return reports, stats


def analyze(log_files: list[InputFile],
            manifest_files: list[InputFile],
            from_version: int,
            to_version: int) -> dict[str, Any]:
    """分析入口：单趟增量重放整个区间，并逐版核对现存数据文件。"""
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

    # 整个区间只做一趟增量重放：同一提交最多应用一次，
    # 遇到更新的检查点即从该检查点起算，每版结果在 emit 时已冻结
    replay_reports, replay_stats = _replay_range(
        from_version, to_version, commits, checkpoints)

    version_reports = []
    summary = {STATUS_RECOVERABLE: 0,
               STATUS_MISSING_COMMIT: 0,
               STATUS_MISSING_FILES: 0}

    for result in replay_reports:
        target = result["version"]
        referenced = result["referenced_files"]
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
        "replay_stats": replay_stats,
        "ignored_files": ignored,
        "duplicate_files": sorted(duplicate),
        "manifests": manifest_names,
        "existing_file_count": len(existing),
        "summary": summary,
        "versions": version_reports,
    }
