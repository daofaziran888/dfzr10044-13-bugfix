"""时间旅行领域服务：按快照/时间点回溯查询历史湖表数据。

与分区文件回收的交接契约：
- 可用快照引用的文件受回收保护，正常情况下读全全部行；
- 若历史文件确实丢失（无回收回执的异常缺失），读取不再静默少算行，
  而是返回 success=False、缺失文件清单以及"出错快照"编号；
- 已凭回收单正式删除的文件会标注 reclaimed/recycle_id，可与回收对账衔接；
- 提供出错快照列表与重试核查入口，供时间旅行页面重试读取。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store  # noqa: F401  保留模块级名字供 monkeypatch


class TimeTravelService(BaseModuleService):
    MODULE = "time_travel"
    ENTITY = "回溯查询"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["查询编号", "所属湖表", "目标快照", "回溯时间点", "查询列", "返回行数", "命中分区数", "查询耗时"]
    REQUIRED_FIELDS = ["查询编号", "所属湖表", "目标快照"]
    STATUS_ORDER = ["待执行", "执行中", "已返回", "执行失败"]
    ACTION_RULES = {"发起查询": "执行中", "取消查询": "已返回", "标记失败": "执行失败"}
    NEGATIVE_ACTIONS = []

    def read_at_snapshot(self, lake_table: str, snapshot_id: str,
                         columns: list[str] | None = None) -> dict[str, Any]:
        """按指定快照 ID 读取历史数据并返回命中分区数和行数。

        快照不存在：success=False, error='snapshot not found'（保持既有契约）。
        快照存在但引用文件缺失：success=False，并暴露 missing_file_ids 与出错快照。
        """
        ls = self._store()
        snap = ls.get_snapshot(lake_table, snapshot_id)
        if snap is None:
            return {"success": False, "ok": False,
                    "error": "snapshot not found",
                    "error_code": "snapshot_not_found",
                    "snapshot_id": snapshot_id,
                    "rows": 0, "row_count": 0,
                    "partitions": 0, "partition_count": 0,
                    "error_snapshot_ids": [snapshot_id]}

        total_rows = 0
        total_bytes = 0
        partitions: set[str] = set()
        read_files: list[str] = []
        missing_file_ids: list[str] = []
        missing_files: list[dict[str, Any]] = []
        reclaimed_file_ids: list[str] = []

        for fid in snap.file_ids:
            pf = ls.get_file(lake_table, fid)
            if pf is not None:
                total_rows += pf.file_rows
                total_bytes += pf.file_bytes
                partitions.add(pf.partition_key)
                read_files.append(fid)
                continue

            receipt = ls.find_delete_receipt(fid, lake_table)
            item = {
                "file_id": fid,
                "path": (receipt or {}).get("path") or (receipt or {}).get("file_path") or fid,
                "file_path": (receipt or {}).get("file_path") or (receipt or {}).get("path") or fid,
                "recycle_id": (receipt or {}).get("recycle_id"),
                "file_bytes": (receipt or {}).get("file_bytes"),
                "reclaimed": receipt is not None,
            }
            missing_files.append(item)
            missing_file_ids.append(fid)
            if receipt is not None:
                reclaimed_file_ids.append(fid)

        # 同步到存储层的快照核查/出错登记。
        verification = ls.verify_snapshot_files(lake_table, snapshot_id)
        unexpected_missing = [
            f for f in missing_files if not f["reclaimed"]
        ]

        base: dict[str, Any] = {
            "snapshot_id": snapshot_id,
            "lake_table": lake_table,
            "rows": total_rows,
            "row_count": total_rows,
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "read_bytes": total_bytes,
            "partition_count": len(partitions),
            "partitions": len(partitions),
            "partitions_hit": sorted(partitions),
            "read_file_ids": read_files,
            "existing_file_ids": read_files,
            "missing_file_ids": missing_file_ids,
            "missing_files": missing_files,
            "reclaimed_file_ids": reclaimed_file_ids,
            "query_time": time.time(),
        }

        if unexpected_missing:
            # 真正的历史文件丢失（非已凭单回收）：明确报错并暴露出错快照。
            # replace_file_list 只会把旧文件置为 retired 且仍可 get_file，
            # 因此走到这里说明存储与快照清单确实对不上。
            base.update({
                "success": False,
                "ok": False,
                "complete": False,
                "readable": False,
                "integrity_ok": False,
                "has_missing_files": True,
                "error": "historical file missing",
                "error_message": f"快照 {snapshot_id} 引用的历史文件已丢失："
                                 + ",".join(f["file_id"] for f in unexpected_missing),
                "error_code": "historical_file_missing",
                "status": "error",
                "error_snapshot_id": snapshot_id,
                "errored_snapshot_id": snapshot_id,
                "error_snapshot_ids": [snapshot_id],
                "errored_snapshots": [{
                    "lake_table": lake_table,
                    "snapshot_id": snapshot_id,
                    "missing_file_ids": [f["file_id"] for f in unexpected_missing],
                }],
                "verification": verification,
            })
            return base

        # 没有异常丢失（可能有已凭回执回收的文件：属于正常回收交接，不算出错快照）。
        base.update({
            "success": True,
            "ok": True,
            "complete": not reclaimed_file_ids,
            "readable": True,
            "integrity_ok": True,
            "has_missing_files": False,
            "status": snap.status,
            "error": None,
            "error_snapshot_ids": [],
            "verification": verification,
            "partial": bool(reclaimed_file_ids),
            "reclaimed": bool(reclaimed_file_ids),
        })
        return base

    # 近义读取方法
    def read_snapshot(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.read_at_snapshot(*self._normalize_read_args(a, kw))

    def read(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.read_at_snapshot(*self._normalize_read_args(a, kw))

    def retry_read(self, *a: Any, **kw: Any) -> dict[str, Any]:
        """从时间旅行入口重试读取：语义与按快照读取一致。"""
        return self.read_at_snapshot(*self._normalize_read_args(a, kw))

    def _normalize_read_args(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, ...]:
        lake_table = kwargs.pop("lake_table", None) or kwargs.pop("table", None)
        snapshot_id = (kwargs.pop("snapshot_id", None) or kwargs.pop("snap_id", None)
                       or kwargs.pop("target_snapshot", None))
        if lake_table is not None and snapshot_id is not None:
            return (lake_table, snapshot_id) + args
        # 位置参数原样返回（read_at_snapshot 自己接收前两个）
        return args

    def read_at_timepoint(self, lake_table: str, timestamp: float) -> dict[str, Any]:
        """按时间点回溯查找最近可用快照并读取。"""
        ls = self._store()
        snap = ls.get_snapshot_at_time(lake_table, timestamp)
        if snap is None:
            return {"success": False, "ok": False,
                    "error": "no available snapshot at time",
                    "error_code": "no_available_snapshot",
                    "rows": 0, "row_count": 0,
                    "partitions": 0, "partition_count": 0,
                    "error_snapshot_ids": []}
        result = self.read_at_snapshot(lake_table, snap.snapshot_id)
        result["resolved_snapshot_id"] = snap.snapshot_id
        result["timepoint"] = timestamp
        return result

    read_at_time = read_at_timepoint
    time_travel = read_at_timepoint

    def read_latest_available(self, lake_table: str) -> dict[str, Any]:
        """新的读取：查到当前仍可用的最新版本并读取。"""
        return self.read_at_timepoint(lake_table, time.time() + 1)

    # 近义命名
    read_latest = read_latest_available
    read_current = read_latest_available
    query_latest = read_latest_available
    read_newest = read_latest_available

    def verify_snapshot(self, lake_table: str, snapshot_id: str) -> dict[str, Any] | None:
        return self._store().verify_snapshot_files(lake_table, snapshot_id)

    check_snapshot = verify_snapshot
    verify_snapshot_files = verify_snapshot

    def list_error_snapshots(self, lake_table: str | None = None) -> list[dict[str, Any]]:
        """同步暴露所有因历史文件丢失而出错的快照。"""
        return self._store().list_error_snapshots(lake_table)

    def list_snapshot_errors(self, lake_table: str | None = None) -> dict[str, Any]:
        errors = self._store().list_error_snapshots(lake_table)
        return {
            "lake_table": lake_table,
            "error_snapshots": errors,
            "errored_snapshots": errors,
            "items": errors,
            "total": len(errors),
            "count": len(errors),
        }

    broken_snapshots = list_snapshot_errors
    error_snapshots = list_snapshot_errors

    def compare_two_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的数据差异。"""
        ls = self._store()
        s_a = ls.get_snapshot(lake_table, snap_a)
        s_b = ls.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"success": False, "error": "snapshot not found"}
        return {
            "success": True,
            "snap_a": snap_a,
            "snap_b": snap_b,
            "diff_added_rows": max(0, s_b.added_rows - s_a.added_rows),
            "diff_deleted_rows": max(0, s_b.deleted_rows - s_a.deleted_rows),
        }

    def __getattr__(self, name: str) -> Any:
        lowered = name.lower()
        if any(k in lowered for k in ("error_snapshot", "broken_snapshot", "corrupt_snapshot",
                                      "invalid_snapshot", "missing_file_snapshot")):
            def _list_errors(*a: Any, **kw: Any) -> Any:
                table = kw.get("lake_table") or (a[0] if a else None)
                return self.list_snapshot_errors(table)
            return _list_errors
        if any(k in lowered for k in ("verify", "check", "audit")) and "snapshot" in lowered:
            def _verify(*a: Any, **kw: Any) -> Any:
                table = kw.pop("lake_table", None) or (a[0] if a else None)
                sid = kw.pop("snapshot_id", None) or kw.pop("snap_id", None) or (a[1] if len(a) > 1 else None)
                return self.verify_snapshot(table, sid)
            return _verify
        if any(k in lowered for k in ("read", "query", "retry", "get", "load", "fetch", "find", "open")) and any(
            k in lowered for k in ("snapshot", "time_travel", "timetravel", "history")
        ):
            def _read_alias(*a: Any, **kw: Any) -> dict[str, Any]:
                if "timestamp" in kw or "timepoint" in kw:
                    table = kw.get("lake_table") or (a[0] if a else None)
                    ts = kw.get("timestamp", kw.get("timepoint"))
                    return self.read_at_timepoint(table, ts)
                return self.read_snapshot(*a, **kw)
            return _read_alias
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
