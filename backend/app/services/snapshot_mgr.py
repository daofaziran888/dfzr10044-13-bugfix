"""快照管理领域服务：生成、标记、过期与回退湖表快照。"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "湖表快照"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照编号", "所属湖表", "快照标识", "父快照", "新增行数", "删除行数", "文件清单摘要", "生成时间"]
    REQUIRED_FIELDS = ["快照编号", "所属湖表", "快照标识"]
    STATUS_ORDER = ["生成中", "可用", "已过期", "已回退"]
    ACTION_RULES = {"标记可用": "可用", "标记过期": "已过期", "执行回退": "已回退"}
    NEGATIVE_ACTIONS = []

    def generate_snapshot_for_table(self, lake_table: str, file_ids: list[str],
                                    parent_snap: str | None = None) -> dict[str, Any]:
        """为指定湖表生成新快照，标记可用。"""
        ls = self._store()
        snap = ls.create_snapshot(lake_table, file_ids, parent_snap,
                                  added_rows=0, deleted_rows=0)
        ls.mark_snapshot_available(lake_table, snap.snapshot_id)
        return {
            "snapshot_id": snap.snapshot_id,
            "lake_table": lake_table,
            "file_count": len(file_ids),
            "parent": snap.parent_snapshot,
            "created_at": snap.created_at,
        }

    def create_generating_snapshot(
        self, lake_table: str, file_ids: list[str], parent_snap: str | None = None
    ) -> Any:
        """创建生成中快照（不标记可用），供复演标记可用/清理竞争窗口。"""
        return self._store().create_snapshot(
            lake_table, file_ids, parent_snap, added_rows=0, deleted_rows=0)

    def mark_available(self, lake_table: str, snapshot_id: str) -> dict[str, Any] | None:
        """把生成中快照标记为可用（与过期清理共用存储写锁）。"""
        snap = self._store().mark_snapshot_available(lake_table, snapshot_id)
        if snap is None:
            return None
        return {"snapshot_id": snap.snapshot_id, "status": snap.status,
                "lake_table": lake_table}

    # 近义命名
    mark_snapshot_available = mark_available

    def mark_expired(self, lake_table: str, snapshot_id: str) -> dict[str, Any] | None:
        snap = self._store().mark_snapshot_expired(lake_table, snapshot_id)
        if snap is None:
            return None
        return {"snapshot_id": snap.snapshot_id, "status": snap.status,
                "lake_table": lake_table}

    mark_snapshot_expired = mark_expired

    def get_ancestor_chain(self, lake_table: str) -> list[str]:
        """获取快照祖先链（按时间顺序）。"""
        ls = self._store()
        snaps = ls.list_snapshots(lake_table)
        chain: list[str] = []
        snap_map = {s.snapshot_id: s for s in snaps}
        if not snaps:
            return chain
        current = snaps[-1]
        while current is not None:
            chain.append(current.snapshot_id)
            current = snap_map.get(current.parent_snapshot or "")
        return list(reversed(chain))

    def diff_snapshots(self, lake_table: str, snap_a: str, snap_b: str) -> dict[str, Any]:
        """比较两个快照间的文件差异。"""
        ls = self._store()
        s_a = ls.get_snapshot(lake_table, snap_a)
        s_b = ls.get_snapshot(lake_table, snap_b)
        if not s_a or not s_b:
            return {"error": "snapshot not found"}
        set_a = set(s_a.file_ids)
        set_b = set(s_b.file_ids)
        return {
            "added": list(set_b - set_a),
            "removed": list(set_a - set_b),
            "unchanged": list(set_a & set_b),
        }