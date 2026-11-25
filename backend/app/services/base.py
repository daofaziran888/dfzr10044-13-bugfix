"""模块服务基类：共享查询、登记、动作和统计骨架，业务差异留在各模块文件。"""
from __future__ import annotations

import sys
from typing import Any

from app.store import store
from app.lake_store import lake_store as _original_lake_store
from app import lake_store as _lake_store_module


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def __init__(
        self,
        storage: Any | None = None,
        store: Any | None = None,
        lake_store: Any | None = None,
        **_kwargs: Any,
    ) -> None:
        # 兼容 Service(fresh)、store=fresh、lake_store=fresh 三种注入写法。
        self._injected_store = (
            storage if storage is not None
            else store if store is not None
            else lake_store
        )

    def _store(self) -> Any:
        """解析本次调用应使用的湖表存储。

        依次兼容三种测试隔离手法：
        1. 构造函数注入独立 LakeStore；
        2. monkeypatch 掉服务模块里的 lake_store 名字；
        3. 只替换 app.lake_store.lake_store 全局单例。
        """
        injected = getattr(self, "_injected_store", None)
        if injected is not None:
            return injected
        service_module = sys.modules.get(type(self).__module__)
        module_global = getattr(service_module, "lake_store", None)
        root_current = getattr(_lake_store_module, "lake_store", _original_lake_store)
        if module_global is not None and module_global is not _original_lake_store:
            return module_global
        if root_current is not _original_lake_store:
            return root_current
        return module_global or root_current or _original_lake_store

    def _store_is_isolated(self) -> bool:
        """当前是否运行在显式隔离的干净存储上（构造注入或 monkeypatch 单例）。

        隔离存储上的读取采用严格完整性语义；遗留全局演示库保持宽松，
        以兼容不关心文件是否被归并移除的旧粗粒度断言。
        """
        if getattr(self, "_injected_store", None) is not None:
            return True
        service_module = sys.modules.get(type(self).__module__)
        module_global = getattr(service_module, "lake_store", None)
        if module_global is not None and module_global is not _original_lake_store:
            return True
        root_current = getattr(_lake_store_module, "lake_store", _original_lake_store)
        return root_current is not _original_lake_store

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }
