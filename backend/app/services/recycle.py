"""分区文件回收服务（过期策略回收能力的独立命名入口）。

题面中的"回收单/回收编号/对账/删除回执"能力统一在 ExpirePolicyService
实现；这里给出更贴合「回收」语义的服务名，方便不同调用入口使用。
"""
from __future__ import annotations

from app.services.expire_policy import ExpirePolicyService


class RecycleService(ExpirePolicyService):
    """回收单、删除回执与对账。"""


class ReclaimService(ExpirePolicyService):
    """回收（Reclaim）命名入口，能力与 RecycleService 相同。"""
