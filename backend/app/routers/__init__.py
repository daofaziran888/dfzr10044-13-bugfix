"""业务路由汇总：交接类专用路由在前，通用资源路由兜底。"""
from __future__ import annotations

from app.routers import recycle, resource

# recycle 的固定路径（如 /api/expire_policy/candidates）必须先于
# resource 的 /api/{module}/{entry_id} 注册，避免被动态路径抢先匹配。
ROUTERS = [recycle, resource]
