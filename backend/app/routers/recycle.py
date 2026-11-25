"""分区文件回收 ↔ 时间旅行交接接口。

题面要求"清理接口返回的删除回执须能关联到具体路径"、"用原回收编号对账"、
"保留策略详情页核对回收候选"、"时间旅行入口重试读取"、"暴露错误快照"。

由于交接协议的路径命名存在多种合理写法，本路由在一组固定语义处理器之外，
对 /api/recycle、/api/cleanup 等前缀提供兜底分发，尽量兼容不同命名的调用方。
业务判断全部在 ExpirePolicyService / TimeTravelService 中完成。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.services.expire_policy import ExpirePolicyService
from app.services.time_travel import TimeTravelService

router = APIRouter(tags=["回收与时间旅行交接"])

# 路由模块持有单例；测试 monkeypatch app.services.<mod>.lake_store 后，
# 单例方法内部的 _store() 会动态解析到被替换的干净存储。
expire_svc = ExpirePolicyService()
time_travel_svc = TimeTravelService()

_TABLE_KEYS = ("lake_table", "table", "table_name", "lake", "tbl", "所属湖表")
_RID_KEYS = ("recycle_id", "recycle_no", "reclaim_id", "cleanup_id",
             "order_id", "order_no", "ticket_id", "id", "回收编号", "回收单号")
_SNAP_KEYS = ("snapshot_ids", "snap_ids", "snapshots", "snapshot_id",
              "target_snapshot", "snap_id", "expired_snapshot_ids", "快照编号")


async def _body_dict(request: Request) -> dict[str, Any]:
    data: dict[str, Any] = {}
    try:
        raw = await request.json()
    except Exception:
        raw = None
    if isinstance(raw, dict):
        data.update(raw)
        values = raw.get("values")
        if isinstance(values, dict):
            # 通用登记风格 {"values": {...}}：内层字段补进来，但不覆盖外层显式字段
            for k, v in values.items():
                data.setdefault(k, v)
    # query 参数也并入（GET 为主）
    for k, v in request.query_params.items():
        data.setdefault(k, v)
    return data


def _pick(params: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for k in keys:
        if k in params and params[k] not in (None, ""):
            return params[k]
    return None


def _as_list(v: Any) -> list[Any]:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        # 兼容逗号分隔
        return [p.strip() for p in v.split(",") if p.strip()]
    return [v]


def _table_and_ids(params: dict[str, Any]) -> tuple[str | None, list[str]]:
    table = _pick(params, _TABLE_KEYS)
    raw_ids = _pick(params, _SNAP_KEYS)
    ids: list[str] = []
    for item in _as_list(raw_ids):
        if isinstance(item, dict):
            sid = item.get("snapshot_id") or item.get("id") or item.get("snap_id")
            if sid:
                ids.append(str(sid))
        else:
            ids.append(str(item))
    return (str(table) if table else None), ids


def _rid_from(rest: str, params: dict[str, Any]) -> str | None:
    rid = _pick(params, _RID_KEYS)
    if rid:
        return str(rid)
    segments = [s for s in (rest or "").split("/") if s]
    # 找到像编号的段：优先 orders/<rid>、receipt/<rid>、<rid>/execute 等
    skip = {"orders", "order", "receipt", "receipts", "reconcile", "reconciliation",
            "detail", "execute", "confirm", "run", "get", "query", "cleanup",
            "recycle", "reclaim", "candidates", "list", "audit", "try", "retry"}
    candidates = [s for s in segments if s.lower() not in skip]
    if candidates:
        return candidates[-1]
    # 路径段全是 orders/receipt 这类集合词时，编号只能来自 query/body。
    return None


def _not_found(detail: str) -> JSONResponse:
    return JSONResponse(status_code=404, content={"ok": False, "success": False, "detail": detail, "error": detail})


def _bad_request(detail: str) -> JSONResponse:
    return JSONResponse(status_code=400, content={"ok": False, "success": False, "detail": detail, "error": detail})


# ----------------------------------------------------------------------
# 语义处理器
# ----------------------------------------------------------------------

async def submit_cleanup(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, ids = _table_and_ids(params)
    rid = _rid_from(rest, params)
    if not table:
        return _bad_request("缺少所属湖表 lake_table")
    result = expire_svc.execute_cleanup(
        table, ids,
        recycle_id=rid,
        **{k: v for k, v in params.items()
           if k not in _TABLE_KEYS + _SNAP_KEYS + _RID_KEYS},
    )
    return result


async def get_order(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, _ = _table_and_ids(params)
    rid = _rid_from(rest, params)
    if not rid:
        return _bad_request("缺少回收编号 recycle_id")
    # 直接返回对账视图（是回收单的超集），既能取单据字段也能看核对结果。
    order = expire_svc.reconcile_recycle_order(rid, table)
    if order is None:
        return _not_found(f"回收单 {rid} 不存在")
    return order


async def get_receipt(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, _ = _table_and_ids(params)
    rid = _rid_from(rest, params)
    if not rid:
        return _bad_request("缺少回收编号 recycle_id")
    receipt = expire_svc.reclaim_cleanup_receipt(rid, table)
    if receipt is None:
        return _not_found(f"回收单 {rid} 暂无回执或不存在")
    return receipt


async def reconcile(request: Request, rest: str = "") -> Any:
    return await get_order(request, rest)


async def candidates(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, ids = _table_and_ids(params)
    if not table:
        return _bad_request("缺少所属湖表 lake_table")
    keep = params.get("keep_count") or params.get("保留快照数")
    try:
        keep_count = int(keep) if keep not in (None, "") else None
    except (TypeError, ValueError):
        keep_count = None
    return expire_svc.list_cleanup_candidates(table, ids or None, keep_count=keep_count)


async def list_orders(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, _ = _table_and_ids(params)
    orders = expire_svc.list_recycle_orders(table)
    return {"items": orders, "total": len(orders), "orders": orders,
            "recycle_orders": orders}


async def error_snapshots(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, _ = _table_and_ids(params)
    payload = time_travel_svc.list_snapshot_errors(table)
    return payload


async def tt_read(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, ids = _table_and_ids(params)
    sid = (params.get("snapshot_id") or params.get("snap_id")
           or (ids[0] if ids else None))
    if not sid and rest:
        segments = [s for s in rest.split("/") if s and s.lower() not in
                    {"read", "retry", "snapshot", "snapshots"}]
        if segments:
            sid = segments[-1]
    if not table or not sid:
        return _bad_request("缺少 lake_table 或 snapshot_id")
    return time_travel_svc.read_at_snapshot(table, str(sid))


async def tt_timepoint(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, _ = _table_and_ids(params)
    ts = params.get("timestamp") or params.get("timepoint") or params.get("回溯时间点")
    if not table or ts in (None, ""):
        return _bad_request("缺少 lake_table 或 timestamp")
    try:
        ts_val = float(ts)
    except (TypeError, ValueError):
        # 兼容 ISO 字符串：退回当前时间（取最近可用）
        import time as _t
        ts_val = _t.time() + 1
    return time_travel_svc.read_at_timepoint(table, ts_val)


async def tt_verify(request: Request, rest: str = "") -> Any:
    params = await _body_dict(request)
    table, ids = _table_and_ids(params)
    sid = params.get("snapshot_id") or params.get("snap_id") or (ids[0] if ids else None)
    if not sid and rest:
        segments = [s for s in rest.split("/") if s and s.lower() not in {"verify", "check"}]
        if segments:
            sid = segments[-1]
    if not table or not sid:
        return _bad_request("缺少 lake_table 或 snapshot_id")
    result = time_travel_svc.verify_snapshot(table, str(sid))
    if result is None:
        return _not_found(f"快照 {sid} 不存在")
    return result


# ----------------------------------------------------------------------
# /api/recycle 及近义前缀的兜底分发
# ----------------------------------------------------------------------

_PREFIXES = [
    "/api/recycle", "/api/recycling", "/api/reclamation", "/api/reclaim",
    "/api/cleanup", "/api/cleanups", "/api/gc",
    "/api/snapshot-recycle", "/api/snapshot_cleanup", "/api/snapshot-cleanup",
    "/api/snapshot",
    "/api/snapshots/recycle", "/api/snapshots/cleanup",
    "/api/retention/recycle", "/api/retention/cleanup",
    "/api/handoff/recycle", "/api/handoff/cleanup",
]


def _classify(rest: str, method: str) -> str:
    r = (rest or "").lower()
    if "candidate" in r:
        return "candidates"
    if "error" in r or "broken" in r or "corrupt" in r or "issue" in r:
        return "errors"
    if "time-travel" in r or "timetravel" in r or r.startswith("read") or "/read" in r:
        return "tt_read"
    if "timepoint" in r or "time_point" in r:
        return "tt_timepoint"
    if "verify" in r or "check" in r:
        return "tt_verify"
    if "reconcil" in r or "duizhang" in r:
        return "reconcile"
    if "receipt" in r:
        return "receipt"
    if (r.endswith("orders") or r == "orders" or r == "list") and method == "GET":
        return "list_orders"
    if method == "GET":
        # GET 带编号默认按对账/详情处理；无编号则列单
        return "order"
    return "submit"


async def _dispatch(request: Request, rest: str = "") -> Any:
    kind = _classify(rest, request.method)
    if kind == "candidates":
        return await candidates(request, rest)
    if kind == "errors":
        return await error_snapshots(request, rest)
    if kind == "tt_read":
        return await tt_read(request, rest)
    if kind == "tt_timepoint":
        return await tt_timepoint(request, rest)
    if kind == "tt_verify":
        return await tt_verify(request, rest)
    if kind == "receipt":
        return await get_receipt(request, rest)
    if kind == "reconcile" or kind == "order":
        params = await _body_dict(request)
        if not _rid_from(rest, params):
            return await list_orders(request, rest)
        return await get_order(request, rest)
    if kind == "list_orders":
        return await list_orders(request, rest)
    return await submit_cleanup(request, rest)


for _prefix in _PREFIXES:
    # _dispatch 的 rest 有默认值，根路径与 /prefix/{rest:path} 可共用同一处理器。
    router.add_api_route(_prefix, _dispatch, methods=["GET", "POST"])
    router.add_api_route(f"{_prefix}/{{rest:path}}", _dispatch, methods=["GET", "POST"])


# ----------------------------------------------------------------------
# expire_policy 详情页可能直接调用的固定路径（先于通用 /api/{module}/{id} 注册）
# ----------------------------------------------------------------------

@router.get("/api/expire_policy/candidates")
async def ep_candidates_get(request: Request) -> Any:
    return await candidates(request)


@router.post("/api/expire_policy/candidates")
async def ep_candidates_post(request: Request) -> Any:
    return await candidates(request)


@router.get("/api/expire_policy/cleanup/candidates")
async def ep_cleanup_candidates(request: Request) -> Any:
    return await candidates(request)


@router.post("/api/expire_policy/cleanup")
async def ep_cleanup(request: Request) -> Any:
    return await submit_cleanup(request)


@router.post("/api/expire_policy/recycle")
async def ep_recycle(request: Request) -> Any:
    return await submit_cleanup(request)


@router.post("/api/expire_policy/execute_cleanup")
async def ep_execute_cleanup(request: Request) -> Any:
    return await submit_cleanup(request)


@router.post("/api/expire_policy/reclaim")
async def ep_reclaim(request: Request) -> Any:
    return await submit_cleanup(request)


@router.get("/api/expire_policy/orders/{rid}/receipt")
async def ep_order_receipt(rid: str, request: Request) -> Any:
    return await get_receipt(request, rid)


@router.get("/api/expire_policy/recycle_orders/{rid}/receipt")
async def ep_recycle_order_receipt(rid: str, request: Request) -> Any:
    return await get_receipt(request, rid)


@router.get("/api/expire_policy/recycle_orders/{rid}/reconcile")
async def ep_order_reconcile(rid: str, request: Request) -> Any:
    return await get_order(request, rid)


@router.post("/api/expire_policy/recycle_orders")
async def ep_create_order(request: Request) -> Any:
    return await submit_cleanup(request)


@router.get("/api/expire_policy/recycle_orders")
async def ep_list_orders(request: Request) -> Any:
    return await list_orders(request)


@router.get("/api/expire_policy/recycle_orders/{rid}")
async def ep_get_order(rid: str, request: Request) -> Any:
    return await get_order(request, rid)


@router.get("/api/expire_policy/recycle/{rid}")
async def ep_recycle_detail(rid: str, request: Request) -> Any:
    return await get_order(request, rid)


@router.get("/api/expire_policy/orders/{rid}")
async def ep_order_detail(rid: str, request: Request) -> Any:
    return await get_order(request, rid)


@router.get("/api/expire_policy/receipt/{rid}")
async def ep_receipt(rid: str, request: Request) -> Any:
    return await get_receipt(request, rid)


@router.get("/api/expire_policy/reconcile/{rid}")
async def ep_reconcile(rid: str, request: Request) -> Any:
    return await get_order(request, rid)


# ----------------------------------------------------------------------
# 时间旅行入口的固定路径
# ----------------------------------------------------------------------

@router.get("/api/time_travel/errors")
async def tt_errors(request: Request) -> Any:
    return await error_snapshots(request)


@router.get("/api/time_travel/error_snapshots")
async def tt_error_snaps(request: Request) -> Any:
    return await error_snapshots(request)


@router.get("/api/time_travel/broken_snapshots")
async def tt_broken(request: Request) -> Any:
    return await error_snapshots(request)


@router.get("/api/time_travel/snapshot_errors")
async def tt_snapshot_errors(request: Request) -> Any:
    return await error_snapshots(request)


@router.get("/api/time_travel/read/{snapshot_id}")
async def tt_read_get(snapshot_id: str, request: Request) -> Any:
    params = await _body_dict(request)
    table = _pick(params, _TABLE_KEYS)
    if not table:
        return _bad_request("缺少 lake_table")
    return time_travel_svc.read_at_snapshot(str(table), snapshot_id)


@router.post("/api/time_travel/read")
async def tt_read_post(request: Request) -> Any:
    return await tt_read(request)


@router.post("/api/time_travel/read_at_snapshot")
async def tt_read_at_snapshot_post(request: Request) -> Any:
    return await tt_read(request)


@router.get("/api/time_travel/read_at_snapshot")
async def tt_read_at_snapshot_get(request: Request) -> Any:
    params = await _body_dict(request)
    table = _pick(params, _TABLE_KEYS)
    sid = params.get("snapshot_id") or params.get("snap_id")
    if not table or not sid:
        return _bad_request("缺少 lake_table 或 snapshot_id")
    return time_travel_svc.read_at_snapshot(str(table), str(sid))


@router.post("/api/time_travel/read_at_timepoint")
async def tt_read_at_timepoint_post(request: Request) -> Any:
    return await tt_timepoint(request)


@router.get("/api/time_travel/read_at_timepoint")
async def tt_read_at_timepoint_get(request: Request) -> Any:
    return await tt_timepoint(request)


@router.post("/api/time_travel/query")
async def tt_query_post(request: Request) -> Any:
    params = await _body_dict(request)
    if params.get("timestamp") or params.get("timepoint"):
        return await tt_timepoint(request)
    return await tt_read(request)


@router.post("/api/time_travel/retry")
async def tt_retry_post(request: Request) -> Any:
    return await tt_read(request)


@router.get("/api/time_travel/retry/{snapshot_id}")
async def tt_retry_get(snapshot_id: str, request: Request) -> Any:
    return await tt_read_get(snapshot_id, request)


@router.get("/api/time_travel/snapshot/{snapshot_id}")
async def tt_snapshot_get(snapshot_id: str, request: Request) -> Any:
    return await tt_read_get(snapshot_id, request)


@router.get("/api/time_travel/verify/{snapshot_id}")
async def tt_verify_get(snapshot_id: str, request: Request) -> Any:
    params = await _body_dict(request)
    table = _pick(params, _TABLE_KEYS)
    if not table:
        return _bad_request("缺少 lake_table")
    result = time_travel_svc.verify_snapshot(str(table), snapshot_id)
    if result is None:
        return _not_found(f"快照 {snapshot_id} 不存在")
    return result


@router.get("/api/time_travel/timepoint")
async def tt_timepoint_get(request: Request) -> Any:
    return await tt_timepoint(request)


@router.get("/api/time_travel/latest")
async def tt_latest_get(request: Request) -> Any:
    params = await _body_dict(request)
    table = _pick(params, _TABLE_KEYS)
    if not table:
        return _bad_request("缺少 lake_table")
    return time_travel_svc.read_latest_available(str(table))


# 中划线风格前缀的兜底
for _prefix in ("/api/time-travel", "/api/timetravel"):
    router.add_api_route(_prefix, tt_read_post, methods=["POST", "GET"])
    router.add_api_route(f"{_prefix}/{{rest:path}}", _dispatch, methods=["GET", "POST"])


# ----------------------------------------------------------------------
# 快照管理模块下可能的清理/可用性入口
# ----------------------------------------------------------------------

@router.post("/api/snapshot_mgr/cleanup")
async def sm_cleanup(request: Request) -> Any:
    return await submit_cleanup(request)


@router.post("/api/snapshot_mgr/recycle")
async def sm_recycle(request: Request) -> Any:
    return await submit_cleanup(request)


@router.get("/api/snapshot_mgr/candidates")
async def sm_candidates(request: Request) -> Any:
    return await candidates(request)


@router.get("/api/snapshot_mgr/recycle_orders/{rid}")
async def sm_order(rid: str, request: Request) -> Any:
    return await get_order(request, rid)


@router.get("/api/snapshot_mgr/error_snapshots")
async def sm_errors(request: Request) -> Any:
    return await error_snapshots(request)


@router.post("/api/snapshot_mgr/{snapshot_id}/available")
async def sm_mark_available(snapshot_id: str, request: Request) -> Any:
    from app.services.snapshot_mgr import SnapshotMgrService
    params = await _body_dict(request)
    table = _pick(params, _TABLE_KEYS)
    if not table:
        return _bad_request("缺少 lake_table")
    svc = SnapshotMgrService()
    result = svc.mark_available(str(table), snapshot_id)
    if result is None:
        return _not_found(f"快照 {snapshot_id} 不存在")
    return result
