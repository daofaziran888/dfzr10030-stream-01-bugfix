"""数据落盘接口：维护输出槽位，覆盖启动写入、暂停写入、标记失败等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.sink_writer import (
    DEFAULT_METRIC_SLOT, SinkPublishError, SinkWriterService,
)

router = APIRouter(prefix="/api/sink_writer", tags=["数据落盘"])

service = SinkWriterService()

LIST_FIELDS = ["槽位编号", "输出类型", "目标地址", "写入模式", "批次大小", "刷新间隔", "压缩算法", "分区策略"]
STATUSES = ["待配置", "写入中", "已暂停", "写入失败"]


class CorrectionPayload(BaseModel):
    """更正发布请求：correction_key 是同一更正批的幂等键，缺省由服务端生成。"""

    slot_id: str | None = None
    correction_key: str | None = None
    fail_at: str | None = None


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按槽位编号检索"),
    status: str | None = Query(default=None, description="待配置、写入中、已暂停、写入失败"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、槽位编号关键字与状态过滤数据落盘列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="sink_writer.csv"'})


@router.get("/metric-batches")
def list_metric_batches(slot_id: str | None = Query(default=None)) -> dict:
    """列出指标批次（含已更正的旧批次），历史版本引用仍可回查。"""
    return {"ok": True, "items": service.list_batches(slot_id)}


@router.get("/metric-batches/{batch_id}")
def get_metric_batch(batch_id: str) -> dict:
    """落盘详情：原批/更正批的数值、订单标识与更正关系一次查齐。"""
    try:
        return {"ok": True, "detail": service.get_batch_detail(batch_id)}
    except SinkPublishError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/metric-corrections")
def list_metric_corrections(slot_id: str | None = Query(default=None)) -> dict:
    """指标更正记录：读到的旧值、更正后的新值与版本成组留痕。"""
    return {"ok": True, "items": service.list_corrections(slot_id)}


@router.get("/slots/{slot_id}/version-view")
def get_version_view(
    slot_id: str, version: int | None = Query(default=None, description="缺省读当前有效版本，指定版本读历史快照"),
) -> dict:
    """一个订阅一次读取只能看到一个可解释的版本，并交代更正关系。"""
    try:
        return {"ok": True, "view": service.get_slot_version_view(slot_id, version)}
    except SinkPublishError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/metric-corrections/publish")
def publish_metric_correction(payload: CorrectionPayload) -> dict:
    """发布去重指标更正版本：并发/重复提交同一更正批只有一笔有效落盘，失败整体回滚。"""
    try:
        result = service.publish_correction_batch(
            slot_id=payload.slot_id or DEFAULT_METRIC_SLOT,
            correction_key=payload.correction_key,
            fail_at=payload.fail_at,
        )
    except SinkPublishError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"ok": True, **result}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条输出槽位明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"输出槽位 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条输出槽位，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="输出槽位已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条输出槽位执行启动写入、暂停写入、标记失败；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
