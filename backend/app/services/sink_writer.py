"""数据落盘业务规则：状态流转、字段校验、指标批次版本管理与更正发布。

指标批次（含独立用户去重指标的更正版本）由本模块实际写入：
- 普通订单量与独立用户数在同一个批次里成组落盘，值与版本一起提交；
- 旧批次发布后只读保留，下游对账时仍可按批次/版本回查；
- 同一更正批并发或重复提交只允许一笔有效落盘（幂等键 + 进程锁）；
- 发布采用“先暂存、后提交、失败回滚”的事务语义，写入失败不会留下半成品。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime
from typing import Any

from app.services.metric_calc import (
    DISTINCT_USER_METRIC, ORDER_COUNT_METRIC, metric_calc_service,
)
from app.store import store

MODULE = "sink_writer"
LIST_FIELDS = ["槽位编号", "输出类型", "目标地址", "写入模式", "批次大小", "刷新间隔", "压缩算法", "分区策略"]
REQUIRED_FIELDS = ["槽位编号", "输出类型", "目标地址"]
STATUS_ORDER = ["待配置", "写入中", "已暂停", "写入失败"]
ACTION_RULES = {"启动写入": "写入中", "暂停写入": "已暂停", "标记失败": "写入失败"}
NEGATIVE_ACTIONS = []

# 指标批次与更正记录的内部表名（下划线前缀，不进业务模块概览）。
BATCH_TABLE = "_sink_metric_batches"
CORRECTION_TABLE = "_sink_metric_corrections"

# 指标落盘默认挂在既有 1 号槽位 SINK-0001 上，历史报告的槽位版本保持兼容。
DEFAULT_METRIC_SLOT = "SINK-0001"

BATCH_STATUS_ACTIVE = "有效"
BATCH_STATUS_SUPERSEDED = "已更正"
BATCH_TYPE_ORIGINAL = "原始批次"
BATCH_TYPE_CORRECTION = "更正批次"
CORRECTION_STATUS_ACTIVE = "有效"


class SinkPublishError(RuntimeError):
    """落盘发布失败：调用方据此提示值班人，且库内已回滚到发布前状态。"""


class SinkWriterService:
    def __init__(self) -> None:
        # 同一进程内串行化批次发布，保证并发提交同一更正批时只有一笔能落盘。
        self._publish_lock = threading.RLock()

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("槽位编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"输出槽位 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据落盘可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"输出槽位已{action}"

    # ------------------------------------------------------------------
    # 指标批次落盘：版本、更正关系与对账线索
    # ------------------------------------------------------------------
    def _batches(self) -> list[dict[str, Any]]:
        return store.rows(BATCH_TABLE)

    def _corrections(self) -> list[dict[str, Any]]:
        return store.rows(CORRECTION_TABLE)

    def _find_batch(self, batch_id: str) -> dict[str, Any] | None:
        for batch in self._batches():
            if batch.get("batch_id") == batch_id:
                return batch
        return None

    def _slot_batches(self, slot_id: str) -> list[dict[str, Any]]:
        return [batch for batch in self._batches() if batch.get("slot_id") == slot_id]

    def _active_batch(self, slot_id: str) -> dict[str, Any] | None:
        for batch in self._slot_batches(slot_id):
            if batch.get("status") == BATCH_STATUS_ACTIVE:
                return batch
        return None

    def _next_batch_id(self, slot_id: str, version: int) -> str:
        return f"{slot_id}-B{version:04d}"

    def _now(self) -> str:
        return datetime.now().isoformat()

    def _group_values(self, metrics: dict[str, Any]) -> dict[str, Any]:
        """普通订单量与独立用户数分别取值，但在一个批次里成组提交。"""
        return {
            "version_group": {
                ORDER_COUNT_METRIC: metrics["order_count"],
                DISTINCT_USER_METRIC: metrics["distinct_users"],
            },
            "values": {
                "order_count": metrics["order_count"],
                "distinct_users": metrics["distinct_users"],
            },
        }

    def publish_correction_batch(
        self,
        slot_id: str = DEFAULT_METRIC_SLOT,
        correction_key: str | None = None,
        *,
        fail_at: str | None = None,
    ) -> dict[str, Any]:
        """发布独立用户去重指标的更正版本。

        流程：锁定槽位 -> 幂等检查 -> 沿指标计算入口重新取数 -> 暂存快照 ->
        写入新批次（值与版本成组）-> 旧批次标记已更正但保留 -> 同步更正记录 ->
        提交；任一写入环节失败都按快照回滚。

        fail_at 仅供故障演练：``batch_write`` 模拟批次写入失败，
        ``correction_sync`` 模拟更正记录同步失败，两种情况都必须整体回滚。
        """
        with self._publish_lock:
            base_batch = self._active_batch(slot_id)
            if base_batch is None:
                raise SinkPublishError(f"槽位 {slot_id} 没有可更正的有效指标批次")

            metrics = metric_calc_service.compute_order_metrics()
            key = correction_key or (
                f"CORR-{slot_id}-{base_batch['batch_id']}-U{metrics['distinct_users']}"
            )

            # 幂等：同一更正批（含并发抢入的那一笔）只保留第一次有效落盘。
            for correction in self._corrections():
                if (
                    correction.get("correction_key") == key
                    and correction.get("status") == CORRECTION_STATUS_ACTIVE
                ):
                    existing_batch = self._find_batch(correction["batch_id"])
                    return {
                        "ok": True,
                        "reused": True,
                        "message": "更正批已存在，重跑未重复落盘",
                        "batch": copy.deepcopy(existing_batch),
                        "correction": copy.deepcopy(correction),
                    }

            # 订单量口径不能被更正动作带动变化：仍为原始订单数。
            if metrics["order_count"] != base_batch["values"]["order_count"]:
                raise SinkPublishError(
                    "更正后订单量与原批不一致，拒绝发布，"
                    f"原批={base_batch['values']['order_count']} 重算={metrics['order_count']}"
                )

            snapshot = {
                BATCH_TABLE: copy.deepcopy(self._batches()),
                CORRECTION_TABLE: copy.deepcopy(self._corrections()),
            }
            try:
                version = int(base_batch["version"]) + 1
                batch_id = self._next_batch_id(slot_id, version)
                grouped = self._group_values(metrics)
                if fail_at == "batch_write":
                    raise SinkPublishError("模拟批次写入失败")

                new_batch = {
                    "batch_id": batch_id,
                    "slot_id": slot_id,
                    "version": version,
                    "batch_type": BATCH_TYPE_CORRECTION,
                    "status": BATCH_STATUS_ACTIVE,
                    "correction_key": key,
                    "correction_of": base_batch["batch_id"],
                    "superseded_by": None,
                    "metrics": grouped["version_group"],
                    "values": grouped["values"],
                    # 对账线索：每一笔都能追到原始订单标识与去重后的用户清单。
                    "order_ids": list(metrics["order_ids"]),
                    "user_ids": list(metrics["user_ids"]),
                    "lineage": [*list(base_batch.get("lineage", [])), batch_id],
                    "published_at": self._now(),
                    "remark": "独立用户数按用户ID去重更正，订单量维持不变",
                }
                self._batches().append(new_batch)

                # 旧批次不删除、不改值，只切换状态，下游手里的旧引用仍可回查。
                base_batch["status"] = BATCH_STATUS_SUPERSEDED
                base_batch["superseded_by"] = batch_id

                if fail_at == "correction_sync":
                    raise SinkPublishError("模拟更正记录同步失败")

                correction_record = {
                    "correction_id": f"{key}",
                    "correction_key": key,
                    "slot_id": slot_id,
                    "status": CORRECTION_STATUS_ACTIVE,
                    "base_batch_id": base_batch["batch_id"],
                    "base_version": base_batch["version"],
                    "batch_id": batch_id,
                    "corrected_version": version,
                    # 读到的旧值与更正后的新值、版本成组留痕。
                    "values_read": copy.deepcopy(base_batch["values"]),
                    "values_corrected": copy.deepcopy(grouped["values"]),
                    "version_group": {
                        "base": {"batch_id": base_batch["batch_id"], "version": base_batch["version"]},
                        "corrected": {"batch_id": batch_id, "version": version},
                    },
                    "order_ids": list(metrics["order_ids"]),
                    "user_ids": list(metrics["user_ids"]),
                    "reason": "独立用户数误按订单行数落盘，改为按用户ID去重计数",
                    "published_at": self._now(),
                }
                self._corrections().append(correction_record)
            except Exception:
                # 回滚未完成的发布：两张表恢复到发布前快照。
                store.rows(BATCH_TABLE)[:] = snapshot[BATCH_TABLE]
                store.rows(CORRECTION_TABLE)[:] = snapshot[CORRECTION_TABLE]
                raise

            return {
                "ok": True,
                "reused": False,
                "message": "去重指标更正版本已落盘并同步更正记录",
                "batch": copy.deepcopy(new_batch),
                "correction": copy.deepcopy(correction_record),
            }

    def list_batches(self, slot_id: str | None = None) -> list[dict[str, Any]]:
        """列出批次（含已更正的旧批次），保证历史引用可回查。"""
        batches = self._batches() if slot_id is None else self._slot_batches(slot_id)
        return [copy.deepcopy(batch) for batch in sorted(batches, key=lambda item: int(item["version"]))]

    def get_batch_detail(self, batch_id: str) -> dict[str, Any]:
        """落盘详情：原批/更正批的数值、订单标识、更正关系一次查齐。"""
        batch = self._find_batch(batch_id)
        if batch is None:
            raise SinkPublishError(f"批次 {batch_id} 不存在或已清理")
        detail = copy.deepcopy(batch)
        linked = [
            copy.deepcopy(item)
            for item in self._corrections()
            if item.get("batch_id") == batch_id or item.get("base_batch_id") == batch_id
        ]
        detail["correction_records"] = linked
        if batch.get("correction_of"):
            detail["base_batch"] = copy.deepcopy(self._find_batch(batch["correction_of"]))
        return detail

    def list_corrections(self, slot_id: str | None = None) -> list[dict[str, Any]]:
        rows = self._corrections()
        if slot_id is not None:
            rows = [row for row in rows if row.get("slot_id") == slot_id]
        return [copy.deepcopy(row) for row in rows]

    def get_slot_version_view(
        self, slot_id: str = DEFAULT_METRIC_SLOT, version: int | None = None,
    ) -> dict[str, Any]:
        """一次读取只返回一个可解释的版本。

        - 不指定版本：返回当前唯一有效批次（新版本），并交代它更正自哪一版；
        - 指定历史版本：即使该版本已被更正，也原样返回并标注为历史版本，
          保证正在对账旧报告的订阅不会读到混合值。
        """
        batches = self._slot_batches(slot_id)
        if not batches:
            raise SinkPublishError(f"槽位 {slot_id} 尚无落盘批次")

        if version is None:
            visible = self._active_batch(slot_id)
            if visible is None:  # 理论上不会发生：没有有效批次时发布流程会拒绝
                raise SinkPublishError(f"槽位 {slot_id} 当前没有有效版本")
        else:
            visible = next((item for item in batches if int(item["version"]) == int(version)), None)
            if visible is None:
                raise SinkPublishError(f"槽位 {slot_id} 不存在版本 {version}")

        correction = next(
            (item for item in self._corrections() if item.get("batch_id") == visible["batch_id"]),
            None,
        )
        return {
            "slot_id": slot_id,
            "visible_version": visible["version"],
            "visible_batch_id": visible["batch_id"],
            "is_historical": visible["status"] != BATCH_STATUS_ACTIVE,
            "batch_status": visible["status"],
            "values": copy.deepcopy(visible["values"]),
            "metrics": copy.deepcopy(visible["metrics"]),
            "order_ids": list(visible.get("order_ids", [])),
            "user_ids": list(visible.get("user_ids", [])),
            "correction_of": visible.get("correction_of"),
            "correction_key": visible.get("correction_key"),
            "correction_relation": copy.deepcopy(correction) if correction else None,
            "lineage": list(visible.get("lineage", [])),
        }


sink_writer_service = SinkWriterService()
