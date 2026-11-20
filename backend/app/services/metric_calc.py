"""指标计算业务规则：状态流转、字段校验、指标取数口径与去重计算都收在这里。"""
from __future__ import annotations

from typing import Any

from app.fixtures import TRANSFORM_RECORDS
from app.store import store

MODULE = "metric_calc"
LIST_FIELDS = ["指标编号", "指标名称", "计算公式", "更新频率", "维度组合", "精度模式", "缓存窗口", "最后计算"]
REQUIRED_FIELDS = ["指标编号", "指标名称", "计算公式"]
STATUS_ORDER = ["草稿", "计算中", "可用", "计算异常"]
ACTION_RULES = {"发布指标": "计算中", "暂停计算": "可用", "标记异常": "计算异常"}
NEGATIVE_ACTIONS = []

# 落盘指标的稳定标识：普通订单量与独立用户数分别建指标、分别计算，不复用同一个口径。
ORDER_COUNT_METRIC = "METR-ORDER-COUNT"
DISTINCT_USER_METRIC = "METR-USER-DISTINCT"


class MetricCalcService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("指标编号", "")).casefold()]
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
            return None, f"实时指标 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于指标计算可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"实时指标已{action}"

    # ------------------------------------------------------------------
    # 指标取数口径：指标计算入口统一从这里取数，避免各调用链各自实现导致口径漂移。
    # ------------------------------------------------------------------
    def order_records(self) -> list[dict[str, Any]]:
        """订单转换记录的取数入口；每条记录都是一笔可按订单标识回查的原始订单。"""
        return [dict(record) for record in TRANSFORM_RECORDS]

    def compute_order_metrics(
        self, records: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """按订单转换记录计算落盘指标。

        普通订单量与独立用户数分别计算：
        - 订单量按订单编号去重计数（同一订单重复到达只算一笔）；
        - 独立用户数按用户ID去重计数，同一用户的两单只增加一个独立用户。

        返回结果同时带上订单标识与去重后的用户清单，落盘后值班人可直接按订单标识对账。
        """
        records = self.order_records() if records is None else records
        order_ids: list[str] = []
        user_ids: set[str] = set()
        for record in records:
            order_id = str(record.get("订单编号", "")).strip()
            user_id = str(record.get("用户ID", "")).strip()
            if order_id and order_id not in order_ids:
                order_ids.append(order_id)
            if user_id:
                user_ids.add(user_id)
        return {
            "order_count": len(order_ids),
            "distinct_users": len(user_ids),
            "order_ids": order_ids,
            "user_ids": sorted(user_ids),
        }


# 模块级单例：编排器、路由与落盘服务共用同一个指标取数口径。
metric_calc_service = MetricCalcService()
