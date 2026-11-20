"""去重指标更正发布的端到端测试。

覆盖任务要点：
- 独立用户按用户ID去重（10 笔订单只有 6 个用户），订单量仍为 10，两者分别计算；
- 更正版本由数据落盘实际写入，再同步更正记录，详情可回查原批与更正批；
- 并发/重复提交同一更正批只有一笔有效落盘（幂等）；
- 写入失败回滚，不留半成品；
- 旧批次引用可回查、一次读取只看到一个可解释版本、历史槽位版本兼容；
- 重跑同一批更正不多写有效结果。
"""
from __future__ import annotations

import threading

import pytest

from app.services.metric_calc import (
    DISTINCT_USER_METRIC, ORDER_COUNT_METRIC, metric_calc_service,
)
from app.services.pipeline_orchestrator import run_metric_sink_pipeline, run_window_metric_pipeline
from app.services.sink_writer import (
    BATCH_STATUS_ACTIVE, BATCH_STATUS_SUPERSEDED, BATCH_TABLE, CORRECTION_TABLE,
    DEFAULT_METRIC_SLOT, SinkPublishError, sink_writer_service,
)
from app.store import store


SLOT = DEFAULT_METRIC_SLOT


class TestDedupCalculation:
    def test_distinct_users_is_six_not_ten(self):
        result = metric_calc_service.compute_order_metrics()
        assert result["order_count"] == 10
        assert result["distinct_users"] == 6
        # 同一用户多单只增加一个独立用户：U1001 有 3 单、U1002/U1003 各 2 单
        assert result["user_ids"] == ["U1001", "U1002", "U1003", "U1004", "U1005", "U1006"]
        assert len(result["order_ids"]) == 10

    def test_duplicate_user_orders_count_once(self):
        records = metric_calc_service.order_records()
        u1001_orders = [r for r in records if r["用户ID"] == "U1001"]
        assert len(u1001_orders) == 3
        result = metric_calc_service.compute_order_metrics(records)
        # 3 笔订单贡献 3 个订单量，但只贡献 1 个独立用户
        assert result["order_count"] == 10
        assert result["distinct_users"] == 6

    def test_order_and_distinct_computed_separately(self):
        result = metric_calc_service.compute_order_metrics()
        assert result["order_count"] != result["distinct_users"]

    def test_pipeline_metric_entry_uses_fixed_dedup(self):
        wm = run_window_metric_pipeline()
        assert wm["metrics"][ORDER_COUNT_METRIC] == 10
        assert wm["metrics"][DISTINCT_USER_METRIC] == 6
        assert wm["WIN-TUMBLE-60s"]["独立用户"] == 6
        assert wm["WIN-TUMBLE-60s"]["记录数"] == 10

    def test_recompute_endpoint_payload(self):
        result = metric_calc_service.compute_order_metrics()
        # 订单标识完整可对账
        assert result["order_ids"][0] == "ORD-001"
        assert result["order_ids"][-1] == "ORD-010"


class TestCorrectionPublish:
    def test_seed_batch_has_wrong_distinct_value(self):
        active = sink_writer_service._active_batch(SLOT)
        assert active is not None
        assert active["version"] == 1
        assert active["values"]["order_count"] == 10
        assert active["values"]["distinct_users"] == 10  # 错误落盘值

    def test_publish_correction_writes_new_version(self):
        out = sink_writer_service.publish_correction_batch()
        assert out["ok"] is True
        assert out["reused"] is False
        batch, correction = out["batch"], out["correction"]

        # 新批次：订单量仍为 10，独立用户更正为 6，值与版本成组
        assert batch["version"] == 2
        assert batch["values"] == {"order_count": 10, "distinct_users": 6}
        assert batch["metrics"][ORDER_COUNT_METRIC] == 10
        assert batch["metrics"][DISTINCT_USER_METRIC] == 6
        assert batch["status"] == BATCH_STATUS_ACTIVE
        assert batch["correction_of"] == "SINK-0001-B0001"

        # 落盘详情可追到原始订单
        assert len(batch["order_ids"]) == 10
        assert batch["user_ids"] == ["U1001", "U1002", "U1003", "U1004", "U1005", "U1006"]

        # 更正记录由落盘结果同步：读到的旧值与新值、版本成组留痕
        assert correction["base_batch_id"] == "SINK-0001-B0001"
        assert correction["batch_id"] == batch["batch_id"]
        assert correction["values_read"]["distinct_users"] == 10
        assert correction["values_corrected"]["distinct_users"] == 6
        assert correction["version_group"]["base"]["version"] == 1
        assert correction["version_group"]["corrected"]["version"] == 2

    def test_old_batch_remains_readable_after_correction(self):
        sink_writer_service.publish_correction_batch()
        old = sink_writer_service.get_batch_detail("SINK-0001-B0001")
        # 旧批次数值不被篡改、状态切换为已更正、引用指向新批次
        assert old["values"]["distinct_users"] == 10
        assert old["status"] == BATCH_STATUS_SUPERSEDED
        assert old["superseded_by"] == "SINK-0001-B0002"
        # 详情里能查到关联的更正记录
        assert any(c["batch_id"] == "SINK-0001-B0002" for c in old["correction_records"])

    def test_batch_detail_links_base_and_correction(self):
        sink_writer_service.publish_correction_batch()
        detail = sink_writer_service.get_batch_detail("SINK-0001-B0002")
        assert detail["base_batch"]["batch_id"] == "SINK-0001-B0001"
        assert detail["base_batch"]["values"]["distinct_users"] == 10
        assert detail["values"]["distinct_users"] == 6

    def test_only_one_active_version_per_slot(self):
        sink_writer_service.publish_correction_batch()
        active = [b for b in sink_writer_service.list_batches(SLOT) if b["status"] == BATCH_STATUS_ACTIVE]
        assert len(active) == 1
        assert active[0]["version"] == 2

    def test_version_view_default_shows_single_explainable_version(self):
        sink_writer_service.publish_correction_batch()
        view = sink_writer_service.get_slot_version_view(SLOT)
        assert view["visible_version"] == 2
        assert view["is_historical"] is False
        assert view["values"]["distinct_users"] == 6
        assert view["correction_of"] == "SINK-0001-B0001"
        # 一次读取内更正关系自洽
        assert view["correction_relation"]["values_read"]["distinct_users"] == 10
        assert view["correction_relation"]["values_corrected"]["distinct_users"] == 6

    def test_historical_version_view_for_old_report(self):
        sink_writer_service.publish_correction_batch()
        old_view = sink_writer_service.get_slot_version_view(SLOT, version=1)
        assert old_view["visible_version"] == 1
        assert old_view["is_historical"] is True
        assert old_view["values"]["distinct_users"] == 10  # 旧报告对账读到旧值
        new_view = sink_writer_service.get_slot_version_view(SLOT, version=2)
        assert new_view["values"]["distinct_users"] == 6

    def test_version_view_unknown_version_raises(self):
        with pytest.raises(SinkPublishError):
            sink_writer_service.get_slot_version_view(SLOT, version=99)


class TestIdempotencyAndConcurrency:
    def test_rerun_same_correction_does_not_duplicate(self):
        first = sink_writer_service.publish_correction_batch(correction_key="CORR-DEDUP-001")
        second = sink_writer_service.publish_correction_batch(correction_key="CORR-DEDUP-001")
        assert first["batch"]["batch_id"] == second["batch"]["batch_id"]
        assert second["reused"] is True

        batches = sink_writer_service.list_batches(SLOT)
        assert len(batches) == 2  # 原始批 + 一笔更正批，重跑不多写
        active = [b for b in batches if b["status"] == BATCH_STATUS_ACTIVE]
        assert len(active) == 1
        corrections = sink_writer_service.list_corrections(SLOT)
        assert len(corrections) == 1
        # 最终值符合预期
        assert active[0]["values"] == {"order_count": 10, "distinct_users": 6}

    def test_concurrent_same_correction_only_one_persisted(self):
        results: list[dict] = []
        errors: list[Exception] = []

        def worker() -> None:
            try:
                results.append(
                    sink_writer_service.publish_correction_batch(correction_key="CORR-CONCURRENT-001"))
            except Exception as exc:  # pragma: no cover - 仅用于暴露并发缺陷
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(results) == 8
        batch_ids = {r["batch"]["batch_id"] for r in results}
        assert batch_ids == {"SINK-0001-B0002"}
        assert len(sink_writer_service.list_batches(SLOT)) == 2
        assert len(sink_writer_service.list_corrections(SLOT)) == 1

    def test_distinct_correction_keys_chain_versions(self):
        first = sink_writer_service.publish_correction_batch(correction_key="K1")
        second = sink_writer_service.publish_correction_batch(correction_key="K2")
        assert first["batch"]["version"] == 2
        assert second["batch"]["version"] == 3
        assert second["batch"]["correction_of"] == "SINK-0001-B0002"
        assert len(sink_writer_service.list_corrections(SLOT)) == 2
        # 每次重算结果一致（数据没变），仍是 10/6
        assert second["batch"]["values"] == {"order_count": 10, "distinct_users": 6}


class TestRollback:
    def test_batch_write_failure_rolls_back(self):
        before_batches = len(store.rows(BATCH_TABLE))
        before_corrections = len(store.rows(CORRECTION_TABLE))
        with pytest.raises(SinkPublishError):
            sink_writer_service.publish_correction_batch(fail_at="batch_write")
        # 两张表恢复到发布前
        assert len(store.rows(BATCH_TABLE)) == before_batches
        assert len(store.rows(CORRECTION_TABLE)) == before_corrections
        active = sink_writer_service._active_batch(SLOT)
        assert active["version"] == 1
        assert active["status"] == BATCH_STATUS_ACTIVE

    def test_correction_sync_failure_rolls_back(self):
        with pytest.raises(SinkPublishError):
            sink_writer_service.publish_correction_batch(fail_at="correction_sync")
        # 新批次与旧批次状态切换都被回滚
        active = sink_writer_service._active_batch(SLOT)
        assert active["batch_id"] == "SINK-0001-B0001"
        assert active["status"] == BATCH_STATUS_ACTIVE
        assert active["superseded_by"] is None
        assert sink_writer_service.list_corrections(SLOT) == []

    def test_retry_after_failed_publish_succeeds(self):
        with pytest.raises(SinkPublishError):
            sink_writer_service.publish_correction_batch(
                correction_key="CORR-RETRY", fail_at="batch_write")
        out = sink_writer_service.publish_correction_batch(correction_key="CORR-RETRY")
        assert out["reused"] is False
        assert out["batch"]["values"]["distinct_users"] == 6


class TestPipelineIntegration:
    def test_metric_sink_pipeline_points_at_persisted_batch(self):
        sink_writer_service.publish_correction_batch()
        result = run_metric_sink_pipeline()
        assert result["sink_count"] == 4
        assert all(s["写入状态"] == "成功" for s in result["sinks"])
        assert result["batch_id"] == "SINK-0001-B0002"
        assert result["version"] == 2
        distinct_sink = next(s for s in result["sinks"] if s["指标编号"] == DISTINCT_USER_METRIC)
        assert distinct_sink["值"] == 6

    def test_historical_slot_version_kept_compatible(self):
        # 历史槽位 SINK-0001 仍然存在且批次挂在同一槽位上
        slot = store.find("sink_writer", 1)
        assert slot["槽位编号"] == SLOT
        sink_writer_service.publish_correction_batch()
        versions = [b["version"] for b in sink_writer_service.list_batches(SLOT)]
        assert versions == [1, 2]
