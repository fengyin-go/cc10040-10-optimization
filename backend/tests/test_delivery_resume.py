"""数据服务续取批次验收测试。

场景（对应需求验收口径）：
  让模板甲正常出数、模板乙无法执行 -> 从首份响应核查甲的行明细与乙的失败原因；
  修复乙并换用新版口径后重试（续取）-> 核对两份响应里的批次身份和行明细：
    - 批次身份：新批次 batch_id/batch_no 不同，并由 resumed_from_batch 串联；
    - 甲已签收：沿用原 execution_id 结果身份，状态 adopted，不重复执行；
    - 乙恢复：产生新的 execution_id（新数据），行内口径版本变为新版；
    - 空行集可追溯到一次成功执行；未交付项必须有失败原因；
    - 可从批次恢复待交付项。
"""
from __future__ import annotations

import pytest

from app.store import store
from app.services.query_builder import QueryBuilderService
from app.services.data_service import DataServiceService
from app.services.caliber_version import CaliberVersionService


# 测试专用 id 段，避开既有测试与种子数据
METRIC_ROW_ID = 8801
TEMPLATE_A_ID = 8811   # 甲：正常出数
TEMPLATE_B_ID = 8812   # 乙：先无法执行，修复后续取
TEMPLATE_C_ID = 8813   # 丙：正常但空行集
API_ID = 8821
CALIBER_V1_ID = 8831
CALIBER_V2_ID = 8832

METRIC_NAME = "续取验收指标"
EMPTY_METRIC_NAME = "续取空行指标"


@pytest.fixture()
def scenario():
    """搭建：指标 + 甲乙丙三个已发布模板 + 已发布数据服务 + v1 口径。"""
    store.rows("metric_definition").append({
        "id": METRIC_ROW_ID, "status": "已发布", "pending": False, "abnormal": False,
        "指标名称": METRIC_NAME, "计算口径": "按日求和",
        "统计粒度": "天", "数据源": "dwd", "业务域": "验收域",
    })
    # 空行指标：不写 metric_definition，模板执行成功但 row_count=0
    for tpl_id, name, metric, dims in (
        (TEMPLATE_A_ID, "验收模板甲", METRIC_NAME, "渠道,大区"),
        (TEMPLATE_B_ID, "验收模板乙", METRIC_NAME, "渠道"),
        (TEMPLATE_C_ID, "验收模板丙_空行", EMPTY_METRIC_NAME, "渠道"),
    ):
        store.rows("query_builder").append({
            "id": tpl_id, "status": "已发布", "pending": False, "abnormal": False,
            "模板名称": name, "关联指标": metric, "维度组合": dims,
            "过滤条件": "", "缓存策略": "短期缓存",
        })
    store.rows("data_service").append({
        "id": API_ID, "status": "已发布", "pending": False, "abnormal": False,
        "接口名称": "验收数据服务", "关联指标或模型": f"{METRIC_NAME},{EMPTY_METRIC_NAME}",
        "请求方式": "POST", "限流配额": "1000/分钟", "发布状态": "已发布",
    })
    store.rows("caliber_version").append({
        "id": CALIBER_V1_ID, "status": "已生效", "pending": True, "abnormal": False,
        "所属指标": METRIC_NAME, "版本号": "验收-v1.0",
        "口径描述": "初版口径：按日求和", "变更类型": "初始创建",
        "生效时间": "2026-10-01",
    })
    store.rows("caliber_version").append({
        "id": CALIBER_V2_ID, "status": "待生效", "pending": True, "abnormal": False,
        "所属指标": EMPTY_METRIC_NAME, "版本号": "验收-空-v1",
        "口径描述": "空行指标口径", "变更类型": "初始创建",
        "生效时间": "2026-10-01",
    })
    # 空行指标也需要已生效口径，否则丙会失败
    CaliberVersionService().activate_version(CALIBER_V2_ID)
    yield
    # 清理（store 为进程内单例，避免污染其他测试模块）
    for module, ident in (
        ("metric_definition", METRIC_ROW_ID),
        ("query_builder", TEMPLATE_A_ID),
        ("query_builder", TEMPLATE_B_ID),
        ("query_builder", TEMPLATE_C_ID),
        ("data_service", API_ID),
    ):
        rows = store.rows(module)
        rows[:] = [r for r in rows if int(r.get("id", 0)) != ident]
    # 口径按指标名清理，覆盖测试中动态新增的新版口径行
    caliber_rows = store.rows("caliber_version")
    caliber_rows[:] = [
        r for r in caliber_rows
        if r.get("所属指标") not in (METRIC_NAME, EMPTY_METRIC_NAME)
    ]
    for tpl_id in (TEMPLATE_A_ID, TEMPLATE_B_ID, TEMPLATE_C_ID):
        store.clear_template_fault(tpl_id)
        store.clear_template_executions(tpl_id)
    store.clear_delivery_batches(API_ID)
    store.cache_invalidate_by_prefix(f"query:{METRIC_NAME}")
    store.cache_invalidate_by_prefix(f"query:{EMPTY_METRIC_NAME}")


def _by_template(response: dict) -> dict[int, dict]:
    return {int(d["template_id"]): d for d in response["deliveries"]}


class TestDeliveryResumeBatch:
    """首调部分失败 -> 修复并换新口径续取的完整对账链路。"""

    def test_first_call_partial_failure_keeps_every_template_visible(self, scenario):
        """甲正常出数、乙无法执行：首份响应必须同时给出甲的数据与乙的失败原因。"""
        builder = QueryBuilderService()
        svc = DataServiceService()

        # 让乙无法执行
        fault = builder.set_fault(TEMPLATE_B_ID, "执行引擎连接超时（模拟）")
        assert fault["ok"] is True

        first = svc.call_service(API_ID, operator="验收调用方")

        # 顶层不再静默漏项：三个模板都在交付明细里
        assert "error" not in first
        assert len(first["deliveries"]) == 3
        assert first["batch_status"] == "部分失败"
        assert first["delivered_count"] == 2          # 甲 + 丙
        assert first["failed_count"] == 1             # 乙
        assert first["adopted_count"] == 0

        deliveries = _by_template(first)

        # 甲：正常出数，带模板、口径、执行结论、行明细
        a = deliveries[TEMPLATE_A_ID]
        assert a["status"] == "delivered"
        assert a["conclusion"] == "SUCCESS"
        assert a["caliber"] == "验收-v1.0"
        assert a["failure_reason"] is None
        assert a["row_count"] >= 1
        assert a["execution_id"].startswith("EXE-")
        assert all(row["口径版本"] == "验收-v1.0" for row in a["rows"])
        assert all("渠道" in row and "大区" in row for row in a["rows"])

        # 乙：未交付项必须有对应失败原因，且有失败执行留痕编号
        b = deliveries[TEMPLATE_B_ID]
        assert b["status"] == "failed"
        assert b["conclusion"] == "FAILED"
        assert b["row_count"] == 0
        assert b["rows"] == []
        assert "执行引擎连接超时" in b["failure_reason"]
        assert b["execution_id"].startswith("EXE-")

        # 丙：空行集可追溯到一次成功执行（不是失败、不是漏项）
        c = deliveries[TEMPLATE_C_ID]
        assert c["status"] == "delivered"
        assert c["conclusion"] == "SUCCESS"
        assert c["empty"] is True
        assert c["row_count"] == 0
        empty_exec = builder.get_execution(c["execution_id"])
        assert empty_exec is not None
        assert empty_exec["conclusion"] == "SUCCESS"
        assert empty_exec["row_count"] == 0

        # result_count 只统计成功交付的行，query_results 不再混入失败项
        assert first["result_count"] == a["row_count"]
        assert len(first["query_results"]) == 2
        assert all(r["conclusion"] == "SUCCESS" for r in first["query_results"])

        # 失败执行也写入查询日志（异常终止），可审计
        journals = store.query_logs()
        assert any(j["status"] == "异常终止" for j in journals)

    def test_recover_pending_and_resume_with_new_caliber(self, scenario):
        """修复乙并启用新版口径后续取：核对批次身份、结果身份与行明细。"""
        builder = QueryBuilderService()
        svc = DataServiceService()
        caliber_svc = CaliberVersionService()

        builder.set_fault(TEMPLATE_B_ID, "执行引擎连接超时（模拟）")
        first = svc.call_service(API_ID, operator="验收调用方")
        first_batch_id = first["batch_id"]
        first_no = first["batch_no"]
        first_items = _by_template(first)
        a_first_exec = first_items[TEMPLATE_A_ID]["execution_id"]
        b_failed_exec = first_items[TEMPLATE_B_ID]["execution_id"]

        # 从批次恢复待交付项：只剩乙，且带失败原因
        pending = svc.get_pending_deliveries(first_batch_id)
        assert pending["pending_count"] == 1
        assert pending["pending_deliveries"][0]["template_id"] == TEMPLATE_B_ID
        assert "执行引擎连接超时" in pending["pending_deliveries"][0]["failure_reason"]

        # 修复乙：解除故障
        cleared = builder.clear_fault(TEMPLATE_B_ID)
        assert cleared["ok"] is True

        # 换用新版口径：新增 v2 并启用（自动过期 v1、失效旧缓存）
        new_caliber_id = store.next_id("caliber_version")
        store.rows("caliber_version").append({
            "id": new_caliber_id, "status": "待生效", "pending": True, "abnormal": False,
            "所属指标": METRIC_NAME, "版本号": "验收-v2.0",
            "口径描述": "新版口径：按自然周去重统计", "变更类型": "口径调整",
            "生效时间": "2026-11-22",
        })
        activated, msg = caliber_svc.activate_version(new_caliber_id)
        assert activated is not None and "已生效" in msg

        # 续取：基于首份批次
        second = svc.call_service(API_ID, operator="验收调用方",
                                  resume_from_batch=first_batch_id)

        # ── 批次身份核对 ──
        assert second["batch_id"] != first_batch_id
        assert second["batch_no"] != first_no
        assert second["resumed_from_batch"] == first_batch_id
        assert second["resumed_from_batch_no"] == first_no
        assert second["attempt"] == first["attempt"] + 1
        assert second["batch_status"] == "全部交付"
        assert second["failed_count"] == 0

        second_items = _by_template(second)

        # ── 甲：沿用已经签收的结果身份，不重新执行 ──
        a2 = second_items[TEMPLATE_A_ID]
        assert a2["status"] == "adopted"
        assert a2["conclusion"] == "SUCCESS"
        assert a2["execution_id"] == a_first_exec       # 结果身份不变
        assert a2["resumed_from_delivery"] == first_items[TEMPLATE_A_ID]["delivery_id"]
        # 甲仍按签收时的 v1 口径交付（续取不重算）
        assert a2["caliber"] == "验收-v1.0"
        assert a2["rows"] == first_items[TEMPLATE_A_ID]["rows"]

        # ── 乙：恢复后产生新数据，新执行身份 + 新版口径 ──
        b2 = second_items[TEMPLATE_B_ID]
        assert b2["status"] == "delivered"
        assert b2["conclusion"] == "SUCCESS"
        assert b2["execution_id"] != b_failed_exec       # 失败留痕与新成功留痕不同
        assert b2["execution_id"].startswith("EXE-")
        assert b2["caliber"] == "验收-v2.0"              # 换用新版口径
        assert b2["row_count"] >= 1
        assert all(row["口径版本"] == "验收-v2.0" for row in b2["rows"])
        assert b2["failure_reason"] is None
        assert b2["resumed_from_delivery"] == first_items[TEMPLATE_B_ID]["delivery_id"]

        # ── 丙：空行成功项同样被采纳，身份不变 ──
        c2 = second_items[TEMPLATE_C_ID]
        assert c2["status"] == "adopted"
        assert c2["empty"] is True
        assert c2["execution_id"] == first_items[TEMPLATE_C_ID]["execution_id"]

        # 批次列表可跨响应核对，批次详情可按身份回读
        batches = svc.list_delivery_batches(API_ID)
        batch_ids = [b["batch_id"] for b in batches]
        assert first_batch_id in batch_ids and second["batch_id"] in batch_ids
        reloaded = svc.get_delivery_batch(first_batch_id)
        assert reloaded["batch_no"] == first_no
        assert len(reloaded["deliveries"]) == 3

    def test_resume_rejects_batch_from_other_service(self, scenario):
        """续取批次必须属于同一数据服务，否则拒绝。"""
        svc = DataServiceService()
        first = svc.call_service(API_ID, operator="验收调用方")

        other_api_id = store.next_id("data_service")
        store.rows("data_service").append({
            "id": other_api_id, "status": "已发布", "pending": False, "abnormal": False,
            "接口名称": "另一个服务", "关联指标或模型": METRIC_NAME,
            "请求方式": "GET", "限流配额": "100/分钟", "发布状态": "已发布",
        })
        try:
            resp = svc.call_service(other_api_id, operator="验收调用方",
                                    resume_from_batch=first["batch_id"])
            assert "error" in resp
            assert "不能用于服务接口" in resp["error"]
        finally:
            store.rows("data_service")[:] = [
                r for r in store.rows("data_service")
                if int(r.get("id", 0)) != other_api_id
            ]

    def test_missing_caliber_is_a_failed_delivery_with_reason(self, scenario):
        """没有已生效口径也是可对账的失败项，而不是静默漏项。"""
        svc = DataServiceService()
        # 新建一个无口径指标的已发布模板并挂到本服务
        no_cal_metric = "续取无口径指标"
        tpl_id = store.next_id("query_builder")
        store.rows("query_builder").append({
            "id": tpl_id, "status": "已发布", "pending": False, "abnormal": False,
            "模板名称": "验收模板丁_无口径", "关联指标": no_cal_metric,
            "维度组合": "渠道", "过滤条件": "", "缓存策略": "",
        })
        api = store.find("data_service", API_ID)
        api["关联指标或模型"] = f"{api['关联指标或模型']},{no_cal_metric}"
        try:
            resp = svc.call_service(API_ID, operator="验收调用方")
            items = _by_template(resp)
            d = items[tpl_id]
            assert d["status"] == "failed"
            assert d["conclusion"] == "FAILED"
            assert "没有已生效的口径版本" in d["failure_reason"]
            assert d["execution_id"].startswith("EXE-")
        finally:
            store.rows("query_builder")[:] = [
                r for r in store.rows("query_builder") if int(r.get("id", 0)) != tpl_id
            ]
            api["关联指标或模型"] = f"{METRIC_NAME},{EMPTY_METRIC_NAME}"


class TestQueryBuilderExecutionTrace:
    """查询构建器执行结论留痕的独立约束。"""

    def test_failure_and_success_both_recorded_in_history(self, scenario):
        builder = QueryBuilderService()
        builder.set_fault(TEMPLATE_B_ID, "模拟中断")
        failed = builder.run_template(TEMPLATE_B_ID, operator="留痕")
        assert failed["conclusion"] == "FAILED"
        assert failed["failure_reason"]

        builder.clear_fault(TEMPLATE_B_ID)
        # 夹具内 v1 为已生效口径；夹具清理按指标名隔离，不受其他测试顺序影响
        success = builder.run_template(TEMPLATE_B_ID, operator="留痕")
        assert success["conclusion"] == "SUCCESS"
        assert success["caliber"] == "验收-v1.0"

        history = builder.get_execution_history(TEMPLATE_B_ID)
        assert history["failed"] == 1 and history["success"] == 1
        conclusions = [e["conclusion"] for e in history["executions"]]
        assert conclusions == ["FAILED", "SUCCESS"]
        # 两次执行身份不同，调用方可区分旧失败与恢复后的新数据
        assert history["executions"][0]["execution_id"] != history["executions"][1]["execution_id"]

    def test_execute_query_legacy_contract_still_works(self, scenario):
        """旧 execute_query 契约保持可用，失败带 error，成功带 rows。"""
        builder = QueryBuilderService()
        ok = builder.execute_query(TEMPLATE_A_ID, operator="兼容")
        assert "rows" in ok and ok["conclusion"] == "SUCCESS"
        assert ok["execution_id"].startswith("EXE-")

        builder.set_fault(TEMPLATE_A_ID, "模拟中断")
        try:
            bad = builder.execute_query(TEMPLATE_A_ID, operator="兼容")
            assert "error" in bad and bad["conclusion"] == "FAILED"
            assert bad["rows"] == []
        finally:
            builder.clear_fault(TEMPLATE_A_ID)
