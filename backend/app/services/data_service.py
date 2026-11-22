"""数据服务领域服务：将指标和模型发布为对外数据服务API。

跨模块数据流: query_builder -> data_service -> cache_layer 以及
              data_service 调用 query_builder.run_template 获取数据

静默漏项治理：
旧实现用 ``if "rows" in result`` 过滤模板结果，模板执行失败时被静默丢弃，
调用方无法按模板分项对账。现在每次调用都组装一个「数据服务续取批次」：
- 查询构建器向批次写明模板、取数所用口径（caliber）和执行结论（SUCCESS/FAILED）；
- 接口响应关联本批每个模板的交付明细（delivered/adopted/failed）；
- 空行集也是一次成功执行（conclusion=SUCCESS, empty=True，带 execution_id）；
- 未交付项必须带 failure_reason，绝不静默漏项；
- 续取（resume_from_batch）时已签收的结果沿用原 execution_id 结果身份，
  仅重跑未交付项，恢复后产生的新数据带新的 execution_id，两份响应可逐项核对。
"""
from __future__ import annotations

import time
from typing import Any

from app.store import store
from app.services.base import BaseModuleService


class DataServiceService(BaseModuleService):
    MODULE = "data_service"
    ENTITY = "服务接口"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["接口名称", "关联指标或模型", "请求方式", "限流配额", "发布状态"]
    REQUIRED_FIELDS = ["接口名称", "关联指标或模型", "请求方式"]
    STATUS_ORDER = ["开发中", "已发布", "已下线"]
    ACTION_RULES = {"发布接口": "已发布", "下线接口": "已下线"}
    NEGATIVE_ACTIONS = ["下线接口"]

    def _resolve_targets(self, api: dict[str, Any]) -> list[str]:
        """解析服务关联的指标/模型，支持逗号分隔的多个关联目标。"""
        raw = str(api.get("关联指标或模型", "") or "")
        return [part.strip() for part in raw.replace("，", ",").split(",") if part.strip()]

    def _resolve_templates(self, targets: list[str]) -> list[dict[str, Any]]:
        """找出关联到任一目标的全部查询模板（含未发布，未发布按失败项列明明细）。"""
        seen: set[int] = set()
        templates: list[dict[str, Any]] = []
        for target in targets:
            for tmpl in store.find_by_field("query_builder", "关联指标", target):
                tmpl_id = int(tmpl.get("id", 0))
                if tmpl_id not in seen:
                    seen.add(tmpl_id)
                    templates.append(tmpl)
        return sorted(templates, key=lambda t: int(t.get("id", 0)))

    def call_service(self, entry_id: int, operator: str = "客户端",
                     resume_from_batch: int | None = None) -> dict[str, Any]:
        """调用数据服务API接口，组装数据服务续取批次。

        跨模块数据流: data_service -> query_builder -> cache_layer -> query_journal

        :param resume_from_batch: 传入历史批次 id 时进入续取：已交付/已采纳的
            模板沿用原 execution_id 结果身份（status=adopted），仅重跑未交付项。
        """
        api = store.find(self.MODULE, entry_id)
        if api is None:
            return {"error": f"服务接口 {entry_id} 不存在"}
        if api["status"] != "已发布":
            return {"error": f"服务接口 {entry_id} 未发布"}

        targets = self._resolve_targets(api)
        if not targets:
            return {"error": "服务接口未关联指标或模型"}

        templates = self._resolve_templates(targets)
        if not templates:
            return {"error": f"没有找到关联 {'、'.join(targets)} 的查询模板"}

        # 续取：载入父批次并校验同一服务
        parent_batch: dict[str, Any] | None = None
        parent_by_template: dict[int, dict[str, Any]] = {}
        attempt = 1
        if resume_from_batch is not None:
            parent_batch = store.get_delivery_batch(int(resume_from_batch))
            if parent_batch is None:
                return {"error": f"续取批次 {resume_from_batch} 不存在"}
            if int(parent_batch.get("api_id", 0)) != entry_id:
                return {"error": (
                    f"批次 {resume_from_batch} 属于服务接口 "
                    f"{parent_batch.get('api_id')}，不能用于服务接口 {entry_id} 的续取")}
            parent_by_template = {
                int(item["template_id"]): item
                for item in parent_batch.get("deliveries", [])
            }
            attempt = int(parent_batch.get("attempt", 1)) + 1

        from app.services.query_builder import QueryBuilderService
        builder = QueryBuilderService()

        deliveries: list[dict[str, Any]] = []
        for tmpl in templates:
            tmpl_id = int(tmpl.get("id", 0))
            prior = parent_by_template.get(tmpl_id)

            # 续取命中：已签收的成功结果沿用结果身份，不再重复执行
            if prior is not None and prior.get("conclusion") == "SUCCESS":
                prior_execution = store.get_template_execution(prior["execution_id"])
                rows = list(prior_execution["rows"]) if prior_execution else list(prior.get("rows", []))
                deliveries.append({
                    "delivery_id": store.next_delivery_id(),
                    "template_id": tmpl_id,
                    "template": tmpl.get("模板名称"),
                    "metric": tmpl.get("关联指标", ""),
                    "caliber": prior.get("caliber"),
                    "conclusion": "SUCCESS",
                    "status": "adopted",
                    "execution_id": prior["execution_id"],
                    "source": prior_execution.get("source") if prior_execution else None,
                    "row_count": len(rows),
                    "empty": len(rows) == 0,
                    "failure_reason": None,
                    "resumed_from_delivery": prior.get("delivery_id"),
                    "rows": rows,
                })
                continue

            # 未发布模板不执行取数，但仍走查询构建器落 FAILED 留痕，
            # 按未交付项列明原因（不再静默跳过）
            record = builder.run_template(tmpl_id, operator=operator)

            deliveries.append(self._build_delivery(tmpl, record, prior))

        batch = self._assemble_batch(api, operator, attempt, parent_batch, deliveries)
        store.save_delivery_batch(batch)
        return self._build_response(api, batch)

    def _build_delivery(self, tmpl: dict[str, Any], record: dict[str, Any],
                        prior: dict[str, Any] | None) -> dict[str, Any]:
        """把模板执行结论组装成批次内的一条交付明细。"""
        success = record.get("conclusion") == "SUCCESS"
        return {
            "delivery_id": store.next_delivery_id(),
            "template_id": int(tmpl.get("id", 0)),
            "template": tmpl.get("模板名称"),
            "metric": tmpl.get("关联指标", ""),
            "caliber": record.get("caliber"),
            "conclusion": record.get("conclusion"),
            # delivered：本批新交付；failed：未交付（带原因）
            "status": "delivered" if success else "failed",
            "execution_id": record.get("execution_id") if success else record.get("execution_id"),
            "source": record.get("source"),
            "row_count": int(record.get("row_count", 0)),
            "empty": bool(record.get("empty", False)),
            "failure_reason": None if success else record.get("failure_reason"),
            "resumed_from_delivery": prior.get("delivery_id") if prior else None,
            "rows": record.get("rows", []),
        }

    def _assemble_batch(self, api: dict[str, Any], operator: str, attempt: int,
                        parent_batch: dict[str, Any] | None,
                        deliveries: list[dict[str, Any]]) -> dict[str, Any]:
        api_id = int(api["id"])
        batch_seq = store.next_batch_no(api_id)
        delivered = [d for d in deliveries if d["status"] in ("delivered", "adopted")]
        adopted = [d for d in deliveries if d["status"] == "adopted"]
        failed = [d for d in deliveries if d["status"] == "failed"]

        if not failed:
            batch_status = "全部交付"
        elif not delivered:
            batch_status = "全部失败"
        else:
            batch_status = "部分失败"

        return {
            "batch_id": batch_seq,
            "batch_no": f"BATCH-{api_id}-{batch_seq:03d}",
            "api_id": api_id,
            "api": api.get("接口名称"),
            "request_method": api.get("请求方式"),
            "attempt": attempt,
            "resumed_from_batch": parent_batch.get("batch_id") if parent_batch else None,
            "resumed_from_batch_no": parent_batch.get("batch_no") if parent_batch else None,
            "operator": operator,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": batch_status,
            "total_templates": len(deliveries),
            "delivered_count": len(delivered),
            "adopted_count": len(adopted),
            "failed_count": len(failed),
            "result_count": sum(d["row_count"] for d in delivered),
            "deliveries": deliveries,
        }

    def _build_response(self, api: dict[str, Any],
                        batch: dict[str, Any]) -> dict[str, Any]:
        """接口响应：关联本批各模板的交付明细，供调用方按实际结果对账。"""
        # 旧契约 query_results 仅保留成功交付的结果负载，失败项不再被静默混入或丢弃
        query_results = [
            {
                "source": d.get("source"),
                "template": d.get("template"),
                "caliber": d.get("caliber"),
                "execution_id": d.get("execution_id"),
                "conclusion": "SUCCESS",
                "empty": d.get("empty"),
                "rows": d.get("rows", []),
            }
            for d in batch["deliveries"]
            if d["conclusion"] == "SUCCESS"
        ]
        return {
            "api": api.get("接口名称"),
            "request_method": api.get("请求方式"),
            "result_count": batch["result_count"],
            # 批次身份：两次调用 batch_id/batch_no 不同，续取时由 resumed_from_batch 串联
            "batch_id": batch["batch_id"],
            "batch_no": batch["batch_no"],
            "attempt": batch["attempt"],
            "batch_status": batch["status"],
            "resumed_from_batch": batch["resumed_from_batch"],
            "resumed_from_batch_no": batch["resumed_from_batch_no"],
            "delivered_count": batch["delivered_count"],
            "adopted_count": batch["adopted_count"],
            "failed_count": batch["failed_count"],
            # 每个模板一条交付明细：模板、口径、执行结论、失败原因、行明细
            "deliveries": batch["deliveries"],
            "query_results": query_results,
        }

    # ── 批次对账与恢复 ──

    def get_delivery_batch(self, batch_id: int) -> dict[str, Any] | None:
        """按批次身份读取交付批次完整详情。"""
        return store.get_delivery_batch(batch_id)

    def list_delivery_batches(self, api_id: int | None = None) -> list[dict[str, Any]]:
        """列出数据服务的交付批次（可按服务过滤），便于跨响应核对批次身份。"""
        return store.list_delivery_batches(api_id)

    def get_pending_deliveries(self, batch_id: int) -> dict[str, Any]:
        """从批次恢复待交付项：返回所有未交付模板及其失败原因。

        调用方据此判断哪些结果当前不可采纳，并在修复后用 resume_from_batch 续取。
        """
        batch = store.get_delivery_batch(batch_id)
        if batch is None:
            return {"error": f"交付批次 {batch_id} 不存在"}
        pending = [
            {
                "delivery_id": d.get("delivery_id"),
                "template_id": d.get("template_id"),
                "template": d.get("template"),
                "metric": d.get("metric"),
                "caliber": d.get("caliber"),
                "conclusion": d.get("conclusion"),
                "execution_id": d.get("execution_id"),
                "failure_reason": d.get("failure_reason"),
            }
            for d in batch.get("deliveries", [])
            if d.get("status") == "failed"
        ]
        return {
            "batch_id": batch["batch_id"],
            "batch_no": batch["batch_no"],
            "api_id": batch["api_id"],
            "api": batch["api"],
            "pending_count": len(pending),
            "pending_deliveries": pending,
        }

    def get_service_health(self, entry_id: int) -> dict[str, Any]:
        """获取服务接口健康状态。

        检查关联的指标口径版本和数据服务可用性。
        """
        api = store.find(self.MODULE, entry_id)
        if api is None:
            return {"error": f"服务接口 {entry_id} 不存在"}

        targets = self._resolve_targets(api)

        # 检查关联指标的口径版本是否有效
        active_versions: list[str] = []
        published = 0
        for target in targets:
            calibers = store.find_by_field("caliber_version", "所属指标", target)
            active_caliber = next((c for c in calibers if c.get("status") == "已生效"), None)
            if active_caliber:
                active_versions.append(str(active_caliber.get("版本号")))
            published += sum(
                1 for t in store.find_by_field("query_builder", "关联指标", target)
                if t.get("status") == "已发布"
            )

        target_display = "、".join(targets)
        return {
            "api_id": entry_id,
            "api_name": api.get("接口名称"),
            "status": api.get("status"),
            "target_metric": target_display,
            "active_caliber": "、".join(active_versions) if active_versions else None,
            "published_query_templates": published,
            "health": "healthy" if active_versions and published else "degraded",
        }
