"""查询构建器领域服务：构建和缓存多维组合查询模板以加速分析。

消费口径版本的指标定义，生成查询模板供数据服务调用。
跨模块数据流: caliber_version -> query_builder -> data_service -> cache_layer -> query_journal

每次模板执行都通过 run_template 落一条「执行结论」（execution 留痕）：
- 成功（含空行集）带 execution_id、所用口径版本与行明细，空行集同样可追溯到一次成功执行；
- 失败带 failure_reason 与执行结论 FAILED，供数据服务组装续取批次时对账；
- 故障注入 set_fault/clear_fault 用于模拟模板无法执行以及修复后重试。
"""
from __future__ import annotations

import time
from typing import Any

from app.store import store
from app.services.base import BaseModuleService


class QueryBuilderService(BaseModuleService):
    MODULE = "query_builder"
    ENTITY = "查询模板"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["模板名称", "关联指标", "维度组合", "过滤条件", "缓存策略"]
    REQUIRED_FIELDS = ["模板名称", "关联指标", "维度组合"]
    STATUS_ORDER = ["草稿", "已发布", "已过期"]
    ACTION_RULES = {"发布模板": "已发布", "过期模板": "已过期"}
    NEGATIVE_ACTIONS = []

    # ── 故障注入：模拟模板执行中断 / 修复后恢复 ──

    def set_fault(self, entry_id: int, reason: str) -> dict[str, Any]:
        """对模板注入执行故障，后续执行将以该原因失败。"""
        template = store.find(self.MODULE, entry_id)
        if template is None:
            return {"error": f"查询模板 {entry_id} 不存在"}
        store.set_template_fault(entry_id, reason)
        return {"template_id": entry_id, "模板名称": template.get("模板名称"),
                "fault": reason, "ok": True}

    def clear_fault(self, entry_id: int) -> dict[str, Any]:
        """修复模板：解除故障注入，模板可重新执行。"""
        removed = store.clear_template_fault(entry_id)
        return {"template_id": entry_id, "ok": removed,
                "message": "故障已解除，模板可重新执行" if removed else "模板无故障记录"}

    # ── 标准化模板执行：所有结论留痕，供数据服务组装交付批次 ──

    def run_template(self, entry_id: int, operator: str = "系统",
                     use_cache: bool = True) -> dict[str, Any]:
        """执行单个查询模板并返回标准化执行结论。

        无论成功还是失败都会写入 store 的模板执行留痕：
        - conclusion=SUCCESS：含 execution_id、caliber（取数所用口径版本）、rows；
          rows 为空也是一次成功执行，空行集据此可追溯。
        - conclusion=FAILED：含 failure_reason，调用方不得把失败项当作漏项静默丢弃。
        """
        template = store.find(self.MODULE, entry_id)
        if template is None:
            return self._record_failure(
                entry_id, template_name=None, metric_name="", caliber=None,
                operator=operator, reason=f"查询模板 {entry_id} 不存在",
                source="none")
        template_name = template.get("模板名称", "")
        metric_name = template.get("关联指标", "")

        if template["status"] != "已发布":
            return self._record_failure(
                entry_id, template_name, metric_name, None, operator,
                reason=f"查询模板 {entry_id} 未发布（当前状态：{template.get('status')}）",
                source="none")
        if not metric_name:
            return self._record_failure(
                entry_id, template_name, metric_name, None, operator,
                reason="查询模板未关联指标", source="none")

        # 检查指标口径版本（取数所用口径）
        calibers = store.find_by_field("caliber_version", "所属指标", metric_name)
        active_caliber = next((c for c in calibers if c.get("status") == "已生效"), None)
        if active_caliber is None:
            return self._record_failure(
                entry_id, template_name, metric_name, None, operator,
                reason=f"指标 {metric_name} 没有已生效的口径版本，无法取数",
                source="none")
        caliber_version = active_caliber.get("版本号", "")

        # 故障注入：模拟模板执行中断（基础设施故障 / 引擎异常等）
        fault = store.get_template_fault(entry_id)
        if fault is not None:
            self._log_journal(template, operator, row_count=0,
                              duration_ms=0.0, status="异常终止")
            return self._record_failure(
                entry_id, template_name, metric_name, caliber_version, operator,
                reason=f"模板执行中断：{fault}", source="none")

        cache_key = f"query:{metric_name}:{template.get('维度组合','')}:{template.get('过滤条件','')}"

        # 尝试读取缓存
        if use_cache:
            cached = store.cache_get(cache_key)
            if cached is not None:
                duration_ms = 0.0
                self._log_journal(template, operator,
                                  row_count=len(cached) if isinstance(cached, list) else 1,
                                  duration_ms=duration_ms, status="正常",
                                  sql_summary=f"缓存命中: {template_name}")
                return self._record_success(
                    entry_id, template_name, metric_name, caliber_version,
                    active_caliber, operator, source="cache", rows=cached,
                    duration_ms=duration_ms, cached_for_seconds=None)

        # 模拟查询执行 - 基于指标数据生成结果
        t_start = time.time()
        metric_data = store.find_by_field("metric_definition", "指标名称", metric_name)
        dimensions = template.get("维度组合", "").split(",") if template.get("维度组合") else []

        result_rows: list[dict[str, Any]] = []
        for idx, m in enumerate(metric_data):
            row = {"序号": idx + 1, "指标": metric_name,
                   "口径版本": caliber_version,
                   "计算口径": m.get("计算口径", ""),
                   "统计粒度": m.get("统计粒度", "")}
            for dim in dimensions:
                row[dim.strip()] = f"维度值_{idx + 1}"
            result_rows.append(row)

        duration_ms = (time.time() - t_start) * 1000
        journal_status = "慢查询" if duration_ms > 100 else "正常"
        self._log_journal(template, operator, row_count=len(result_rows),
                          duration_ms=duration_ms, status=journal_status)

        # 写入缓存
        cache_ttl = 300  # 默认 5 分钟
        cache_policy = template.get("缓存策略", "")
        if "长期" in str(cache_policy):
            cache_ttl = 3600
        elif "短期" in str(cache_policy):
            cache_ttl = 60
        store.cache_set(cache_key, result_rows, cache_ttl)

        # 空行集同样是成功执行：row_count=0 但 conclusion=SUCCESS，可追溯到本次 execution
        return self._record_success(
            entry_id, template_name, metric_name, caliber_version,
            active_caliber, operator, source="computed", rows=result_rows,
            duration_ms=duration_ms, cached_for_seconds=cache_ttl)

    # ── 执行留痕组装 ──

    def _record_success(self, entry_id: int, template_name: str, metric_name: str,
                        caliber_version: str, active_caliber: dict[str, Any],
                        operator: str, *, source: str, rows: list[dict[str, Any]],
                        duration_ms: float,
                        cached_for_seconds: int | None) -> dict[str, Any]:
        execution_id = store.next_execution_id()
        record = {
            "execution_id": execution_id,
            "template_id": entry_id,
            "模板名称": template_name,
            "关联指标": metric_name,
            "caliber": caliber_version,
            "caliber_id": active_caliber.get("id"),
            "口径描述": active_caliber.get("口径描述", ""),
            "source": source,
            "conclusion": "SUCCESS",
            "failure_reason": None,
            "row_count": len(rows),
            "empty": len(rows) == 0,
            "duration_ms": round(duration_ms, 3),
            "cached_for_seconds": cached_for_seconds,
            "operator": operator,
            "executed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "rows": rows,
        }
        store.save_template_execution(record)
        return dict(record)

    def _record_failure(self, entry_id: int, template_name: str | None,
                        metric_name: str, caliber_version: str | None,
                        operator: str, *, reason: str, source: str) -> dict[str, Any]:
        execution_id = store.next_execution_id()
        record = {
            "execution_id": execution_id,
            "template_id": entry_id,
            "模板名称": template_name,
            "关联指标": metric_name,
            "caliber": caliber_version,
            "caliber_id": None,
            "口径描述": None,
            "source": source,
            "conclusion": "FAILED",
            "failure_reason": reason,
            "row_count": 0,
            "empty": False,
            "duration_ms": 0.0,
            "cached_for_seconds": None,
            "operator": operator,
            "executed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "rows": [],
        }
        store.save_template_execution(record)
        return dict(record)

    def _log_journal(self, template: dict[str, Any], operator: str, *,
                     row_count: int, duration_ms: float, status: str,
                     sql_summary: str | None = None) -> None:
        if sql_summary is None:
            sql_summary = (
                f"查询模板: {template.get('模板名称')}, "
                f"维度: {template.get('维度组合')}"
            )
        store.log_query(
            metric_id=template.get("id", 0),
            operator=operator,
            sql_summary=sql_summary,
            row_count=row_count,
            duration_ms=duration_ms,
            status=status,
        )

    # ── 执行结论查询（空行集/失败原因追溯） ──

    def get_execution(self, execution_id: str) -> dict[str, Any] | None:
        """按执行编号读取一次模板执行结论。"""
        return store.get_template_execution(execution_id)

    def get_execution_history(self, entry_id: int) -> dict[str, Any]:
        """读取模板的全部执行结论，供对账与恢复后核对新数据。"""
        template = store.find(self.MODULE, entry_id)
        if template is None:
            return {"error": f"查询模板 {entry_id} 不存在"}
        records = store.get_template_executions(entry_id)
        return {
            "template_id": entry_id,
            "模板名称": template.get("模板名称"),
            "total": len(records),
            "success": sum(1 for r in records if r.get("conclusion") == "SUCCESS"),
            "failed": sum(1 for r in records if r.get("conclusion") == "FAILED"),
            "executions": records,
        }

    # ── 向后兼容：旧调用方仍可使用 execute_query ──

    def execute_query(self, entry_id: int, operator: str = "系统",
                      use_cache: bool = True) -> dict[str, Any] | None:
        """执行查询模板并返回结果与日志（兼容旧契约）。

        内部统一走 run_template 留痕：
        - 成功返回含 rows 的结果（字段保持旧契约，并附带 execution_id/conclusion）；
        - 失败返回 {"error": 失败原因, ...}，同样带 execution_id 与结论，
          数据服务可凭 failure_reason 组装未交付项，而不是静默漏项。
        """
        record = self.run_template(entry_id, operator=operator, use_cache=use_cache)
        if record.get("conclusion") == "FAILED":
            return {
                "error": record.get("failure_reason"),
                "execution_id": record.get("execution_id"),
                "conclusion": "FAILED",
                "template": record.get("模板名称"),
                "caliber": record.get("caliber"),
                "rows": [],
            }
        result = {
            "source": record.get("source"),
            "template": record.get("模板名称"),
            "caliber": record.get("caliber"),
            "duration_ms": record.get("duration_ms"),
            "execution_id": record.get("execution_id"),
            "conclusion": "SUCCESS",
            "empty": record.get("empty"),
            "rows": record.get("rows", []),
        }
        if record.get("cached_for_seconds") is not None:
            result["cached_for_seconds"] = record.get("cached_for_seconds")
        return result

    def invalidate_query_cache(self, metric_name: str) -> int:
        """当口径版本变更时，失效指标相关的所有查询缓存。

        跨模块数据流: caliber_version -> query_builder -> cache_layer
        """
        return store.cache_invalidate_by_prefix(f"query:{metric_name}:")
