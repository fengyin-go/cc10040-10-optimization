"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
同时提供跨模块查询和数据流处理能力。
"""
from __future__ import annotations

import time
import threading
from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._id_counters: dict[str, int] = {}
        # Cache layer storage
        self._cache: dict[str, tuple[float, Any]] = {}
        # Cross-module journal
        self._query_log: list[dict[str, Any]] = []
        # Cohort snapshots
        self._cohort_snapshots: dict[str, list[dict[str, Any]]] = {}
        # Anomaly records
        self._anomaly_records: list[dict[str, Any]] = []
        # Recalc history
        self._recalc_history: list[dict[str, Any]] = []
        # 查询模板执行留痕（按模板 id 归集，空行成功与执行失败都可追溯）
        self._template_executions: dict[int, list[dict[str, Any]]] = {}
        # 数据服务交付批次（批次 id -> 批次详情）
        self._delivery_batches: dict[int, dict[str, Any]] = {}
        # 查询模板执行故障注入（模板 id -> 故障原因），模拟模板无法执行
        self._template_faults: dict[int, str] = {}
        # 自增序列
        self._execution_seq = 0
        self._delivery_seq = 0
        self._batch_seq_by_api: dict[int, int] = {}
        # Lock for thread safety
        self._lock = threading.Lock()

    @property
    def lock(self):
        return self._lock

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, module: str) -> int:
        rows = self.rows(module)
        if not rows:
            return 1
        return max(int(row.get("id", 0)) for row in rows) + 1

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    # ── Cross-module query helpers ──

    def find_by_field(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        """查找模块中 field 等于 value 的所有记录。"""
        return [row for row in self.rows(module) if row.get(field) == value]

    def find_by_status(self, module: str, status: str) -> list[dict[str, Any]]:
        return [row for row in self.rows(module) if row.get("status") == status]

    # ── Query journal ──

    def log_query(self, metric_id: int, operator: str, sql_summary: str,
                  row_count: int, duration_ms: float, status: str = "正常") -> dict[str, Any]:
        entry = {
            "id": len(self._query_log) + 1,
            "查询时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "操作人": operator,
            "涉及指标": f"metric_{metric_id}",
            "查询语句摘要": sql_summary,
            "返回行数": str(row_count),
            "status": status,
            "duration_ms": duration_ms,
            "pending": status != "正常",
            "abnormal": status in ("慢查询", "异常终止"),
        }
        self._query_log.append(entry)
        return entry

    def query_logs(self) -> list[dict[str, Any]]:
        return self._query_log

    # ── Cache operations ──

    def cache_get(self, key: str) -> Any | None:
        """获取缓存，过期返回 None。"""
        entry = self._cache.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.time() > expires_at:
            del self._cache[key]
            return None
        return value

    def cache_set(self, key: str, value: Any, ttl: int) -> None:
        """设置缓存，ttl 单位为秒。"""
        self._cache[key] = (time.time() + ttl, value)

    def cache_invalidate(self, key: str) -> bool:
        return self._cache.pop(key, None) is not None

    def cache_invalidate_by_prefix(self, prefix: str) -> int:
        count = 0
        for key in list(self._cache):
            if key.startswith(prefix):
                del self._cache[key]
                count += 1
        return count

    # ── Cohort operations ──

    def save_cohort_snapshot(self, cohort_id: int, members: list[dict[str, Any]]) -> None:
        self._cohort_snapshots[str(cohort_id)] = members

    def get_cohort_snapshot(self, cohort_id: int) -> list[dict[str, Any]] | None:
        return self._cohort_snapshots.get(str(cohort_id))

    # ── Anomaly detection operations ──

    def record_anomaly(self, metric_id: int, severity: str, description: str,
                       value: float, threshold: float) -> dict[str, Any]:
        record = {
            "id": len(self._anomaly_records) + 1,
            "指标ID": metric_id,
            "严重程度": severity,
            "描述": description,
            "当前值": value,
            "阈值": threshold,
            "时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._anomaly_records.append(record)
        return record

    def anomaly_records(self) -> list[dict[str, Any]]:
        return self._anomaly_records

    # ── Recalc operations ──

    def record_recalc(self, metric_id: int, old_version: str, new_version: str,
                      time_range: str, diff_summary: str) -> dict[str, Any]:
        record = {
            "id": len(self._recalc_history) + 1,
            "关联指标": f"metric_{metric_id}",
            "旧口径": old_version,
            "新口径": new_version,
            "历史时间范围": time_range,
            "产出差异": diff_summary,
            "重算时间": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self._recalc_history.append(record)
        return record

    def recalc_history(self) -> list[dict[str, Any]]:
        return self._recalc_history

    # ── 查询模板执行留痕 ──

    def next_execution_id(self) -> str:
        """生成全局唯一的模板执行结论编号（结果身份的一部分）。"""
        self._execution_seq += 1
        return f"EXE-{self._execution_seq:06d}"

    def save_template_execution(self, record: dict[str, Any]) -> dict[str, Any]:
        """保存一次模板执行结论。成功（含空行集）与失败都必须留痕。"""
        template_id = int(record.get("template_id", 0))
        self._template_executions.setdefault(template_id, []).append(record)
        return record

    def get_template_execution(self, execution_id: str) -> dict[str, Any] | None:
        for records in self._template_executions.values():
            for record in records:
                if record.get("execution_id") == execution_id:
                    return record
        return None

    def get_template_executions(self, template_id: int) -> list[dict[str, Any]]:
        """读取某模板的全部历史执行结论，按发生时间排列。"""
        return list(self._template_executions.get(int(template_id), []))

    def clear_template_executions(self, template_id: int | None = None) -> None:
        """清除模板执行留痕；不传 id 时清除全部（主要用于测试隔离与重算复位）。"""
        if template_id is None:
            self._template_executions.clear()
        else:
            self._template_executions.pop(int(template_id), None)

    # ── 数据服务交付批次 ──

    def next_batch_no(self, api_id: int) -> int:
        """同一数据服务内批次序号递增，用于跨响应核对批次身份。"""
        seq = self._batch_seq_by_api.get(api_id, 0) + 1
        self._batch_seq_by_api[api_id] = seq
        return seq

    def next_delivery_id(self) -> str:
        self._delivery_seq += 1
        return f"DLV-{self._delivery_seq:06d}"

    def save_delivery_batch(self, batch: dict[str, Any]) -> dict[str, Any]:
        self._delivery_batches[int(batch["batch_id"])] = batch
        return batch

    def get_delivery_batch(self, batch_id: int) -> dict[str, Any] | None:
        return self._delivery_batches.get(int(batch_id))

    def list_delivery_batches(self, api_id: int | None = None) -> list[dict[str, Any]]:
        batches = list(self._delivery_batches.values())
        if api_id is not None:
            batches = [b for b in batches if int(b.get("api_id", 0)) == int(api_id)]
        return sorted(batches, key=lambda b: int(b.get("batch_id", 0)))

    def clear_delivery_batches(self, api_id: int | None = None) -> None:
        """清除交付批次；不传 api_id 时清除全部（主要用于测试隔离）。"""
        if api_id is None:
            self._delivery_batches.clear()
            self._batch_seq_by_api.clear()
        else:
            for batch_id in list(self._delivery_batches):
                if int(self._delivery_batches[batch_id].get("api_id", 0)) == int(api_id):
                    self._delivery_batches.pop(batch_id, None)
            self._batch_seq_by_api.pop(int(api_id), None)

    # ── 查询模板故障注入（用于模拟模板无法执行与修复后重试） ──

    def set_template_fault(self, template_id: int, reason: str) -> None:
        self._template_faults[int(template_id)] = reason

    def clear_template_fault(self, template_id: int) -> bool:
        return self._template_faults.pop(int(template_id), None) is not None

    def get_template_fault(self, template_id: int) -> str | None:
        return self._template_faults.get(int(template_id))

    def template_faults(self) -> dict[int, str]:
        return dict(self._template_faults)

    # ── Data artifacts for cross-module consumption ──

    def get_metric_data(self, metric_id: int) -> dict[str, Any] | None:
        """Get metric data with its calibers, queries, and services."""
        metric = self.find("metric_definition", metric_id)
        if metric is None:
            return None
        return {
            "metric": metric,
            "calibers": self.find_by_field("caliber_version", "所属指标",
                                           metric.get("指标名称", "")),
            "queries": self.find_by_field("query_builder", "关联指标",
                                          metric.get("指标名称", "")),
            "services": [row for row in self.rows("data_service")
                         if metric.get("指标名称", "") in str(row.get("关联指标或模型", ""))],
            "subscriptions": self.find_by_field("data_subscription", "关联指标",
                                                 metric.get("指标名称", "")),
        }

    def get_dimension_model_data(self, model_id: int) -> dict[str, Any] | None:
        """Get dimension model with its drill-down paths and pivot configs."""
        model = self.find("dimension_model", model_id)
        if model is None:
            return None
        return {
            "model": model,
            "drill_paths": self.find_by_field("drill_down", "起始指标",
                                              model.get("模型名称", "")),
            "pivot_configs": self.find_by_field("pivot_table", "数据源指标",
                                                 model.get("模型名称", "")),
        }

    def get_cohort_analysis(self, cohort_id: int) -> dict[str, Any] | None:
        """Get cohort with experiments and segment comparisons."""
        cohort = self.find("user_cohort", cohort_id)
        if cohort is None:
            return None
        cohort_name = cohort.get("分群名称", "")
        return {
            "cohort": cohort,
            "experiments": [row for row in self.rows("experiment_group")
                            if cohort_name in str(row.get("筛选条件", ""))],
            "comparisons": [row for row in self.rows("segment_compare")
                            if cohort_name in (str(row.get("分群A", "")) + str(row.get("分群B", "")))],
            "anomaly_rules": [row for row in self.rows("anomaly_detector")
                              if cohort_name in str(row.get("目标指标", ""))],
        }

    def get_field_lineage(self, field_name: str) -> dict[str, Any]:
        """Trace a field's complete lineage chain."""
        entries = self.find_by_field("field_lineage", "目标字段", field_name)
        if not entries:
            return {"field": field_name, "lineage": [], "depth": 0}

        lineage = [{
            "source_fields": entry.get("来源字段列表", ""),
            "transform": entry.get("转换逻辑", ""),
            "intermediate_table": entry.get("中间表", ""),
            "depth": entry.get("血缘深度", ""),
            "status": entry.get("status", ""),
        } for entry in entries]

        return {
            "field": field_name,
            "lineage": lineage,
            "depth": len(lineage),
        }


store = Store()