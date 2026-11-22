#!/usr/bin/env python
"""端到端核验：甲正常出数、乙无法执行 -> 修复乙并换新口径续取 -> 对账。

仅依赖标准库，用 -I 隔离运行；通过 HTTP 打真实 FastAPI 服务。
"""
import json
import sys
import urllib.request
import urllib.error
import urllib.parse

BASE = "http://127.0.0.1:8015/api"
PASS, FAIL = 0, 0


def q(value) -> str:
    return urllib.parse.quote(str(value), safe="")


def call(method: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read().decode())


def check(name: str, cond: bool, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def post_row(module: str, values: dict, publish_action: str | None = None) -> dict:
    """创建资源，可选地通过动作接口发布（返回的 entry 是拷贝，不能就地改状态）。"""
    res = call("POST", f"/{module}", {"values": values})
    if not res.get("ok"):
        raise RuntimeError(f"创建 {module} 失败: {res}")
    row = res["entry"]
    if publish_action:
        act = call("POST", f"/{module}/{row['id']}/actions",
                   {"action": publish_action})
        if not act.get("ok"):
            raise RuntimeError(f"发布 {module} 失败: {act}")
        row = act["entry"]
    return row


def main() -> int:
    # ── 搭数据：指标、v1 口径、甲/乙两个已发布模板、已发布服务 ──
    metric = "E2E对账指标"
    post_row("metric_definition", {
        "指标名称": metric, "计算口径": "按日求和", "统计粒度": "天",
        "数据源": "dwd", "业务域": "E2E域"}, publish_action="发布指标")

    call("POST", "/caliber_version", {"values": {
        "所属指标": metric, "版本号": "e2e-v1.0", "口径描述": "初版口径",
        "变更类型": "初始创建", "生效时间": "2026-10-01"}})
    call("POST", "/caliber_version", {"values": {
        "所属指标": metric, "版本号": "e2e-v2.0", "口径描述": "新版口径：自然周去重",
        "变更类型": "口径调整", "生效时间": "2026-11-22"}})
    caliber_rows = call("GET", "/caliber_version?size=200")["items"]
    v1 = next(r for r in caliber_rows if r["版本号"] == "e2e-v1.0"
              and r["所属指标"] == metric)
    v2 = next(r for r in caliber_rows if r["版本号"] == "e2e-v2.0"
              and r["所属指标"] == metric)

    post_row("query_builder", {
        "模板名称": "E2E模板甲", "关联指标": metric, "维度组合": "渠道,大区",
        "过滤条件": "", "缓存策略": "短期缓存"}, publish_action="发布模板")
    post_row("query_builder", {
        "模板名称": "E2E模板乙", "关联指标": metric, "维度组合": "渠道",
        "过滤条件": "", "缓存策略": ""}, publish_action="发布模板")
    qrows = call("GET", "/query_builder?size=200")["items"]
    a_id = next(r["id"] for r in qrows if r["模板名称"] == "E2E模板甲")
    b_id = next(r["id"] for r in qrows if r["模板名称"] == "E2E模板乙")

    post_row("data_service", {
        "接口名称": "E2E数据服务", "关联指标或模型": metric,
        "请求方式": "POST", "限流配额": "1000/分钟", "发布状态": "已发布"},
        publish_action="发布接口")
    srows = call("GET", "/data_service?size=200")["items"]
    api_id = next(r["id"] for r in srows if r["接口名称"] == "E2E数据服务")

    # 启用 v1 口径
    act = call("POST", f"/cross/caliber/{v1['id']}/activate")
    check("启用 v1 口径", act.get("ok"), act)

    # ── 让乙无法执行，首次调用服务 ──
    fault = call("POST", f"/cross/query/{b_id}/fault?reason={q('执行引擎连接超时')}")
    check("对乙注入故障", fault.get("ok"), fault)

    print("\n== 首次调用：甲正常、乙失败 ==")
    first = call("POST",
                 f"/cross/data-service/{api_id}/call?operator={q('E2E调用方')}")
    print(json.dumps({k: first[k] for k in (
        "batch_no", "batch_status", "attempt", "delivered_count",
        "failed_count", "adopted_count", "result_count")}, ensure_ascii=False))
    check("首响应无顶层 error", "error" not in first, first.get("error"))
    check("首响应批次状态=部分失败", first["batch_status"] == "部分失败")
    check("首响应含两个模板交付明细", len(first["deliveries"]) == 2)

    dmap = {int(d["template_id"]): d for d in first["deliveries"]}
    a1, b1 = dmap[a_id], dmap[b_id]
    check("甲已交付且结论 SUCCESS", a1["status"] == "delivered"
          and a1["conclusion"] == "SUCCESS")
    check("甲带 v1 口径与执行身份", a1["caliber"] == "e2e-v1.0"
          and str(a1["execution_id"]).startswith("EXE-"))
    check("甲有行明细且口径为 v1", a1["row_count"] >= 1
          and all(r["口径版本"] == "e2e-v1.0" for r in a1["rows"]))
    check("乙未交付且有失败原因", b1["status"] == "failed"
          and b1["conclusion"] == "FAILED"
          and "执行引擎连接超时" in b1["failure_reason"])
    check("乙未交付无行数据", b1["row_count"] == 0 and b1["rows"] == [])
    first_batch = first["batch_id"]

    # 待交付项恢复
    print("\n== 从批次恢复待交付项 ==")
    pending = call("GET", f"/cross/data-service/batch/{first_batch}/pending")
    print(json.dumps(pending, ensure_ascii=False))
    check("待交付项仅乙且带原因", pending["pending_count"] == 1
          and pending["pending_deliveries"][0]["template_id"] == b_id
          and "执行引擎连接超时" in
          pending["pending_deliveries"][0]["failure_reason"])

    # ── 修复乙 + 换新口径 v2，续取 ──
    cleared = call("POST", f"/cross/query/{b_id}/fault/clear")
    check("修复乙（解除故障）", cleared.get("ok"), cleared)
    act2 = call("POST", f"/cross/caliber/{v2['id']}/activate")
    check("启用 v2 新口径", act2.get("ok"), act2)

    print("\n== 续取调用 ==")
    second = call("POST",
                  f"/cross/data-service/{api_id}/call?operator={q('E2E调用方')}"
                  f"&resume_from_batch={first_batch}")
    print(json.dumps({k: second[k] for k in (
        "batch_no", "batch_status", "attempt", "resumed_from_batch_no",
        "delivered_count", "failed_count", "adopted_count")}, ensure_ascii=False))

    # 批次身份核对
    check("产生新批次号", second["batch_id"] != first_batch
          and second["batch_no"] != first["batch_no"])
    check("新批次串联父批次", second["resumed_from_batch"] == first_batch
          and second["resumed_from_batch_no"] == first["batch_no"])
    check("续取批次全部交付", second["batch_status"] == "全部交付"
          and second["failed_count"] == 0)

    dmap2 = {int(d["template_id"]): d for d in second["deliveries"]}
    a2, b2 = dmap2[a_id], dmap2[b_id]
    check("甲沿用签收结果身份(adopted)", a2["status"] == "adopted"
          and a2["execution_id"] == a1["execution_id"])
    check("甲行明细与首响应一致", a2["rows"] == a1["rows"]
          and a2["caliber"] == "e2e-v1.0")
    check("乙恢复后产生新执行身份", b2["status"] == "delivered"
          and b2["execution_id"] != b1["execution_id"]
          and str(b2["execution_id"]).startswith("EXE-"))
    check("乙按新版口径出数", b2["caliber"] == "e2e-v2.0"
          and all(r["口径版本"] == "e2e-v2.0" for r in b2["rows"])
          and b2["row_count"] >= 1)

    # 批次列表与执行留痕可回读
    batches = call("GET", f"/cross/data-service/batches?api_id={api_id}")
    ids = [b["batch_id"] for b in batches["items"]]
    check("两份批次均可回读", first_batch in ids and second["batch_id"] in ids)
    hist = call("GET", f"/cross/query/{b_id}/executions")
    check("乙执行历史含 1 失败 + 1 成功",
          hist["failed"] == 1 and hist["success"] == 1, hist)

    print(f"\n结果：{PASS} 通过 / {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
