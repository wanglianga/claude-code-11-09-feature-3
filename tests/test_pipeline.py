"""端到端断言：覆盖事件型仓库的全部关键需求。

前置条件：bash scripts/run_local.sh 已完成建仓与灌数（本文件只读断言，
除 test_12_schema_evolution 以 IF NOT EXISTS 加列外不修改业务数据）。
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from common.spark_utils import (
    BACKFILL_LOG, DIM_TABLES, DQ_TABLE, DWD_EVENT, ODS_EVENT,
    RELATION, RELATION_LOG, WM_LOG,
)

ROOT = Path(__file__).resolve().parent.parent


# ---------- 1. ODS 原始层：原样保存、不清洗不合并 ----------
def test_01_ods_keeps_duplicate_and_raw_payload(spark):
    # E401 被两个落地文件重复推送：ODS 必须保留两份（去重只允许发生在 DWD）
    ods_dup = (spark.table(ODS_EVENT)
               .where("event_id = 'E401'").count())
    assert ods_dup == 2, f"ODS 应原样保留重复上报，实际 {ods_dup} 行"

    raw = (spark.table(ODS_EVENT)
           .where("event_id = 'E502'").select("raw_payload").collect()[0][0])
    payload = json.loads(raw)
    assert payload["exception_reason"] == "外包装破损"
    # 原始报文全文保留（含业务字段），未做裁剪
    assert payload["hub_code"] == "HUB_SH" and payload["exception_code"] == "EX_DAMAGE"

    # 二级分区字段存在
    parts = spark.sql(
        f"SELECT DISTINCT biz_date, source_system FROM {ODS_EVENT}").count()
    assert parts >= 4


# ---------- 2. DWD 去重：同 event_id 只留一条 ----------
def test_02_dwd_dedup_by_event_id(spark):
    n = (spark.table(DWD_EVENT).where("event_id = 'E401'").count())
    assert n == 1, f"DWD 按 event_id 去重后应剩 1 行，实际 {n}"
    total_dup = (spark.table(ODS_EVENT).groupBy("event_id")
                 .count().where("count > 1").count())
    assert total_dup == 1  # 只有 E401 一组重复


# ---------- 3. 乱序：序号按业务发生时间，而非上报时间 ----------
def test_03_out_of_order_seq_uses_event_time(spark):
    rows = (spark.table(DWD_EVENT).where("waybill_no = 'WB003'")
            .orderBy("event_seq").collect())
    seq_map = {r.event_type: r.event_seq for r in rows}
    # ARRIVE 业务时间 15:00 早于 DEPART 18:00，尽管它更晚上报
    assert seq_map["ARRIVE"] < seq_map["DEPART"]
    assert [r.event_seq for r in rows] == list(range(1, len(rows) + 1))


# ---------- 4. 多次异常独立保留，兼容视图末次原因口径正确 ----------
def test_04_multiple_exceptions_kept_and_compat_view(spark):
    exc = (spark.table(DWD_EVENT)
           .where("waybill_no = 'WB005' AND event_type = 'EXCEPTION'")
           .orderBy("event_seq").collect())
    assert len(exc) == 2, "多次异常必须独立保留，不能只留最后一次"
    assert [r.exception_reason for r in exc] == ["外包装破损", "冷链温度超标"]

    wide = spark.sql(
        "SELECT last_exception_reason, exception_chain "
        "FROM ltl.app.v_waybill_wide_compat WHERE waybill_no = 'WB005'").collect()[0]
    assert wide["last_exception_reason"] == "冷链温度超标"
    assert len(wide["exception_chain"]) == 2


# ---------- 5. 缺失节点补齐后重放：迟到 ARRIVE 插入正确序号 ----------
def test_05_late_event_replay_resequences(spark):
    rows = (spark.table(DWD_EVENT).where("waybill_no = 'WB006'")
            .orderBy("event_seq").collect())
    type_seq = {r.event_type: r.event_seq for r in rows}
    assert type_seq["ARRIVE"] == 3, "迟到补报的 ARRIVE 应重排为第3个事件"
    assert [r.event_seq for r in rows] == list(range(1, len(rows) + 1))
    # 迟到事件业务日期仍为 11-01（event_date 取自 event_time，不是上报日）
    arrive = next(r for r in rows if r.event_type == "ARRIVE")
    assert str(arrive.event_date) == "2026-11-01"


# ---------- 6. 拉链维表：时点关联，禁止最新维表回刷历史 ----------
def test_06_scd2_point_in_time_join(spark):
    enriched = spark.sql("""
        SELECT event_date, hub_name_at_event, service_area_at_event,
               customer_level_at_event
        FROM ltl.app.v_waybill_event_enriched
        WHERE waybill_no = 'WB009' ORDER BY event_seq
    """).collect()
    old = next(r for r in enriched if str(r.event_date) == "2026-11-01")
    new = next(r for r in enriched if str(r.event_date) == "2026-11-03")
    # 11-01 的事实必须关联到当时版本，而不是 11-02 改名后的最新名称
    assert old["hub_name_at_event"] == "北京一集配站"
    assert old["service_area_at_event"] == "浦东A片区"
    assert old["customer_level_at_event"] == "KA"
    assert new["hub_name_at_event"] == "北京华北转运中心"
    assert new["service_area_at_event"] == "浦东B片区"
    assert new["customer_level_at_event"] == "STD"

    # 维表本身保留两个版本，旧版本已按快照日闭合
    versions = (spark.table(DIM_TABLES["hub"])
                .where("hub_code = 'HUB_BJ'").orderBy("eff_date").collect())
    assert len(versions) == 2
    assert str(versions[0].exp_date) == "2026-11-02"
    assert versions[0].is_current is False
    assert versions[1].is_current is True

    # 合并撤销不物理删除：HUB_OLD 保留 MERGED 历史版本
    merged = (spark.table(DIM_TABLES["hub"])
              .where("hub_code = 'HUB_OLD' AND status = 'MERGED'").count())
    assert merged >= 1


# ---------- 7. 分拨压货定位：停留 >48 小时可回答客服 ----------
def test_07_hub_dwell_over_two_days(spark):
    row = spark.sql("""
        SELECT dwell_minutes, dwell_over_2d_flag, hub_name_at_event
        FROM ltl.app.v_hub_dwell
        WHERE waybill_no = 'WB002' AND hub_code = 'HUB_BJ'
    """).collect()[0]
    assert row["dwell_over_2d_flag"] is True
    assert row["dwell_minutes"] >= 48 * 60
    assert row["hub_name_at_event"] == "北京一集配站"  # 11-01 时点名称


# ---------- 8. 维表未匹配：代理键 -1 可被 DQ 捕获 ----------
def test_08_unmatched_dim_sk_minus_one(spark):
    rows = (spark.table(DWD_EVENT)
            .where("waybill_no = 'WB007'").select("hub_sk").distinct().collect())
    assert {r.hub_sk for r in rows} == {-1}


# ---------- 9. DQ 日检：五类指标齐全且可下钻到来源/分拨 ----------
def test_09_dq_metrics_and_drilldown(spark):
    codes = {r.metric_code for r in spark.table(DQ_TABLE).collect()}
    for base in ["EVENT_MISSING", "OUT_OF_ORDER", "DUPLICATE",
                 "DIM_UNMATCHED_HUB", "PARTITION_LATENCY"]:
        assert base in codes, f"缺少质量指标 {base}"
    # 汇总行 + 来源下钻 + 分拨下钻三级
    drill = spark.sql(f"""
        SELECT DISTINCT source_system, hub_code FROM {DQ_TABLE}
    """).collect()
    assert any(r.source_system == "ALL" and r.hub_code == "ALL" for r in drill)
    assert any(r.source_system != "ALL" and r.hub_code != "ALL" for r in drill)
    # 异常行带阈值与 breach 标志，可直接定位责任来源
    breaches = spark.sql(
        f"SELECT * FROM {DQ_TABLE} WHERE is_breach = true").collect()
    assert len(breaches) > 0
    assert all(r.source_system is not None for r in breaches)


# ---------- 10. 补数：只重跑受影响运单，影子表对比 + 台账 + 受控切换 ----------
def test_10_backfill_audit_trail(spark):
    logs = spark.sql(f"""
        SELECT * FROM {BACKFILL_LOG}
        WHERE scope_type = 'WAYBILL' AND status = 'SWITCHED'
    """).collect()
    assert len(logs) == 1
    log = logs[0]
    assert "WB006" in log.scope_value
    affected = json.loads(log.affected_partitions)
    # 只重放受影响分区，且包含迟到事件所属的 11-01 HUB 分区
    assert any(p.startswith("2026-11-01|") and p.endswith("HUB") for p in affected)
    diff = json.loads(log.diff_summary)
    assert diff["added_rows"] == 1  # 新增迟到 ARRIVE
    assert diff["seq_reordered_rows"] >= 1
    # 对比结果文件落盘
    files = list((ROOT / "data/backfill").glob("*_diff.json"))
    assert files, "必须产出补数前后对比文件"
    payload = json.loads(files[0].read_text(encoding="utf-8"))
    assert payload["rows_before"] < payload["rows_after"]


# ---------- 11. 水位线按日推进，失败不写入（本夹具全部成功）----------
def test_11_watermark_advanced(spark):
    rows = spark.sql(
        f"SELECT DISTINCT layer, biz_date FROM {WM_LOG} "
        "WHERE status = 'SUCCESS' ORDER BY biz_date").collect()
    layers = {r.layer for r in rows}
    assert {"ODS", "DWD"}.issubset(layers)
    assert len({r.biz_date for r in rows}) >= 4


# ---------- 12. Schema 演进：只加列 + 回填默认值，既有视图与报表不受影响 ----------
def test_12_schema_evolution_add_only(spark):
    from common.spark_utils import add_column_if_absent
    # 只加列（Spark 3.5 不支持 ADD COLUMN IF NOT EXISTS，用存在性检查保证幂等）
    add_column_if_absent(
        spark, "ltl.dwd.waybill_event_detail",
        "evol_test_col", "STRING", "演进演练列")
    # 历史分区回填默认值（schema_evolution_job 的标准动作，幂等）
    spark.sql("""
        UPDATE ltl.dwd.waybill_event_detail
        SET evol_test_col = 'NA' WHERE evol_test_col IS NULL
    """)
    # 既有下游视图无需改动仍可查询（视图是明细层之上的稳定投影）
    cnt = spark.sql(
        "SELECT count(*) FROM ltl.app.v_waybill_event_enriched").collect()[0][0]
    assert cnt > 0
    default_val = spark.sql("""
        SELECT DISTINCT evol_test_col FROM ltl.dwd.waybill_event_detail
    """).collect()
    assert {r.evol_test_col for r in default_val} == {"NA"}
    # 演进作业幂等：重复加列不报错
    assert add_column_if_absent(
        spark, "ltl.dwd.waybill_event_detail",
        "evol_test_col", "STRING") is False
    # 新作业节点枚举值无需 DDL：EXCEPTION 等枚举可直接落地（模型可扩展）
    assert spark.table(DWD_EVENT).where("event_type = 'EXCEPTION'").count() >= 2


# ---------- 13. 旧宽表兼容视图：一行一运单，关键时间列可透视 ----------
def test_13_wide_compat_view_rows(spark):
    row = spark.sql("""
        SELECT pickup_time, sign_time, appointment_window, is_cold_chain
        FROM ltl.app.v_waybill_wide_compat WHERE waybill_no = 'WB001'
    """).collect()[0]
    assert row["pickup_time"] is not None and row["sign_time"] is not None
    assert row["appointment_window"] == "2026-11-02 09:00-11:00"
    assert row["is_cold_chain"] == "true"


# ---------- 14. 关系台账只增不改，关系修正以新版本表达 ----------
def test_14_relation_append_only_and_versioning(spark):
    # 事件台账 APPEND-ONLY：11-02 十条 + 11-04 一条修正，共 11 条全部保留
    assert spark.table(RELATION_LOG).count() == 11
    rel013_log = (spark.table(RELATION_LOG)
                  .where("relation_id = 'REL013'").orderBy("event_time").collect())
    assert [r.change_type for r in rel013_log] == ["CREATE", "CORRECT"]

    # 拉链表：REL013 有两个版本，旧版本已闭合、新版本当前，历史版本物理保留
    versions = (spark.table(RELATION)
                .where("relation_id = 'REL013'").orderBy("change_seq").collect())
    assert len(versions) == 2
    v1, v2 = versions
    assert v1.change_seq == 1 and v1.child_waybill_no == "WB013X"
    assert v1.is_current is False and v2.is_current is True
    assert v2.change_seq == 2 and v2.child_waybill_no == "WB013Y"
    # 旧版本失效时间 = 修正事件生效时间，区间首尾相接、无空洞无重叠
    assert v1.exp_time == v2.eff_time


# ---------- 15. 历史查询按查询时点可用的关系快照展开（可复现、不被后续修正改变）----------
def test_15_relation_snapshot_as_of(spark):
    def members(as_of: str):
        return {r.waybill_no for r in spark.sql(f"""
            SELECT DISTINCT waybill_no FROM ltl.app.v_waybill_group_member
            WHERE group_no = 'G013'
              AND TIMESTAMP'{as_of}' >= eff_time
              AND TIMESTAMP'{as_of}' <  exp_time
        """).collect()}

    # 11-03 查询：修正尚未发生，只能看到误挂的 WB013X
    assert members("2026-11-03 00:00:00") == {"WB013", "WB013X"}
    # 11-05 查询：修正已生效，看到 WB013Y；同一历史时点的查询结果不被改写
    assert members("2026-11-05 00:00:00") == {"WB013", "WB013Y"}
    # 再次以 11-03 查询，结果仍与首次一致（历史快照可复现）
    assert members("2026-11-03 00:00:00") == {"WB013", "WB013X"}


# ---------- 16. 一票货完整轨迹沿关系展开，跨运单去重不重复计数 ----------
def test_16_full_track_expand_without_double_count(spark):
    # G010 合票：主单 5 条 + 两个成员各 2 条 = 9 个不同事件
    g010 = spark.sql("""
        SELECT * FROM ltl.app.v_waybill_full_track WHERE group_no = 'G010'
    """)
    assert g010.count() == 9
    assert g010.select("event_id").distinct().count() == 9
    # SIBLING 兄弟边不会让成员运单事件翻倍：去重位次恒为 1
    assert {r.group_event_dedup_rn for r in g010.collect()} == {1}
    owners = {r.owner_waybill for r in g010.select("owner_waybill").distinct().collect()}
    assert owners == {"WB010", "WB010A", "WB010B"}

    # G012 退件重派：原单拒收 + 重派新单签收，一票货轨迹跨两条运单完整展开
    g012 = spark.sql("""
        SELECT DISTINCT event_type FROM ltl.app.v_waybill_full_track
        WHERE group_no = 'G012'
    """).collect()
    types = {r.event_type for r in g012}
    assert {"REJECT", "SIGN"}.issubset(types)
    n012 = spark.sql(
        "SELECT count(*) c FROM ltl.app.v_waybill_full_track "
        "WHERE group_no = 'G012'").collect()[0]["c"]
    assert n012 == 7  # 原单 5 条 + 重派单 2 条


# ---------- 17. 合票时效只按主运单计算，成员运单不单独计时 ----------
def test_17_merge_timeliness_uses_master_only(spark):
    row = spark.sql("""
        SELECT * FROM ltl.app.v_waybill_merge_timeliness WHERE group_no = 'G010'
    """).collect()[0]
    assert row["master_waybill"] == "WB010"
    # 揽收 11-01 08:00 -> 签收 11-02 16:00 = 32 小时 = 1920 分钟
    assert row["total_aging_minutes"] == 1920
    # 干线 10:00 发 -> 20:00 到 = 600 分钟，全部取自主运单事件
    assert row["line_haul_minutes"] == 600


# ---------- 18. 分票干线节点按规则分摊到子运单（权重/均摊，比例之和=1）----------
def test_18_split_linehaul_allocation(spark):
    # WEIGHT 30/20/50：归一化 0.3/0.2/0.5
    ratios = {r.child_waybill_no: r.alloc_ratio for r in spark.sql("""
        SELECT child_waybill_no, alloc_ratio FROM ltl.dwd.waybill_relation
        WHERE group_no = 'G011' AND is_current = true
    """).collect()}
    assert ratios == {"WB011A": 0.3, "WB011B": 0.2, "WB011C": 0.5}

    # 主运单 DEPART(09:00)->ARRIVE(21:00) 段长 720 分钟，按比例分摊
    alloc = spark.sql("""
        SELECT child_waybill_no, alloc_rule, linehaul_node, parent_segment_minutes,
               allocated_segment_minutes
        FROM ltl.app.v_waybill_split_linehaul_alloc
        WHERE group_no = 'G011' AND linehaul_node = 'DEPART'
    """).collect()
    got = {r.child_waybill_no: round(r.allocated_segment_minutes, 2) for r in alloc}
    assert got == {"WB011A": 216.0, "WB011B": 144.0, "WB011C": 360.0}
    # 三个子单分摊量之和 = 主运单原始干线段时长（不丢不重）
    assert round(sum(r.allocated_segment_minutes for r in alloc), 2) == 720.0
    # 分摊规则逐行可见
    assert {r.alloc_rule for r in alloc} == {"WEIGHT"}

    # EQUAL 均摊：两个子单各 1/2；主运单 DEPART->ARRIVE 600 分钟，各分 300
    eq = spark.sql("""
        SELECT child_waybill_no, alloc_ratio, allocated_segment_minutes
        FROM ltl.app.v_waybill_split_linehaul_alloc
        WHERE group_no = 'G014' AND linehaul_node = 'DEPART'
    """).collect()
    assert {r.child_waybill_no: r.alloc_ratio for r in eq} == {
        "WB014A": 0.5, "WB014B": 0.5}
    assert {round(r.allocated_segment_minutes, 2) for r in eq} == {300.0}


# ---------- 19. 关系变更不改写已生成事件：事件原样保留，当前轨迹按新关系展开 ----------
def test_19_relation_change_never_rewrites_events(spark):
    # WB013X 被移出当前关系，但其已生成的两条事件在明细层原样保留、未被删除/改写
    x_events = spark.table(DWD_EVENT).where("waybill_no = 'WB013X'").count()
    assert x_events == 2
    # 当前关系快照下 G013 已不含 WB013X，沿当前关系展开的轨迹只挂 WB013/WB013Y
    cur_members = {r.waybill_no for r in spark.sql("""
        SELECT DISTINCT waybill_no FROM ltl.app.v_waybill_group_member
        WHERE group_no = 'G013' AND is_current = true
    """).collect()}
    assert cur_members == {"WB013", "WB013Y"}
    # 关系表只存关系与分摊，不存任何事件字段；事件明细行数不随关系作业变化
    rel_cols = set(spark.table(RELATION).columns)
    assert "event_id" not in rel_cols and "event_type" not in rel_cols


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
