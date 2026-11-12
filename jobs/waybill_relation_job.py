"""运单拆合关系接入作业：关系变更台账（APPEND-ONLY）-> 关系拉链（SCD2 快照）。

业务场景：合票 MERGE / 分票 SPLIT / 退件重派 RESEND_REASSIGN /
主副运单 PARENT_CHILD / 兄弟运单 SIBLING。拆合后轨迹分散在多条运单上，
本作业负责把"运单之间的父子/兄弟关系及其生效时间"沉淀到明细层。

两条硬约束的落地方式（对应需求）：
  1. 关系变更不得改写已生成的事件，只能新增关系记录：
       作业只写 waybill_relation_event_log（追加）与 waybill_relation
       （闭旧版本+插新版本），全程不 touch waybill_event_detail 任何行；
       event_log 按 (event_id, source_file) MERGE，文件级幂等、永不更新删除。
  2. 历史查询按查询时点可用的关系快照展开：
       关系拉链带 [eff_time, exp_time) 与 is_current，
       修正 = 闭合旧版本 + 新增 change_seq+1 版本，历史版本物理保留。

分摊口径（分票 SPLIT 把干线节点分摊到子运单）：
  alloc_rule: WEIGHT 按重量 / VOLUME 按体积 / EQUAL 按子单数均摊 / NONE 不分摊；
  同 parent 下、在某版本生效时点仍有效的子单为兄弟集合，alloc_ratio 在该集合内
  归一化（WEIGHT/VOLUME 用 alloc_weight 占比，EQUAL 用 1/兄弟数），同组之和=1。
  历史版本的 ratio 在闭版时冻结，修正权重只影响新版本，不回刷历史。

用法：
  spark-submit jobs/waybill_relation_job.py --biz-date 2026-11-03
  spark-submit jobs/waybill_relation_job.py --biz-date 2026-11-03 --rebuild-relations REL001
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)
from pyspark.sql.window import Window

from common.spark_utils import (
    ALLOC_RULES, FUTURE_TS, RELATION, RELATION_LOG, RELATION_TYPES,
    WM_LOG, build_spark, load_conf, new_batch_id,
)

# 关系落地报文解析模式（缺字段得 NULL，不报错；全文另存 raw_payload）
_HEADER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("relation_id", StringType()),
    StructField("relation_type", StringType()),
    StructField("change_type", StringType()),
    StructField("parent_waybill_no", StringType()),
    StructField("child_waybill_no", StringType()),
    StructField("group_no", StringType()),
    StructField("event_time", TimestampType()),
    StructField("alloc_rule", StringType()),
    StructField("alloc_weight", DoubleType()),
    StructField("source_system", StringType()),
])

_CREATE_LIKE = ["CREATE", "CORRECT"]


def load_landing_relations(spark, landing_dir: str, biz_date: str, batch_id: str):
    """读取当日关系落地 JSONL，全文进 raw_payload。"""
    path = os.path.join(landing_dir, f"biz_date={biz_date}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"关系落地目录不存在: {path}")
    raw = (
        spark.read.text(path, wholetext=False)
        .withColumn("source_file", F.input_file_name())
        .filter(F.trim(F.col("value")) != "")
    )
    parsed = raw.withColumn("h", F.from_json(F.col("value"), _HEADER_SCHEMA))
    df = (
        parsed.select(
            F.col("h.event_id").alias("event_id"),
            F.col("h.relation_id").alias("relation_id"),
            F.col("h.relation_type").alias("relation_type"),
            F.col("h.change_type").alias("change_type"),
            F.col("h.parent_waybill_no").alias("parent_waybill_no"),
            F.col("h.child_waybill_no").alias("child_waybill_no"),
            F.col("h.group_no").alias("group_no"),
            F.col("h.event_time").alias("event_time"),
            # 分摊规则缺省 NONE（合票/主副/兄弟不做干线分摊）
            F.coalesce(F.col("h.alloc_rule"), F.lit("NONE")).alias("alloc_rule"),
            F.col("h.alloc_weight").alias("alloc_weight"),
            F.col("h.source_system").alias("source_system"),
            F.col("value").alias("raw_payload"),
            F.col("source_file"),
            F.lit(batch_id).alias("ingest_batch_id"),
            F.current_timestamp().alias("ingest_time"),
        )
        .withColumn("biz_date", F.to_date(F.col("event_time")))
    )
    bad = df.filter(
        F.col("event_id").isNull() | F.col("relation_id").isNull()
        | F.col("event_time").isNull()
        | ~F.col("relation_type").isin(RELATION_TYPES)
        | ~F.col("alloc_rule").isin(ALLOC_RULES))
    if bad.limit(1).count() > 0:
        samples = bad.select("source_file", "event_id").limit(10).collect()
        raise ValueError(f"关系报文枚举/必填校验失败: {[(r.source_file, r.event_id) for r in samples]}")
    return df


def append_event_log(spark, df) -> int:
    """关系变更事件追加进台账：文件级幂等，永不更新/删除。"""
    df.createOrReplaceTempView("stg_relation_event")
    spark.sql(f"""
        MERGE INTO {RELATION_LOG} t
        USING stg_relation_event s
        ON t.event_id = s.event_id AND t.source_file = s.source_file
        WHEN NOT MATCHED THEN INSERT *
    """)
    return df.count()


def _build_versions(spark, relation_ids: list[str]):
    """从事件台账重放指定 relation_id 的全部拉链版本（确定性可复现）。

    版本推导（按 event_time,event_id 排序）：
      CREATE/CORRECT 各产生一个开放版本；其后第一条事件（CORRECT/CLOSE）
      的 event_time 即为本版本 exp_time；无后继则为当前版本（exp=9999）。
      CLOSE 只闭版不产生新版本。change_seq = 关系内 CREATE/CORRECT 累计序号。
    """
    log = (spark.table(RELATION_LOG)
           .filter(F.col("relation_id").isin(relation_ids)))
    w = Window.partitionBy("relation_id").orderBy(
        F.col("event_time").asc(), F.col("event_id").asc())
    ordered = (
        log.withColumn("_rn", F.row_number().over(w))
           .withColumn("_next_time", F.lead("event_time").over(w))
           .withColumn("change_seq",
                       F.sum(F.when(F.col("change_type").isin(_CREATE_LIKE), 1)
                             .otherwise(0)).over(w))
    )
    opens = ordered.filter(F.col("change_type").isin(_CREATE_LIKE))
    versions = (
        opens.select(
            F.col("relation_id"),
            F.col("change_seq").cast("int"),
            F.col("relation_type"),
            F.col("parent_waybill_no"),
            F.col("child_waybill_no"),
            F.col("group_no"),
            F.col("event_time").alias("eff_time"),
            F.coalesce(F.col("_next_time"),
                       F.to_timestamp(F.lit(FUTURE_TS))).alias("exp_time"),
            F.to_date(F.col("event_time")).alias("eff_date"),
            F.when(F.col("_next_time").isNull(), F.lit(True))
             .otherwise(F.lit(False)).alias("is_current"),
            F.col("alloc_rule"),
            F.col("alloc_weight"),
            F.lit(None).cast("double").alias("alloc_ratio"),  # 下一步归一化
            F.lit("ALL_LINE_HAUL").alias("line_haul_scope"),
            F.col("source_system"),
            F.col("raw_payload"),
            F.col("ingest_batch_id"),
            F.current_timestamp().alias("ingest_time"),
        )
    )
    return versions


def _normalize_alloc_ratio(spark, new_versions):
    """按版本生效时点的兄弟集合归一化分票分摊比例。

    兄弟集合 = 同 parent、relation_type=SPLIT 且在该版本 eff_time 时点有效
    （s.eff_time <= v.eff_time < s.exp_time）的全部子单关系（含本版本）。
    WEIGHT/VOLUME：ratio = 本单权重 / 兄弟权重和；
    EQUAL：ratio = 1 / 兄弟数；NONE 或权重缺失：ratio=NULL（不分摊）。
    """
    new_versions.createOrReplaceTempView("tmp_rel_new_ver")
    # 重放后的全量快照 = 表中不受影响关系 + 本批新版本
    affected = spark.table("tmp_rel_new_ver").select("relation_id").distinct()
    kept = spark.table(RELATION).join(affected, on="relation_id", how="left_anti")
    kept.createOrReplaceTempView("tmp_rel_kept")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW tmp_rel_snapshot AS
        SELECT * FROM tmp_rel_kept
        UNION ALL
        SELECT * FROM tmp_rel_new_ver
    """)
    # 先按 (relation_id, change_seq) 聚合成一行，避免兄弟集合一对多 JOIN 翻倍
    enriched = spark.sql("""
        WITH split_ver AS (
            SELECT * FROM tmp_rel_new_ver WHERE relation_type = 'SPLIT'
        ), sib_agg AS (
            SELECT
                v.relation_id, v.change_seq,
                sum(coalesce(s.alloc_weight, 0.0)) AS weight_sum,
                count(*)                           AS sib_cnt
            FROM split_ver v
            JOIN tmp_rel_snapshot s
              ON s.parent_waybill_no = v.parent_waybill_no
             AND s.relation_type = 'SPLIT'
             AND s.eff_time <= v.eff_time
             AND s.exp_time >  v.eff_time
            GROUP BY v.relation_id, v.change_seq
        )
        SELECT
            v.*,
            CASE
              WHEN v.relation_type <> 'SPLIT' OR v.alloc_rule = 'NONE' THEN NULL
              WHEN v.alloc_rule = 'EQUAL' THEN 1.0 / a.sib_cnt
              WHEN a.weight_sum > 0 THEN coalesce(v.alloc_weight, 0.0) / a.weight_sum
              ELSE NULL
            END AS alloc_ratio_new
        FROM tmp_rel_new_ver v
        LEFT JOIN sib_agg a
          ON a.relation_id = v.relation_id AND a.change_seq = v.change_seq
    """)
    return (enriched.drop("alloc_ratio")
                    .withColumnRenamed("alloc_ratio_new", "alloc_ratio")
                    .select(*spark.table(RELATION).columns))


def materialize_relations(spark, relation_ids: list[str], batch_id: str) -> dict:
    """受影响 relation_id 整体重放：删旧版本 + 插重算版本（单表幂等）。"""
    if not relation_ids:
        return {"relations": 0, "versions": 0}
    versions = _build_versions(spark, relation_ids)
    versions = _normalize_alloc_ratio(spark, versions)
    versions.cache()
    n_ver = versions.count()

    # 仅删除受影响关系的旧版本，其它关系物理不动；Iceberg v2 行级删除
    in_list = ",".join("'" + r.replace("'", "''") + "'" for r in relation_ids)
    spark.sql(f"DELETE FROM {RELATION} WHERE relation_id IN ({in_list})")
    versions.writeTo(RELATION).append()

    # 分摊比例校验：同一 parent 当前有效 SPLIT 子单 ratio 之和必须为 1（允许浮点误差）
    cur = spark.table(RELATION).filter(
        (F.col("relation_type") == "SPLIT") & (F.col("is_current") == True))  # noqa: E712
    sums = (cur.groupBy("parent_waybill_no")
               .agg(F.sum("alloc_ratio").alias("ratio_sum")).collect())
    bad = [r for r in sums if r.ratio_sum is not None and abs(r.ratio_sum - 1.0) > 1e-9]
    if bad:
        raise ValueError(f"分票分摊比例归一化失败（同 parent 之和应=1）: {bad}")
    return {"relations": len(relation_ids), "versions": n_ver}


def run(spark, conf: dict, biz_date: str,
        rebuild_relations: list[str] | None = None) -> dict:
    batch_id = new_batch_id("rel")
    landing_dir = conf.get("landing.relation.dir",
                           os.path.join(os.path.dirname(
                               conf.get("landing.event.dir", "/opt/data/landing/events")),
                               "relations"))
    staged = load_landing_relations(spark, landing_dir, biz_date, batch_id)
    n_log = append_event_log(spark, staged)

    # 本批落地涉及的关系 + 显式要求重放的关系（补数场景）
    ids = [r.relation_id for r in
           staged.select("relation_id").distinct().collect()]
    if rebuild_relations:
        ids = sorted(set(ids) | set(rebuild_relations))
    stats = materialize_relations(spark, ids, batch_id)

    wm = staged.agg(F.max("event_time").alias("hwm"),
                    F.count("*").alias("cnt")).collect()[0]
    if wm["hwm"] is not None:
        spark.sql(f"""
            INSERT INTO {WM_LOG}
            VALUES ('RELATION', 'ALL', DATE'{biz_date}',
                    timestamp'{wm['hwm'].strftime('%Y-%m-%d %H:%M:%S')}',
                    {wm['cnt']}, '{batch_id}', current_timestamp(), 'SUCCESS')
        """)
    result = {"biz_date": biz_date, "event_log_rows": n_log, **stats}
    print("[relation] " + str(result))
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--biz-date", required=True)
    ap.add_argument("--rebuild-relations", default="",
                    help="逗号分隔 relation_id，补数时强制重放这些关系")
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()
    conf = load_conf(args.conf)
    spark = build_spark("ltl_waybill_relation", conf)
    try:
        rebuild = [x.strip() for x in args.rebuild_relations.split(",") if x.strip()] or None
        run(spark, conf, args.biz_date, rebuild_relations=rebuild)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
