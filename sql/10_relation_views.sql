-- =====================================================================
-- 10_relation_views.sql  运单拆合关系应用视图
--
-- 构建在 dwd.waybill_relation（关系拉链）+ dwd.waybill_event_detail（事件
-- 明细，不可变）之上，满足四个查询诉求：
--   A. 一票货完整轨迹：沿 group_no 展开成员运单的全部事件，按 event_id 去重，
--      跨多条运单也绝不重复计数（v_waybill_full_track）。
--   B. 历史快照展开：v_waybill_group_member 带 [eff_time,exp_time) 区间，
--      历史查询用"查询时点 as_of 可见的关系版本"过滤后再展开，关系修正只
--      新增版本，因此同一 as_of 的查询结果可复现、不被后续修正改变。
--   C. 合票时效：合票 MERGE 的时效只按主运单（parent）计算，成员运单不
--      单独计时（v_waybill_merge_timeliness）。
--   D. 分票干线分摊：分票 SPLIT 把主运单的干线节点（DEPART/ARRIVE/TRANSFER）
--      按 alloc_rule + alloc_ratio 分摊到子运单，规则与比例逐行可见
--      （v_waybill_split_linehaul_alloc）。
--
-- 历史查询范式（查询时点 as_of 的关系快照展开，本文件头部固定口径）：
--   WITH mem AS (
--     SELECT DISTINCT group_no, parent_waybill_no AS waybill_no
--     FROM ltl.dwd.waybill_relation
--     WHERE TIMESTAMP'<as_of>' >= eff_time AND TIMESTAMP'<as_of>' < exp_time
--     UNION
--     SELECT DISTINCT group_no, child_waybill_no AS waybill_no
--     FROM ltl.dwd.waybill_relation
--     WHERE TIMESTAMP'<as_of>' >= eff_time AND TIMESTAMP'<as_of>' < exp_time
--   )
--   SELECT m.group_no, e.* FROM mem m
--   JOIN ltl.dwd.waybill_event_detail e ON e.waybill_no = m.waybill_no;
-- 方言：Spark 3.5 SQL（Iceberg 表上的视图）。
-- =====================================================================

-- ---------- 1. 关系组成员快照（时点展开的基础块，带生效区间）----------
-- 一条关系的父子两端都映射到同一 group_no；独立运单退化为"自身成组"。
CREATE OR REPLACE VIEW ltl.app.v_waybill_group_member AS
WITH rel_side AS (
    SELECT group_no, parent_waybill_no AS waybill_no,
           eff_time, exp_time, is_current,
           'PARENT' AS member_role, relation_type
    FROM ltl.dwd.waybill_relation
    UNION ALL
    SELECT group_no, child_waybill_no AS waybill_no,
           eff_time, exp_time, is_current,
           'CHILD' AS member_role, relation_type
    FROM ltl.dwd.waybill_relation
), standalone AS (
    -- 没有任何关系的运单：group_no 取自身，全区间有效，保证任意运单都可查轨迹
    SELECT d.waybill_no AS group_no, d.waybill_no AS waybill_no,
           to_timestamp('1970-01-01 00:00:00') AS eff_time,
           to_timestamp('9999-12-31 00:00:00') AS exp_time,
           true AS is_current, 'STANDALONE' AS member_role,
           'NONE' AS relation_type
    FROM (SELECT DISTINCT waybill_no FROM ltl.dwd.waybill_event_detail) d
    LEFT JOIN (SELECT DISTINCT group_no, parent_waybill_no AS w
               FROM ltl.dwd.waybill_relation
               UNION
               SELECT DISTINCT group_no, child_waybill_no AS w
               FROM ltl.dwd.waybill_relation) r
      ON r.w = d.waybill_no
    WHERE r.w IS NULL
)
SELECT group_no, waybill_no, member_role, relation_type,
       eff_time, exp_time, is_current
FROM rel_side
UNION ALL
SELECT group_no, waybill_no, member_role, relation_type,
       eff_time, exp_time, is_current
FROM standalone;

-- ---------- 2. 一票货完整轨迹（当前关系快照，沿组展开 + 去重不重复计数）----------
-- 成员关系先 DISTINCT（同一运单可能被父子/兄弟多条关系引用），
-- 再关联事件；事件物理上只归属一条运单，按 (group_no, event_id) 去重兜底，
-- 保证跨运单展开后同一事件只计一次。
CREATE OR REPLACE VIEW ltl.app.v_waybill_full_track AS
WITH cur_mem AS (
    -- 同一运单可能在一条关系里是 parent、在另一条里是 child，
    -- 必须按 (group_no, waybill_no) 唯一化，否则关联事件会重复计数。
    SELECT group_no, waybill_no,
           min(member_role)   AS member_role,
           min(relation_type) AS relation_type
    FROM ltl.app.v_waybill_group_member
    WHERE is_current = true
    GROUP BY group_no, waybill_no
), joined AS (
    SELECT
        m.group_no,
        m.waybill_no AS owner_waybill,
        m.member_role,
        m.relation_type,
        d.event_id,
        d.waybill_no,
        d.event_seq,
        d.event_type,
        d.event_time,
        d.event_date,
        d.hub_code,
        d.source_system,
        d.exception_code,
        d.exception_reason
    FROM cur_mem m
    JOIN ltl.dwd.waybill_event_detail d
      ON d.waybill_no = m.waybill_no
)
SELECT
    group_no,
    owner_waybill,
    member_role,
    relation_type,
    event_id,
    waybill_no,
    event_seq,
    event_type,
    event_time,
    event_date,
    hub_code,
    source_system,
    exception_code,
    exception_reason,
    -- 组内事件去重序号：同一 event_id 在一票货内只占一个位次
    row_number() OVER (
        PARTITION BY group_no, event_id ORDER BY owner_waybill) AS group_event_dedup_rn
FROM joined;

-- ---------- 3. 合票时效：只按主运单计算，成员运单不单独计时 ----------
-- 主运单 = MERGE 关系当前有效的 parent；关键时点全部取自主运单自身事件，
-- 成员单即便有同节点扫描也不参与时效，避免合票后时效被重复/提前计算。
CREATE OR REPLACE VIEW ltl.app.v_waybill_merge_timeliness AS
WITH master AS (
    SELECT DISTINCT group_no, parent_waybill_no AS master_waybill
    FROM ltl.dwd.waybill_relation
    WHERE relation_type = 'MERGE' AND is_current = true
), m_evt AS (
    SELECT
        mm.group_no,
        mm.master_waybill,
        min(CASE WHEN d.event_type = 'PICKUP' THEN d.event_time END) AS pickup_time,
        min(CASE WHEN d.event_type = 'DEPART' THEN d.event_time END) AS line_haul_depart_time,
        min(CASE WHEN d.event_type = 'ARRIVE' THEN d.event_time END) AS arrive_hub_time,
        min(CASE WHEN d.event_type = 'DELIVER' THEN d.event_time END) AS deliver_time,
        min(CASE WHEN d.event_type = 'SIGN'   THEN d.event_time END) AS sign_time,
        min(CASE WHEN d.event_type = 'REJECT' THEN d.event_time END) AS reject_time
    FROM master mm
    JOIN ltl.dwd.waybill_event_detail d
      ON d.waybill_no = mm.master_waybill
    GROUP BY mm.group_no, mm.master_waybill
)
SELECT
    group_no,
    master_waybill,
    pickup_time,
    line_haul_depart_time,
    arrive_hub_time,
    deliver_time,
    sign_time,
    reject_time,
    timestampdiff(MINUTE, pickup_time, sign_time) AS total_aging_minutes,
    timestampdiff(MINUTE, line_haul_depart_time, arrive_hub_time) AS line_haul_minutes,
    timestampdiff(MINUTE, arrive_hub_time, sign_time) AS delivery_aging_minutes
FROM m_evt;

-- ---------- 4. 分票干线节点分摊到子运单（规则 + 比例逐行可见）----------
-- 分摊对象仅为干线节点 DEPART/ARRIVE/TRANSFER（末端 DELIVER/SIGN 不分摊）。
-- 先在主运单内按时间把相邻干线节点配成段，段时长 = 下一干线节点时间差；
-- 每个子运单继承主运单干线节点，并按 alloc_ratio 分摊段时长与权重，
-- alloc_rule 显式标注（WEIGHT/VOLUME/EQUAL）；同 parent 子单 ratio 之和=1。
CREATE OR REPLACE VIEW ltl.app.v_waybill_split_linehaul_alloc AS
WITH split_rel AS (
    SELECT
        group_no, parent_waybill_no, child_waybill_no,
        alloc_rule, alloc_weight, alloc_ratio
    FROM ltl.dwd.waybill_relation
    WHERE relation_type = 'SPLIT' AND is_current = true
), parent_linehaul AS (
    SELECT
        waybill_no AS parent_waybill_no,
        event_id,
        event_seq,
        event_type,
        event_time,
        hub_code,
        lead(event_time) OVER (
            PARTITION BY waybill_no ORDER BY event_time, event_id) AS next_linehaul_time
    FROM ltl.dwd.waybill_event_detail
    WHERE event_type IN ('DEPART', 'ARRIVE', 'TRANSFER')
)
SELECT
    r.group_no,
    r.parent_waybill_no,
    r.child_waybill_no,
    r.alloc_rule,
    r.alloc_weight,
    r.alloc_ratio,
    p.event_type  AS linehaul_node,
    p.event_id    AS parent_event_id,
    p.event_time  AS node_time,
    p.hub_code,
    timestampdiff(MINUTE, p.event_time, p.next_linehaul_time) AS parent_segment_minutes,
    -- 子运单分摊到的干线段时长 = 主运单段时长 × 归一化比例
    CASE WHEN p.next_linehaul_time IS NOT NULL
         THEN timestampdiff(MINUTE, p.event_time, p.next_linehaul_time) * r.alloc_ratio
         ELSE NULL END AS allocated_segment_minutes
FROM split_rel r
JOIN parent_linehaul p
  ON p.parent_waybill_no = r.parent_waybill_no;
