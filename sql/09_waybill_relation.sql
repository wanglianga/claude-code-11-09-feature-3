-- =====================================================================
-- 09_waybill_relation.sql  运单拆合关系层（合票/分票/退件重派/主副/兄弟）
--
-- 业务背景：
--   实际业务存在合票（多票合一，干线走主运单）、分票（一票拆多子单）、
--   退件重派（拒收后换新运单重派）与主副运单关系；拆合之后轨迹分散在多条
--   运单上。明细层需要记录运单之间的父子/兄弟关系及其生效时间。
--
-- 两张表分工（对应需求硬约束）：
--   1. waybill_relation_event_log（关系变更事件台账，APPEND-ONLY）：
--      上游每来一条关系变更指令就追加一行，永不更新、永不删除。
--      "关系变更不得改写已生成的事件，只能新增关系记录"由本表强制：
--      DWD 事件明细表 waybill_event_detail 与关系完全解耦，关系变更只往
--      本表 + 拉链表新增记录，绝不回写/删除任何事件行。
--   2. waybill_relation（关系拉链表，SCD2 风格）：
--      由 event_log 物化，每条关系带 [eff_time, exp_time) 生效区间与
--      is_current；关系修正 = 闭旧版本 + 插新版本（change_seq+1），
--      历史版本物理保留。历史查询按"查询时点可用的关系快照"展开：
--        WHERE as_of_time >= eff_time AND as_of_time < exp_time
--
-- 关系类型 relation_type：
--   MERGE           合票：parent=主运单，child=被合入的成员运单（可多条）
--   SPLIT           分票：parent=被拆的原运单，child=拆出的子运单（可多条）
--   RESEND_REASSIGN 退件重派：parent=拒收原单，child=重派新单
--   PARENT_CHILD    通用主副运单关系（无明确合/分票业务动作的挂靠）
--   SIBLING         兄弟运单：parent/child 存无向兄弟对的两个端点
--
-- 一票货归组：group_no 为同一票货所有运单共享的归组键（合票/分票/重派
--   全程继承），完整轨迹沿 group_no 展开，无需递归；独立运单 group_no
--   退化为自身运单号（见应用层视图 10_relation_views.sql）。
--
-- 干线分摊 alloc_rule（仅分票 SPLIT 需要把干线节点分摊到子运单）：
--   WEIGHT 按重量权重 alloc_weight 占比分摊；VOLUME 按体积（同口径）；
--   EQUAL  按子单数量均摊；NONE 不分摊（合票主运单自己承担干线时效）。
--   alloc_ratio 由接入作业按同组兄弟权重归一化后落库，同 parent 下之和=1。
-- 方言：Spark 3.5 SQL + Iceberg v2。
-- =====================================================================

-- ---------- 1. 关系变更事件台账（只增不改，关系域的不可变真相源）----------
CREATE TABLE IF NOT EXISTS ltl.dwd.waybill_relation_event_log (
    event_id            STRING    COMMENT '关系变更事件ID，与上游一致，幂等键',
    relation_id         STRING    COMMENT '关系业务ID；同一关系修正时保持不变，版本靠 change_seq 区分',
    relation_type       STRING    COMMENT 'MERGE/SPLIT/RESEND_REASSIGN/PARENT_CHILD/SIBLING',
    change_type         STRING    COMMENT 'CREATE 建立 / CLOSE 解除 / CORRECT 修正',
    parent_waybill_no   STRING    COMMENT '主/父运单号（合票主单、分票原单、重派原单）',
    child_waybill_no    STRING    COMMENT '子/副运单号（成员单、分票子单、重派新单；SIBLING 为兄弟对端点）',
    group_no            STRING    COMMENT '一票货归组号，同票货所有运单共享，沿关系继承',
    event_time          TIMESTAMP COMMENT '关系变更业务发生时间（拉链生效时间依据）',
    alloc_rule          STRING    COMMENT 'WEIGHT/VOLUME/EQUAL/NONE 干线分摊规则',
    alloc_weight        DOUBLE    COMMENT '分摊权重（重量/体积；EQUAL 可空）',
    source_system       STRING    COMMENT '来源系统：TMS 运单系统 / HUB 分拨系统',
    source_file         STRING    COMMENT '落地文件名，文件级幂等与血缘',
    raw_payload         STRING    COMMENT '关系变更原始报文全文(JSON)',
    ingest_batch_id     STRING,
    ingest_time         TIMESTAMP,
    biz_date            DATE      COMMENT '业务日期=to_date(event_time)，分区键'
)
USING iceberg
PARTITIONED BY (biz_date, source_system)
COMMENT '运单关系变更事件台账：APPEND-ONLY，关系变更只新增不改写事件'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'history.expire.max-snapshot-age-ms' = '2592000000'
);

-- ---------- 2. 运单关系拉链表（由 event_log 物化，含生效区间与分摊口径）----------
CREATE TABLE IF NOT EXISTS ltl.dwd.waybill_relation (
    relation_id         STRING    COMMENT '关系业务ID（自然键，跨版本稳定）',
    change_seq          INT       COMMENT '版本序号：同 relation_id 第几个版本，修正递增',
    relation_type       STRING    COMMENT 'MERGE/SPLIT/RESEND_REASSIGN/PARENT_CHILD/SIBLING',
    parent_waybill_no   STRING    COMMENT '主/父运单号',
    child_waybill_no    STRING    COMMENT '子/副运单号',
    group_no            STRING    COMMENT '一票货归组号',
    eff_time            TIMESTAMP COMMENT '本版本生效时间（含）',
    exp_time            TIMESTAMP COMMENT '本版本失效时间（不含）；9999-12-31 为当前版本',
    eff_date            DATE      COMMENT 'to_date(eff_time)，分区键',
    is_current          BOOLEAN   COMMENT '是否当前有效版本',
    alloc_rule          STRING    COMMENT '干线分摊规则 WEIGHT/VOLUME/EQUAL/NONE',
    alloc_weight        DOUBLE    COMMENT '本运单在该关系下的分摊权重',
    alloc_ratio         DOUBLE    COMMENT '归一化分摊比例；同一 parent 同期子单 ratio 之和=1',
    line_haul_scope     STRING    COMMENT '参与分摊的干线节点范围，默认 ALL_LINE_HAUL（DEPART/ARRIVE/TRANSFER）',
    source_system       STRING,
    raw_payload         STRING    COMMENT '产生本版本的关系变更报文快照',
    ingest_batch_id     STRING,
    ingest_time         TIMESTAMP
)
USING iceberg
PARTITIONED BY (eff_date)
COMMENT '运单关系拉链：父子/兄弟关系+生效时间+分摊规则，修正只增版本不改历史'
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.update.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write'
);

-- 时点查询范式（历史按查询时点的关系快照展开）：
--   SELECT * FROM ltl.dwd.waybill_relation
--   WHERE TIMESTAMP'2026-11-04 12:00:00' >= eff_time
--     AND TIMESTAMP'2026-11-04 12:00:00' <  exp_time;
-- 关系幂等键：(relation_id, change_seq)；event_log 按 (event_id, source_file) 幂等。
