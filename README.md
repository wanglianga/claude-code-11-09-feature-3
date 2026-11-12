# 零担物流事件型数据仓库（LTL Event Warehouse）

把"三十多个时间列、4.2 亿行"的运单宽表改造为**四层事件型仓库**：原始层原样保存每次
推送，明细层以"运单 × 事件序号 × 事件类型"为粒度并支持乱序/去重/重放，维表独立拉链
按时点关联，应用层以视图替代旧宽表。本仓库**没有业务界面、没有常驻服务**，交付物是
建表语句、Spark 批处理作业、Airflow 编排、补数脚本与迁移说明。

## 1. 解决的核心问题

| 旧宽表痛点 | 本方案对应机制 |
|---|---|
| 只留最后一次异常，无法回答"在哪个分拨压了两天" | 事件明细逐行保留多次异常；`app.v_hub_dwell` 逐段计算分拨停留，>48h 打标 |
| 网点改名/派件员换片区，用今年架构重算去年报表对不上 | 4 张 SCD2 拉链维表 + 严格时点关联，禁止最新维表回刷历史 |
| 上游丢包/断连/回调丢失要补 30 天数据，一改带偏整月汇总 | 独立补数入口，按单号/区间只重跑受影响分区，影子表对比后受控切换 |
| 新增预约时段、冷链、明年新作业节点，加字段阻力大 | 只加列+默认值回填的演进规则；新节点只是 `event_type` 新枚举，无需加时间列 |

## 2. 分层与目录结构

```
sql/                         四层建表与视图（Spark 3.5 SQL + Iceberg 1.8）
  00_setup.sql               命名空间
  01_ods.sql                 ODS 原始事件表（分区 biz_date × source_system）
  02_dwd.sql                 DWD 事件明细表（去重/乱序序号/代理键）
  03_dim_stg.sql             维表每日快照落地
  04_dim.sql                 网点/派件员/路由/客户 4 张 SCD2 拉链维表
  05_meta.sql                水位线 / DQ 结果 / 补数台账
  06_app_views.sql           应用层视图（含旧宽表兼容视图、压货、绩效、投诉归因）
  07_evolution.sql           Schema 演进规则参考（实际由作业幂等执行）
  08_legacy_source.sql       旧宽表迁移源登记
  09_waybill_relation.sql    运单拆合关系：关系变更台账(APPEND-ONLY)+关系拉链(SCD2)
  10_relation_views.sql      一票货轨迹展开/合票主单时效/分票干线分摊视图
jobs/                        PySpark 作业
  ddl_init.py                一键建仓（幂等）
  ods_load.py                原始接入（原样保存、文件级幂等）
  dwd_detail_job.py          去重/乱序排序/重放/代理键解析
  waybill_relation_job.py    合票/分票/退件重派关系接入（只增关系不改事件、分摊归一化）
  dim_stg_load.py            维表快照落地
  dim_scd2_job.py            SCD2 拉链（--preview 出重算影响预览）
  backfill_job.py            补数（影子表+前后对比+受控切换/回滚）
  dq_check_job.py            五类质量指标按天×来源×分拨
  schema_evolution_job.py    只加列+历史回填（幂等）
  legacy_migrate_job.py      旧宽表时间列逆透视迁移
orchestration/dags/          Airflow DAG（每日增量、手动补数）
docker/                      Dockerfile + entrypoint
docker-compose.yml           一键启动 Airflow+Spark
conf/pipeline.conf           全局配置（catalog、阈值、目录）
scripts/run_local.sh         本地无 Docker 一键验证
tests/                       夹具生成、19 项端到端断言、旧宽表迁移冒烟
docs/migration_guide.md      迁移步骤、执行顺序与回滚点（R0–R6）
```

## 3. 技术栈与 Catalog

- Spark 3.5（PySpark / Spark SQL）+ Iceberg 1.8（format-version=2，行级 MERGE/OVERWRITE）。
- Catalog（`conf/pipeline.conf` 的 `catalog.type`）：
  - **jdbc（本地默认）**：Iceberg JdbcCatalog + H2 文件库，免外部依赖且**支持视图**；
  - **hive（生产推荐）**：`catalog.uri=thrift://<metastore>:9083`；
  - **hadoop**：极简模式，元数据随表目录，不支持视图。
- 编排：Airflow 2.9（本地 standalone；生产可换 DolphinScheduler，作业命令不变）。

> 项目类型说明：本项目是**纯批处理数据仓库工程**，没有 HTTP 接口、没有常驻业务服务、
> 也没有前端页面（与需求"最终没有界面也没有常驻服务"一致），因此不提供
> OpenAPI/Swagger UI；对外的"服务"就是 Airflow 编排下的批处理作业与可查询的表/视图。

## 4. 一键启动（Docker，推荐）

前置：Docker 24+、Compose v2。

```bash
docker compose up -d --build
# Airflow UI: http://localhost:8080
```

容器首次启动自动执行 `ddl_init.py` 建仓。两个服务：
- `ltl-airflow`：Airflow standalone（含本地 Spark，DAG 通过 spark-submit 调作业）；
- `ltl-spark`（profiles=tools）：手动跑作业的一次性容器，例如
  ```bash
  docker compose run --rm ltl-spark \
    spark-submit /opt/ltl/jobs/ddl_init.py --conf /opt/ltl/conf/pipeline.conf
  ```

### 测试账号

| 系统 | 地址 | 用户名 | 密码 |
|---|---|---|---|
| Airflow UI | http://localhost:8080 | `ltl_admin` | `ltl_admin_2026` |

账号可通过 `.env` 或 compose 的 `AIRFLOW_USER/AIRFLOW_PASSWORD` 覆盖。
本项目无业务前端、无常驻 API 服务，因此不需要其他账号；Airflow 仅作编排器。

## 5. 本地开发与验证（无需 Docker）

前置：Python 3.11、JDK 17、Spark 3.5（脚本默认取 `/tmp/spark-3.5.9-bin-hadoop3`，
可用 `SPARK_HOME` 覆盖），Iceberg/H2 jar 已放入 `$SPARK_HOME/jars`（版本见 Dockerfile）。

```bash
python3 -m venv .venv && .venv/bin/pip install duckdb pytest
bash scripts/run_local.sh
```

该脚本会：生成夹具 → 建仓 → 两期维表拉链 → 四天事件增量 → DQ →
11-05 迟到事件补数 → 运单拆合关系接入（含 11-04 关系事后修正）→ 跑 `pytest`。
结束打印 `ALL LOCAL STEPS DONE` 与 `19 passed` 即通过。

也可单步执行（均为幂等）：

```bash
export SPARK_HOME=/path/to/spark PYTHONPATH=$PWD/jobs \
       PIPELINE_CONF=$PWD/conf/pipeline.local.conf
$SPARK_HOME/bin/spark-submit --master local[2] jobs/ddl_init.py            --conf $PIPELINE_CONF
$SPARK_HOME/bin/spark-submit --master local[2] jobs/dim_scd2_job.py        --conf $PIPELINE_CONF --snapshot-date 2026-11-02 --preview
$SPARK_HOME/bin/spark-submit --master local[2] jobs/backfill_job.py        --conf $PIPELINE_CONF --scope WAYBILL --waybill-nos WB006 --switch
```

## 6. 验证方式与覆盖点

`tests/test_pipeline.py` 的 19 项断言（已在本地 Spark+Iceberg 实际执行通过）覆盖：

1. ODS 原样保存重复上报与原始报文，不清洗不合并；
2. DWD 按 `event_id` 去重；
3. 乱序事件按**业务发生时间**而非上报时间生成连续 `event_seq`；
4. 多次异常独立保留，兼容视图末次原因取最后一次、异常链完整；
5. 缺失节点迟到补报后整单重放、序号重排、业务日期仍取事件时间；
6. SCD2 时点关联：改名/换片区/客户降级前后，历史事实关联到当时版本；合并撤销不物理删除；
7. 分拨压货 >48 小时可定位（`v_hub_dwell`，带当时网点名）；
8. 未知网点代理键 = -1；
9. DQ 五类指标齐全、可下钻到来源系统×分拨、超阈值带 breach；
10. 补数只动受影响运单，台账含分区清单/前后对比/差异，对比文件落盘；
11. 水位线按 ODS/DWD 分层按日推进；
12. Schema 只加列+回填，既有视图与报表不受影响、加列幂等；
13. 旧宽表兼容视图一行一运单、关键时间列与新列正确透视；
14. 关系台账只增不改，关系修正以"闭旧版本+新版本"表达，历史版本物理保留；
15. 历史查询按查询时点可用的关系快照展开，修正后旧时点结果仍可复现；
16. 一票货完整轨迹沿关系展开，跨运单按 `event_id` 去重、不重复计数（含兄弟边）；
17. 合票时效只按主运单计算，成员运单不单独计时；
18. 分票干线节点按 WEIGHT/EQUAL 规则分摊到子单，比例之和=1、分摊量之和=原始段时长；
19. 关系变更不改写已生成事件：被移出关系的运单事件原样保留，关系表不含事件字段。

另有 `tests/smoke_legacy_migration.py` 验证旧宽表逆透视迁移与重跑幂等。

> 说明：以上结论均来自本机实际执行（Spark 3.5.9 + Iceberg 1.8.1 + H2 JdbcCatalog）。
> 未在本环境构建/启动 Docker 镜像（沙箱无 Docker daemon），Dockerfile/compose 已按
> 相同版本与命令编写，需在具备 Docker 的环境执行第 4 节验证。

## 7. 关键作业口径速查

- **增量**：`meta.watermark_log` 记事件时间水位，每日推进；失败不写 SUCCESS、不推进。
- **补数**：默认只产出对比（`DIFF_READY`），加 `--switch` 才生效；
  全量回刷只能 `--full-rebuild --switch`（独立表重建+换表，旧表留 backup）。
- **维表变更**：先 `--preview` 看重算影响（历史不受影响行数），再正式拉链；
  调整只在快照日新分区生效。
- **质量阈值**：`conf/pipeline.conf` 中 `dq.threshold.*`。

## 8. 运单拆合关系（合票/分票/退件重派）

业务存在合票、分票、退件重派、主副与兄弟运单关系，拆合后轨迹分散在多条运单上。
本仓库用**独立的关系域**承载，与事件明细物理解耦（`sql/09_waybill_relation.sql`、
`sql/10_relation_views.sql`、`jobs/waybill_relation_job.py`）：

- **两张关系表**：`dwd.waybill_relation_event_log`（变更台账，APPEND-ONLY，永不改删）
  与 `dwd.waybill_relation`（关系拉链，带 `[eff_time,exp_time)`、`is_current`、
  分摊规则 `alloc_rule` 与归一化比例 `alloc_ratio`）。
- **只增关系，不改事件**：关系作业只写两张关系表，绝不回写/删除
  `waybill_event_detail`。关系修正 = 闭合旧版本 + 插入 `change_seq+1` 新版本，
  历史版本物理保留（测试 14、19）。
- **历史按快照展开**：查询时点 `as_of` 用 `as_of >= eff_time AND as_of < exp_time`
  取当时可用的关系版本，修正后旧时点查询结果仍可复现（测试 15）。
- **一票货完整轨迹**：同票货共享 `group_no`，`app.v_waybill_full_track` 沿组展开
  成员运单事件；成员按 `(group_no, waybill_no)` 唯一化、事件按 `(group_no, event_id)`
  去重，跨多条运单也不重复计数（测试 16）。
- **合票时效按主运单**：`app.v_waybill_merge_timeliness` 关键时点全部取 MERGE
  主运单自身事件，成员运单不单独计时（测试 17）。
- **分票干线分摊规则**：仅干线节点 `DEPART/ARRIVE/TRANSFER` 参与；
  `WEIGHT/VOLUME` 按本单权重占兄弟权重和、`EQUAL` 按 1/兄弟数归一化，
  同 parent 子单 `alloc_ratio` 之和恒为 1；子单分摊段时长 = 主运单段时长 × 比例，
  分摊量之和 = 原始段时长。比例随关系版本冻结，修正只影响新版本（测试 18）。

迁移的完整执行顺序与回滚点见 **[docs/migration_guide.md](docs/migration_guide.md)**（R0–R6）。
