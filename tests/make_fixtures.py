"""本地端到端夹具生成器（仅用标准库）。

产出：
  conf/pipeline.local.conf        本地仓库配置
  data/landing/events/biz_date=*  上游事件 JSON Lines（按到达日分区落地）
  data/landing/dim/biz_date=*     维表每日快照
  data/landing/relations/biz_date=* 运单拆合关系变更 JSON Lines

场景覆盖：
  WB001 正常冷链件（预约时段/冷链新列）
  WB002 分拨压货 >48 小时（v_hub_dwell 验证）
  WB003 乱序到达（report_time 与 event_time 顺序倒置，序号仍按 event_time）
  WB004 同 event_id 跨文件重复上报（ODS 留两份，DWD 去重留一份）
  WB005 多次异常独立保留（破损 -> 温控异常，兼容视图末次原因取后者）
  WB006 关键节点缺失后迟到补报（11-05 才补 11-01 的 ARRIVE，补数重放）
  WB007 未知网点 HUB_GHOST（代理键 -1，DQ 维表未匹配率）
  WB009 网点改名/派件员换片区/客户等级调整的时点关联验证
  WB010 合票（主单承担干线时效，成员合入，另含 SIBLING 兄弟边验证去重）
  WB011 分票按重量分摊（WEIGHT 30/20/50，干线节点分摊到三个子单）
  WB012 退件重派（拒收原单 -> 重派新单，沿关系展开一票货轨迹）
  WB013 关系事后修正（11-02 误挂成员，11-04 CORRECT，历史快照可复现）
  WB014 分票均摊（EQUAL，两个子单各分 1/2 干线时长）
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

# ---------- 事件构造助手 ----------
def ev(event_id, waybill, etype, source, event_time, report_time, **payload):
    row = {
        "event_id": event_id, "waybill_no": waybill, "event_type": etype,
        "source_system": source, "event_time": event_time,
        "report_time": report_time,
    }
    row.update(payload)
    return row


def build_event_files() -> dict[str, list[dict]]:
    f: dict[str, list[dict]] = {d: [] for d in
                                ["2026-11-01", "2026-11-02", "2026-11-03",
                                 "2026-11-04", "2026-11-05"]}
    common = dict(route_code="R001")

    # ---- WB001 正常冷链件 ----
    f["2026-11-01"] += [
        ev("E101", "WB001", "PICKUP", "SCANNER",
           "2026-11-01 08:00:00", "2026-11-01 08:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH", courier_code="C001",
           customer_code="K001", appointment_window="2026-11-02 09:00-11:00",
           is_cold_chain=True, **common),
        ev("E102", "WB001", "DEPART", "HUB",
           "2026-11-01 10:00:00", "2026-11-01 10:10:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           next_hub_code="HUB_BJ", customer_code="K001", **common),
        ev("E103", "WB001", "ARRIVE", "HUB",
           "2026-11-01 20:00:00", "2026-11-01 20:15:00",
           hub_code="HUB_BJ", customer_code="K001", **common),
    ]
    f["2026-11-02"] += [
        ev("E104", "WB001", "DELIVER", "SCANNER",
           "2026-11-02 09:30:00", "2026-11-02 09:35:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", is_cold_chain=True, **common),
        ev("E105", "WB001", "SIGN", "SCANNER",
           "2026-11-02 12:00:00", "2026-11-02 12:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
    ]

    # ---- WB002 压货 >48h（11-01 21:00 到 HUB_BJ，11-04 08:00 才发）----
    f["2026-11-01"] += [
        ev("E201", "WB002", "PICKUP", "SCANNER",
           "2026-11-01 07:00:00", "2026-11-01 07:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K002", **common),
        ev("E202", "WB002", "DEPART", "HUB",
           "2026-11-01 09:00:00", "2026-11-01 09:10:00",
           hub_code="HUB_SH", next_hub_code="HUB_BJ",
           customer_code="K002", **common),
        ev("E203", "WB002", "ARRIVE", "HUB",
           "2026-11-01 21:00:00", "2026-11-01 21:20:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
    ]
    f["2026-11-04"] += [
        ev("E204", "WB002", "DEPART", "HUB",
           "2026-11-04 08:00:00", "2026-11-04 08:10:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
        ev("E205", "WB002", "DELIVER", "SCANNER",
           "2026-11-04 10:00:00", "2026-11-04 10:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E206", "WB002", "SIGN", "SCANNER",
           "2026-11-04 12:00:00", "2026-11-04 12:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
    ]

    # ---- WB003 乱序：ARRIVE 业务时间更早却更晚上报 ----
    f["2026-11-01"] += [
        ev("E301", "WB003", "PICKUP", "SCANNER",
           "2026-11-01 06:00:00", "2026-11-01 06:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K002", **common),
        ev("E302", "WB003", "DEPART", "HUB",
           "2026-11-01 18:00:00", "2026-11-01 18:05:00",
           hub_code="HUB_SH", next_hub_code="HUB_GZ",
           customer_code="K002", **common),
        ev("E303", "WB003", "ARRIVE", "HUB",
           "2026-11-01 15:00:00", "2026-11-01 18:40:00",
           hub_code="HUB_GZ", customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E304", "WB003", "SIGN", "SCANNER",
           "2026-11-02 11:00:00", "2026-11-02 11:10:00",
           hub_code="HUB_GZ", courier_code="C002",
           customer_code="K002", **common),
    ]

    # ---- WB004 重复上报：E401 出现在两个落地文件中（不同 source_file）----
    f["2026-11-01"] += [
        ev("E401", "WB004", "PICKUP", "SCANNER",
           "2026-11-01 08:30:00", "2026-11-01 08:35:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001", **common),
    ]
    f["2026-11-02"] += [
        ev("E402", "WB004", "SIGN", "SCANNER",
           "2026-11-02 10:00:00", "2026-11-02 10:05:00",
           hub_code="HUB_SH", courier_code="C001",
           customer_code="K001", **common),
    ]

    # ---- WB005 多次异常 ----
    f["2026-11-01"] += [
        ev("E501", "WB005", "PICKUP", "SCANNER",
           "2026-11-01 08:00:00", "2026-11-01 08:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001",
           is_cold_chain=True, **common),
        ev("E502", "WB005", "EXCEPTION", "HUB",
           "2026-11-01 14:00:00", "2026-11-01 14:10:00",
           hub_code="HUB_SH", exception_code="EX_DAMAGE",
           exception_reason="外包装破损", customer_code="K001", **common),
    ]
    f["2026-11-02"] += [
        ev("E503", "WB005", "EXCEPTION", "SCANNER",
           "2026-11-02 09:00:00", "2026-11-02 09:05:00",
           hub_code="HUB_BJ", exception_code="EX_COLD_TEMP",
           exception_reason="冷链温度超标", customer_code="K001",
           is_cold_chain=True, **common),
        ev("E504", "WB005", "SIGN", "SCANNER",
           "2026-11-02 15:00:00", "2026-11-02 15:10:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
    ]

    # ---- WB006 缺失 ARRIVE，11-05 迟到补报 11-01 的节点 ----
    f["2026-11-01"] += [
        ev("E601", "WB006", "PICKUP", "SCANNER",
           "2026-11-01 08:00:00", "2026-11-01 08:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K002", **common),
        ev("E602", "WB006", "DEPART", "HUB",
           "2026-11-01 11:00:00", "2026-11-01 11:10:00",
           hub_code="HUB_SH", next_hub_code="HUB_BJ",
           customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E603", "WB006", "DELIVER", "SCANNER",
           "2026-11-02 16:00:00", "2026-11-02 16:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E604", "WB006", "SIGN", "SCANNER",
           "2026-11-02 18:00:00", "2026-11-02 18:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
    ]
    f["2026-11-05"] += [
        # 分拨系统夜间断连补传：业务时间 11-01，到达落地日 11-05
        ev("E605", "WB006", "ARRIVE", "HUB",
           "2026-11-01 17:00:00", "2026-11-05 09:00:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
    ]

    # ---- WB007 未知网点（维表未匹配）----
    f["2026-11-01"] += [
        ev("E701", "WB007", "PICKUP", "SCANNER",
           "2026-11-01 09:00:00", "2026-11-01 09:05:00",
           origin_hub_code="HUB_GHOST", hub_code="HUB_GHOST",
           courier_code="C001", customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E702", "WB007", "SIGN", "SCANNER",
           "2026-11-02 13:00:00", "2026-11-02 13:05:00",
           hub_code="HUB_GHOST", courier_code="C001",
           customer_code="K002", **common),
    ]

    # ---- WB009 时点关联：11-01 用旧网点名/旧片区/KA，11-03 用新版本 ----
    f["2026-11-01"] += [
        ev("E901", "WB009", "PICKUP", "SCANNER",
           "2026-11-01 10:00:00", "2026-11-01 10:05:00",
           origin_hub_code="HUB_BJ", hub_code="HUB_BJ",
           courier_code="C001", customer_code="K001", **common),
    ]
    f["2026-11-03"] += [
        ev("E902", "WB009", "ARRIVE", "HUB",
           "2026-11-03 09:00:00", "2026-11-03 09:10:00",
           hub_code="HUB_BJ", courier_code="C001",
           customer_code="K001", **common),
        ev("E903", "WB009", "SIGN", "SCANNER",
           "2026-11-03 15:00:00", "2026-11-03 15:05:00",
           hub_code="HUB_BJ", courier_code="C001",
           customer_code="K001", **common),
    ]

    # ---- WB010 合票：主单 WB010 承担干线与时效，成员 WB010A/WB010B 合入（G010）----
    f["2026-11-01"] += [
        ev("E1001", "WB010", "PICKUP", "SCANNER",
           "2026-11-01 08:00:00", "2026-11-01 08:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001", **common),
        ev("E1002", "WB010", "DEPART", "HUB",
           "2026-11-01 10:00:00", "2026-11-01 10:10:00",
           hub_code="HUB_SH", next_hub_code="HUB_BJ",
           customer_code="K001", **common),
        ev("E1003", "WB010", "ARRIVE", "HUB",
           "2026-11-01 20:00:00", "2026-11-01 20:15:00",
           hub_code="HUB_BJ", customer_code="K001", **common),
        ev("E1011", "WB010A", "PICKUP", "SCANNER",
           "2026-11-01 07:30:00", "2026-11-01 07:35:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001", **common),
        ev("E1021", "WB010B", "PICKUP", "SCANNER",
           "2026-11-01 09:00:00", "2026-11-01 09:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001", **common),
    ]
    f["2026-11-02"] += [
        ev("E1004", "WB010", "DELIVER", "SCANNER",
           "2026-11-02 14:00:00", "2026-11-02 14:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
        ev("E1005", "WB010", "SIGN", "SCANNER",
           "2026-11-02 16:00:00", "2026-11-02 16:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
        ev("E1012", "WB010A", "SIGN", "SCANNER",
           "2026-11-02 15:00:00", "2026-11-02 15:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
        ev("E1022", "WB010B", "SIGN", "SCANNER",
           "2026-11-02 15:30:00", "2026-11-02 15:35:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
    ]

    # ---- WB011 分票：主单走完干线后拆为 A/B/C 三个子单（G011，WEIGHT 30/20/50）----
    f["2026-11-01"] += [
        ev("E1101", "WB011", "PICKUP", "SCANNER",
           "2026-11-01 06:00:00", "2026-11-01 06:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K002", **common),
        ev("E1102", "WB011", "DEPART", "HUB",
           "2026-11-01 09:00:00", "2026-11-01 09:10:00",
           hub_code="HUB_SH", next_hub_code="HUB_BJ",
           customer_code="K002", **common),
        ev("E1103", "WB011", "ARRIVE", "HUB",
           "2026-11-01 21:00:00", "2026-11-01 21:15:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E1104", "WB011", "TRANSFER", "HUB",
           "2026-11-02 08:30:00", "2026-11-02 08:35:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
        ev("E1111", "WB011A", "DELIVER", "SCANNER",
           "2026-11-02 13:00:00", "2026-11-02 13:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E1112", "WB011A", "SIGN", "SCANNER",
           "2026-11-02 15:00:00", "2026-11-02 15:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E1121", "WB011B", "DELIVER", "SCANNER",
           "2026-11-02 14:00:00", "2026-11-02 14:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E1122", "WB011B", "SIGN", "SCANNER",
           "2026-11-02 16:00:00", "2026-11-02 16:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E1131", "WB011C", "DELIVER", "SCANNER",
           "2026-11-02 15:00:00", "2026-11-02 15:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E1132", "WB011C", "SIGN", "SCANNER",
           "2026-11-02 17:00:00", "2026-11-02 17:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
    ]

    # ---- WB012 退件重派：WB012 拒收后换新单 WB012R 重派（G012）----
    f["2026-11-01"] += [
        ev("E1201", "WB012", "PICKUP", "SCANNER",
           "2026-11-01 08:00:00", "2026-11-01 08:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K002", **common),
        ev("E1202", "WB012", "DEPART", "HUB",
           "2026-11-01 11:00:00", "2026-11-01 11:10:00",
           hub_code="HUB_SH", next_hub_code="HUB_BJ",
           customer_code="K002", **common),
        ev("E1203", "WB012", "ARRIVE", "HUB",
           "2026-11-01 20:00:00", "2026-11-01 20:15:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E1204", "WB012", "DELIVER", "SCANNER",
           "2026-11-02 10:00:00", "2026-11-02 10:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E1205", "WB012", "REJECT", "SCANNER",
           "2026-11-02 12:00:00", "2026-11-02 12:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           exception_code="EX_REJECT", exception_reason="收件人拒收",
           customer_code="K002", **common),
    ]
    f["2026-11-03"] += [
        ev("E1211", "WB012R", "DELIVER", "SCANNER",
           "2026-11-03 09:00:00", "2026-11-03 09:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E1212", "WB012R", "SIGN", "SCANNER",
           "2026-11-03 12:00:00", "2026-11-03 12:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
    ]

    # ---- WB013 关系事后修正：11-02 误挂成员 WB013X，11-04 修正为 WB013Y（G013）----
    f["2026-11-01"] += [
        ev("E1301", "WB013", "PICKUP", "SCANNER",
           "2026-11-01 10:00:00", "2026-11-01 10:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001", **common),
        ev("E1311", "WB013X", "PICKUP", "SCANNER",
           "2026-11-01 10:30:00", "2026-11-01 10:35:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001", **common),
        ev("E1321", "WB013Y", "PICKUP", "SCANNER",
           "2026-11-01 11:00:00", "2026-11-01 11:05:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           courier_code="C001", customer_code="K001", **common),
    ]
    f["2026-11-02"] += [
        ev("E1302", "WB013", "SIGN", "SCANNER",
           "2026-11-02 14:00:00", "2026-11-02 14:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
        ev("E1312", "WB013X", "SIGN", "SCANNER",
           "2026-11-02 14:30:00", "2026-11-02 14:35:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
        ev("E1322", "WB013Y", "SIGN", "SCANNER",
           "2026-11-02 15:00:00", "2026-11-02 15:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K001", **common),
    ]

    # ---- WB014 分票均摊：主单干线后拆 A/B 两个子单（G014，EQUAL）----
    f["2026-11-01"] += [
        ev("E1401", "WB014", "DEPART", "HUB",
           "2026-11-01 08:00:00", "2026-11-01 08:10:00",
           origin_hub_code="HUB_SH", hub_code="HUB_SH",
           next_hub_code="HUB_BJ", customer_code="K002", **common),
        ev("E1402", "WB014", "ARRIVE", "HUB",
           "2026-11-01 18:00:00", "2026-11-01 18:15:00",
           hub_code="HUB_BJ", customer_code="K002", **common),
    ]
    f["2026-11-02"] += [
        ev("E1411", "WB014A", "SIGN", "SCANNER",
           "2026-11-02 13:00:00", "2026-11-02 13:05:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
        ev("E1421", "WB014B", "SIGN", "SCANNER",
           "2026-11-02 13:30:00", "2026-11-02 13:35:00",
           hub_code="HUB_BJ", courier_code="C002",
           customer_code="K002", **common),
    ]
    return f


def rel(event_id, relation_id, rtype, change_type, parent, child, group_no,
        event_time, alloc_rule="NONE", alloc_weight=None, source="TMS"):
    """构造一条运单关系变更报文。"""
    return {
        "event_id": event_id, "relation_id": relation_id,
        "relation_type": rtype, "change_type": change_type,
        "parent_waybill_no": parent, "child_waybill_no": child,
        "group_no": group_no, "event_time": event_time,
        "alloc_rule": alloc_rule, "alloc_weight": alloc_weight,
        "source_system": source,
    }


def build_relation_files() -> dict[str, list[dict]]:
    """关系变更落地：按业务发生日分区（biz_date=to_date(event_time)）。"""
    rf: dict[str, list[dict]] = {
        "2026-11-02": [],
        "2026-11-04": [],
    }
    # 合票 G010：主单 WB010 与两个成员（每条父子边一个 relation_id）
    rf["2026-11-02"] += [
        rel("RE010_1", "REL010_01", "MERGE", "CREATE",
            "WB010", "WB010A", "G010", "2026-11-02 08:00:00"),
        rel("RE010_2", "REL010_02", "MERGE", "CREATE",
            "WB010", "WB010B", "G010", "2026-11-02 08:00:00"),
        # 兄弟边：WB010A 与 WB010B 互为兄弟（同组，用于验证跨关系类型去重）
        rel("RE015_1", "REL015", "SIBLING", "CREATE",
            "WB010A", "WB010B", "G010", "2026-11-02 08:05:00"),
        # 分票 G011：重量权重 30/20/50
        rel("RE011_1", "REL011_01", "SPLIT", "CREATE",
            "WB011", "WB011A", "G011", "2026-11-02 09:00:00",
            alloc_rule="WEIGHT", alloc_weight=30.0),
        rel("RE011_2", "REL011_02", "SPLIT", "CREATE",
            "WB011", "WB011B", "G011", "2026-11-02 09:00:00",
            alloc_rule="WEIGHT", alloc_weight=20.0),
        rel("RE011_3", "REL011_03", "SPLIT", "CREATE",
            "WB011", "WB011C", "G011", "2026-11-02 09:00:00",
            alloc_rule="WEIGHT", alloc_weight=50.0),
        # 退件重派 G012
        rel("RE012_1", "REL012_01", "RESEND_REASSIGN", "CREATE",
            "WB012", "WB012R", "G012", "2026-11-02 13:00:00"),
        # 合票 G013：误挂 WB013X（11-04 修正为 WB013Y）
        rel("RE013_1", "REL013", "MERGE", "CREATE",
            "WB013", "WB013X", "G013", "2026-11-02 08:00:00"),
        # 分票 G014：按子单数量均摊
        rel("RE014_1", "REL014_01", "SPLIT", "CREATE",
            "WB014", "WB014A", "G014", "2026-11-02 09:30:00",
            alloc_rule="EQUAL"),
        rel("RE014_2", "REL014_02", "SPLIT", "CREATE",
            "WB014", "WB014B", "G014", "2026-11-02 09:30:00",
            alloc_rule="EQUAL"),
    ]
    rf["2026-11-04"] += [
        # 事后修正：relation_id 不变，CORRECT 闭旧版本、开新版本
        rel("RE013_2", "REL013", "MERGE", "CORRECT",
            "WB013", "WB013Y", "G013", "2026-11-04 10:00:00"),
    ]
    return rf


def build_dim_files() -> dict[str, dict[str, list[dict]]]:
    return {
        "2026-10-31": {
            "hub": [
                {"hub_code": "HUB_SH", "hub_name": "上海华东分拨中心",
                 "hub_type": "HUB", "region_code": "EC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_BJ", "hub_name": "北京一集配站",
                 "hub_type": "HUB", "region_code": "NC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_GZ", "hub_name": "广州南分拨",
                 "hub_type": "HUB", "region_code": "SC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_OLD", "hub_name": "苏州老站",
                 "hub_type": "STATION", "region_code": "EC",
                 "parent_hub_code": "HUB_SH", "status": "ACTIVE",
                 "merged_into": None},
            ],
            "courier": [
                {"courier_code": "C001", "courier_name": "张伟",
                 "hub_code": "HUB_SH", "service_area": "浦东A片区", "status": "ACTIVE"},
                {"courier_code": "C002", "courier_name": "李娜",
                 "hub_code": "HUB_BJ", "service_area": "朝阳片区", "status": "ACTIVE"},
            ],
            "route": [
                {"route_code": "R001", "route_name": "沪京干线",
                 "origin_hub_code": "HUB_SH", "dest_hub_code": "HUB_BJ",
                 "transit_hubs": json.dumps(["HUB_GZ"], ensure_ascii=False),
                 "status": "ACTIVE"},
            ],
            "customer": [
                {"customer_code": "K001", "customer_name": "华美食品",
                 "customer_level": "KA", "sales_owner": "王销售", "status": "ACTIVE"},
                {"customer_code": "K002", "customer_name": "恒建商贸",
                 "customer_level": "SMB", "sales_owner": "赵销售", "status": "ACTIVE"},
            ],
        },
        # 11-02 组织调整：HUB_BJ 改名、HUB_OLD 合并撤销、C001 换片区、K001 降级
        "2026-11-02": {
            "hub": [
                {"hub_code": "HUB_SH", "hub_name": "上海华东分拨中心",
                 "hub_type": "HUB", "region_code": "EC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_BJ", "hub_name": "北京华北转运中心",
                 "hub_type": "HUB", "region_code": "NC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_GZ", "hub_name": "广州南分拨",
                 "hub_type": "HUB", "region_code": "SC",
                 "parent_hub_code": None, "status": "ACTIVE", "merged_into": None},
                {"hub_code": "HUB_OLD", "hub_name": "苏州老站",
                 "hub_type": "STATION", "region_code": "EC",
                 "parent_hub_code": "HUB_SH", "status": "MERGED",
                 "merged_into": "HUB_SH"},
            ],
            "courier": [
                {"courier_code": "C001", "courier_name": "张伟",
                 "hub_code": "HUB_SH", "service_area": "浦东B片区", "status": "ACTIVE"},
                {"courier_code": "C002", "courier_name": "李娜",
                 "hub_code": "HUB_BJ", "service_area": "朝阳片区", "status": "ACTIVE"},
            ],
            "route": [
                {"route_code": "R001", "route_name": "沪京干线",
                 "origin_hub_code": "HUB_SH", "dest_hub_code": "HUB_BJ",
                 "transit_hubs": json.dumps(["HUB_GZ"], ensure_ascii=False),
                 "status": "ACTIVE"},
            ],
            "customer": [
                {"customer_code": "K001", "customer_name": "华美食品",
                 "customer_level": "STD", "sales_owner": "王销售",
                 "status": "ACTIVE"},
                {"customer_code": "K002", "customer_name": "恒建商贸",
                 "customer_level": "SMB", "sales_owner": "赵销售",
                 "status": "ACTIVE"},
            ],
        },
    }


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    args = ap.parse_args()
    root = Path(args.root)

    for sub in ["runtime/warehouse", "data/landing/events",
                "data/landing/dim", "data/landing/relations",
                "data/backfill"]:
        p = root / sub
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    # 重复上报：E401 再次出现在 scanner_retry 文件（不同 source_file）
    event_files = build_event_files()
    for biz_date, rows in event_files.items():
        write_jsonl(root / f"data/landing/events/biz_date={biz_date}/scanner_hub.jsonl",
                    rows)
    dup_row = next(r for r in event_files["2026-11-01"] if r["event_id"] == "E401")
    write_jsonl(root / "data/landing/events/biz_date=2026-11-01/scanner_retry.jsonl",
                [dup_row])

    # 运单拆合关系变更落地（合票/分票/退件重派/兄弟/事后修正/均摊）
    relation_files = build_relation_files()
    for biz_date, rows in relation_files.items():
        write_jsonl(
            root / f"data/landing/relations/biz_date={biz_date}/tms_relation.jsonl",
            rows)

    for snap_date, dims in build_dim_files().items():
        for dim_name, rows in dims.items():
            write_jsonl(
                root / f"data/landing/dim/biz_date={snap_date}/{dim_name}.jsonl",
                rows)

    conf = f"""[DEFAULT]
warehouse.dir={root}/runtime/warehouse
catalog.name=ltl
catalog.type=jdbc
catalog.uri=
jdbc.user=sa
jdbc.password=
catalog.impl=org.apache.iceberg.spark.SparkCatalog
landing.event.dir={root}/data/landing/events
landing.dim.dir={root}/data/landing/dim
landing.relation.dir={root}/data/landing/relations
watermark.lookback.days=30
dq.threshold.event_missing_rate=0.02
dq.threshold.out_of_order_rate=0.05
dq.threshold.duplicate_rate=0.01
dq.threshold.dim_unmatched_rate=0.01
dq.threshold.partition_latency_minutes=180
backfill.shadow.suffix=_shadow
backfill.output.dir={root}/data/backfill
"""
    (root / "conf/pipeline.local.conf").write_text(conf, encoding="utf-8")
    print(f"[fixtures] root={root} 事件落地日={list(event_files)} "
          f"维表快照日={list(build_dim_files())}")


if __name__ == "__main__":
    main()
