"""分层顺序日志存储。

研究仪器把一次运行的事件按分区追加进顺序日志，存储服务为每条事件返回
稳定位置；同一请求重送（相同 id）不会得到新位置。已关闭的段在热盘紧张
时迁往冷盘：冷端先提交复制回执，服务核对完整范围、内容以及位置对应的
事件，核对通过后冷副本才接替读取，热副本才允许移除。复制结束、核对通过、
接替读取是三个不同事实。读者只面对逻辑位置：换层、重启、冷端暂时损坏
都不改变已确认位置的读取语义；保留期回收与冷端故障都会明确报告缺口，
绝不拿后续位置填前面的洞。

本地布局：

    hot/manifest.json        全部元数据（分区、段、会话、配置）
    hot/active/<分区>.jsonl   进行中分段（未关闭的追加）
    hot/segments/<段>.jsonl   已关闭段的热副本
    cold/objects/<段>.jsonl   冷端归档对象（仿真冷盘）
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# ---- 段状态 ----------------------------------------------------------------
SEALED = "sealed"        # 已关闭，只有热副本
COPIED = "copied"        # 冷端已报告，但核对未通过，热副本仍是唯一完整来源
VERIFIED = "verified"    # 冷副本核对通过，可以接替读取
COLD_ONLY = "cold_only"  # 热副本已移除，只剩冷副本
EXPIRED = "expired"      # 保留期回收，副本已清除

# ---- 错误码 ----------------------------------------------------------------
INVALID = "invalid"
UNKNOWN_PARTITION = "unknown_partition"
UNKNOWN_SEGMENT = "unknown_segment"
UNKNOWN_CLIENT = "unknown_client"
SESSION_EXPIRED = "session_expired"
HOT_FULL = "hot_full"
NOT_DURABLE = "not_durable"
GAP = "gap"
RANGE_UNREADABLE = "range_unreadable"


class StoreError(Exception):
    """结构化存储错误：code 供程序判断，message 与 details 供人排查。"""

    def __init__(self, code, message, **details):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


def _check_name(kind, value):
    if (
        not isinstance(value, str)
        or not value
        or "/" in value
        or value in {".", ".."}
        or value.startswith(".")
    ):
        raise StoreError(INVALID, f"{kind}名 {value!r} 不能用于本地目录")
    return value


def _check_int(value, label, minimum=0):
    if type(value) is not int or value < minimum:
        raise StoreError(INVALID, f"{label}应为不小于 {minimum} 的整数")
    return value


class TieredLogStore:
    """可反复使用的分层日志存储入口。

    热目录保存进行中的分段与尚未被冷端接替的已关闭段；冷目录保存归档
    对象。全部元数据持久化在热目录 manifest.json，重启后按已核对的事实
    恢复，不把未切完的归档当当前副本。hot_capacity_bytes 给热盘设上限，
    写满时暂停新追加，但读、换层与已核对段的清理不受影响。
    """

    def __init__(self, hot_dir, cold_dir, hot_capacity_bytes=None):
        self.hot_dir = Path(hot_dir)
        self.cold_dir = Path(cold_dir)
        self._active_dir = self.hot_dir / "active"
        self._segment_dir = self.hot_dir / "segments"
        self._object_dir = self.cold_dir / "objects"
        for directory in (self._active_dir, self._segment_dir, self._object_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self._manifest_path = self.hot_dir / "manifest.json"
        if self._manifest_path.exists():
            self._load()
            if hot_capacity_bytes is not None:
                self._config["hot_capacity_bytes"] = hot_capacity_bytes
                self._save()
        else:
            self._config = {"hot_capacity_bytes": hot_capacity_bytes}
            self.now = 0
            self._partitions = {}
            self._segments = {}
            self._sessions = {}
            self._save()

    # ------------------------------------------------------------------
    # 持久化
    # ------------------------------------------------------------------

    def _load(self):
        data = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        self._config = data["config"]
        self.now = data["now"]
        self._partitions = data["partitions"]
        self._segments = data["segments"]
        self._sessions = data["sessions"]

    def _save(self):
        payload = {
            "config": self._config,
            "now": self.now,
            "partitions": self._partitions,
            "segments": self._segments,
            "sessions": self._sessions,
        }
        tmp = self._manifest_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp, self._manifest_path)

    def restart(self):
        """重启服务：从清单恢复，已核对的来源关系保持不变。"""
        before = self.now
        self._load()
        self.now = max(before, self.now)
        self._save()
        return {"restarted": True, "partitions": sorted(self._partitions)}

    # ------------------------------------------------------------------
    # 本地文件
    # ------------------------------------------------------------------

    def _active_path(self, partition):
        return self._active_dir / f"{partition}.jsonl"

    def _segment_hot_path(self, segment):
        return self._segment_dir / f"{segment}.jsonl"

    def _object_path(self, segment):
        return self._object_dir / f"{segment}.jsonl"

    @staticmethod
    def _dump_record(position, record_id, text):
        row = {"position": position, "id": record_id, "text": text}
        return json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"

    def _read_records(self, path):
        records = {}
        if path.exists():
            with path.open(encoding="utf-8") as stream:
                for line in stream:
                    line = line.strip()
                    if line:
                        row = json.loads(line)
                        records[row["position"]] = row
        return records

    def _write_records(self, path, rows):
        """按位置升序写文件，返回 {位置: [偏移, 字节数]}。"""
        ranges = {}
        offset = 0
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as stream:
            for row in sorted(rows, key=lambda item: item["position"]):
                line = self._dump_record(row["position"], row.get("id"), row["text"])
                stream.write(line)
                size = len(line.encode("utf-8"))
                ranges[str(row["position"])] = [offset, size]
                offset += size
        os.replace(tmp, path)
        return ranges

    def _hot_usage(self):
        total = 0
        for directory in (self._active_dir, self._segment_dir):
            for entry in directory.glob("*.jsonl"):
                total += entry.stat().st_size
        return total

    # ------------------------------------------------------------------
    # 内部查询
    # ------------------------------------------------------------------

    def _partition(self, name, create=False):
        _check_name("分区", name)
        partition = self._partitions.get(name)
        if partition is None:
            if not create:
                raise StoreError(UNKNOWN_PARTITION, f"分区 {name} 不存在")
            partition = {
                "next_position": 1,
                "durable_through": 0,
                "expired_through": 0,
                "last_sealed_end": 0,
                "ids": {},
            }
            self._partitions[name] = partition
        return partition

    def _segment(self, name):
        _check_name("段", name)
        segment = self._segments.get(name)
        if segment is None:
            raise StoreError(UNKNOWN_SEGMENT, f"段 {name} 不存在")
        return segment

    def _session(self, client):
        session = self._sessions.get(client)
        if session is None:
            raise StoreError(UNKNOWN_CLIENT, f"读者 {client} 未打开")
        if self.now > session["valid_until"]:
            raise StoreError(
                SESSION_EXPIRED,
                f"读者 {client} 的会话已于 {session['valid_until']} 到期，读取不再保留",
            )
        return session

    def _segment_covering(self, partition, position):
        for seg_id, segment in self._segments.items():
            span = segment["span"]
            if segment["partition"] == partition and span[0] <= position <= span[1]:
                return seg_id, segment
        return None, None

    # ------------------------------------------------------------------
    # 写入路径
    # ------------------------------------------------------------------

    def append(self, partition, record_id, text):
        """按分区追加事件，返回稳定位置；相同 id 重送返回原位置。"""
        if not isinstance(record_id, str) or not record_id:
            raise StoreError(INVALID, "追加需要非空字符串 id")
        if not isinstance(text, str):
            raise StoreError(INVALID, "追加内容应为字符串")
        part = self._partition(partition, create=True)
        known = part["ids"].get(record_id)
        if known is not None:
            return {"position": known, "duplicate": True}
        position = part["next_position"]
        line = self._dump_record(position, record_id, text)
        size = len(line.encode("utf-8"))
        capacity = self._config["hot_capacity_bytes"]
        usage = self._hot_usage()
        if capacity is not None and usage + size > capacity:
            raise StoreError(
                HOT_FULL,
                "热盘已满，新追加暂停；读、换层与已核对段的清理不受影响",
                usage=usage,
                capacity=capacity,
            )
        with self._active_path(partition).open("a", encoding="utf-8") as stream:
            stream.write(line)
        part["next_position"] = position + 1
        part["ids"][record_id] = position
        self._save()
        return {"position": position, "duplicate": False}

    def durable(self, partition, through):
        """登记持久边界：through（含）之前的位置已持久，可参与归档。"""
        part = self._partition(partition)
        written = part["next_position"] - 1
        _check_int(through, "持久边界")
        if through > written:
            raise StoreError(INVALID, f"持久边界 {through} 超出已写入位置 {written}")
        if through < part["durable_through"]:
            raise StoreError(INVALID, "持久边界不能倒退")
        part["durable_through"] = through
        self._save()
        return {"durable_through": through}

    def seal(self, partition, segment, span):
        """关闭指定位置范围为段。只影响本分区，其它分区照常追加。"""
        part = self._partition(partition)
        _check_name("段", segment)
        if segment in self._segments:
            raise StoreError(INVALID, f"段名 {segment} 已使用")
        if (
            not isinstance(span, list)
            or len(span) != 2
            or any(type(value) is not int for value in span)
        ):
            raise StoreError(INVALID, "段区间应为 [首, 尾] 两个整数")
        first, last = span
        expect = part["last_sealed_end"] + 1
        if first != expect or last < first:
            raise StoreError(INVALID, f"段必须从首个未关闭位置 {expect} 开始且首尾有序")
        written = part["next_position"] - 1
        if last > written:
            raise StoreError(INVALID, f"关闭范围超出已写入位置 {written}")
        active = self._read_records(self._active_path(partition))
        sealed_rows = []
        for position in range(first, last + 1):
            row = active.get(position)
            if row is None:
                raise StoreError(INVALID, f"位置 {position} 不在进行中分段里")
            sealed_rows.append(row)
        self._write_records(self._segment_hot_path(segment), sealed_rows)
        remaining = [row for position, row in active.items() if position > last]
        self._write_records(self._active_path(partition), remaining)
        part["last_sealed_end"] = last
        self._segments[segment] = {
            "partition": partition,
            "span": [first, last],
            "state": SEALED,
            "copied": 0,
            "verification": "冷端尚未报告",
            "cold_fault": False,
            "cold_ranges": {},
        }
        self._save()
        return {"segment": segment, "span": [first, last], "state": SEALED}

    # ------------------------------------------------------------------
    # 换层：复制报告、核对、热副本移除、冷端故障
    # ------------------------------------------------------------------

    def register_copy(self, segment, records):
        """登记冷端复制回执并核对。

        回执给出段内每条位置及内容；服务把报告落成冷端对象并记录字节
        范围，再对照热端原记录核对：范围必须完整覆盖段区间、每个位置的
        内容必须与原来对应的事件一致。核对通过才进入 VERIFIED。
        """
        seg = self._segment(segment)
        if seg["state"] in (COLD_ONLY, EXPIRED):
            raise StoreError(INVALID, f"段 {segment} 处于 {seg['state']}，不再接受归档报告")
        part = self._partitions[seg["partition"]]
        first, last = seg["span"]
        if part["durable_through"] < last:
            raise StoreError(
                NOT_DURABLE,
                f"段 {segment} 到位置 {last} 未达到持久边界"
                f"（当前 {part['durable_through']}），不能归档",
                durable_through=part["durable_through"],
                needed=last,
            )
        if not isinstance(records, list):
            raise StoreError(INVALID, "复制报告应为记录列表")
        rows = []
        for item in records:
            if (
                not isinstance(item, list)
                or len(item) != 2
                or type(item[0]) is not int
                or not isinstance(item[1], str)
            ):
                raise StoreError(INVALID, "复制报告的每条记录应为 [位置, 内容]")
            rows.append({"position": item[0], "id": None, "text": item[1]})
        ranges = self._write_records(self._object_path(segment), rows)
        seg["cold_ranges"] = ranges
        seg["cold_fault"] = False
        expected = list(range(first, last + 1))
        received = [row["position"] for row in rows]
        original = self._read_records(self._segment_hot_path(segment))
        problems = []
        if received != expected:
            if received != sorted(received) or len(set(received)) != len(received):
                problems.append("报告位置乱序或重复")
            missing = [pos for pos in expected if pos not in received]
            extra = [pos for pos in received if pos not in expected]
            if missing:
                problems.append(f"缺少位置 {missing}")
            if extra:
                problems.append(f"多出位置 {extra}")
        for row in rows:
            source = original.get(row["position"])
            if source is None:
                problems.append(f"位置 {row['position']} 在热端原记录中不存在")
            elif source["text"] != row["text"]:
                problems.append(f"位置 {row['position']} 的内容与热端原记录不符")
        seg["copied"] = len(set(received) & set(expected))
        if problems:
            seg["state"] = COPIED
            seg["verification"] = "；".join(problems)
        else:
            seg["state"] = VERIFIED
            seg["verification"] = "ok"
        self._save()
        return {
            "segment": segment,
            "state": seg["state"],
            "verified": not problems,
            "copied": seg["copied"],
            "expected": len(expected),
            "verification": seg["verification"],
            "byte_ranges": ranges,
        }

    def _removal_block(self, seg_id, seg):
        """热副本不可移除的原因；可以移除时返回 None。"""
        state = seg["state"]
        if state == COLD_ONLY:
            return "热副本已移除"
        if state == EXPIRED:
            return "段已被保留期回收"
        if state == SEALED:
            return "冷端尚未报告归档，热段是唯一完整来源"
        if state == COPIED:
            return f"冷副本未通过核对（{seg['verification']}），热段仍是唯一完整来源"
        if seg["cold_fault"]:
            return "冷端对象当前不可读，热段仍是唯一完整来源"
        if not self._object_path(seg_id).exists():
            return "冷端对象缺失，热段仍是唯一完整来源"
        return None

    def evict(self, segment):
        """运维请求移除热副本。只有冷副本核对通过且当前可读才放行。"""
        seg = self._segment(segment)
        block = self._removal_block(segment, seg)
        if block is not None:
            return {"evicted": False, "reason": block}
        path = self._segment_hot_path(segment)
        freed = path.stat().st_size if path.exists() else 0
        path.unlink(missing_ok=True)
        seg["state"] = COLD_ONLY
        self._save()
        return {"evicted": True, "freed_bytes": freed}

    def fault(self, segment):
        """仿真冷端已归档对象不可读。"""
        seg = self._segment(segment)
        if seg["state"] in (SEALED, EXPIRED):
            raise StoreError(INVALID, f"段 {segment} 没有冷端对象")
        seg["cold_fault"] = True
        self._save()
        return {"fault": segment, "state": seg["state"]}

    # ------------------------------------------------------------------
    # 读取路径
    # ------------------------------------------------------------------

    def open_reader(self, client, partition, next_position, valid_until):
        """打开读会话：从 next 读到打开时已持久的上界，上界此后不挪动。

        起始位置已被保留期回收时明确报出最早可用位置与原因，不跳过。
        同名 client 重新打开会替换旧会话（续租）。
        """
        if not isinstance(client, str) or not client:
            raise StoreError(INVALID, "读者需要非空 client 名")
        part = self._partition(partition, create=True)
        _check_int(next_position, "起始位置", minimum=1)
        _check_int(valid_until, "会话期限")
        if valid_until < self.now:
            raise StoreError(INVALID, f"会话期限 {valid_until} 早于当前时刻 {self.now}")
        earliest = part["expired_through"] + 1
        if next_position < earliest:
            raise StoreError(
                GAP,
                f"位置 {next_position} 已被保留期回收",
                earliest_available=earliest,
                gap_reason=f"≤ {part['expired_through']} 的位置已按保留期回收",
            )
        upper = part["durable_through"]
        self._sessions[client] = {
            "partition": partition,
            "acked": next_position - 1,
            "upper": upper,
            "valid_until": valid_until,
            "opened_at": self.now,
        }
        self._save()
        return {
            "client": client,
            "partition": partition,
            "from": next_position,
            "upper": upper,
            "valid_until": valid_until,
        }

    def read(self, client, limit):
        """取下一批：从最近确认位置之后开始，最多 limit 条。

        未确认的批次重复返回同一批位置；换层后按原位置续读，读者不需要
        知道物理偏移。范围内某段不可读时整个读取报错并指明段区间，绝不
        拿后续位置填前面的缺口。
        """
        session = self._session(client)
        _check_int(limit, "批量上限", minimum=1)
        start = session["acked"] + 1
        end = min(start + limit - 1, session["upper"])
        records = []
        sources = []
        for position in range(start, end + 1):
            text, source = self._read_position(session["partition"], position)
            records.append([position, text])
            if source not in sources:
                sources.append(source)
        return {"records": records, "from": start, "upper": session["upper"], "served_from": sources}

    def ack(self, client, through):
        """确认 through（含）之前的位置；确认后下一批从 through+1 开始。"""
        session = self._session(client)
        _check_int(through, "确认位置")
        if through > session["upper"]:
            raise StoreError(
                INVALID, f"确认位置 {through} 超出读取范围上界 {session['upper']}"
            )
        if through > session["acked"]:
            session["acked"] = through
            self._save()
        return {"acked": session["acked"]}

    def _read_position(self, partition, position):
        """按逻辑位置读取，返回 (内容, 来源层)。"""
        part = self._partitions[partition]
        if position <= part["expired_through"]:
            raise StoreError(
                GAP,
                f"位置 {position} 已被保留期回收",
                earliest_available=part["expired_through"] + 1,
                gap_reason=f"≤ {part['expired_through']} 的位置已按保留期回收",
            )
        if position > part["last_sealed_end"]:
            active = self._read_records(self._active_path(partition))
            row = active.get(position)
            if row is None:
                raise StoreError(INVALID, f"位置 {position} 尚未写入")
            return row["text"], "hot"
        seg_id, seg = self._segment_covering(partition, position)
        if seg is None:
            raise StoreError(
                GAP,
                f"位置 {position} 不属于任何已关闭的段",
                earliest_available=position,
                gap_reason="段信息缺失",
            )
        state = seg["state"]
        if state in (SEALED, COPIED):
            return self._read_hot(seg_id, seg, position), "hot"
        if state == VERIFIED:
            if seg["cold_fault"]:
                return self._read_hot(seg_id, seg, position), "hot_fallback"
            text = self._read_cold(seg_id, position)
            if text is None:
                return self._read_hot(seg_id, seg, position), "hot_fallback"
            return text, "cold"
        if state == COLD_ONLY:
            if seg["cold_fault"]:
                raise StoreError(
                    RANGE_UNREADABLE,
                    f"段 {seg_id} 范围 {seg['span']} 的冷副本当前不可读，热副本已移除",
                    segment=seg_id,
                    span=seg["span"],
                )
            text = self._read_cold(seg_id, position)
            if text is None:
                raise StoreError(
                    RANGE_UNREADABLE,
                    f"段 {seg_id} 范围 {seg['span']} 的冷端对象缺失，热副本已移除",
                    segment=seg_id,
                    span=seg["span"],
                )
            return text, "cold"
        raise StoreError(
            GAP,
            f"位置 {position} 所在段 {seg_id} 已回收",
            earliest_available=part["expired_through"] + 1,
            gap_reason="段已按保留期回收",
        )

    def _read_hot(self, seg_id, seg, position):
        row = self._read_records(self._segment_hot_path(seg_id)).get(position)
        if row is None:
            raise StoreError(
                RANGE_UNREADABLE,
                f"段 {seg_id} 的热副本缺少位置 {position}",
                segment=seg_id,
                span=seg["span"],
            )
        return row["text"]

    def _read_cold(self, seg_id, position):
        row = self._read_records(self._object_path(seg_id)).get(position)
        return None if row is None else row["text"]

    # ------------------------------------------------------------------
    # 保留期回收
    # ------------------------------------------------------------------

    def expire(self, partition, through):
        """按保留期回收 ≤ through 的位置。

        仍在会话期限内的读者钉住自己未确认的位置，回收边界只推进到
        钉住位置之前；已过期却挂着的会话不参与钉住，只列入回执。
        """
        part = self._partition(partition)
        _check_int(through, "回收边界")
        target = min(through, part["next_position"] - 1)
        holding = []
        idle = []
        for client, session in self._sessions.items():
            if session["partition"] != partition:
                continue
            if session["valid_until"] >= self.now:
                holding.append((client, session))
            else:
                idle.append(client)
        blocked_by = []
        if holding:
            floor = min(session["acked"] + 1 for _, session in holding)
            if target >= floor:
                blocked_by = [
                    {"client": client, "needs_from": session["acked"] + 1}
                    for client, session in holding
                    if session["acked"] + 1 <= target
                ]
                target = floor - 1
        reclaimed = []
        if target > part["expired_through"]:
            part["expired_through"] = target
            for seg_id, seg in self._segments.items():
                if (
                    seg["partition"] == partition
                    and seg["state"] != EXPIRED
                    and seg["span"][1] <= target
                ):
                    self._segment_hot_path(seg_id).unlink(missing_ok=True)
                    self._object_path(seg_id).unlink(missing_ok=True)
                    seg["state"] = EXPIRED
                    seg["cold_ranges"] = {}
                    reclaimed.append(seg_id)
            active = self._read_records(self._active_path(partition))
            remaining = [row for pos, row in active.items() if pos > target]
            if len(remaining) != len(active):
                self._write_records(self._active_path(partition), remaining)
        self._save()
        return {
            "requested": through,
            "expired_through": part["expired_through"],
            "earliest_available": part["expired_through"] + 1,
            "blocked_by": blocked_by,
            "reclaimed_segments": reclaimed,
            "idle_sessions_ignored": idle,
        }

    # ------------------------------------------------------------------
    # 运维视图
    # ------------------------------------------------------------------

    def inspect(self, partition=None):
        """运维视图：哪段占热盘、冷端核对到哪里、为什么可删或不可删。"""
        if partition is not None:
            self._partition(partition)
            names = [partition]
        else:
            names = sorted(self._partitions)
        capacity = self._config["hot_capacity_bytes"]
        usage = self._hot_usage()
        return {
            "now": self.now,
            "hot": {
                "used_bytes": usage,
                "capacity_bytes": capacity,
                "appends_paused": capacity is not None and usage >= capacity,
            },
            "partitions": {name: self._partition_view(name) for name in names},
        }

    def _partition_view(self, name):
        part = self._partitions[name]
        segments = [
            self._segment_view(seg_id, seg)
            for seg_id, seg in self._segments.items()
            if seg["partition"] == name
        ]
        segments.sort(key=lambda item: item["span"][0])
        sessions = []
        for client, session in self._sessions.items():
            if session["partition"] != name:
                continue
            active = session["valid_until"] >= self.now
            sessions.append(
                {
                    "client": client,
                    "next_unacked": session["acked"] + 1,
                    "upper": session["upper"],
                    "valid_until": session["valid_until"],
                    "status": "active" if active else "expired_idle",
                    "pins_from": session["acked"] + 1 if active else None,
                }
            )
        active_path = self._active_path(name)
        return {
            "next_position": part["next_position"],
            "durable_through": part["durable_through"],
            "expired_through": part["expired_through"],
            "earliest_available": part["expired_through"] + 1,
            "active_segment": {
                "first_position": part["last_sealed_end"] + 1,
                "records": len(self._read_records(active_path)),
                "bytes": active_path.stat().st_size if active_path.exists() else 0,
                "location": "hot",
            },
            "segments": segments,
            "sessions": sessions,
        }

    def _segment_view(self, seg_id, seg):
        state = seg["state"]
        location = {
            SEALED: "hot",
            COPIED: "hot",
            VERIFIED: "both",
            COLD_ONLY: "cold",
            EXPIRED: "none",
        }[state]
        if state in (SEALED, COPIED):
            serving = "hot"
        elif state == VERIFIED:
            serving = "hot_fallback" if seg["cold_fault"] else "cold"
        elif state == COLD_ONLY:
            serving = "unreadable" if seg["cold_fault"] else "cold"
        else:
            serving = "none"
        block = self._removal_block(seg_id, seg)
        hot_path = self._segment_hot_path(seg_id)
        cold_path = self._object_path(seg_id)
        span_len = seg["span"][1] - seg["span"][0] + 1
        return {
            "segment": seg_id,
            "span": seg["span"],
            "state": state,
            "location": location,
            "hot_bytes": hot_path.stat().st_size if hot_path.exists() else 0,
            "cold_bytes": cold_path.stat().st_size if cold_path.exists() else 0,
            "copied": f"{seg['copied']}/{span_len}",
            "verification": seg["verification"],
            "cold_fault": seg["cold_fault"],
            "serving": serving,
            "cold_ranges": seg["cold_ranges"],
            "removable": block is None,
            "removal_reason": "冷副本已核对，可移除热副本" if block is None else block,
        }

    # ------------------------------------------------------------------
    # 演练消息入口
    # ------------------------------------------------------------------

    def handle(self, message):
        """处理一条演练消息，返回结构化回执（不抛 StoreError）。"""
        kind = message.get("kind")
        record = message.get("record") or {}
        at = message.get("at")
        if type(at) is int:
            self.now = at
        try:
            if kind == "append":
                out = self.append(record["partition"], record["id"], record["text"])
            elif kind == "durable":
                out = self.durable(record["partition"], record["through"])
            elif kind == "seal":
                out = self.seal(record["partition"], record["segment"], record["span"])
            elif kind == "reader":
                out = self.open_reader(
                    record["client"], record["partition"], record["next"], record["valid_until"]
                )
            elif kind == "read":
                out = self.read(record["client"], record["limit"])
            elif kind == "ack":
                out = self.ack(record["client"], record["through"])
            elif kind == "copy":
                out = self.register_copy(record["segment"], record["records"])
            elif kind == "evict":
                out = self.evict(record["segment"])
            elif kind == "fault":
                out = self.fault(record["segment"])
            elif kind == "expire":
                out = self.expire(record["partition"], record["through"])
            elif kind == "restart":
                out = self.restart()
            elif kind == "inspect":
                out = self.inspect(record.get("partition"))
            else:
                raise StoreError(INVALID, f"未登记的消息名称 {kind!r}")
        except StoreError as error:
            return {"ok": False, "error": error.code, "reason": error.message, **error.details}
        return {"ok": True, **out}
