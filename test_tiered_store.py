"""分层日志存储的行为检验：稳定位置、换层、续读、回收与运维视图。"""

import json
import tempfile
import unittest
from pathlib import Path

from scenario_driver import run_scenario
from tiered_store import (
    COLD_ONLY,
    GAP,
    HOT_FULL,
    NOT_DURABLE,
    RANGE_UNREADABLE,
    SEALED,
    SESSION_EXPIRED,
    VERIFIED,
    StoreError,
    TieredLogStore,
)


def drive(store, at, kind, **record):
    return store.handle({"at": at, "kind": kind, "record": record})


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.hot = root / "hot"
        self.cold = root / "cold"
        self.store = TieredLogStore(self.hot, self.cold)

    def reopen(self, **kwargs):
        self.store = TieredLogStore(self.hot, self.cold, **kwargs)
        return self.store

    def seed(self, partition, count, prefix="m"):
        """追加 count 条并持久到该位置，返回位置列表。"""
        positions = []
        for index in range(1, count + 1):
            receipt = drive(self.store, index, "append", partition=partition,
                            id=f"{prefix}{index}", text=f"{prefix}-{index}")
            self.assertTrue(receipt["ok"])
            positions.append(receipt["position"])
        drive(self.store, count + 1, "durable", partition=partition, through=count)
        return positions


class AppendTests(StoreTestCase):
    def test_positions_are_stable_and_resend_dedupes(self):
        first = drive(self.store, 0, "append", partition="a", id="x", text="first")
        second = drive(self.store, 1, "append", partition="a", id="y", text="second")
        self.assertEqual((first["position"], second["position"]), (1, 2))
        again = drive(self.store, 2, "append", partition="a", id="x", text="changed")
        self.assertEqual(again, {"ok": True, "position": 1, "duplicate": True})
        view = self.store.inspect("a")["partitions"]["a"]
        self.assertEqual(view["next_position"], 3)

    def test_resend_after_restart_keeps_original_position(self):
        drive(self.store, 0, "append", partition="a", id="x", text="first")
        self.reopen()
        again = drive(self.store, 1, "append", partition="a", id="x", text="first")
        self.assertEqual(again["position"], 1)
        self.assertTrue(again["duplicate"])
        view = self.store.inspect("a")["partitions"]["a"]
        self.assertEqual(view["next_position"], 2)

    def test_seal_is_per_partition_and_appends_continue(self):
        self.seed("a", 1)
        self.seed("b", 1)
        drive(self.store, 5, "seal", partition="a", segment="a1", span=[1, 1])
        more_a = drive(self.store, 6, "append", partition="a", id="a2", text="a-2")
        more_b = drive(self.store, 7, "append", partition="b", id="b2", text="b-2")
        self.assertEqual(more_a["position"], 2)
        self.assertEqual(more_b["position"], 2)
        view = self.store.inspect()["partitions"]
        self.assertEqual(view["a"]["segments"][0]["state"], SEALED)
        self.assertEqual(view["b"]["segments"], [])


class ArchiveTests(StoreTestCase):
    def test_copy_requires_durable_boundary(self):
        self.seed("p", 2)
        drive(self.store, 5, "seal", partition="p", segment="p1", span=[1, 2])
        # 重新把持久边界收回 1，模拟只持久了一半
        self.store._partitions["p"]["durable_through"] = 1
        receipt = drive(self.store, 6, "copy", segment="p1",
                        records=[[1, "m-1"], [2, "m-2"]])
        self.assertFalse(receipt["ok"])
        self.assertEqual(receipt["error"], NOT_DURABLE)
        drive(self.store, 7, "durable", partition="p", through=2)
        receipt = drive(self.store, 8, "copy", segment="p1",
                        records=[[1, "m-1"], [2, "m-2"]])
        self.assertTrue(receipt["verified"])

    def test_incomplete_copy_keeps_hot_as_sole_source(self):
        self.seed("p", 2)
        drive(self.store, 5, "seal", partition="p", segment="p1", span=[1, 2])
        partial = drive(self.store, 6, "copy", segment="p1", records=[[1, "m-1"]])
        self.assertFalse(partial["verified"])
        self.assertEqual((partial["copied"], partial["expected"]), (1, 2))
        refused = drive(self.store, 7, "evict", segment="p1")
        self.assertFalse(refused["evicted"])
        self.assertIn("唯一完整来源", refused["reason"])
        complete = drive(self.store, 8, "copy", segment="p1",
                         records=[[1, "m-1"], [2, "m-2"]])
        self.assertTrue(complete["verified"])
        self.assertEqual(sorted(complete["byte_ranges"]), ["1", "2"])
        done = drive(self.store, 9, "evict", segment="p1")
        self.assertTrue(done["evicted"])
        self.assertGreater(done["freed_bytes"], 0)

    def test_copy_with_wrong_content_is_rejected(self):
        self.seed("p", 2)
        drive(self.store, 5, "seal", partition="p", segment="p1", span=[1, 2])
        receipt = drive(self.store, 6, "copy", segment="p1",
                        records=[[1, "m-1"], [2, "tampered"]])
        self.assertFalse(receipt["verified"])
        self.assertIn("不符", receipt["verification"])
        view = self.store.inspect("p")["partitions"]["p"]["segments"][0]
        self.assertFalse(view["removable"])

    def test_unfinished_archive_is_not_current_copy_after_restart(self):
        self.seed("p", 2)
        drive(self.store, 5, "seal", partition="p", segment="p1", span=[1, 2])
        drive(self.store, 6, "copy", segment="p1", records=[[1, "m-1"]])
        self.reopen()
        view = self.store.inspect("p")["partitions"]["p"]["segments"][0]
        self.assertEqual(view["state"], "copied")
        self.assertEqual(view["serving"], "hot")
        self.assertFalse(view["removable"])
        drive(self.store, 7, "reader", client="r", partition="p", next=1, valid_until=50)
        batch = drive(self.store, 8, "read", client="r", limit=2)
        self.assertEqual(batch["served_from"], ["hot"])
        refused = drive(self.store, 9, "evict", segment="p1")
        self.assertFalse(refused["evicted"])


class ReadTests(StoreTestCase):
    def test_read_range_is_snapshot_of_open_time(self):
        self.seed("p", 2)
        opened = drive(self.store, 5, "reader", client="r", partition="p",
                       next=1, valid_until=50)
        self.assertEqual(opened["upper"], 2)
        drive(self.store, 6, "append", partition="p", id="m3", text="m-3")
        drive(self.store, 7, "durable", partition="p", through=3)
        drive(self.store, 8, "ack", client="r", through=0)
        batch = drive(self.store, 9, "read", client="r", limit=10)
        self.assertEqual([row[0] for row in batch["records"]], [1, 2])

    def test_unacked_batch_is_replayable(self):
        self.seed("p", 3)
        drive(self.store, 5, "reader", client="r", partition="p", next=1, valid_until=50)
        first = drive(self.store, 6, "read", client="r", limit=2)
        replay = drive(self.store, 7, "read", client="r", limit=2)
        self.assertEqual(first["records"], replay["records"])
        self.assertEqual([row[0] for row in first["records"]], [1, 2])
        drive(self.store, 8, "ack", client="r", through=2)
        following = drive(self.store, 9, "read", client="r", limit=2)
        self.assertEqual([row[0] for row in following["records"]], [3])

    def test_ack_beyond_upper_is_rejected(self):
        self.seed("p", 1)
        drive(self.store, 5, "reader", client="r", partition="p", next=1, valid_until=50)
        receipt = drive(self.store, 6, "ack", client="r", through=2)
        self.assertFalse(receipt["ok"])

    def test_expired_session_cannot_read(self):
        self.seed("p", 1)
        drive(self.store, 5, "reader", client="r", partition="p", next=1, valid_until=6)
        receipt = drive(self.store, 7, "read", client="r", limit=1)
        self.assertFalse(receipt["ok"])
        self.assertEqual(receipt["error"], SESSION_EXPIRED)

    def test_two_reader_kinds_share_one_partition(self):
        self.seed("p", 4)
        drive(self.store, 5, "reader", client="duty", partition="p", next=3, valid_until=50)
        drive(self.store, 5, "reader", client="replay", partition="p", next=1, valid_until=50)
        duty = drive(self.store, 6, "read", client="duty", limit=10)
        self.assertEqual([row[0] for row in duty["records"]], [3, 4])
        drive(self.store, 7, "ack", client="duty", through=4)
        replay = drive(self.store, 8, "read", client="replay", limit=2)
        self.assertEqual([row[0] for row in replay["records"]], [1, 2])
        # 换层之后两类读者持原游标继续读
        drive(self.store, 9, "seal", partition="p", segment="p1", span=[1, 4])
        drive(self.store, 10, "copy", segment="p1",
              records=[[i, f"m-{i}"] for i in range(1, 5)])
        drive(self.store, 11, "evict", segment="p1")
        drive(self.store, 12, "ack", client="replay", through=2)
        replay2 = drive(self.store, 13, "read", client="replay", limit=2)
        self.assertEqual([row[0] for row in replay2["records"]], [3, 4])
        self.assertEqual(replay2["served_from"], ["cold"])
        done = drive(self.store, 14, "read", client="duty", limit=1)
        self.assertEqual(done["records"], [])


class FaultTests(StoreTestCase):
    def prepare_verified(self):
        self.seed("p", 2)
        drive(self.store, 5, "seal", partition="p", segment="p1", span=[1, 2])
        drive(self.store, 6, "copy", segment="p1", records=[[1, "m-1"], [2, "m-2"]])

    def test_cold_fault_falls_back_to_hot_copy(self):
        self.prepare_verified()
        drive(self.store, 7, "fault", segment="p1")
        drive(self.store, 8, "reader", client="r", partition="p", next=1, valid_until=50)
        batch = drive(self.store, 9, "read", client="r", limit=2)
        self.assertTrue(batch["ok"])
        self.assertEqual(batch["served_from"], ["hot_fallback"])
        self.assertEqual([row[0] for row in batch["records"]], [1, 2])
        refused = drive(self.store, 10, "evict", segment="p1")
        self.assertFalse(refused["evicted"])

    def test_fault_after_evict_reports_range_without_gap_filling(self):
        self.prepare_verified()
        drive(self.store, 7, "append", partition="p", id="m3", text="m-3")
        drive(self.store, 8, "durable", partition="p", through=3)
        drive(self.store, 9, "evict", segment="p1")
        drive(self.store, 10, "fault", segment="p1")
        drive(self.store, 11, "reader", client="r", partition="p", next=1, valid_until=50)
        receipt = drive(self.store, 12, "read", client="r", limit=5)
        self.assertFalse(receipt["ok"])
        self.assertEqual(receipt["error"], RANGE_UNREADABLE)
        self.assertEqual(receipt["segment"], "p1")
        self.assertEqual(receipt["span"], [1, 2])


class RetentionTests(StoreTestCase):
    def test_expire_respects_active_reader_and_ignores_idle(self):
        self.seed("p", 1)
        drive(self.store, 5, "seal", partition="p", segment="p1", span=[1, 1])
        drive(self.store, 6, "copy", segment="p1", records=[[1, "m-1"]])
        drive(self.store, 7, "reader", client="r", partition="p", next=1, valid_until=9)
        blocked = drive(self.store, 8, "expire", partition="p", through=1)
        self.assertEqual(blocked["expired_through"], 0)
        self.assertEqual(blocked["blocked_by"], [{"client": "r", "needs_from": 1}])
        done = drive(self.store, 10, "expire", partition="p", through=1)
        self.assertEqual(done["expired_through"], 1)
        self.assertEqual(done["reclaimed_segments"], ["p1"])
        self.assertEqual(done["idle_sessions_ignored"], ["r"])
        view = self.store.inspect("p")["partitions"]["p"]
        self.assertEqual(view["earliest_available"], 2)
        self.assertEqual(view["segments"][0]["location"], "none")

    def test_reader_below_earliest_gets_gap_not_skip(self):
        self.seed("p", 1)
        drive(self.store, 5, "expire", partition="p", through=1)
        receipt = drive(self.store, 6, "reader", client="new", partition="p",
                        next=1, valid_until=50)
        self.assertFalse(receipt["ok"])
        self.assertEqual(receipt["error"], GAP)
        self.assertEqual(receipt["earliest_available"], 2)
        self.assertIn("保留期", receipt["gap_reason"])


class CapacityTests(StoreTestCase):
    def test_full_hot_pauses_appends_but_protects_sole_source(self):
        drive(self.store, 0, "append", partition="p", id="m1", text="x" * 20)
        usage = self.store.inspect()["hot"]["used_bytes"]
        self.reopen(hot_capacity_bytes=usage * 2 + 5)
        drive(self.store, 1, "append", partition="p", id="m2", text="x" * 20)
        paused = drive(self.store, 2, "append", partition="p", id="m3", text="x" * 20)
        self.assertFalse(paused["ok"])
        self.assertEqual(paused["error"], HOT_FULL)
        # 写满时读照常
        drive(self.store, 3, "durable", partition="p", through=2)
        drive(self.store, 4, "reader", client="r", partition="p", next=1, valid_until=50)
        batch = drive(self.store, 5, "read", client="r", limit=2)
        self.assertEqual(len(batch["records"]), 2)
        # 未核对的段不能为了腾空间被清掉
        drive(self.store, 6, "seal", partition="p", segment="p1", span=[1, 2])
        refused = drive(self.store, 7, "evict", segment="p1")
        self.assertFalse(refused["evicted"])
        self.assertIn("唯一完整来源", refused["reason"])
        # 核对通过并移除热副本后，追加恢复
        drive(self.store, 8, "copy", segment="p1",
              records=[[1, "x" * 20], [2, "x" * 20]])
        drive(self.store, 9, "evict", segment="p1")
        resumed = drive(self.store, 10, "append", partition="p", id="m3", text="x" * 20)
        self.assertTrue(resumed["ok"])
        self.assertEqual(resumed["position"], 3)


class RestartTests(StoreTestCase):
    def test_restart_keeps_verified_source_and_sessions(self):
        self.seed("p", 2)
        drive(self.store, 5, "seal", partition="p", segment="p1", span=[1, 2])
        drive(self.store, 6, "copy", segment="p1", records=[[1, "m-1"], [2, "m-2"]])
        drive(self.store, 7, "evict", segment="p1")
        drive(self.store, 8, "reader", client="r", partition="p", next=1, valid_until=50)
        drive(self.store, 9, "ack", client="r", through=1)
        self.reopen()
        view = self.store.inspect("p")["partitions"]["p"]["segments"][0]
        self.assertEqual(view["state"], COLD_ONLY)
        self.assertEqual(view["serving"], "cold")
        batch = drive(self.store, 10, "read", client="r", limit=2)
        self.assertEqual(batch["records"], [[2, "m-2"]])
        self.assertEqual(batch["served_from"], ["cold"])


class ScenarioTests(unittest.TestCase):
    def run_example(self, name):
        folder = Path(__file__).with_name("examples")
        supported = set(json.loads((folder / "message_types.json").read_text(encoding="utf-8")))
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        store = TieredLogStore(root / "hot", root / "cold")
        return run_scenario(folder / name, store, supported), store

    def test_incomplete_cold_copy_scenario(self):
        steps, store = self.run_example("incomplete-cold-copy.jsonl")
        receipts = [receipt for _, receipt in steps]
        # 前半段复制不通过核对，热副本拒绝移除
        self.assertFalse(receipts[6]["verified"])
        self.assertFalse(receipts[7]["evicted"])
        self.assertIn("唯一完整来源", receipts[7]["reason"])
        # 补齐后核对通过，重启不移交未切完的归档，移除放行
        self.assertTrue(receipts[9]["verified"])
        self.assertTrue(receipts[10]["restarted"])
        self.assertTrue(receipts[11]["evicted"])
        # 未确认的批次重启后仍能取回同一批位置，且由冷副本接替
        self.assertEqual(receipts[12]["records"], [[1, "first"]])
        self.assertEqual(receipts[12]["served_from"], ["cold"])
        # 确认后读到位置 2；打开范围之外的后续追加不塞进来
        self.assertEqual(receipts[13]["acked"], 1)
        self.assertEqual(receipts[14]["records"], [[2, "second"]])

    def test_expired_cursor_meets_corruption_scenario(self):
        steps, store = self.run_example("expired-cursor-meets-corruption.jsonl")
        receipts = [receipt for _, receipt in steps]
        # 冷端损坏时仍完整的热副本接替读取
        self.assertEqual(receipts[6]["records"], [[1, "blue"]])
        self.assertEqual(receipts[6]["served_from"], ["hot_fallback"])
        # 活跃读者钉住位置，回收受阻；会话过期后回收推进
        self.assertEqual(receipts[7]["expired_through"], 0)
        self.assertEqual(receipts[7]["blocked_by"], [{"client": "r", "needs_from": 1}])
        self.assertEqual(receipts[8]["expired_through"], 1)
        self.assertEqual(receipts[8]["reclaimed_segments"], ["b1"])
        # 新读者从已回收位置打开：明确报最早可用位置与原因，不跳过
        self.assertFalse(receipts[9]["ok"])
        self.assertEqual(receipts[9]["error"], GAP)
        self.assertEqual(receipts[9]["earliest_available"], 2)
        # 运维视图说明段已回收、热盘不再被占用
        view = receipts[10]["partitions"]["b"]
        self.assertEqual(view["earliest_available"], 2)
        self.assertEqual(view["segments"][0]["location"], "none")
        self.assertEqual(view["segments"][0]["state"], "expired")


if __name__ == "__main__":
    unittest.main()
