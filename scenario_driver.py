"""把演练记录逐条交给分层日志存储服务，并打印每步回执。

用法：

    python3 scenario_driver.py examples/incomplete-cold-copy.jsonl /tmp/drill
    python3 scenario_driver.py examples/expired-cursor-meets-corruption.jsonl /tmp/drill2 --hot-capacity 4096

演练目录中创建 hot/ 与 cold/ 两个本地目录；重复运行会延续上次状态
（manifest.json 仍在），要重新演练请换一个空目录。
"""

import argparse
import json
from pathlib import Path

from scenario_reader import iter_messages
from tiered_store import TieredLogStore


def run_scenario(path, store, supported):
    """按到达顺序处理演练消息，返回 [(消息, 回执), ...]。"""
    steps = []
    for message in iter_messages(path, supported):
        steps.append((message, store.handle(message)))
    return steps


def main(argv=None):
    parser = argparse.ArgumentParser(description="驱动分层日志存储演练")
    parser.add_argument("scenario", type=Path, help="JSONL 演练记录")
    parser.add_argument(
        "workdir",
        type=Path,
        help="演练目录（其中创建 hot/ 与 cold/；重复运行会延续上次状态）",
    )
    parser.add_argument(
        "--hot-capacity",
        type=int,
        default=None,
        help="热盘容量上限（字节），写满后暂停新追加",
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=None,
        help="消息名称登记表，默认取演练文件旁的 message_types.json",
    )
    args = parser.parse_args(argv)
    registry = args.registry if args.registry else args.scenario.with_name("message_types.json")
    supported = set(json.loads(registry.read_text(encoding="utf-8")))
    store = TieredLogStore(
        args.workdir / "hot", args.workdir / "cold", hot_capacity_bytes=args.hot_capacity
    )
    for message, receipt in run_scenario(args.scenario, store, supported):
        line = {"at": message["at"], "kind": message["kind"], "receipt": receipt}
        print(json.dumps(line, ensure_ascii=False))


if __name__ == "__main__":
    main()
