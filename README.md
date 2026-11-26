# 过程记录顺序日志的热冷换档与精确续读

分区位置从 1 起，段区间首尾均包含。内容为合成短文本，复制报告中的 records 保持原位置和字节值。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **append**　partition、id 与 text。
- **durable**　持久到 through，包括该位置。
- **seal**　关闭指定稳定位置范围，segment 为身份。
- **reader**　client 打开 partition，从 next 读到当时已持久上界，valid_until 为会话期限。
- **read**　查询 client 的下一批，最多 limit 条。
- **ack**　client 确认 through。
- **copy**　冷端提供 segment 的 records，每条为位置及内容。
- **evict**　运维请求移除 segment 的热副本。
- **fault**　仿真冷端已归档对象不可读。
- **expire**　按 through 回收过保留范围的位置。
- **restart**　重启服务。
- **inspect**　查询当前存储状态。

## 示例

- incomplete-cold-copy.jsonl
- expired-cursor-meets-corruption.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 分层日志存储服务

- `tiered_store.py`　可反复使用的存储入口 `TieredLogStore`：按分区追加并返回稳定位置（相同 id 重送不产新位置）；段关闭且写入达到持久边界后才接受冷端复制报告；核对完整范围、内容与位置对应的事件后冷副本才接替读取，热副本才允许移除；复制结束、核对通过、接替读取是三个不同事实。读者打开范围时固定当时已持久上界，未确认的批次可重复取回，换层与重启不改变已确认位置的读取语义；保留期回收与冷端故障都明确报出最早可用位置或具体段区间，绝不拿后续位置填缺口。热盘写满时暂停新追加，但仍是唯一完整来源的热段不可清除。
- `scenario_driver.py`　把演练记录逐条交给服务并打印每步回执。

        python3 scenario_driver.py examples/incomplete-cold-copy.jsonl /tmp/drill1
        python3 scenario_driver.py examples/expired-cursor-meets-corruption.jsonl /tmp/drill2 --hot-capacity 4096

演练目录中创建 `hot/`（进行中分段、已关闭段热副本、manifest.json）与 `cold/`（归档对象）。重复运行同一目录会延续上次状态，重新演练请换空目录。

回执约定：`ok` 为 `false` 时 `error` 给出机器可判的码——`hot_full`（热盘写满，追加暂停）、`not_durable`（未到持久边界，不能归档）、`gap`（位置已回收，附 `earliest_available` 与原因）、`range_unreadable`（段区间不可读，附段名与区间）、`session_expired` 等。`inspect` 回执是运维视图：每段占热盘还是冷盘、冷端核对进度与字节范围、可删或不可删的原因、各读者会话钉住的位置。
