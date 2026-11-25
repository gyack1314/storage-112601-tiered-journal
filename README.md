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
