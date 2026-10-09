## 1. Replay 1,000 handled messages

| Setup | Storage | Consumption | Read again |
| --- | --- | --- | --- |
| kafka consumer group | log | offset | 1000 |
| kafka share group | log | per-message ack | 1000 |
| rabbitmq quorum queue | queue | per-message ack | 0 |
| rabbitmq stream | log | offset | 1000 |
| nats work-queue stream | queue | per-message ack | 0 |
| nats limits stream | log | per-message ack | 1000 |

## 2. One poison message among 100 (45 s budget)

| Setup | Other 99 handled | Poison attempts | Where the poison ended |
| --- | --- | --- | --- |
| kafka consumer group | 10 | 89 | still the next offset; the partition does not move past it |
| kafka share group | 99 | 5 | archived by the share group: no member gets it again; it is still in the topic at its offset |
| rabbitmq quorum queue (defaults, basic.nack) | 99 | 5383 | still in the queue, retried in a loop |
| rabbitmq quorum queue (limit 5 + dead letter, basic.nack) | 99 | 6064 | still in the queue, retried in a loop |
| rabbitmq quorum queue (limit 5 + dead letter, basic.reject) | 99 | 6 | in the dead letter queue |
| rabbitmq stream | 10 | 928 | still the next offset; the reader does not move past it |
| nats work-queue stream (max_deliver 5) | 99 | 5 | left in the stream, never delivered to this consumer again |
| nats limits stream (max_deliver 5) | 99 | 5 | kept in the stream with the rest; this consumer stops delivering it |

## 3. Six consumers, 120 messages of 50 ms each

| Setup | Messages per consumer | Busy | Handles | First to last handle (s) |
| --- | --- | --- | --- | --- |
| kafka consumer group | 0 0 40 40 40 0 | 3 | 120 | 2.1 |
| kafka share group | 40 40 0 0 0 40 | 3 | 120 | 6.01 |
| kafka share group (max.poll.records=1) | 0 40 0 0 40 40 | 3 | 120 | 7.08 |
| kafka share group (one record per producer batch) | 21 19 20 19 20 21 | 6 | 120 | 1.33 |
| rabbitmq quorum queue | 20 20 20 20 20 20 | 6 | 120 | 1.13 |
| rabbitmq stream | 120 120 120 120 120 120 | 6 | 720 | 6.12 |
| nats work-queue stream | 20 20 20 20 20 20 | 6 | 120 | 1.09 |
| nats limits stream | 20 20 20 20 20 20 | 6 | 120 | 1.08 |

## 4. Ten events, e3 fails once (5 runs each)

| Setup | Runs in order | Order seen |
| --- | --- | --- |
| kafka consumer group | 5/5 | e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 |
| kafka share group (default) | 0/5 | e1 e2 e4 e5 e6 e7 e8 e9 e10 e3 |
| kafka share group (max.poll.records=1) | 0/5 | e1 e2 e4 e5 e6 e7 e8 e9 e10 e3 |
| rabbitmq quorum queue (prefetch 10) | 0/5 | e1 e2 e4 e5 e6 e7 e8 e9 e10 e3 |
| rabbitmq quorum queue (prefetch 1) | 5/5 | e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 |
| rabbitmq stream | 5/5 | e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 |
| nats workqueue stream (fetch 10, max_ack_pending 1000) | 0/5 | e1 e2 e4 e5 e6 e7 e8 e9 e10 e3 |
| nats workqueue stream (fetch 1, max_ack_pending 1000) | 2/5 | e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 / e1 e2 e4 e3 e5 e6 e7 e8 e9 e10 |
| nats workqueue stream (fetch 1, max_ack_pending 1) | 5/5 | e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 |
| nats limits stream (fetch 10, max_ack_pending 1000) | 0/5 | e1 e2 e4 e5 e6 e7 e8 e9 e10 e3 |
| nats limits stream (fetch 1, max_ack_pending 1000) | 2/5 | e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 / e1 e2 e4 e3 e5 e6 e7 e8 e9 e10 |
| nats limits stream (fetch 1, max_ack_pending 1) | 5/5 | e1 e2 e3 e4 e5 e6 e7 e8 e9 e10 |
