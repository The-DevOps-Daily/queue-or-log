# queue-or-log

Kafka 4.3, RabbitMQ 4.3 and NATS 2.15 can each be a queue and a log now. This repo runs the same four failure cases against six setups across the three brokers and records what each one does:

1. Replay 1,000 messages after a bad deploy.
2. One poison message among 100.
3. Six consumers on a topic with three partitions.
4. Ten events for one key, where one event fails once.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./scripts/up.sh                      # Kafka, RabbitMQ and NATS in Docker, host network
cd scripts
../.venv/bin/python 1_replay.py      # each writes runs/<name>.json
../.venv/bin/python 2_poison.py
../.venv/bin/python 3_parallel.py
../.venv/bin/python 4_order.py
../.venv/bin/python 5_summary.py     # the tables below
cd .. && ./scripts/down.sh
```

Set `DOCKER` if your docker CLI needs flags, for example `DOCKER="docker -H unix:///path/to/docker.sock"`.

The write-up is on DevOps Daily: [Queue or log? Kafka, RabbitMQ and NATS all do both now](https://devops-daily.com/posts/queue-or-log-kafka-rabbitmq-nats).

## The six setups

| Setup | Storage keeps handled messages | Consumer tracks |
| --- | --- | --- |
| Kafka consumer group | yes | an offset per partition |
| Kafka share group (KIP-932) | yes | each record: accept, release, reject |
| RabbitMQ quorum queue | no | each message: ack, nack, reject |
| RabbitMQ stream | yes | an offset |
| NATS JetStream work-queue stream | no | each message: ack, nak |
| NATS JetStream limits stream | yes | each message: ack, nak |

On a failure, each setup uses its own mechanism: a share group releases the record, a quorum queue gets a nack (or a reject), NATS gets a nak, and the two offset readers go back to the failed offset and try again, because an offset reader has no broker-side way to set one message aside.

## Files

- `CLAIM.md`: the claim, written before the experiments ran, and what would have proved it wrong.
- `scripts/up.sh`, `scripts/down.sh`: start and remove the brokers.
- `scripts/brokers.py`: the six setups behind one small API.
- `scripts/1_replay.py` to `scripts/4_order.py`: one experiment each. Every run uses fresh topics, queues and streams.
- `scripts/5_summary.py`: builds the tables from `runs/`.
- `runs/`: the recorded results, the logs of the poison and order runs, and `versions.txt`.

## Results

These are the recorded runs in `runs/`, from single-node brokers on a Raspberry Pi 4 (arm64). They show behaviour, not throughput.

See `runs/summary.md` for all four tables. In short:

- **Replay** worked on every setup whose storage keeps handled messages, including the Kafka share group and the NATS limits stream, which hand out work one message at a time. It returned nothing on the two setups that delete acked messages.
- **Poison:**
  - The two offset readers stopped at the poison message: 10 of 100 handled.
  - The Kafka share group and both NATS setups gave up after 5 deliveries, and all 99 other messages were handled.
  - The RabbitMQ quorum queue never gave up when the consumer used `basic.nack`, even with `x-delivery-limit: 5`: over 5,000 attempts in 45 seconds. With `basic.reject` it dead-lettered the message after 6 deliveries. On RabbitMQ 4.x, only reject counts towards the delivery limit.
- **Parallelism:**
  - The consumer group used 3 of 6 consumers, one per partition.
  - The share group also used only 3 when the producer batched records, because a member acquires whole producer batches. With one record per producer batch it used all 6.
  - Each RabbitMQ stream consumer read all 120 messages.
- **Order:** the offset readers kept order in 5 of 5 runs. The per-message-ack setups lost it whenever more than one message was in flight, and kept it only with one message in flight. The Kafka share group lost it even with `max.poll.records=1`.

## What this does not test

- **Throughput and latency at scale:** the brokers run single-node on a Pi.
- **The share group `record_limit` acquire mode (KIP-1206):** librdkafka 2.15 has no `share.acquire.mode` setting, so it cannot be set from Python.
- **RabbitMQ super streams** (partitioned streams with single active consumers), which are how a stream spreads work across consumers.
- **Kafka dead letter queues:** Kafka 4.3 has none for consumer groups or share groups. KIP-1191 adds a dead-letter topic for share groups and is planned for Kafka 4.4, which was in release candidates when these runs were recorded.

## License

MIT
